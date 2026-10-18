"""No-service regression tests for comparison-runner process cleanup."""
import asyncio
import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent


def load_run_module():
    workloads = types.ModuleType("workloads")
    workloads.canonical = lambda index: str(index)
    workloads.validate = lambda output, payload: None
    driver = types.ModuleType("langgraph_driver")
    driver.execute = lambda *args, **kwargs: {"__interrupt__": True}
    workflow = types.ModuleType("temporal_workflow")
    workflow.ArtifactWorkflow = type("ArtifactWorkflow", (), {})
    resource = types.ModuleType("resource")
    resource.RUSAGE_SELF = 0
    resource.getrusage = lambda who: None
    temporalio = types.ModuleType("temporalio")
    temporalio.__path__ = []
    client_module = types.ModuleType("temporalio.client")

    class Client:
        @staticmethod
        async def connect(address):
            raise AssertionError("Client.connect must be patched by the test")

    client_module.Client = Client
    temporalio.client = client_module
    stubs = {
        "workloads": workloads,
        "langgraph_driver": driver,
        "temporal_workflow": workflow,
        "resource": resource,
        "temporalio": temporalio,
        "temporalio.client": client_module,
    }
    spec = importlib.util.spec_from_file_location("comparison_run", HERE / "run.py")
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    return module


run = load_run_module()


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="qcg-run-cleanup-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        binary = root / "qcg"
        binary.write_bytes(b"test binary")
        patches = [
            mock.patch.object(run, "BINARY", binary),
            mock.patch.object(run, "REPORT", root / "report.json"),
            mock.patch.object(run, "COUNT", 1),
            mock.patch.object(run.platform, "platform", return_value="test-platform"),
            mock.patch.object(run.os, "sysconf", return_value=100, create=True),
            mock.patch.object(run.subprocess, "check_output", return_value="test-revision"),
            mock.patch.object(run.importlib.metadata, "version", return_value="0.0.0"),
        ]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def fake_qcg(events, permission_error=None):
        class FakeQcg:
            def __init__(self, root):
                self.samples = []
                events.append("qcg.start")

            def permission(self):
                if permission_error is not None:
                    raise permission_error
                return {"status": "verified"}

            def stop(self, crash=False):
                events.append("qcg.stop")

        return FakeQcg

    @staticmethod
    def fake_worker(events, wait_error=None):
        class FakeWorker:
            def __init__(self, command, log):
                events.append("worker.start")

            def wait_text(self, pattern):
                if wait_error is not None:
                    raise wait_error
                return True

            def stop(self, crash=False):
                events.append("worker.stop")
                return {}

        return FakeWorker

    def run_main(self, qcg, worker, connect):
        with mock.patch.object(run, "Qcg", qcg):
            with mock.patch.object(run, "Managed", worker):
                with mock.patch.object(run.Client, "connect", connect):
                    asyncio.run(run.main())

    def test_temporal_connect_failure_stops_started_qcg(self):
        events = []
        qcg = self.fake_qcg(events)

        async def connect(address):
            raise ConnectionError("Temporal prerequisite unavailable")

        with self.assertRaises(ConnectionError):
            self.run_main(qcg, self.fake_worker(events), connect)
        self.assertEqual(events, ["qcg.start", "qcg.stop"])

    def test_worker_readiness_failure_stops_worker_and_qcg(self):
        events = []
        qcg = self.fake_qcg(events)
        worker = self.fake_worker(events, RuntimeError("worker not ready"))

        async def connect(address):
            return object()

        with self.assertRaisesRegex(RuntimeError, "worker not ready"):
            self.run_main(qcg, worker, connect)
        self.assertEqual(
            events,
            ["qcg.start", "worker.start", "qcg.stop", "worker.stop"],
        )

    def test_permission_failure_stops_worker_and_qcg(self):
        events = []
        qcg = self.fake_qcg(events, RuntimeError("permission probe failed"))
        worker = self.fake_worker(events)

        async def connect(address):
            return object()

        with self.assertRaisesRegex(RuntimeError, "permission probe failed"):
            self.run_main(qcg, worker, connect)
        self.assertEqual(
            events,
            ["qcg.start", "worker.start", "qcg.stop", "worker.stop"],
        )

    def test_qcg_constructor_stops_server_when_readiness_fails(self):
        events = []

        class FailingServer:
            def __init__(self, command, log):
                events.append("server.start")

            def wait_text(self, pattern):
                raise RuntimeError("server not ready")

            def stop(self, crash=False):
                events.append("server.stop")
                return {}

        with mock.patch.object(run, "Managed", FailingServer):
            with self.assertRaisesRegex(RuntimeError, "server not ready"):
                run.Qcg(Path(self.temp.name) / "qcg-root")
        self.assertEqual(events, ["server.start", "server.stop"])

    def test_managed_closes_log_if_process_spawn_fails(self):
        log = Path(self.temp.name) / "worker.log"
        opened = []
        real_open = Path.open

        def tracking_open(path, *args, **kwargs):
            handle = real_open(path, *args, **kwargs)
            opened.append(handle)
            return handle

        with mock.patch.object(Path, "open", tracking_open):
            with mock.patch.object(run.subprocess, "Popen", side_effect=OSError("spawn failed")):
                with self.assertRaisesRegex(OSError, "spawn failed"):
                    run.Managed(["missing"], log)
        self.assertEqual(len(opened), 1)
        self.assertTrue(opened[0].closed)


if __name__ == "__main__":
    unittest.main()
