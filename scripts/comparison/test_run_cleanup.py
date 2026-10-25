"""No-service regression tests for comparison-runner process cleanup."""
import asyncio
from contextlib import redirect_stdout
import importlib.util
import io
import json
import subprocess
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


class DifySetupCleanupTests(unittest.TestCase):
    def run_setup(self, root, *, network_internal="true", up_error=None, cleanup_error=None):
        lock = json.loads((HERE / "runtime-lock.json").read_text())
        directory = root / "fixture"
        events = []
        stdout = io.StringIO()

        def fake_run(command, **options):
            command = list(command)
            events.append((command, options.copy()))
            if command[:2] == ["git", "init"]:
                docker_dir = Path(command[2]) / "docker"
                docker_dir.mkdir(parents=True)
                (docker_dir / ".env.example").write_text("# fixture\n")
            if "up" in command and up_error is not None:
                raise up_error
            if "down" in command and cleanup_error is not None:
                raise cleanup_error
            return None

        def fake_check_output(command, **options):
            command = list(command)
            if command[:2] == ["docker", "info"]:
                return f'{lock["docker"]["version"]} name=rootless'
            if command[:2] == ["docker-compose", "version"]:
                return f'v{lock["compose"]["version"]}'
            if command[:2] == ["git", "-C"] and command[-2:] == ["rev-parse", "HEAD"]:
                return lock["dify"]["revision"]
            if command[:2] == ["docker", "network"]:
                return network_internal
            raise AssertionError(f"unexpected output command: {command}")

        error = None
        with mock.patch.object(sys, "argv", [
            "setup-dify.py", "--compose", "docker-compose", "--directory", str(directory)
        ]):
            with mock.patch.object(subprocess, "run", side_effect=fake_run):
                with mock.patch.object(subprocess, "check_output", side_effect=fake_check_output):
                    with redirect_stdout(stdout):
                        spec = importlib.util.spec_from_file_location(
                            "comparison_setup_dify", HERE / "setup-dify.py"
                        )
                        module = importlib.util.module_from_spec(spec)
                        try:
                            spec.loader.exec_module(module)
                        except (Exception, SystemExit, KeyboardInterrupt) as caught:
                            error = caught
        return error, events, stdout.getvalue(), directory

    @staticmethod
    def cleanup_calls(events):
        return [(command, options) for command, options in events if "down" in command]

    def assert_cleanup_for_started_project(self, events, cleanup):
        up = next(command for command, _options in events if "up" in command)
        self.assertEqual(cleanup[:-2], up[:up.index("up")])

    def test_startup_failure_attempts_bounded_cleanup_and_preserves_error(self):
        with tempfile.TemporaryDirectory(prefix="qcg-dify-cleanup-") as temp:
            original = subprocess.CalledProcessError(1, ["docker-compose", "up"])
            error, events, stdout, _directory = self.run_setup(
                Path(temp), up_error=original, cleanup_error=OSError("cleanup failed")
            )

        self.assertIs(error, original)
        cleanup = self.cleanup_calls(events)
        self.assertEqual(len(cleanup), 1)
        command, options = cleanup[0]
        self.assertEqual(command[-2:], ["down", "--volumes"])
        self.assertEqual(options["timeout"], 60)
        self.assert_cleanup_for_started_project(events, command)
        self.assertNotIn("Cleanup only this owned fixture with:", stdout)

    def test_keyboard_interrupt_during_startup_cleans_up_and_preserves_interrupt(self):
        with tempfile.TemporaryDirectory(prefix="qcg-dify-cleanup-") as temp:
            original = KeyboardInterrupt("startup cancelled")
            error, events, stdout, _directory = self.run_setup(
                Path(temp),
                up_error=original,
                cleanup_error=KeyboardInterrupt("cleanup interrupted"),
            )

        self.assertIs(error, original)
        cleanup = self.cleanup_calls(events)
        self.assertEqual(len(cleanup), 1)
        command, options = cleanup[0]
        self.assertEqual(command[-2:], ["down", "--volumes"])
        self.assertEqual(options["timeout"], 60)
        self.assert_cleanup_for_started_project(events, command)
        self.assertNotIn("Cleanup only this owned fixture with:", stdout)

    def test_network_verification_failure_cleans_up_owned_project(self):
        with tempfile.TemporaryDirectory(prefix="qcg-dify-cleanup-") as temp:
            error, events, stdout, _directory = self.run_setup(
                Path(temp), network_internal="false"
            )

        self.assertIsInstance(error, SystemExit)
        self.assertEqual(str(error), "Dify application network is not internal")
        cleanup = self.cleanup_calls(events)
        self.assertEqual(len(cleanup), 1)
        command, options = cleanup[0]
        self.assertEqual(command[-2:], ["down", "--volumes"])
        self.assertEqual(options["timeout"], 60)
        self.assert_cleanup_for_started_project(events, command)
        self.assertNotIn("Cleanup only this owned fixture with:", stdout)

    def test_success_keeps_the_cleanup_handle_on_the_success_path(self):
        with tempfile.TemporaryDirectory(prefix="qcg-dify-cleanup-") as temp:
            error, events, stdout, directory = self.run_setup(Path(temp))
            config = json.loads((directory / "comparison-config.json").read_text())

        self.assertIsNone(error)
        self.assertEqual(self.cleanup_calls(events), [])
        self.assertIn("Cleanup only this owned fixture with:", stdout)
        up = next(command for command, _options in events if "up" in command)
        self.assertEqual(config["project"], up[up.index("-p") + 1])


if __name__ == "__main__":
    unittest.main()
