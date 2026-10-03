"""Deterministic, reproducible comparison; raw trials and limitations retained.

No score aggregates architectural differences into overall product superiority.
"""
import asyncio
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import resource
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

os.environ["LANGSMITH_TRACING"] = "false"
os.environ["LANGCHAIN_TRACING_V2"] = "false"

from workloads import canonical, validate
from langgraph_driver import execute as graph_execute
from temporal_workflow import ArtifactWorkflow
from temporalio.client import Client

ROOT = Path(__file__).resolve().parents[2]
COUNT = int(os.environ.get("QCG_COMPARISON_TRIALS", "30"))
REPORT = Path(os.environ.get("QCG_COMPARISON_REPORT", ROOT / "docs/comparison-results.json"))
BINARY = Path(os.environ.get("QCG_COMPARISON_BINARY", ROOT / "target/release/qcg"))
TEMPORAL = os.environ.get("QCG_TEMPORAL_ADDRESS", "127.0.0.1:17233")


def process_stats(pid: int) -> dict:
    try:
        status = Path(f"/proc/{pid}/status").read_text()
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return {"cpu_ticks": int(stat[11]) + int(stat[12]), "max_rss_kib": int(re.search(r"VmHWM:\s*(\d+)", status)[1])}
    except (OSError, TypeError):
        return {"cpu_ticks": 0, "max_rss_kib": 0}


class Managed:
    def __init__(self, command, log: Path):
        self.log = log
        self.file = log.open("w")
        self.process = subprocess.Popen(command, stdout=self.file, stderr=subprocess.STDOUT)
        self.metrics = {"cpu_ticks": 0, "max_rss_kib": 0}

    def wait_text(self, pattern):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and self.process.poll() is None:
            match = re.search(pattern, self.log.read_text())
            if match:
                return match
            time.sleep(.02)
        raise RuntimeError(f"startup failed: {self.log.read_text()[-4000:]}")

    def stop(self, crash=False):
        self.metrics = process_stats(self.process.pid)
        if self.process.poll() is None:
            self.process.kill() if crash else self.process.terminate()
        try:
            self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self.file.close()
        return self.metrics


def request(url, value=None, method=None):
    data = json.dumps(value).encode() if value is not None else None
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method=method), timeout=20) as response:
        body = response.read()
        return json.loads(body) if body else None


def manifest(name: str, gate=False, forbidden=False) -> str:
    permissions = '[]' if forbidden else '["workspace"]'
    question = '''[[flow]]
id = "approval"
type = "ask_user"
[flow.params]
content = "Approve the artifact write"
[[flow.params.fields]]
id = "approved"
type = "select"
required = true
options = ["yes"]
''' if gate else ''
    return f'''[generator]
id = "{name}"
name = "Comparison"
version = "0.1.0"
[permissions]
fs_read = []
fs_write = {permissions}
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
[permissions.containers]
enabled = false
[[inputs.stages]]
id = "input"
[[inputs.stages.fields]]
id = "payload"
type = "string"
required = true
[[flow]]
id = "prepare"
type = "write"
[flow.params]
content = "{{{{ inputs.payload }}}}"
output_file = "prepared.json"
{question}
[[flow]]
id = "emit"
type = "write"
artifact = {{ label = "Result", required = true }}
[flow.params]
content = "{{{{ inputs.payload }}}}"
output_file = "result.json"
'''


class Qcg:
    def __init__(self, root):
        self.root = root
        self.server = None
        self.samples = []
        generators = root / "generators"
        for name, gate, deny in [("generate", False, False), ("approval", True, False), ("denied", False, True)]:
            directory = generators / name
            directory.mkdir(parents=True)
            (directory / "qcg.toml").write_text(manifest(name, gate, deny))
        self.start()

    def start(self):
        self.server = Managed([str(BINARY), "serve", "--port", "0", "--generators-dir", str(self.root / "generators"), "--runs-dir", str(self.root / "runs")], self.root / "server.log")
        self.base = self.server.wait_text(r"listening on (http://\S+)")[1]

    def stop(self, crash=False):
        if self.server:
            self.samples.append(self.server.stop(crash))
            self.server = None

    def snapshot(self, run, states):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            value = request(f"{self.base}/api/runs/{run}")
            if value["state"] in states:
                return value
            time.sleep(.01)
        raise RuntimeError(f"qcg run {run} timed out: {value}")

    def trial(self, case, index):
        payload = canonical(index)
        run = request(f"{self.base}/api/runs", {"generator_id": "generate" if case == "generation" else "approval", "inputs": {"payload": payload}})["run_id"]
        directory = self.root / "runs" / run / "workspace"
        if case != "generation":
            waiting = self.snapshot(run, {"waiting", "failed"})
            assert waiting["state"] == "waiting", waiting
            assert not (directory / "result.json").exists()
            prepared_mtime = (directory / "prepared.json").stat().st_mtime_ns
            if case == "restart_pending_approval":
                self.stop(crash=True)
                self.start()
                waiting = self.snapshot(run, {"waiting"})
            request(f"{self.base}/api/runs/{run}/questions/{waiting['question']['id']}", {"values": {"approved": "yes"}}, method="PUT")
        final = self.snapshot(run, {"succeeded", "failed", "interrupted"})
        assert final["state"] == "succeeded", final
        validate(str(directory), payload)
        if case != "generation":
            assert prepared_mtime == (directory / "prepared.json").stat().st_mtime_ns, "preparation repeated after resume"
        return {"digest": hashlib.sha256(payload.encode()).hexdigest(), "last_seq": final["seq"]}

    def permission(self):
        before = set((self.root / "runs").iterdir())
        try:
            result = request(f"{self.base}/api/runs", {"generator_id": "denied", "inputs": {"payload": canonical(0)}})
            run = result["run_id"]
            final = self.snapshot(run, {"failed"})
            assert final["state"] == "failed"
        except urllib.error.HTTPError as error:
            assert error.code in (400, 403, 422), error.code
        for directory in set((self.root / "runs").iterdir()) - before:
            assert not (directory / "workspace/prepared.json").exists()
        return {"status": "verified", "scope": "write without declared workspace permission is refused before effects"}


async def main():
    rows = []
    failures = []
    system = {"platform": platform.platform(), "python": platform.python_version(), "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "binary": str(BINARY.relative_to(ROOT)) if BINARY.is_relative_to(ROOT) else str(BINARY), "qcg_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(), "versions": {p: importlib.metadata.version(p) for p in ["langgraph", "langgraph-checkpoint-sqlite", "temporalio"]}, "temporal_cli": "1.9.1", "temporal_server": "1.32.0", "binary_sha256": hashlib.sha256(BINARY.read_bytes()).hexdigest(), "working_tree": "modified implementation under review"}
    with tempfile.TemporaryDirectory(prefix="qcg-comparison-") as directory:
        root = Path(directory)
        qcg_root = root / "qcg"
        qcg_root.mkdir()
        qcg = Qcg(qcg_root)
        client = await Client.connect(TEMPORAL)
        queue = "qcg-comparison-" + uuid.uuid4().hex
        worker = Managed([sys.executable, str(Path(__file__).with_name("temporal_worker.py")), TEMPORAL, queue], root / "worker.log")
        worker.wait_text("ready")
        worker_samples = []
        permissions = {"qcg": qcg.permission(), "langgraph": {"status": "application_defined", "scope": "This library does not supply qcg's declarative filesystem allowlist; deployment/application controls were not added"}, "temporal": {"status": "application_defined", "scope": "Activities use host permissions; no comparable built-in qcg filesystem allowlist is configured"}}
        try:
            for engine in ["qcg", "langgraph", "temporal"]:
                for case in ["generation", "approval_resume", "restart_pending_approval"]:
                    trials = []
                    start_usage = resource.getrusage(resource.RUSAGE_SELF)
                    for index in range(-2, COUNT):
                        begin = time.perf_counter()
                        payload = canonical(index)
                        output = root / engine / case / str(index)
                        state = {"payload": payload, "output": str(output), "approval": case != "generation", "approved": False}
                        try:
                            if engine == "qcg":
                                details = qcg.trial(case, index)
                            elif engine == "langgraph":
                                db = str(root / "langgraph.sqlite")
                                thread = f"{case}-{index}"
                                if case == "restart_pending_approval":
                                    child = subprocess.run([sys.executable, str(Path(__file__).with_name("langgraph_driver.py")), db, thread, "start"], input=json.dumps(state), text=True, capture_output=True, timeout=30, check=True)
                                    assert json.loads(child.stdout)["waiting"]
                                    assert not (output / "result.json").exists()
                                    subprocess.run([sys.executable, str(Path(__file__).with_name("langgraph_driver.py")), db, thread, "resume"], capture_output=True, timeout=30, check=True)
                                else:
                                    result = graph_execute(db, thread, state)
                                    if state["approval"]:
                                        assert result["__interrupt__"] and not (output / "result.json").exists()
                                        graph_execute(db, thread, None)
                                validate(str(output), payload)
                                details = {"digest": hashlib.sha256(payload.encode()).hexdigest()}
                            else:
                                handle = await client.start_workflow(ArtifactWorkflow.run, state, id=f"{queue}-{case}-{index}", task_queue=queue)
                                if state["approval"]:
                                    deadline = time.monotonic() + 20
                                    while await asyncio.wait_for(handle.query(ArtifactWorkflow.current_phase), timeout=20) != "waiting":
                                        assert time.monotonic() < deadline, "Temporal did not suspend"
                                        await asyncio.sleep(.01)
                                    assert not (output / "result.json").exists()
                                    if case == "restart_pending_approval":
                                        worker_samples.append(worker.stop(crash=True))
                                        worker = Managed([sys.executable, str(Path(__file__).with_name("temporal_worker.py")), TEMPORAL, queue], root / "worker.log")
                                        worker.wait_text("ready")
                                    await handle.signal(ArtifactWorkflow.approve)
                                result = await asyncio.wait_for(handle.result(), timeout=30)
                                assert result == payload
                                validate(str(output), payload)
                                details = {"digest": hashlib.sha256(payload.encode()).hexdigest()}
                            trial = {"index": index, "success": True, "wall_ms": (time.perf_counter() - begin) * 1000, **details}
                        except Exception as error:
                            trial = {"index": index, "success": False, "wall_ms": (time.perf_counter() - begin) * 1000, "error": str(error)}
                            failures.append({"engine": engine, "case": case, **trial})
                        if index >= 0:
                            trials.append(trial)
                    end_usage = resource.getrusage(resource.RUSAGE_SELF)
                    times = sorted(t["wall_ms"] for t in trials)
                    rows.append({"engine": engine, "case": case, "trials": trials, "successes": sum(t["success"] for t in trials), "p50_ms": statistics.median(times), "p95_ms": times[min(len(times)-1, int(.95*len(times)))], "driver_cpu_seconds": end_usage.ru_utime+end_usage.ru_stime-start_usage.ru_utime-start_usage.ru_stime, "driver_max_rss_kib": end_usage.ru_maxrss})
                    REPORT.write_text(json.dumps({"recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "environment": system, "results": rows, "permission_boundaries": permissions, "failures": failures, "external_model": {"used": False, "tokens": 0, "cost": 0}, "limitations": ["No LLM calls: model and token cost are not compared.", "LangGraph in-process library, qcg HTTP service and Temporal gRPC service are different architectures; raw latency is not an overall rank.", "LangGraph restart replaces the client process after an interrupt; qcg and Temporal worker restart use SIGKILL at the equivalent pending gate.", "Temporal service remains available during worker interruption; qcg uses a local journal, LangGraph SQLite and Temporal a persistent dev server.", "Dify results are stored separately because its native output-variable delivery differs from filesystem artifacts."]}, indent=2)+"\n")
                    print(f"{engine}/{case}: {rows[-1]['successes']}/{COUNT}", flush=True)
        finally:
            qcg.stop()
            worker_samples.append(worker.stop())
        data = json.loads(REPORT.read_text())
        data["qcg_server_instances"] = qcg.samples
        data["temporal_worker_instances"] = worker_samples
        REPORT.write_text(json.dumps(data, indent=2)+"\n")
    if failures:
        raise SystemExit(f"{len(failures)} comparison trials failed; see {REPORT}")

if __name__ == "__main__":
    asyncio.run(main())
