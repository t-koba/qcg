"""Real Dify API comparison using imported native workflow DSL, no LLM.

Dify returns a validated output variable; it does not claim qcg's artifact
registry or declarative local filesystem permissions.
"""
import base64
import hashlib
import http.client
import socket
import json
import os
import platform
from pathlib import Path
import secrets
import statistics
import subprocess
import time
import uuid
import requests
import yaml
from workloads import canonical

BASE = os.environ.get("QCG_DIFY_URL", "http://127.0.0.1:15001")
COUNT = int(os.environ.get("QCG_COMPARISON_TRIALS", "30"))
ROOT = Path(__file__).resolve().parents[2]
REPORT = Path(os.environ.get("QCG_DIFY_REPORT", ROOT / "docs/dify-comparison-results.json"))
DOCKER = os.environ.get("QCG_COMPARISON_DOCKER", "docker")
CONTAINER = os.environ.get("QCG_DIFY_API_CONTAINER", "qcg-comparison-api-1")


def infrastructure_sample():
    host = os.environ.get("DOCKER_HOST", "")
    if not host.startswith("unix://"):
        return {"available": False}
    class UnixHTTP(http.client.HTTPConnection):
        def connect(self):
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.settimeout(10)
            self.sock.connect(host[7:])
    def get(path):
        connection = UnixHTTP("localhost", timeout=10)
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            if response.status != 200:
                raise RuntimeError(f"Docker stats HTTP {response.status}")
            return json.loads(response.read())
        finally:
            connection.close()
    project = os.environ.get("QCG_DIFY_PROJECT", "qcg-comparison")
    containers = get('/containers/json')
    samples = []
    for container in containers:
        if container.get("Labels", {}).get("com.docker.compose.project") != project:
            continue
        value = get(f"/containers/{container['Id']}/stats?stream=false&one-shot=true")
        memory = value.get("memory_stats", {})
        samples.append({"name": container["Names"][0].removeprefix("/"), "cpu_nanoseconds": value.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0), "memory_usage_bytes": memory.get("usage", 0), "memory_limit_bytes": memory.get("limit", 0)})
    return {"available": True, "containers": samples, "total_memory_bytes": sum(row["memory_usage_bytes"] for row in samples)}


def dsl(name: str, approval: bool) -> dict:
    values = [
        ("start", {"type": "start", "title": "Start", "variables": [{"variable": "payload", "label": "payload", "type": "paragraph", "required": True, "max_length": 65536}]}),
        ("prepare", {"type": "template-transform", "title": "Prepare", "variables": [{"variable": "payload", "value_selector": ["start", "payload"]}], "template": "{{ payload }}"}),
    ]
    if approval:
        values.append(("approval", {"type": "human-input", "title": "Approval", "form_content": "Approve the artifact generation", "inputs": [], "user_actions": [{"id": "approve", "title": "Approve", "button_style": "primary"}], "delivery_methods": [{"type": "webapp", "enabled": True, "config": {}}], "timeout": 1, "timeout_unit": "hour"}))
    values.append(("end", {"type": "end", "title": "End", "outputs": [{"variable": "result", "value_selector": ["prepare", "output"], "value_type": "string"}]}))
    nodes = [{"id": name, "type": "custom", "data": data, "position": {"x": 100+index*300, "y": 100}, "width": 244, "height": 100, "sourcePosition": "right", "targetPosition": "left"} for index, (name, data) in enumerate(values)]
    edges = [{"id": f"{left[0]}-{right[0]}", "source": left[0], "target": right[0], "sourceHandle": "approve" if left[0] == "approval" else "source", "targetHandle": "target", "type": "custom", "data": {"sourceType": left[1]["type"], "targetType": right[1]["type"], "isInLoop": False}} for left, right in zip(values, values[1:])]
    return {"kind": "app", "version": "0.7.0", "app": {"name": name, "mode": "workflow", "description": "Deterministic qcg comparison", "icon": "🔬", "icon_background": "#FFEAD5", "use_icon_as_answer_icon": False}, "dependencies": [], "workflow": {"conversation_variables": [], "environment_variables": [], "features": {"file_upload": {"enabled": False}}, "graph": {"nodes": nodes, "edges": edges, "viewport": {"x": 0, "y": 0, "zoom": 1}}}}


class Dify:
    def __init__(self):
        self.session = requests.Session()
        deadline = time.monotonic()+120
        while time.monotonic()<deadline:
            try:
                health = self.session.get(BASE+"/health", timeout=3)
                if health.ok:
                    assert health.json()["version"] == "1.17.1", "Dify version differs from the fixed comparison target"
                    break
            except requests.RequestException:
                pass
            time.sleep(.1)
        else:
            raise RuntimeError("Dify startup deadline exceeded")
        # Own temporary local deployment only. No external account is modified.
        state_file = Path(os.environ.get("QCG_DIFY_ACCOUNT_FILE", "/tmp/qcg-dify-test-account.json"))
        if state_file.exists():
            account = json.loads(state_file.read_text())
        else:
            account = {"email": "qcg-comparison@example.com", "name": "qcg comparison", "password": secrets.token_hex(16)+"Aa1", "language": "en-US"}
            state_file.write_text(json.dumps(account))
            state_file.chmod(0o600)
        status = self.call("GET", "/console/api/setup")
        if status["step"] != "finished":
            self.call("POST", "/console/api/setup", account)
        self.call("POST", "/console/api/login", {"email": account["email"], "password": base64.b64encode(account["password"].encode()).decode(), "remember_me": True})
        csrf = next((cookie.value for cookie in self.session.cookies if "csrf" in cookie.name), None)
        if csrf:
            self.session.headers["X-CSRF-Token"] = csrf
        self.keys = {}
        self.apps = {}
        for gate in [False, True]:
            native = dsl("qcg-compare-"+uuid.uuid4().hex[:8], gate)
            result = self.call("POST", "/console/api/apps/imports", {"mode": "yaml-content", "yaml_content": yaml.safe_dump(native, allow_unicode=True)})
            if result["status"] == "pending":
                result = self.call("POST", f"/console/api/apps/imports/{result['id']}/confirm", {})
            if result.get("status") != "completed":
                raise RuntimeError(f"Dify import failed: status={result.get('status')} error={result.get('error')} versions={result.get('current_dsl_version')}/{result.get('imported_dsl_version')}")
            app = result["app_id"]
            self.call("POST", f"/console/api/apps/{app}/workflows/publish", {})
            key = self.call("POST", f"/console/api/apps/{app}/api-keys", {})["token"]
            self.keys[gate] = key
            self.apps[gate] = app

    def call(self, method, path, value=None, key=None):
        headers = {"Authorization": f"Bearer {key}"} if key else {}
        response = self.session.request(method, BASE+path, json=value, headers=headers, timeout=30)
        if not response.ok:
            # Endpoints contain no tokens except form paths; never include URL
            # or authentication response payloads in persistent failure output.
            message = response.json().get("message", "request failed") if response.headers.get("Content-Type", "").startswith("application/json") else "non-JSON error"
            raise RuntimeError(f"Dify HTTP {response.status_code}: {message}")
        return response.json() if response.content else {}

    def trial(self, case, index):
        gate = case != "generation"
        key = self.keys[gate]
        payload = canonical(index)
        response = self.session.post(BASE+"/v1/workflows/run", headers={"Authorization": f"Bearer {key}"}, json={"inputs": {"payload": payload}, "response_mode": "streaming", "user": "qcg-comparison"}, stream=True, timeout=30)
        response.raise_for_status()
        run = None
        token = None
        output = None
        prepare_count = 0
        event_names = []
        with response:
            for line in response.iter_lines():
                if not line.startswith(b"data:"):
                    continue
                value = json.loads(line[5:])
                event_names.append(value.get("event"))
                data = value.get("data", {})
                if value.get("event") == "workflow_started":
                    run = value.get("workflow_run_id") or data.get("id")
                if value.get("event") == "node_finished" and data.get("node_id") == "prepare":
                    prepare_count += 1
                if value.get("event") == "human_input_required":
                    token = data.get("form_token")
                if value.get("event") == "workflow_finished":
                    if data.get("status") == "failed":
                        raise RuntimeError(data.get("error"))
                    output = data.get("outputs", {}).get("result")
                if value.get("event") == "error":
                    raise RuntimeError(value.get("message", "workflow error"))
        assert run and prepare_count == 1, f"workflow did not execute one preparation: {event_names}"
        if gate:
            assert token and output is None, f"approval gate did not suspend: {event_names}"
            if case == "restart_pending_approval":
                subprocess.run([DOCKER, "kill", "--signal", "KILL", CONTAINER], capture_output=True, timeout=30, check=True)
                subprocess.run([DOCKER, "start", CONTAINER], capture_output=True, timeout=30, check=True)
                deadline = time.monotonic()+90
                while time.monotonic()<deadline:
                    try:
                        if self.session.get(BASE+"/health", timeout=3).ok:
                            break
                    except requests.RequestException:
                        pass
                    time.sleep(.1)
                else:
                    raise RuntimeError("Dify API did not recover within 90 seconds")
            self.call("POST", f"/v1/form/human_input/{token}", {"inputs": {}, "action": "approve", "user": "qcg-comparison"}, key=key)
            deadline = time.monotonic()+30
            while time.monotonic()<deadline:
                value = self.call("GET", f"/v1/workflows/run/{run}", key=key)
                if value["status"] == "succeeded":
                    output = value["outputs"]["result"]
                    break
                if value["status"] == "failed":
                    raise RuntimeError(value.get("error"))
                time.sleep(.05)
            else:
                raise RuntimeError("Dify resumed run did not settle")
        assert output == payload
        assert json.loads(output)["items"] == [1,2,3]
        return {"digest": hashlib.sha256(output.encode()).hexdigest(), "prepared_steps_observed": prepare_count}


def main():
    dify = Dify()
    rows = []
    deadline = time.monotonic()+2700
    for case in ["generation", "approval_resume", "restart_pending_approval"]:
        trials = []
        infrastructure = [infrastructure_sample()]
        for index in range(-2, COUNT):
            if time.monotonic()>deadline:
                raise RuntimeError("Dify comparison exceeded its 45 minute deadline")
            begin = time.perf_counter()
            try:
                details = dify.trial(case,index)
                trial = {"index": index, "success": True, "wall_ms": (time.perf_counter()-begin)*1000, **details}
            except Exception as error:
                trial = {"index": index, "success": False, "wall_ms": (time.perf_counter()-begin)*1000, "error": str(error)}
            if index >= 0:
                trials.append(trial)
                infrastructure.append(infrastructure_sample())
        times = sorted(trial["wall_ms"] for trial in trials)
        rows.append({"engine": "dify", "case": case, "infrastructure_samples": infrastructure, "successes": sum(trial["success"] for trial in trials), "trials": trials, "p50_ms": statistics.median(times), "p95_ms": times[min(len(times)-1, int(.95*len(times)))]})
        REPORT.write_text(json.dumps({"recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()), "version": "1.17.1", "environment": {"platform": platform.platform(), "python": platform.python_version(), "runtime_lock": json.loads(Path(__file__).with_name("runtime-lock.json").read_text()), "telemetry_disabled": True, "application_network_internal": True}, "external_model": {"used": False, "tokens": 0, "cost": 0}, "results": rows, "limitations": ["No LLM/model/provider calls.", "Native template workflow emits an output variable; qcg's artifact registry is not attributed to Dify.", "API container is SIGKILLed at the durable human gate; PostgreSQL, Redis and worker remain available.", "Other products' local filesystem allowlists are not synthesized in the Dify adapter.", "Container CPU and memory are sampled after each trial; memory samples are observed values, not peak-RSS guarantees."]},indent=2)+"\n")
        print(f"dify/{case}: {rows[-1]['successes']}/{COUNT}",flush=True)
    if any(row["successes"] != COUNT for row in rows):
        raise SystemExit("Dify comparison failed; raw failures retained")

if __name__ == "__main__":
    main()
