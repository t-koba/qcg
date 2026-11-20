#!/usr/bin/env python3
"""Real-daemon gate for destination-scoped container networking.

Run only on a disposable local Docker daemon (hosted CI): the probe creates
two bridge networks, one host-side HTTP fixture on the declared gateway and
one short-lived pinned-container fixture on the other bridge, then invokes a
generated qcg contract. Every resource it owns is removed in `finally`. All
bounds are explicit arguments; daemon output is parsed as data and never
executed.
"""

import argparse
from contextlib import contextmanager
import hashlib
import http.server
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

PINNED_IMAGE = "busybox@sha256:fd7dc98638c8e305f4dc34e979f1c0fdfdcaeb0fbf8fcff77ae834b6da3d7e6e"
# Bridge A: the declared gateway is also the host-side fixture address.
DECLARED_HOST = "10.231.10.1"
# Bridge B: its gateway stays host-local; the undeclared fixture is a container
# at a distinct IP so a packet from the qcg container must cross FORWARD chains.
UNDECLARED_GATEWAY = "10.231.11.1"
UNDECLARED_HOST = "10.231.11.2"
SUBNETS = {"declared": "10.231.10.0/24", "undeclared": "10.231.11.0/24"}
GATEWAYS = {"declared": DECLARED_HOST, "undeclared": UNDECLARED_GATEWAY}
BODIES = {"declared": b"declared-ok\n", "undeclared": b"undeclared-ok\n"}
MARKER = "PROBE_OK"
NETWORK_LABEL = "io.qcg.network-probe"
FIXTURE_LABEL = "io.qcg.network-probe.undeclared"
CLEANUP_SECONDS = 60.0
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
VERSION = re.compile(r"[0-9]+\.[0-9]+(?:\.[0-9]+)?(?:[-+][0-9A-Za-z.-]+)?\Z")
RUN_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
TOKEN = re.compile(r"[0-9a-f]{32}\Z")
SOCKET = re.compile(r"unix:///[-A-Za-z0-9._/]+\Z")


def network_name(host):
    """Derive the runner network exactly as ContainerNetwork::destination does."""
    return "qcg-egress-" + hashlib.sha256(host.encode("ascii").lower()).hexdigest()


def docker_endpoint(env):
    """Accept only an absolute local Unix socket; refuse remote contexts."""
    endpoint = env.get("DOCKER_HOST") or "unix:///var/run/docker.sock"
    if not SOCKET.fullmatch(endpoint) or ".." in Path(endpoint[len("unix://"):]).parts:
        raise RuntimeError("only a local Docker Unix socket is accepted")
    return endpoint


def command_env(endpoint, base=None):
    """Pin every child process to the vetted socket, without proxies."""
    env = dict(os.environ if base is None else base)
    env["DOCKER_HOST"] = endpoint
    for key in ("DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"):
        env.pop(key, None)
    env["NO_PROXY"] = "*"
    env["no_proxy"] = "*"
    return env


class Deadline:
    def __init__(self, seconds):
        self.end = time.monotonic() + seconds

    def slice(self, limit):
        remaining = self.end - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("probe deadline expired")
        return min(limit, remaining)

    def remaining(self):
        return self.slice(float("inf"))


@contextmanager
def wall_clock(seconds):
    def fire(_signum, _frame):
        raise TimeoutError("probe deadline expired")

    previous = signal.signal(signal.SIGALRM, fire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def result_template():
    declared = network_name(DECLARED_HOST)
    undeclared = network_name(UNDECLARED_HOST)
    return {
        "pass": False,
        "runtime": {"server_version": None},
        "hosts": {
            "declared": DECLARED_HOST, "undeclared": UNDECLARED_HOST, "port": None,
            "undeclared_gateway": UNDECLARED_GATEWAY,
            "undeclared_fixture_container": None,
        },
        "networks": {
            "declared": {"name": declared, "subnet": SUBNETS["declared"],
                         "gateway": None, "driver": None},
            "undeclared": {"name": undeclared, "subnet": SUBNETS["undeclared"],
                           "gateway": None, "driver": None},
        },
        "egress_policy": {
            "family": "docker-bridge", "separate_bridges": False,
            "declared_gateway_routed": False, "undeclared_gateway_routed": False,
            "trusted_host_fallback": False,
            "undeclared_fixture_in_separate_bridge": False,
            "deployment_egress_rules_certified": False,
            "scope": (
                "Two isolated Docker bridges: the qcg container is routed to the "
                "declared bridge-A gateway, where a host-side fixture serves; the "
                "undeclared fixture is a pinned container at a non-gateway IP on "
                "bridge B, blocked from the qcg container only by Docker's "
                "inter-network FORWARD isolation. No deployment-owned egress "
                "rules are certified."
            ),
        },
        "image": PINNED_IMAGE,
        "endpoint_health": {
            "declared": {"ok": False, "exit_code": None},
            "undeclared": {"ok": False, "exit_code": None},
        },
        "qcg": {
            "command": [], "exit_code": None, "run_finished_status": None,
            "marker_seen": False, "container_network": None, "network_argv_ok": False,
        },
        "assertions": {
            "declared_reachable": False, "undeclared_unreachable": False,
            "no_trusted_host_fallback": False,
        },
        "cleanup": {"ok": False, "errors": []},
        "failure_stage": None,
        "failure": None,
    }


def render_manifest(port, image=PINNED_IMAGE):
    """Render the one-step generator consumed by `qcg run`."""
    port = int(port)
    if not 1 <= port <= 65535 or image != PINNED_IMAGE:
        raise ValueError("manifest inputs are outside the pinned probe shape")
    script = (
        f"wget -q -O /dev/null -T 3 -t 1 http://{DECLARED_HOST}:{port}/ && "
        f"! wget -q -O /dev/null -T 3 -t 1 http://{UNDECLARED_HOST}:{port}/ && "
        f"echo {MARKER}"
    )
    q = json.dumps
    return "\n".join([
        "[generator]",
        'id = "container-network-probe"',
        'name = "Container network probe"',
        'version = "0.1.0"',
        "",
        "[[flow]]",
        'id = "probe"',
        'type = "check.container"',
        "",
        "[flow.params]",
        f"command = {q(['sh', '-c', script])}",
        f"image = {q(image)}",
        f"expect = {{ stdout_contains = {q(MARKER)} }}",
        "",
        "[permissions]",
        f"network = {q([DECLARED_HOST])}",
        "fs_read = []",
        "fs_write = []",
        "commands = []",
        'side_effects = "none"',
        "",
        "[permissions.containers]",
        "enabled = true",
        'runtime = "docker"',
        f"images = {q([image])}",
        'on_missing = "error"',
        "",
        "[permissions.containers.network]",
        'mode = "destination"',
        f"host = {q(DECLARED_HOST)}",
        "",
    ])


def undeclared_fixture_argv(port, token, network):
    """Docker argv for the short-lived, pinned undeclared HTTP fixture."""
    port = int(port)
    if not 1 <= port <= 65535 or not isinstance(token, str) or not TOKEN.fullmatch(token):
        raise ValueError("undeclared fixture inputs are outside the pinned probe shape")
    body = BODIES["undeclared"].decode().strip()
    script = f"printf '%s\\n' '{body}' > /index.html && httpd -f -p {port} -h /"
    return [
        "run", "--detach", "--name", f"qcg-network-probe-undeclared-{token}",
        "--network", network, "--ip", UNDECLARED_HOST,
        "--label", f"{FIXTURE_LABEL}={token}",
        PINNED_IMAGE, "sh", "-c", script,
    ]


def network_argv_ok(argv, declared, undeclared):
    """Only one network flag: the declared one, never host-like or undeclared."""
    if not isinstance(argv, list) or not all(isinstance(arg, str) for arg in argv):
        return False
    selected = [argv[index + 1] for index, arg in enumerate(argv[:-1]) if arg == "--network"]
    forbidden = {"host", "bridge", "trusted_host", undeclared}
    return selected == [declared] and not any(arg in forbidden for arg in argv)


def parse_events(stdout):
    events = []
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class FixtureHandler(http.server.BaseHTTPRequestHandler):
    body = b""

    def do_GET(self):
        if self.path != "/":
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, _format, *_args):
        pass


def passed(result):
    qcg = result["qcg"]
    return bool(
        result["runtime"]["server_version"]
        and result["endpoint_health"]["declared"]["ok"]
        and result["endpoint_health"]["undeclared"]["ok"]
        and qcg["exit_code"] == 0
        and qcg["run_finished_status"] == "success"
        and qcg["marker_seen"]
        and qcg["network_argv_ok"]
        and result["egress_policy"]["separate_bridges"]
        and result["egress_policy"]["undeclared_fixture_in_separate_bridge"]
        and result["cleanup"]["ok"]
        and all(result["assertions"].values())
    )


class Probe:
    def __init__(self, endpoint, qcg, command_timeout, deadline):
        self.env = command_env(endpoint)
        # The child runs with cwd=<tempdir>. POSIX resolves an executable
        # containing a slash against that child cwd, so a relative path such
        # as the default target/debug/qcg could never launch. Bare names keep
        # PATH lookup; explicit paths are frozen before the cwd override.
        self.qcg = qcg if not os.path.dirname(qcg) else os.path.abspath(qcg)
        self.command_timeout = command_timeout
        self.deadline = deadline
        self.token = uuid.uuid4().hex
        self.owned_networks = {}
        self.servers = []
        self.undeclared_fixture = None
        self.qcg_run_id = None
        self.stage = "startup"

    def call(self, *args, check=True):
        result = subprocess.run(
            ["docker", *args], capture_output=True, text=True, check=False,
            env=self.env, timeout=self.deadline.slice(self.command_timeout),
        )
        if check and result.returncode:
            raise RuntimeError(f"docker {args[0]} failed")  # never publish daemon text
        return result

    def run(self, result):
        self.stage = "daemon_version"
        version = self.call("version", "--format", "{{.Server.Version}}").stdout.strip()
        if not VERSION.fullmatch(version):
            raise RuntimeError("unrecognized daemon version")
        result["runtime"]["server_version"] = version

        self.stage = "image"
        if self.call("image", "inspect", "--format", "{{.Id}}", PINNED_IMAGE,
                     check=False).returncode:
            self.call("pull", PINNED_IMAGE)  # digest-pinned only; never a mutable tag

        self.stage = "network_provision"
        for key in ("declared", "undeclared"):
            gateway = GATEWAYS[key]
            name = result["networks"][key]["name"]
            created = self.call(
                "network", "create", "--driver", "bridge", "--subnet", SUBNETS[key],
                "--gateway", gateway, "--label", f"{NETWORK_LABEL}={self.token}", name,
            ).stdout.strip()
            if not HEX64.fullmatch(created):
                raise RuntimeError("network creation unconfirmed")
            self.owned_networks[name] = created
            driver = self.call("network", "inspect", "--format", "{{.Driver}}", name).stdout.strip()
            ipam = json.loads(self.call(
                "network", "inspect", "--format", "{{json .IPAM.Config}}", name).stdout)
            if (driver != "bridge" or len(ipam) != 1
                    or ipam[0].get("Gateway") != gateway
                    or ipam[0].get("Subnet") != SUBNETS[key]):
                raise RuntimeError("network provision mismatch")
            result["networks"][key]["gateway"] = ipam[0]["Gateway"]
            result["networks"][key]["driver"] = driver
        policy = result["egress_policy"]
        policy["separate_bridges"] = (
            result["networks"]["declared"]["subnet"] != result["networks"]["undeclared"]["subnet"]
            and result["networks"]["declared"]["driver"] == "bridge"
            and result["networks"]["undeclared"]["driver"] == "bridge"
        )

        self.stage = "endpoint_fixtures"
        port = self._start_fixtures()
        result["hosts"]["port"] = port
        result["endpoint_health"]["declared"] = self._health(
            DECLARED_HOST, port, BODIES["declared"])

        self.stage = "undeclared_fixture"
        self._start_undeclared_fixture(port, result)

        self.stage = "endpoint_fixtures"
        result["endpoint_health"]["undeclared"] = self._wait_health(
            UNDECLARED_HOST, port, BODIES["undeclared"])
        # A negative container assertion is meaningless if its endpoint was down.
        if not all(result["endpoint_health"][key]["ok"] for key in ("declared", "undeclared")):
            raise RuntimeError("fixture endpoint health check failed")

        self.stage = "qcg_run"
        with tempfile.TemporaryDirectory(prefix="qcg-network-probe-") as directory:
            # qcg run takes a generator directory containing qcg.toml.
            contract = Path(directory) / "generator"
            contract.mkdir()
            (contract / "qcg.toml").write_text(render_manifest(port), encoding="utf-8")
            command = [self.qcg, "run", str(contract), "--yes", "--json"]
            result["qcg"]["command"] = command
            completed = subprocess.run(
                command, cwd=directory, env=self.env, capture_output=True, text=True,
                check=False, timeout=self.deadline.remaining(),
            )
            result["qcg"]["exit_code"] = completed.returncode
            self._read_events(parse_events(completed.stdout), result)

    def _start_fixtures(self):
        """Only the declared gateway is a host-local fixture address."""
        port = free_port()
        handler = type("Fixture", (FixtureHandler,), {"body": BODIES["declared"]})
        server = http.server.ThreadingHTTPServer(
            (DECLARED_HOST, port), handler, bind_and_activate=False)
        server.daemon_threads = True
        server.server_bind()
        server.server_activate()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.servers.append((server, thread))
        return port

    def _start_undeclared_fixture(self, port, result):
        """Serve the undeclared body from a container on bridge B, not the host."""
        network = network_name(UNDECLARED_HOST)
        created = self.call(
            *undeclared_fixture_argv(port, self.token, network)).stdout.strip()
        if not HEX64.fullmatch(created):
            raise RuntimeError("undeclared fixture container unconfirmed")
        self.undeclared_fixture = created
        attached = json.loads(self.call(
            "container", "inspect", "--format", "{{json .NetworkSettings.Networks}}",
            created).stdout).get(network, {})
        if attached.get("IPAddress") != UNDECLARED_HOST:
            raise RuntimeError("undeclared fixture address mismatch")
        result["hosts"]["undeclared_fixture_container"] = created
        result["egress_policy"]["undeclared_fixture_in_separate_bridge"] = (
            result["networks"]["undeclared"]["name"] == network
            and result["networks"]["undeclared"]["gateway"] == GATEWAYS["undeclared"]
        )

    def _health(self, host, port, body):
        completed = subprocess.run(
            ["curl", "--fail", "--silent", "--show-error", "--max-time",
             str(min(self.command_timeout, 30)), "--noproxy", "*",
             f"http://{host}:{port}/"],
            capture_output=True, text=True, check=False, env=self.env,
            timeout=self.deadline.slice(self.command_timeout),
        )
        return {
            "ok": completed.returncode == 0 and completed.stdout.strip() == body.decode().strip(),
            "exit_code": completed.returncode,
        }

    def _wait_health(self, host, port, body):
        """Give a freshly started container fixture a bounded chance to listen."""
        deadline = time.monotonic() + min(self.command_timeout, 15.0)
        while True:
            health = self._health(host, port, body)
            if health["ok"] or time.monotonic() >= deadline:
                return health
            time.sleep(0.25)

    def _read_events(self, events, result):
        run_ids = {event["run_id"] for event in events
                   if isinstance(event.get("run_id"), str)}
        if len(run_ids) == 1 and RUN_ID.fullmatch(next(iter(run_ids))):
            self.qcg_run_id = run_ids.pop()
        finished = [event for event in events if event.get("kind") == "run_finished"]
        if finished:
            status = finished[-1].get("data", {}).get("status")
            if status in ("success", "failed"):
                result["qcg"]["run_finished_status"] = status
        # Only a successful step's stdout counts: the argv event itself
        # contains the marker text in the generated shell command.
        result["qcg"]["marker_seen"] = any(
            event.get("kind") == "step_finished"
            and isinstance(event.get("data"), dict)
            and event["data"].get("status") in ("success", "repaired")
            and isinstance(event["data"].get("output"), dict)
            and MARKER in str(event["data"]["output"].get("stdout", ""))
            for event in events
        )
        container_events = [
            event for event in events
            if event.get("kind") == "tool_backend_resolved"
            and isinstance(event.get("data"), dict)
            and event["data"].get("backend") == "container"
        ]
        argv = container_events[-1]["data"].get("argv") if container_events else None
        declared = result["networks"]["declared"]["name"]
        undeclared = result["networks"]["undeclared"]["name"]
        selected = ([argv[i + 1] for i, arg in enumerate(argv[:-1]) if arg == "--network"]
                    if isinstance(argv, list) else [])
        result["qcg"]["container_network"] = selected[0] if len(selected) == 1 else None
        result["qcg"]["network_argv_ok"] = network_argv_ok(argv, declared, undeclared)
        fallback = isinstance(argv, list) and any(
            isinstance(arg, str) and arg in {"host", "bridge", "trusted_host", undeclared}
            for arg in argv
        )
        policy = result["egress_policy"]
        policy["declared_gateway_routed"] = result["qcg"]["network_argv_ok"]
        policy["undeclared_gateway_routed"] = result["qcg"]["container_network"] == undeclared
        policy["trusted_host_fallback"] = fallback

        # With trusted-host/host networking this container could reach BOTH
        # fixtures, including the container fixture on bridge B. The negative
        # assertion plus the resolved network argv is the no-fallback proof.
        qcg_ok = (result["qcg"]["exit_code"] == 0
                  and result["qcg"]["run_finished_status"] == "success"
                  and result["qcg"]["marker_seen"])
        assertions = result["assertions"]
        assertions["declared_reachable"] = qcg_ok and result["endpoint_health"]["declared"]["ok"]
        assertions["undeclared_unreachable"] = qcg_ok and result["endpoint_health"]["undeclared"]["ok"]
        assertions["no_trusted_host_fallback"] = (
            assertions["undeclared_unreachable"] and result["qcg"]["network_argv_ok"] and not fallback
        )

    def cleanup(self):
        errors = []
        # The undeclared fixture container must go before its bridge can be removed.
        if self.undeclared_fixture is not None:
            try:
                cid = self.undeclared_fixture
                if not HEX64.fullmatch(cid):
                    errors.append("undeclared fixture identity unconfirmed")
                else:
                    labels = json.loads(self.call(
                        "container", "inspect", "--format", "{{json .Config.Labels}}",
                        cid, check=False).stdout)
                    if labels.get(FIXTURE_LABEL) != self.token:
                        errors.append("undeclared fixture ownership unconfirmed")
                    elif self.call("container", "rm", "--force", cid,
                                   check=False).returncode:
                        errors.append("undeclared fixture cleanup unconfirmed")
            except Exception:
                errors.append("undeclared fixture cleanup unconfirmed")
        # qcg removes its own --rm container; this only retires a container
        # left behind by this probe's run, verified by its exact run label.
        if self.qcg_run_id:
            try:
                listed = self.call("container", "ls", "--all", "--quiet",
                                   "--filter", f"label=qcg.run={self.qcg_run_id}", check=False)
                for cid in listed.stdout.split():
                    if not HEX64.fullmatch(cid):
                        errors.append("container identity unconfirmed")
                        continue
                    labels = json.loads(self.call(
                        "container", "inspect", "--format", "{{json .Config.Labels}}",
                        cid, check=False).stdout)
                    if labels.get("qcg.run") != self.qcg_run_id:
                        errors.append("container ownership unconfirmed")
                        continue
                    if self.call("container", "rm", "--force", cid, check=False).returncode:
                        errors.append("container cleanup unconfirmed")
            except Exception:
                errors.append("container cleanup unconfirmed")
        for name in self.owned_networks:
            try:
                labels = json.loads(self.call(
                    "network", "inspect", "--format", "{{json .Labels}}", name,
                    check=False).stdout)
                if labels.get(NETWORK_LABEL) != self.token:
                    errors.append(f"network ownership unconfirmed: {name}")
                    continue
                if self.call("network", "rm", name, check=False).returncode:
                    errors.append(f"network cleanup unconfirmed: {name}")
            except Exception:
                errors.append(f"network cleanup unconfirmed: {name}")
        for server, thread in self.servers:
            try:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
                if thread.is_alive():
                    errors.append("HTTP fixture cleanup unconfirmed")
            except Exception:
                errors.append("HTTP fixture cleanup unconfirmed")
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qcg", default="target/debug/qcg", help="qcg CLI binary path")
    parser.add_argument("--result", type=Path, default=Path("container-network.json"))
    parser.add_argument("--timeout", type=float, default=600.0, help="whole-probe seconds")
    parser.add_argument("--command-timeout", type=float, default=30.0, help="per-command seconds")
    parser.add_argument("--i-accept-disposable-daemon", action="store_true",
                        help="confirm the daemon and its containers are disposable")
    args = parser.parse_args()
    if not args.i_accept_disposable_daemon:
        parser.error("--i-accept-disposable-daemon is required on a disposable local daemon")
    for label, value in (("--timeout", args.timeout), ("--command-timeout", args.command_timeout)):
        if not 0 < value < float("inf"):
            parser.error(f"{label} must be finite and positive")

    result = result_template()
    probe = None
    try:
        endpoint = docker_endpoint(os.environ.copy())
        probe = Probe(endpoint, args.qcg, args.command_timeout, Deadline(args.timeout))
        with wall_clock(args.timeout):
            probe.run(result)
    except Exception as exc:
        result["failure_stage"] = probe.stage if probe else "startup"
        result["failure"] = type(exc).__name__  # exception text may contain local paths
    finally:
        if probe is not None:
            probe.deadline = Deadline(CLEANUP_SECONDS)
            try:
                with wall_clock(CLEANUP_SECONDS):
                    errors = probe.cleanup()
            except Exception:
                errors = ["cleanup deadline expired"]
            result["cleanup"] = {"ok": not errors, "errors": errors}
    result["pass"] = passed(result)
    try:
        args.result.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    except OSError:
        print("failed to write result file", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
