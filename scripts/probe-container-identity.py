#!/usr/bin/env python3
"""Real-daemon diagnostic, NOT a crash-cleanup implementation or acceptance test.

Only use on a disposable local daemon without concurrent resource mutation.
Builds a static fixture locally; imports a scratch filesystem without pulling.
A successful probe may demonstrate UNSAFE full-ID/name fallback. See
docs/container-crash-recovery.md. All bounds are explicit command arguments.
This does not test qcg SIGKILL recovery or asynchronous-create fencing.
"""

import argparse
from contextlib import contextmanager
import http.client
import io
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tarfile
import tempfile
import time
import uuid

LABEL = "io.qcg.identity-probe"


class DeadlineReader(socket.SocketIO):
    def __init__(self, sock, deadline):
        super().__init__(sock, "rb")
        self.deadline = deadline

    def readinto(self, buffer):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("API deadline expired")
        self._sock.settimeout(remaining)
        return super().readinto(buffer)


class UnixAPI:
    def __init__(self, path, timeout, response_limit):
        self.path = path
        self.timeout = timeout
        self.response_limit = response_limit

    def request(self, method, route):
        # Never use HTTP proxies, TCP, redirects, credentials, or daemon output.
        deadline = time.monotonic() + self.timeout
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(self.timeout)
            sock.connect(self.path)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("API deadline expired")
            sock.settimeout(remaining)
            sock.sendall((f"{method} {route} HTTP/1.1\r\n"
                          "Host: localhost\r\nConnection: close\r\n"
                          "Content-Length: 0\r\n\r\n").encode("ascii"))
            # HTTPResponse also applies the standard library's HTTP header bounds.
            class ResponseSocket:
                def makefile(self, mode):
                    return io.BufferedReader(DeadlineReader(sock, deadline))

            with http.client.HTTPResponse(ResponseSocket()) as response:
                response.begin()
                body = response.read(self.response_limit + 1)
                if len(body) > self.response_limit:
                    raise RuntimeError("API response exceeded configured bound")
                if response.length not in (None, 0):
                    raise RuntimeError("API response body incomplete")
                return response.status, response.headers, body


def version_string(value):
    # Publish small version fields only, never arbitrary server text or endpoints.
    if not isinstance(value, str) or len(value) > 128 or not re.fullmatch(
            r"[0-9]+\.[0-9]+(?:\.[0-9]+)?(?:[-+][0-9A-Za-z.-]+)?", value):
        raise RuntimeError("unrecognized API version metadata")
    return value


@contextmanager
def local_api(probe, directory, response_limit, poll_interval):
    if probe.backend == "docker":
        # Resolve the CLI's effective endpoint, then pin subsequent CLI calls to
        # that same Unix socket. Refuse remote contexts instead of substituting.
        if os.environ.get("DOCKER_HOST") and not os.environ.get("DOCKER_CONTEXT"):
            endpoint = os.environ["DOCKER_HOST"]
        else:
            context, = json.loads(probe.call("context", "inspect").stdout)
            endpoint = context["Endpoints"]["docker"]["Host"]
        if not endpoint.startswith("unix:///"):
            raise RuntimeError("Docker API requires a local Unix socket")
        probe.docker_endpoint = endpoint
        yield UnixAPI(endpoint[len("unix://"):], probe.timeout, response_limit)
        return

    # A private rootless service shares the local CLI's storage and fixtures.
    if os.geteuid() == 0:
        raise RuntimeError("Podman API diagnostic requires rootless execution")
    probe.cli_options = ["--remote=false"]
    info = json.loads(probe.call("info", "--format", "json").stdout)
    if info["host"]["security"]["rootless"] is not True:
        raise RuntimeError("Podman rootless identity unconfirmed")
    path = str(directory / "podman.sock")
    service = subprocess.Popen(
        ["podman", "--remote=false", "system", "service", "--time=0", "unix://" + path],
        env=probe.command_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + probe.timeout
        while not Path(path).is_socket():
            if service.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError("Podman service startup unconfirmed")
            time.sleep(min(poll_interval, max(0, deadline - time.monotonic())))
        yield UnixAPI(path, probe.timeout, response_limit)
    finally:
        if service.poll() is None:
            service.terminate()
            try:
                service.wait(timeout=probe.timeout)
            except subprocess.TimeoutExpired:
                service.kill()
                service.wait(timeout=probe.timeout)


def full_id(value):
    value = value.strip()
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise RuntimeError("backend did not return a full container ID")
    return value


def name_matches(value, name):
    return value == name or value == "/" + name


class Probe:
    def __init__(self, backend, timeout):
        self.backend = backend
        self.timeout = timeout
        self.token = uuid.uuid4().hex
        self.tag = f"qcg-identity-probe:{self.token}"
        self.owned = {}
        self.image_id = None
        self.cli_options = []
        self.docker_endpoint = None

    def command_env(self):
        env = os.environ.copy()
        if self.backend == "docker" and self.docker_endpoint:
            # An inherited named context overrides DOCKER_HOST. Pin all fixture
            # operations to the Unix endpoint used by the direct API client.
            env.pop("DOCKER_CONTEXT", None)
            env["DOCKER_HOST"] = self.docker_endpoint
        if self.backend == "podman":
            # The diagnostic is local/rootless only; ignore inherited remotes.
            env.pop("CONTAINER_HOST", None)
            env.pop("CONTAINER_CONNECTION", None)
        return env

    def call(self, *args, check=True):
        result = subprocess.run(
            [self.backend, *self.cli_options, *args], capture_output=True, text=True,
            timeout=self.timeout, check=False, env=self.command_env(),
        )
        if check and result.returncode:
            # Do not publish daemon diagnostics, endpoints, or local paths.
            raise RuntimeError(f"{self.backend} {args[0]} failed ({result.returncode})")
        return result

    def inspect_owned(self, cid):
        record, = json.loads(self.call("container", "inspect", cid).stdout)
        if (record["Id"] != cid
                or record["Config"].get("Labels", {}).get(LABEL) != self.token):
            raise RuntimeError("ownership mismatch; refusing destructive call")
        return record

    def remove_owned(self, cid):
        # This is safe only under the diagnostic's disposable/quiescent-daemon
        # precondition. Production reconciliation cannot assume that exclusion.
        self.inspect_owned(cid)
        self.call("container", "rm", "--force", cid)
        del self.owned[cid]

    def create(self, name):
        result = self.call(
            "container", "create", "--pull=never", "--network=none",
            "--name", name, "--label", f"{LABEL}={self.token}",
            self.tag, "/hold", check=False,
        )
        if result.returncode:
            return None
        cid = full_id(result.stdout)
        self.owned[cid] = name
        self.inspect_owned(cid)
        return cid

    def build_image(self, directory, compiler):
        executable = directory / "hold"
        subprocess.run(
            [compiler, "-static", "-Os", "-s", "-o", str(executable),
             str(Path(__file__).parent / "fixtures/container-hold.c")],
            check=True, timeout=self.timeout, capture_output=True,
        )
        archive = directory / "rootfs.tar"
        with tarfile.open(archive, "w") as output:
            output.add(executable, arcname="hold")
        self.call("image", "import", str(archive), self.tag)
        self.image_id = self.call("image", "inspect", "--format", "{{.Id}}", self.tag).stdout.strip()

    def fixture(self):
        original = self.create(f"qcg-probe-{self.token}")
        if original is None:
            raise RuntimeError("ordinary fixture creation failed")
        self.remove_owned(original)
        # A replacement deliberately has the original's full ID as its NAME.
        # Its label and actual ID are ours, not another user's resource.
        replacement = self.create(original)
        if replacement is None:
            # A failed create is NOT proof of a naming restriction or absence.
            raise RuntimeError("full-ID-name fixture creation failed; result inconclusive")
        if replacement == original:
            raise RuntimeError("backend reused a container ID; result inconclusive")
        self.call("container", "start", replacement)
        record = self.inspect_owned(replacement)
        if not record["State"]["Running"]:
            raise RuntimeError("fixture is not running")
        return original, replacement

    def run(self):
        original, replacement = self.fixture()
        resolved, = json.loads(self.call("container", "inspect", original).stdout)
        aliases = resolved["Id"] == replacement
        # Deliberate protocol probe, NOT the production removal algorithm.
        # Verify the alias belongs to this test immediately before trying it.
        if (not aliases or not name_matches(resolved["Name"], original)
                or resolved["Config"].get("Labels", {}).get(LABEL) != self.token):
            raise RuntimeError("unexpected alias resolution; refusing destructive probe")
        removal = self.call("container", "rm", "--force", original, check=False)
        # All creates here completed synchronously; this list is not being used
        # to clear an asynchronous production intent or authorize run deletion.
        remaining = self.call("container", "ls", "--all", "--no-trunc", "--format", "{{.ID}}").stdout.split()
        removed = replacement not in remaining
        if removed:
            del self.owned[replacement]
        else:
            self.inspect_owned(replacement)
        if removal.returncode != 0:
            raise RuntimeError("alias removal failed; result inconclusive (not ID-only proof)")
        if not removed:
            raise RuntimeError("removal acknowledged but fixture remains; result inconclusive")
        return {
            "backend": self.backend,
            "inspect_full_id_fell_back_to_name": aliases,
            "rm_full_id_removed_different_id": removed,
            "inspect_then_rm_protocol_safe": False,
            "crash_cleanup_certified": False,
        }

    def api_record(self, record, cid, name):
        if (record["Id"] != cid or not name_matches(record["Name"], name)
                or record["Config"].get("Labels", {}).get(LABEL) != self.token):
            raise RuntimeError("API identity mismatch; refusing destructive call")

    def run_api(self, api):
        result = {
            "outcome": "inconclusive", "server_version": None,
            "api_versions": {}, "inspect_status": None,
            "inspect_full_id_fell_back_to_name": None, "delete_status": None,
            "delete_full_id_removed_different_id": None,
            "crash_cleanup_certified": False,
        }
        stage = "version_lookup"
        try:
            status, headers, body = api.request("GET", "/version")
            if status != 200:
                raise RuntimeError("API version lookup failed")
            stage = "version_metadata"
            metadata = json.loads(body)
            result["server_version"] = version_string(metadata["Version"])
            if self.backend == "docker":
                version = version_string(metadata["ApiVersion"])
                result["api_versions"] = {"engine": version}
                prefix = f"/v{version}/containers/"
                success = 204
            else:
                # Podman's compatibility /version body is not a Libpod version.
                # Select the native route only from the advertised native header.
                version = version_string(headers.get("Libpod-API-Version"))
                compat = version_string(headers.get("API-Version"))
                result["api_versions"] = {"libpod": version, "compat": compat}
                prefix = f"/v{version}/libpod/containers/"
                success = 200
            stage = "fixture_creation"
            original, replacement = self.fixture()
            stage = "alias_inspection"
            status, _, body = api.request("GET", prefix + original + "/json")
            result["inspect_status"] = status
            if status not in (200, 404):
                raise RuntimeError("API alias lookup unconfirmed")
            if status == 200:
                self.api_record(json.loads(body), replacement, original)
            result["inspect_full_id_fell_back_to_name"] = status == 200
            # Independently re-check the known target ID, name and ownership at
            # the API boundary, even when the alias lookup reported absence.
            stage = "known_identity_verification"
            known_status, _, known = api.request("GET", prefix + replacement + "/json")
            if known_status != 200:
                raise RuntimeError("API fixture lookup unconfirmed")
            self.api_record(json.loads(known), replacement, original)
            if status == 404:
                result["delete_full_id_removed_different_id"] = False
                result["outcome"] = "no_alias_observed"
                return result
            stage = "delete"
            status, _, _ = api.request("DELETE", prefix + original + "?force=true")
            result["delete_status"] = status
            if status != success:
                raise RuntimeError("API deletion unconfirmed")
            stage = "post_delete_inspection"
            status, _, body = api.request("GET", prefix + replacement + "/json")
            if status == 404:
                del self.owned[replacement]
                removed = True
            elif status == 200:
                self.api_record(json.loads(body), replacement, original)
                removed = False
            else:
                raise RuntimeError("API removal outcome unconfirmed")
            result["delete_full_id_removed_different_id"] = removed
            if removed:
                result["outcome"] = "removed"
            else:
                result["inconclusive_at"] = "post_delete_fixture_remains"
        except Exception:
            # Do not publish exception/response text (may contain local paths).
            # Unknown schemas, missing metadata and errors are never ID-only proof.
            result["inconclusive_at"] = stage
        return result

    def cleanup(self):
        errors = []
        for cid in list(self.owned):
            try:
                self.remove_owned(cid)
            except Exception:
                errors.append("container cleanup unconfirmed: " + cid)
        if self.image_id:
            try:
                current = self.call("image", "inspect", "--format", "{{.Id}}", self.tag).stdout.strip()
                if current != self.image_id:
                    raise RuntimeError("image identity changed")
                self.call("image", "rm", self.tag)
            except Exception:
                errors.append("image cleanup unconfirmed: " + self.tag)
        if errors:
            raise RuntimeError("; ".join(errors))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backend", choices=["docker", "podman"])
    parser.add_argument("--command-timeout", type=float, required=True,
                        help="positive per-command deadline in seconds (no retries)")
    parser.add_argument("--compiler", default="cc", help="local static C compiler (default: cc)")
    parser.add_argument("--api-response-limit", type=int, required=True,
                        help="positive maximum API response body size in bytes")
    parser.add_argument("--service-poll-interval", type=float, default=0.05,
                        help="Podman socket startup poll seconds (default: 0.05)")
    args = parser.parse_args()
    if not 0 < args.command_timeout < float("inf"):
        parser.error("--command-timeout must be finite and positive")
    if args.api_response_limit <= 0:
        parser.error("--api-response-limit must be positive")
    if not 0 < args.service_poll_interval < float("inf"):
        parser.error("--service-poll-interval must be finite and positive")
    probe = Probe(args.backend, args.command_timeout)
    result = {"backend": args.backend, "outcome": "inconclusive",
              "crash_cleanup_certified": False}
    try:
        with tempfile.TemporaryDirectory(prefix="qcg-identity-probe-") as directory:
            with local_api(probe, Path(directory), args.api_response_limit,
                           args.service_poll_interval) as api:
                probe.call("info")
                version = version_string(probe.call(
                    "version", "--format", "{{.Client.Version}}").stdout.strip())
                try:
                    probe.build_image(Path(directory), args.compiler)
                    result = probe.run()
                    result["client_version"] = version
                    result["api_probe"] = probe.run_api(api)
                    result["outcome"] = result["api_probe"]["outcome"]
                finally:
                    probe.cleanup()
    except Exception:
        result["outcome"] = "inconclusive"
        result["failure"] = "diagnostic_or_cleanup_unconfirmed"
    print(json.dumps(result, sort_keys=True))
    return 1 if result["outcome"] == "inconclusive" else 0


if __name__ == "__main__":
    raise SystemExit(main())
