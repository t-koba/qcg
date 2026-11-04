#!/usr/bin/env python3
"""Real-daemon diagnostic, NOT a crash-cleanup implementation or acceptance test.

Only use on a disposable local daemon without concurrent resource mutation.
Builds a static fixture locally; imports a scratch filesystem without pulling.
A successful probe may demonstrate UNSAFE full-ID/name fallback. See
docs/container-crash-recovery.md. All bounds are explicit command arguments.
"""

import argparse
import json
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import uuid

LABEL = "io.qcg.identity-probe"


def full_id(value):
    value = value.strip()
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise RuntimeError("backend did not return a full container ID")
    return value


class Probe:
    def __init__(self, backend, timeout):
        self.backend = backend
        self.timeout = timeout
        self.token = uuid.uuid4().hex
        self.tag = f"qcg-identity-probe:{self.token}"
        self.owned = {}
        self.image_id = None

    def call(self, *args, check=True):
        result = subprocess.run(
            [self.backend, *args], capture_output=True, text=True,
            timeout=self.timeout, check=False,
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

    def run(self):
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
        resolved, = json.loads(self.call("container", "inspect", original).stdout)
        aliases = resolved["Id"] == replacement
        # Deliberate protocol probe, NOT the production removal algorithm.
        # Verify the alias belongs to this test immediately before trying it.
        if (not aliases or resolved["Name"].lstrip("/") != original
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
    args = parser.parse_args()
    if not 0 < args.command_timeout < float("inf"):
        parser.error("--command-timeout must be finite and positive")
    probe = Probe(args.backend, args.command_timeout)
    # Fail if the backend is unavailable; never interpret that as absence.
    probe.call("info")
    version = probe.call("--version").stdout.strip()
    try:
        with tempfile.TemporaryDirectory(prefix="qcg-identity-probe-") as directory:
            probe.build_image(Path(directory), args.compiler)
            result = probe.run()
    finally:
        probe.cleanup()
    result["client_version"] = version
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
