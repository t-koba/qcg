#!/usr/bin/env python3
"""Tests diagnostic safety checks only; these are not daemon-contract evidence."""
import importlib.util
from email.message import Message
import json
from pathlib import Path
import subprocess
import socket
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "probe", Path(__file__).with_name("probe-container-identity.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)
CID = "a" * 64


def output(value, code=0):
    return subprocess.CompletedProcess([], code, value, "")


class SafetyChecks(unittest.TestCase):
    def test_mismatch_and_unavailability_never_remove(self):
        for response in (
            output(json.dumps([{"Id": "b" * 64, "Config": {"Labels": {}}}])),
            output(json.dumps([{"Id": CID, "Config": {"Labels": {probe.LABEL: "other"}}}])),
            RuntimeError("daemon unavailable"),
        ):
            with self.subTest(response=response):
                subject = probe.Probe("docker", 1)
                subject.owned[CID] = "fixture"
                subject.call = Mock(side_effect=[response])
                with self.assertRaises(RuntimeError):
                    subject.remove_owned(CID)
                self.assertEqual(subject.call.call_count, 1)
                self.assertIn(CID, subject.owned)

    def test_cleanup_failure_retains_tracking_and_is_visible(self):
        subject = probe.Probe("podman", 1)
        subject.owned[CID] = "fixture"
        subject.call = Mock(side_effect=[
            output(json.dumps([{"Id": CID, "Config": {"Labels": {probe.LABEL: subject.token}}}])),
            RuntimeError("rm failed"),
        ])
        with self.assertRaisesRegex(RuntimeError, "container cleanup unconfirmed"):
            subject.cleanup()
        self.assertIn(CID, subject.owned)
        self.assertEqual(subject.call.call_args.args, ("container", "rm", "--force", CID))

    def test_inconclusive_create_is_not_an_absence_claim(self):
        subject = probe.Probe("docker", 1)
        subject.call = Mock(return_value=output("", 1))
        self.assertIsNone(subject.create("fixture"))
        self.assertEqual(subject.owned, {})
        with self.assertRaisesRegex(RuntimeError, "ordinary fixture creation failed"):
            subject.run()

    def test_alias_probe_refuses_unverified_replacement(self):
        for field, wrong in (("Id", "c" * 64), ("Name", "other"),
                             ("Config", {"Labels": {probe.LABEL: "other"}})):
            with self.subTest(field=field):
                subject = probe.Probe("docker", 1)
                subject.create = Mock(side_effect=[CID, "b" * 64])
                subject.remove_owned = Mock()  # Original fixture only.
                subject.inspect_owned = Mock(return_value={"State": {"Running": True}})
                alias = {"Id": "b" * 64, "Name": "/" + CID,
                         "Config": {"Labels": {probe.LABEL: subject.token}}}
                alias[field] = wrong
                subject.call = Mock(side_effect=[output(""), output(json.dumps([alias]))])
                with self.assertRaisesRegex(RuntimeError, "refusing destructive probe"):
                    subject.run()
                subject.remove_owned.assert_called_once_with(CID)
                self.assertEqual(subject.call.call_count, 2)  # start, inspect; no rm.

    def test_only_full_ids_are_accepted(self):
        for invalid in ("abc", "a" * 63, "a" * 65, "A" * 64, "fixture"):
            with self.assertRaises(RuntimeError):
                probe.full_id(invalid)
        self.assertEqual(probe.full_id(CID + "\n"), CID)


class APIChecks(unittest.TestCase):
    def fixture(self, backend):
        subject = probe.Probe(backend, 1)
        replacement = "b" * 64
        subject.owned[replacement] = CID
        subject.fixture = Mock(return_value=(CID, replacement))
        record = {"Id": replacement, "Name": "/" + CID,
                  "Config": {"Labels": {probe.LABEL: subject.token}}}
        headers = Message()
        headers["Libpod-API-Version"] = "4.9.3"
        headers["API-Version"] = "1.41"
        version = json.dumps({"Version": "28.0.4" if backend == "docker" else "4.9.3",
                              "ApiVersion": "1.48"}).encode()
        return subject, record, (200, headers, version)

    def test_api_mismatch_or_uncertain_inspection_never_deletes(self):
        for backend in ("docker", "podman"):
            for stage in ("alias", "known_id"):
                for field, wrong in (("Id", "c" * 64), ("Name", "other"),
                                     ("Name", "//" + CID),
                                     ("Config", {"Labels": {probe.LABEL: "other"}}),
                                     (None, None)):
                    with self.subTest(backend=backend, stage=stage, field=field):
                        subject, record, version = self.fixture(backend)
                        bad = dict(record)
                        bad[field] = wrong
                        invalid = ((200, {}, json.dumps(bad).encode()) if field
                                   else (503, {}, b"unavailable"))
                        valid = (200, {}, json.dumps(record).encode())
                        responses = [version, invalid] if stage == "alias" else [version, valid, invalid]
                        api = Mock()
                        api.request.side_effect = responses
                        result = subject.run_api(api)
                        self.assertEqual(result["outcome"], "inconclusive")
                        self.assertIsNone(result["delete_status"])
                        self.assertFalse(any(c.args[0] == "DELETE" for c in api.request.call_args_list))
                        self.assertIn("b" * 64, subject.owned)

    def test_api_remove_and_no_remove_are_observations_not_certification(self):
        for backend in ("docker", "podman"):
            for removed in (True, False):
                with self.subTest(backend=backend, removed=removed):
                    subject, record, version = self.fixture(backend)
                    valid = (200, {}, json.dumps(record).encode())
                    api = Mock()
                    success = 204 if backend == "docker" else 200
                    api.request.side_effect = [version, valid, valid, (success, {}, b""),
                                               (404, {}, b"{}") if removed else valid]
                    result = subject.run_api(api)
                    self.assertEqual(result["outcome"], "removed" if removed else "inconclusive")
                    if not removed:
                        self.assertEqual(result["inconclusive_at"], "post_delete_fixture_remains")
                    self.assertTrue(result["inspect_full_id_fell_back_to_name"])
                    self.assertEqual(result["delete_status"], success)
                    self.assertEqual(result["delete_full_id_removed_different_id"], removed)
                    self.assertEqual("b" * 64 in subject.owned, not removed)
                    self.assertFalse(result["crash_cleanup_certified"])
                    prefix = "/v1.48/containers/" if backend == "docker" else "/v4.9.3/libpod/containers/"
                    self.assertEqual(api.request.call_args_list[3].args,
                                     ("DELETE", prefix + CID + "?force=true"))
                    self.assertEqual(result["server_version"], "28.0.4" if backend == "docker" else "4.9.3")
                    self.assertEqual(result["api_versions"], {"engine": "1.48"} if backend == "docker"
                                     else {"libpod": "4.9.3", "compat": "1.41"})

    def test_no_alias_never_deletes_and_retains_fixture(self):
        subject, record, version = self.fixture("docker")
        api = Mock()
        api.request.side_effect = [version, (404, {}, b"{}"),
                                   (200, {}, json.dumps(record).encode())]
        result = subject.run_api(api)
        self.assertEqual(result["outcome"], "no_alias_observed")
        self.assertFalse(result["inspect_full_id_fell_back_to_name"])
        self.assertFalse(result["delete_full_id_removed_different_id"])
        self.assertIsNone(result["delete_status"])
        self.assertEqual(api.request.call_count, 3)
        self.assertIn("b" * 64, subject.owned)

    def test_unknown_metadata_never_creates_api_fixture(self):
        for backend in ("docker", "podman"):
            subject, _, _ = self.fixture(backend)
            api = Mock()
            api.request.return_value = (200, {}, b'{"Version":"4.9.3"}')
            result = subject.run_api(api)
            self.assertEqual(result["outcome"], "inconclusive")
            self.assertEqual(result["inconclusive_at"], "version_metadata")
            subject.fixture.assert_not_called()

    def test_delete_errors_and_unknown_post_state_are_inconclusive(self):
        for failure in ("delete", "post_delete_inspection"):
            subject, record, version = self.fixture("docker")
            valid = (200, {}, json.dumps(record).encode())
            responses = [version, valid, valid]
            if failure == "post_delete_inspection":
                responses.append((204, {}, b""))
            responses.append((500, {}, b"error"))
            api = Mock()
            api.request.side_effect = responses
            result = subject.run_api(api)
            self.assertEqual(result["outcome"], "inconclusive")
            self.assertEqual(result["inconclusive_at"], failure)
            self.assertIsNone(result["delete_full_id_removed_different_id"])
            self.assertIn("b" * 64, subject.owned)

    def test_remote_docker_endpoint_is_refused(self):
        subject = probe.Probe("docker", 1)
        with patch.dict(probe.os.environ, {"DOCKER_HOST": "tcp://example.invalid:2375",
                                         "DOCKER_CONTEXT": ""}):
            with self.assertRaisesRegex(RuntimeError, "local Unix socket"):
                with probe.local_api(subject, Path("unused"), 1024, 0.05):
                    self.fail("remote API must not be used")

    def test_fixture_cli_is_pinned_to_local_backend(self):
        cases = (
            ("docker", {"DOCKER_HOST": "tcp://remote.invalid:2375",
                         "DOCKER_CONTEXT": "remote"}, "DOCKER_HOST",
             "unix:///tmp/qcg-test.sock", "DOCKER_CONTEXT"),
            ("podman", {"CONTAINER_HOST": "ssh://remote.invalid",
                         "CONTAINER_CONNECTION": "remote"}, "CONTAINER_HOST",
             "local", "CONTAINER_CONNECTION"),
        )
        for backend, inherited, host_key, expected, removed_key in cases:
            with self.subTest(backend=backend):
                subject = probe.Probe(backend, 1)
                if backend == "docker":
                    subject.docker_endpoint = expected
                else:
                    subject.cli_options = ["--remote=false"]
                with patch.dict(probe.os.environ, inherited, clear=False):
                    with patch.object(probe.subprocess, "run", return_value=output("")) as run:
                        subject.call("info")
                    env = run.call_args.kwargs["env"]
                self.assertNotIn(removed_key, env)
                if backend == "docker":
                    self.assertEqual(env[host_key], expected)

    def test_unix_transport_and_body_bound(self):
        # A local HTTP fixture checks transport mechanics only, not daemon semantics.
        for limit in (2, 1):
            with self.subTest(limit=limit), tempfile.TemporaryDirectory() as directory:
                path = str(Path(directory) / "api.sock")
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                    listener.settimeout(1)
                    listener.bind(path)
                    listener.listen(1)
                    errors = []

                    def serve():
                        try:
                            with listener.accept()[0] as client:
                                client.settimeout(1)
                                request = b""
                                while b"\r\n\r\n" not in request and len(request) < 4096:
                                    chunk = client.recv(4096)
                                    if not chunk:
                                        raise RuntimeError("incomplete test request")
                                    request += chunk
                                client.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                        except Exception as error:
                            errors.append(error)

                    thread = threading.Thread(target=serve)
                    thread.start()
                    try:
                        api = probe.UnixAPI(path, 1, limit)
                        if limit == 2:
                            status, _, body = api.request("GET", "/version")
                            self.assertEqual((status, body), (200, b"{}"))
                        else:
                            with self.assertRaisesRegex(RuntimeError, "response exceeded"):
                                api.request("GET", "/version")
                    finally:
                        thread.join(timeout=2)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
