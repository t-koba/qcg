#!/usr/bin/env python3
"""Mock safety tests, not evidence of a real Incus daemon contract."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "incus_probe", Path(__file__).with_name("probe-incus-identity.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)
ORIGINAL = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
REPLACEMENT = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
OPERATION = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"


def response(metadata, code=200):
    return code, {}, json.dumps({"type": "sync", "status_code": 200,
                                 "metadata": metadata}).encode()


def missing():
    return 404, {}, b'{"type":"error","error_code":404}'


class Daemon:
    def __init__(self):
        self.subject = probe.Probe(self, 1, "/unused-local-socket", "/unused-config")
        self.subject.call = Mock(side_effect=self.create)
        self.project = None
        self.instance = None
        self.inspections = 0
        self.deletes = []
        self.transform = lambda record: record
        self.same_uuid = False
        self.delete_result = None
        self.wait_result = None
        self.project_delete_result = None
        self.operation_query = ""

    def create(self, *args):
        s = self.subject
        if args[0] == "project":
            self.project = {"name": s.project, "config": {
                probe.OWNER: s.token, "features.images": "false", "features.profiles": "false"}}
        else:
            generation = args[-1].split("=", 1)[1]
            self.instance = {"name": s.name, "type": "container", "status_code": 102,
                             "config": {probe.OWNER: s.token, probe.GENERATION: generation,
                                        "volatile.uuid": ORIGINAL if generation == "original"
                                        or self.same_uuid else REPLACEMENT}}

    def request(self, method, route):
        s = self.subject
        if method == "DELETE":
            self.deletes.append(route)
            if self.delete_result:
                return self.delete_result
            if route == "/1.0/projects/" + s.project:
                if self.project_delete_result:
                    return self.project_delete_result
                self.project = None
                return response({})
            self.instance = None
            return 202, {}, json.dumps({"type": "async", "status_code": 100,
                                        "operation": "/1.0/operations/" + OPERATION
                                        + self.operation_query}).encode()
        if route == "/1.0":
            return response({"api_version": "1.0", "api_status": "stable", "environment": {
                "server": "incus", "server_clustered": False, "server_version": "6.0.0"}})
        if route == "/1.0/projects/" + s.project:
            return response(self.project) if self.project else missing()
        if route == s.route("/1.0/instances"):
            return response([] if self.instance is None else ["unexpected-instance"])
        if route.startswith("/1.0/operations/"):
            return self.wait_result or response({"status_code": 200, "err": ""})
        if route == s.route("/1.0/instances/" + s.name):
            if self.instance is None:
                return missing()
            self.inspections += 1
            return response(self.transform(copy.deepcopy(self.instance)))
        raise AssertionError("unexpected route")


class SafetyChecks(unittest.TestCase):
    def assert_inconclusive(self, daemon, delete_count):
        result = daemon.subject.run()
        self.assertEqual(result["outcome"], "inconclusive")
        self.assertFalse(result["resources_retired"])
        self.assertFalse(result["crash_cleanup_certified"])
        self.assertFalse(result["qcg_crash_recovery_test"])
        self.assertEqual(len(daemon.deletes), delete_count)
        return result

    def test_owned_replacement_observation_is_not_certification(self):
        daemon = Daemon()
        result = daemon.subject.run()
        self.assertEqual(result["outcome"], "observed_name_keyed_replacement_deletion")
        self.assertTrue(result["stale_name_resolved_to_replacement"])
        self.assertTrue(result["stale_name_delete_removed_replacement"])
        self.assertTrue(result["resources_retired"])
        self.assertFalse(result["crash_cleanup_certified"])
        self.assertFalse(result["qcg_crash_recovery_test"])
        s = daemon.subject
        self.assertEqual(daemon.deletes, [s.route("/1.0/instances/" + s.name)] * 2
                         + ["/1.0/projects/" + s.project])
        for command in s.call.call_args_list[1:]:
            generation = command.args[-1].split("=", 1)[1]
            self.assertEqual(command.args, (
                "--project", s.project, "create", "--empty", s.name,
                "--storage", "default", "--config", probe.OWNER + "=" + s.token,
                "--config", probe.GENERATION + "=" + generation))

    def test_unavailable_storage_stops_without_cleanup_or_retry(self):
        for failure in (subprocess.CalledProcessError(1, "incus", stderr="private storage"),
                        subprocess.TimeoutExpired("incus", 1, stderr="private storage")):
            with self.subTest(failure=type(failure)):
                daemon = Daemon()
                create = daemon.create

                def unavailable(*args):
                    if "--storage" in args:
                        raise failure
                    create(*args)

                daemon.subject.call.side_effect = unavailable
                result = self.assert_inconclusive(daemon, 0)
                self.assertEqual(result["inconclusive_at"], "original_creation")
                self.assertEqual(daemon.subject.call.call_count, 2)  # Project, then one create.
                self.assertIsNotNone(daemon.project)
                self.assertNotIn("private storage", json.dumps(result))

    def test_uncertain_project_delete_never_retries(self):
        async_reply = 202, {}, json.dumps({"type": "async", "status_code": 100,
                                          "operation": "/1.0/operations/" + OPERATION}).encode()
        for reply in (async_reply,
                      response({}), (503, {}, b"private failure"),
                      (200, {}, b'{"type":"sync","status_code":400}')):
            with self.subTest(reply=reply):
                daemon = Daemon()
                daemon.project_delete_result = reply
                result = self.assert_inconclusive(daemon, 3)
                self.assertEqual(result["inconclusive_at"], "project_retirement")
                self.assertIsNotNone(daemon.project)
                self.assertIsNone(daemon.instance)

    def test_mismatched_instance_identity_prevents_delete(self):
        for inspection, deletes in ((2, 0), (5, 1)):
            for field, value in (("name", "other"), ("type", "virtual-machine"),
                                 ("status_code", 103), (probe.OWNER, "foreign"),
                                 (probe.GENERATION, "foreign"), ("volatile.uuid", OPERATION),
                                 ("volatile.uuid", None), ("volatile.uuid", "invalid")):
                with self.subTest(inspection=inspection, field=field, value=value):
                    daemon = Daemon()

                    def corrupt(record):
                        if daemon.inspections == inspection:
                            target = record if field in record else record["config"]
                            target[field] = value
                        return record

                    daemon.transform = corrupt
                    self.assert_inconclusive(daemon, deletes)
                    self.assertIsNotNone(daemon.instance)

    def test_unavailable_or_malformed_identity_prevents_delete(self):
        for inspection, deletes in ((2, 0), (5, 1)):
            for failure in (TimeoutError("private endpoint"), RuntimeError("secret"), KeyError("schema")):
                with self.subTest(inspection=inspection, failure=type(failure)):
                    daemon = Daemon()

                    def unavailable(record):
                        if daemon.inspections == inspection:
                            raise failure
                        return record

                    daemon.transform = unavailable
                    result = self.assert_inconclusive(daemon, deletes)
                    self.assertNotIn(str(failure), json.dumps(result))

    def test_project_mismatch_prevents_delete(self):
        for key, value in (("name", "other"), (probe.OWNER, "foreign"),
                           ("features.images", "true")):
            with self.subTest(key=key):
                daemon = Daemon()
                original = daemon.subject.verify_project

                def mismatch():
                    if daemon.inspections == 1:
                        target = daemon.project if key == "name" else daemon.project["config"]
                        target[key] = value
                    original()

                daemon.subject.verify_project = mismatch
                self.assert_inconclusive(daemon, 0)

    def test_missing_untracked_or_reused_identity_prevents_delete(self):
        daemon = Daemon()
        with self.assertRaises(RuntimeError):
            daemon.subject.delete("original", ORIGINAL)
        self.assertEqual(daemon.deletes, [])
        daemon.same_uuid = True
        self.assert_inconclusive(daemon, 1)  # Only the verified original was removed.

    def test_uncertain_delete_never_retries_or_cleans_up(self):
        for reply in ((503, {}, b"private daemon text"), response({}),
                      (202, {}, b'{"type":"async","status_code":100,"operation":"https://private"}')):
            with self.subTest(reply=reply):
                daemon = Daemon()
                daemon.delete_result = reply
                self.assert_inconclusive(daemon, 1)
        daemon = Daemon()
        daemon.wait_result = response({"status_code": 400, "err": "private failure"})
        self.assert_inconclusive(daemon, 1)

    def test_daemon_mismatch_or_api_failure_prevents_all_mutation(self):
        for reply in (response({}), (200, {}, b"not json"), missing(),
                      response({"api_version": "1.0", "api_status": "stable", "environment": {
                          "server": "lxd", "server_clustered": False, "server_version": "6.0.0"}})):
            with self.subTest(reply=reply):
                daemon = Daemon()
                daemon.subject.api = Mock()
                daemon.subject.api.request.return_value = reply
                self.assert_inconclusive(daemon, 0)
                daemon.subject.call.assert_not_called()

    def test_operation_query_must_match_owned_project(self):
        daemon = Daemon()
        daemon.operation_query = "?project=" + daemon.subject.project
        self.assertEqual(daemon.subject.run()["outcome"],
                         "observed_name_keyed_replacement_deletion")
        for query in ("?project=foreign", "?project=foreign&target=remote", "?"):
            with self.subTest(query=query):
                daemon = Daemon()
                daemon.operation_query = query
                self.assert_inconclusive(daemon, 1)

    def test_cli_pinned_local_empty_create_and_finite_timeout(self):
        subject = probe.Probe(Mock(), 7, "/local-socket", "/private-config")
        with patch.dict(os.environ, {"INCUS_REMOTE": "remote", "INCUS_CONF": "foreign"}), \
                patch.object(probe.subprocess, "run") as run:
            subject.call("create", "--empty", subject.name)
        args, kwargs = run.call_args
        self.assertEqual(args[0][:2], ["incus", "--force-local"])
        self.assertEqual(kwargs["timeout"], 7)
        self.assertEqual(kwargs["env"]["INCUS_SOCKET"], "/local-socket")
        self.assertNotIn("INCUS_REMOTE", kwargs["env"])
        self.assertEqual(kwargs["stdout"], subprocess.DEVNULL)
        self.assertEqual(kwargs["stderr"], subprocess.DEVNULL)


if __name__ == "__main__":
    unittest.main()
