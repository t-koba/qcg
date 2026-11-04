#!/usr/bin/env python3
"""Tests diagnostic safety checks only; these are not daemon-contract evidence."""
import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock

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


if __name__ == "__main__":
    unittest.main()
