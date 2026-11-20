#!/usr/bin/env python3
"""Pure guard/logic checks for the real-daemon probe; no Docker is used."""

import importlib.util
from pathlib import Path
import subprocess
import tomllib
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "network_probe", Path(__file__).with_name("probe-container-network.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class Derivation(unittest.TestCase):
    def test_network_names_match_the_rust_sha256_derivation(self):
        self.assertEqual(
            probe.network_name("10.231.10.1"),
            "qcg-egress-b62d550ede5de5b492c3595b5d2dd6bdb770b8e200434f99ca29eecc27da6f80",
        )
        self.assertEqual(
            probe.network_name("10.231.11.1"),
            "qcg-egress-2c49ac7da4df93d03ec2558b15c5dab4587c4b33d218e19fc05f17a2ac51aba8",
        )
        self.assertEqual(probe.network_name("API.Example.com"),
                         probe.network_name("api.example.com"))
        self.assertNotEqual(probe.network_name(probe.DECLARED_HOST),
                            probe.network_name(probe.UNDECLARED_HOST))

    def test_manifest_declares_only_the_declared_destination(self):
        document = tomllib.loads(probe.render_manifest(41337))
        self.assertEqual(len(document["flow"]), 1)
        step = document["flow"][0]
        self.assertEqual(step["type"], "check.container")
        params = step["params"]
        self.assertEqual(params["command"][:2], ["sh", "-c"])
        script = params["command"][2]
        self.assertIn(f"http://{probe.DECLARED_HOST}:41337/", script)
        self.assertIn(f"! wget -q -O /dev/null http://{probe.UNDECLARED_HOST}:41337/", script)
        self.assertIn("echo PROBE_OK", script)
        self.assertEqual(params["image"], probe.PINNED_IMAGE)
        self.assertEqual(params["expect"], {"stdout_contains": "PROBE_OK"})
        permissions = document["permissions"]
        self.assertEqual(permissions["network"], [probe.DECLARED_HOST])
        self.assertEqual(permissions["containers"]["network"],
                         {"mode": "destination", "host": probe.DECLARED_HOST})
        self.assertEqual(permissions["containers"]["images"], [probe.PINNED_IMAGE])
        self.assertTrue(permissions["containers"]["enabled"])
        self.assertEqual(permissions["containers"]["runtime"], "docker")
        self.assertEqual(permissions["containers"]["on_missing"], "error")
        self.assertIn("@sha256:", params["image"])

    def test_result_shape_and_gate_evidence(self):
        result = probe.result_template()
        self.assertEqual(set(result), {
            "pass", "runtime", "hosts", "networks", "egress_policy", "image",
            "endpoint_health", "qcg", "assertions", "cleanup", "failure_stage", "failure",
        })
        self.assertEqual(set(result["assertions"]), {
            "declared_reachable", "undeclared_unreachable", "no_trusted_host_fallback",
        })
        self.assertEqual(result["networks"]["declared"]["name"],
                         probe.network_name(probe.DECLARED_HOST))
        self.assertFalse(result["pass"])
        self.assertFalse(probe.passed(result))

    def test_network_argv_guard_refuses_fallbacks(self):
        declared = probe.network_name(probe.DECLARED_HOST)
        undeclared = probe.network_name(probe.UNDECLARED_HOST)
        good = ["docker", "run", "--network", declared, probe.PINNED_IMAGE, "sh", "-c", "PROBE_OK"]
        self.assertTrue(probe.network_argv_ok(good, declared, undeclared))
        for bad in (
            ["docker", "run", "--network", undeclared],
            ["docker", "run", "--network", "host"],
            ["docker", "run", "--network", "bridge"],
            ["docker", "run", "--network", "trusted_host"],
            ["docker", "run", "--network", declared, "--network", undeclared],
            ["docker", "run"],
            None,
        ):
            with self.subTest(argv=bad):
                self.assertFalse(probe.network_argv_ok(bad, declared, undeclared))


class OrchestrationLogic(unittest.TestCase):
    def fixture(self, network=None):
        result = probe.result_template()
        result["runtime"]["server_version"] = "28.0.4"
        result["endpoint_health"]["declared"]["ok"] = True
        result["endpoint_health"]["undeclared"]["ok"] = True
        result["egress_policy"]["separate_bridges"] = True
        result["qcg"]["exit_code"] = 0
        result["cleanup"]["ok"] = True
        network = network or result["networks"]["declared"]["name"]
        argv = ["docker", "run", "--network", network, probe.PINNED_IMAGE, "sh", "-c", "echo PROBE_OK"]
        events = [
            {"kind": "run_started", "run_id": "run-1", "data": {}},
            {"kind": "tool_backend_resolved", "data": {"backend": "container", "argv": argv}},
            {"kind": "step_finished", "data": {"status": "success",
                                                "output": {"stdout": "PROBE_OK\n"}}},
            {"kind": "run_finished", "run_id": "run-1", "data": {"status": "success"}},
        ]
        return result, events

    def test_events_yield_three_distinct_assertions(self):
        result, events = self.fixture()
        subject = probe.Probe("unix:///var/run/docker.sock", "qcg", 5, probe.Deadline(10))
        subject._read_events(events, result)
        self.assertEqual(subject.qcg_run_id, "run-1")
        self.assertEqual(result["qcg"]["run_finished_status"], "success")
        self.assertTrue(result["qcg"]["marker_seen"])
        self.assertTrue(result["qcg"]["network_argv_ok"])
        self.assertTrue(all(result["assertions"].values()))
        self.assertFalse(result["egress_policy"]["trusted_host_fallback"])
        self.assertTrue(probe.passed(result))

    def test_host_fallback_fails_the_negative_assertion(self):
        result, events = self.fixture(network="host")
        subject = probe.Probe("unix:///var/run/docker.sock", "qcg", 5, probe.Deadline(10))
        subject._read_events(events, result)
        self.assertFalse(result["assertions"]["no_trusted_host_fallback"])
        self.assertTrue(result["egress_policy"]["trusted_host_fallback"])
        self.assertFalse(probe.passed(result))

    def test_docker_calls_are_pinned_to_the_vetted_socket(self):
        subject = probe.Probe("unix:///run/docker.sock", "qcg", 5, probe.Deadline(10))
        completed = subprocess.CompletedProcess([], 0, "28.0.4\n", "")
        with patch.object(probe.subprocess, "run", return_value=completed) as run:
            subject.call("version", "--format", "{{.Server.Version}}")
        argv = run.call_args.args[0]
        env = run.call_args.kwargs["env"]
        self.assertEqual(argv, ["docker", "version", "--format", "{{.Server.Version}}"])
        self.assertEqual(env["DOCKER_HOST"], "unix:///run/docker.sock")
        self.assertNotIn("DOCKER_CONTEXT", env)
        self.assertEqual(env["NO_PROXY"], "*")


class Guards(unittest.TestCase):
    def test_remote_docker_hosts_are_refused(self):
        for endpoint in ("tcp://localhost:2375", "http://127.0.0.1:2375",
                         "ssh://user@host", "npipe:////./pipe/docker_engine",
                         "unix://relative.sock", "unix:///tmp/../etc/docker.sock"):
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(RuntimeError):
                    probe.docker_endpoint({"DOCKER_HOST": endpoint})
        self.assertEqual(probe.docker_endpoint({}), "unix:///var/run/docker.sock")
        self.assertEqual(
            probe.docker_endpoint({"DOCKER_HOST": "unix:///run/docker.sock"}),
            "unix:///run/docker.sock",
        )

    def test_inherited_remote_context_cannot_override_the_socket(self):
        env = probe.command_env(
            "unix:///run/docker.sock",
            {"DOCKER_CONTEXT": "remote", "DOCKER_TLS_VERIFY": "1", "DOCKER_CERT_PATH": "/keys"},
        )
        self.assertEqual(env["DOCKER_HOST"], "unix:///run/docker.sock")
        for key in ("DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"):
            self.assertNotIn(key, env)


if __name__ == "__main__":
    unittest.main()
