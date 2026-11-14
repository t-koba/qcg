#!/usr/bin/env python3
"""Disposable local Incus diagnostic; NOT a qcg crash-recovery test.

Requires an initialized, quiescent ephemeral daemon. Never starts instances or
fetches images. On uncertainty, stop: do not attempt best-effort cleanup.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

# Share the bounded Unix-only HTTP transport, without changing Docker/Podman.
spec = importlib.util.spec_from_file_location(
    "container_probe", Path(__file__).with_name("probe-container-identity.py"))
transport = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport)
OWNER = "user.qcg-identity-probe"
GENERATION = "user.qcg-identity-generation"


def require(condition):
    if not condition:
        raise RuntimeError("unconfirmed diagnostic precondition")


def identity(value):
    require(isinstance(value, str) and str(uuid.UUID(value)) == value)
    return value


class Probe:
    def __init__(self, api, timeout, socket_path, config_dir):
        self.api = api
        self.timeout = timeout
        self.socket_path = socket_path
        self.config_dir = config_dir
        self.token = uuid.uuid4().hex
        self.project = "qcg-probe-" + self.token
        self.name = "qcg-instance-" + self.token
        self.project_created = False
        self.owned = None
        self.stage = "daemon_identity"

    def call(self, *args):
        # A private CLI configuration and an explicit socket pin both API and
        # CLI operations to the same local daemon; never inherit remote config.
        env = {"PATH": os.defpath, "HOME": self.config_dir,
               "INCUS_CONF": self.config_dir, "INCUS_SOCKET": self.socket_path}
        subprocess.run(["incus", "--force-local", *args], env=env,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       check=True, timeout=self.timeout)

    def route(self, path):
        return path + "?project=" + self.project

    def response(self, method, route, status=200):
        code, _, body = self.api.request(method, route)
        require(code == status)
        result = json.loads(body)
        require(isinstance(result, dict))
        return result

    def get(self, route):
        result = self.response("GET", route)
        require(result.get("type") == "sync" and result.get("status_code") == 200)
        return result["metadata"]

    def absent(self, route):
        result = self.response("GET", route, 404)
        require(result.get("type") == "error" and result.get("error_code") == 404)

    def verify_project(self):
        require(self.project_created)
        record = self.get("/1.0/projects/" + self.project)
        require(record["name"] == self.project
                and record["config"].get(OWNER) == self.token
                and record["config"].get("features.images") == "false"
                and record["config"].get("features.profiles") == "false")

    def inspect(self, generation, expected=None):
        self.verify_project()
        record = self.get(self.route("/1.0/instances/" + self.name))
        require(record["name"] == self.name and record["type"] == "container"
                and record["status_code"] == 102
                and record["config"].get(OWNER) == self.token
                and record["config"].get(GENERATION) == generation)
        current = identity(record["config"].get("volatile.uuid"))
        require(expected is None or current == expected)
        return current

    def create(self, generation):
        self.verify_project()
        self.absent(self.route("/1.0/instances/" + self.name))
        # Select storage explicitly: with profiles disabled, do not rely on
        # an inherited root disk. Missing/unusable default storage fails closed.
        self.call("--project", self.project, "create", "--empty", self.name,
                  "--storage", "default",
                  "--config", OWNER + "=" + self.token,
                  "--config", GENERATION + "=" + generation)
        current = self.inspect(generation)
        self.owned = (generation, current)
        return current

    def delete(self, generation, expected):
        # This is intentionally name-keyed, under exclusive disposable-daemon
        # conditions. This check is NOT atomic with delete or a cleanup protocol.
        require(self.owned == (generation, expected))
        self.inspect(generation, expected)
        result = self.response("DELETE", self.route("/1.0/instances/" + self.name), 202)
        require(result.get("type") == "async" and result.get("status_code") == 100)
        operation = result["operation"]
        require(isinstance(operation, str))
        operation, separator, query = operation.partition("?")
        require(not separator or query == "project=" + self.project)
        prefix = "/1.0/operations/"
        require(operation.startswith(prefix))
        identity(operation[len(prefix):])  # No redirects, endpoints, or query injection.
        completed = self.get(self.route(operation + "/wait") + "&timeout=" + str(self.timeout))
        require(completed["status_code"] == 200 and completed.get("err") == "")
        self.absent(self.route("/1.0/instances/" + self.name))
        self.owned = None

    def run(self):
        result = {"backend": "incus", "outcome": "inconclusive",
                  "crash_cleanup_certified": False, "qcg_crash_recovery_test": False,
                  "stale_name_resolved_to_replacement": None,
                  "stale_name_delete_removed_replacement": None,
                  "resources_retired": False}
        try:
            daemon = self.get("/1.0")
            require(daemon["api_version"] == "1.0" and daemon["api_status"] == "stable"
                    and daemon["environment"]["server"] == "incus"
                    and daemon["environment"]["server_clustered"] is False)
            result["server_version"] = transport.version_string(daemon["environment"]["server_version"])
            self.stage = "project_creation"
            self.absent("/1.0/projects/" + self.project)
            self.call("project", "create", self.project,
                      "--config", OWNER + "=" + self.token,
                      "--config", "features.images=false",
                      "--config", "features.profiles=false")
            self.project_created = True
            self.stage = "original_creation"
            original = self.create("original")
            self.stage = "original_deletion"
            self.delete("original", original)
            self.stage = "replacement_creation"
            replacement = self.create("replacement")
            require(replacement != original)
            self.stage = "stale_name_inspection"
            self.inspect("replacement", replacement)
            result["stale_name_resolved_to_replacement"] = True
            self.stage = "stale_name_deletion"
            self.delete("replacement", replacement)
            result["stale_name_delete_removed_replacement"] = True
            self.stage = "project_retirement"
            self.verify_project()
            require(self.get(self.route("/1.0/instances")) == [])
            retired = self.response("DELETE", "/1.0/projects/" + self.project)
            require(retired.get("type") == "sync" and retired.get("status_code") == 200)
            self.absent("/1.0/projects/" + self.project)
            result["resources_retired"] = True
            result["outcome"] = "observed_name_keyed_replacement_deletion"
        except Exception:
            # Never emit arbitrary API/CLI/exception text, identities or paths.
            # No destructive fallback after a timeout, mismatch or ambiguity;
            # remaining resources expire with the ephemeral runner, not a sweep.
            result["inconclusive_at"] = self.stage
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--command-timeout", type=int, required=True)
    parser.add_argument("--api-response-limit", type=int, required=True)
    parser.add_argument("--socket", required=True)
    args = parser.parse_args()
    if args.command_timeout <= 0 or args.api_response_limit <= 0:
        parser.error("bounds must be positive")
    with tempfile.TemporaryDirectory(prefix="qcg-incus-probe-") as config:
        api = transport.UnixAPI(args.socket, args.command_timeout, args.api_response_limit)
        result = Probe(api, args.command_timeout, args.socket, config).run()
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
