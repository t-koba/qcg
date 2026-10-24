#!/usr/bin/env python3
"""Focused unittest coverage for scripts/ci-job-minutes.py (stdlib only).

Run: python3 scripts/test-ci-job-minutes.py
All network access is mocked; fixtures below mirror the GitHub Actions REST
shapes the reporter consumes (run, jobs with filter=all, artifacts, runs).
"""

import importlib.util
import io
import json
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError

HERE = Path(__file__).resolve().parent
REPO = HERE / "ci-job-minutes.py"
WORKFLOW = HERE.parent / ".github" / "workflows" / "ci-job-minutes.yml"


def load_reporter():
    spec = importlib.util.spec_from_file_location("ci_job_minutes", REPO)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


reporter = load_reporter()

TOKEN = "test-token-sentinel-abc123"
RUN_ID = 123456
REPO_NAME = "o/r"
BRANCH = "mizu"
DAY = "2026-10-22"


def job(name, start, end, conclusion="success", attempt=1):
    return {
        "id": abs(hash((name, attempt))) % 10**9,
        "run_id": RUN_ID,
        "run_attempt": attempt,
        "name": name,
        "status": "completed",
        "conclusion": conclusion,
        "started_at": start,
        "completed_at": end,
    }


SOURCE_RUN = {
    "id": RUN_ID,
    "name": "CI",
    "run_number": 42,
    "run_attempt": 2,
    "head_branch": BRANCH,
    "head_sha": "abcdef1234567890",
    "status": "completed",
    "conclusion": "success",
    "created_at": "2026-10-22T01:02:03Z",
    "updated_at": "2026-10-22T01:40:00Z",
    "workflow_id": 999,
    "html_url": "https://github.com/o/r/actions/runs/123456",
}

SOURCE_JOBS = [
    job("Rust (ubuntu-latest)", "2026-10-22T01:03:00Z", "2026-10-22T01:16:32.500Z"),
    job("Rust (macos-latest)", "2026-10-22T01:03:01Z", "2026-10-22T01:20:00Z"),
    job("skipped-thing", None, None, conclusion="skipped"),
    job("cancelled-early", None, None, conclusion="cancelled"),
    job("flaky", "2026-10-22T01:03:00Z", "2026-10-22T01:05:00Z", conclusion="failure", attempt=1),
    job("flaky", "2026-10-22T01:10:00Z", "2026-10-22T01:13:00Z", conclusion="success", attempt=2),
]

SOURCE_ARTIFACTS = [
    {"id": 111, "name": "qcg-test-time-ubuntu-latest-123456-1", "size_in_bytes": 10, "expired": False},
    {"id": 222, "name": "qcg-test-time-macos-latest-123456-1", "size_in_bytes": 12, "expired": False},
    {"id": 333, "name": "unrelated-build", "size_in_bytes": 99, "expired": False},
]

DAY_RUN_A = dict(SOURCE_RUN)
DAY_RUN_B = dict(
    SOURCE_RUN,
    id=654321,
    conclusion="failure",
    created_at="2026-10-22T05:00:00Z",
    html_url="https://github.com/o/r/actions/runs/654321",
)
DAY_RUN_OTHER_DAY = dict(SOURCE_RUN, id=777, created_at="2026-10-23T00:00:01Z")
DAY_RUN_IN_PROGRESS = dict(SOURCE_RUN, id=888, status="in_progress", conclusion=None)


class FakeResp:
    def __init__(self, payload, link=None):
        self._body = json.dumps(payload).encode("utf-8")
        self.headers = {"Link": link} if link else {}

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def make_urlopen(routes, seen=None):
    """routes: list of (predicate, behavior); behavior is payload/link or exception."""
    calls = []

    def fake(request, *args, **kwargs):
        url = request.full_url if hasattr(request, "full_url") else request
        calls.append((url, dict(request.header_items())))
        for predicate, behavior in routes:
            if predicate(url):
                if isinstance(behavior, Exception):
                    raise behavior
                payload, link = behavior
                return FakeResp(payload, link)
        raise AssertionError("unexpected API URL: %s" % url)

    if seen is not None:
        seen.extend([])  # placeholder to keep signature stable
    fake.calls = calls
    return fake


def contains(url_part):
    return lambda url: url_part in url


class ReporterTests(unittest.TestCase):
    def test_parse_ts_z_and_offsets(self):
        dt = reporter.parse_ts("2026-10-22T01:02:03.456Z")
        self.assertEqual(dt.isoformat(), "2026-10-22T01:02:03.456000+00:00")
        dt2 = reporter.parse_ts("2026-10-22T03:02:03+02:00")
        self.assertEqual(dt2.isoformat(), "2026-10-22T01:02:03+00:00")
        for bad in ("", None, "not-a-time", "2026-10-22 01:02:03", "2026-13-45T99:99:99Z"):
            with self.assertRaises(reporter.ReportError, msg=bad):
                reporter.parse_ts(bad)

    def test_job_seconds_exact(self):
        seconds = reporter.job_seconds(SOURCE_JOBS[0])
        self.assertAlmostEqual(seconds, 812.5, places=3)

    def test_job_seconds_skipped_and_no_start_zero(self):
        self.assertEqual(reporter.job_seconds(SOURCE_JOBS[2]), 0.0)
        self.assertEqual(reporter.job_seconds(SOURCE_JOBS[3]), 0.0)

    def test_job_seconds_missing_or_inconsistent_fails_closed(self):
        with self.assertRaises(reporter.ReportError):
            reporter.job_seconds(job("x", None, "2026-10-22T01:04:00Z"))
        with self.assertRaises(reporter.ReportError):
            reporter.job_seconds(job("x", "2026-10-22T01:04:00Z", None))
        with self.assertRaises(reporter.ReportError):
            reporter.job_seconds(job("x", "2026-10-22T01:05:00Z", "2026-10-22T01:04:00Z"))
        with self.assertRaises(reporter.ReportError):
            reporter.job_seconds(job("skip", "2026-10-22T01:00:00Z", "2026-10-22T01:00:01Z", conclusion="skipped"))
        with self.assertRaises(reporter.ReportError):
            reporter.job_seconds(job("cancelled", "2026-10-22T01:00:00Z", None, conclusion="cancelled"))

    def test_pagination_independent_parsing(self):
        base = "https://api.github.com/repos/o/r/actions/runs/1/jobs?filter=all&per_page=100"
        first = {"total_count": 3, "jobs": SOURCE_JOBS[:1]}
        rest = {"total_count": 3, "jobs": SOURCE_JOBS[1:]}
        link = '<%s&page=2>; rel="next", <%s&page=2>; rel="last"' % (base, base)
        fake = make_urlopen([(contains("page=2"), (rest, None)), (contains("jobs"), (first, link))])
        with mock.patch.object(reporter, "urlopen", fake):
            paged = reporter.api_get_all(base, TOKEN, "jobs")
        with mock.patch.object(reporter, "urlopen", make_urlopen([(contains("jobs"), ({"total_count": 3, "jobs": SOURCE_JOBS}, None))])):
            single = reporter.api_get_all(base, TOKEN, "jobs")
        self.assertEqual(paged, single)
        self.assertEqual(len(paged), len(SOURCE_JOBS))
        self.assertEqual(reporter.summarize_jobs(paged)["seconds"], reporter.summarize_jobs(single)["seconds"])

    def test_pagination_rejects_cross_origin_next_link(self):
        base = "https://api.github.com/repos/o/r/actions/runs/1/jobs?per_page=100"
        link = '<https://example.invalid/steal>; rel="next"'
        fake = make_urlopen([(contains("jobs"), ({"jobs": []}, link))])
        with mock.patch.object(reporter, "urlopen", fake):
            with self.assertRaises(reporter.ReportError):
                reporter.api_get_all(base, TOKEN, "jobs")
        self.assertEqual(len(fake.calls), 1)

    def test_redirect_handler_refuses_redirect_targets(self):
        handler = reporter._NoRedirectHandler()
        request = reporter.Request(
            "https://api.github.com/repos/o/r/actions/runs/1", headers={"Authorization": "Bearer " + TOKEN}
        )
        self.assertIsNone(
            handler.redirect_request(request, None, 302, "Found", {}, "https://example.invalid/collect")
        )

    def test_retries_then_success(self):
        err = HTTPError("https://api.github.com/x", 503, "Service Unavailable", None, None)
        calls = {"n": 0}

        def flaky(request, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] < 3:
                raise err
            return FakeResp({"ok": True})

        with mock.patch.object(reporter, "urlopen", flaky), mock.patch.object(reporter.time, "sleep") as asleep:
            data, _ = reporter.api_get_json("https://api.github.com/x", TOKEN)
        self.assertEqual(data, {"ok": True})
        self.assertEqual(calls["n"], 3)
        self.assertEqual(asleep.call_count, 2)

    def test_retry_exhausted_raises_without_token(self):
        err = HTTPError("https://api.github.com/x", 503, "Service Unavailable", None, None)
        with mock.patch.object(reporter, "urlopen", mock.Mock(side_effect=err)), mock.patch.object(
            reporter.time, "sleep"
        ):
            with self.assertRaises(reporter.ReportError) as ctx:
                reporter.api_get_json("https://api.github.com/x", TOKEN)
        self.assertNotIn(TOKEN, str(ctx.exception))

    def test_all_attempts_aggregated(self):
        summary = reporter.summarize_jobs(SOURCE_JOBS)
        # flaky attempt 1 (120 s, failure) + attempt 2 (180 s, success) both count.
        self.assertAlmostEqual(summary["seconds"], 812.5 + 1019.0 + 0.0 + 0.0 + 120.0 + 180.0, places=3)
        self.assertEqual(summary["jobs"], 6)
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(summary["failed"], 1)

    def test_daily_grouping_dedupes_and_filters(self):
        runs = [DAY_RUN_A, DAY_RUN_B, DAY_RUN_A, DAY_RUN_OTHER_DAY, DAY_RUN_IN_PROGRESS]
        fake = make_urlopen([(contains("/runs"), ({"total_count": 5, "workflow_runs": runs}, None))])
        with mock.patch.object(reporter, "urlopen", fake):
            kept = reporter.get_day_runs(REPO_NAME, 999, BRANCH, DAY, TOKEN)
        self.assertEqual([r["id"] for r in kept], [RUN_ID, 654321])
        requested = fake.calls[0][0]
        self.assertIn("status=completed", requested)
        self.assertIn("branch=mizu", requested)
        self.assertIn("created=2026-10-22..2026-10-22", requested)

    def test_url_encoding_of_branch(self):
        fake = make_urlopen([(contains("/runs"), ({"total_count": 0, "workflow_runs": []}, None))])
        with mock.patch.object(reporter, "urlopen", fake):
            reporter.get_day_runs(REPO_NAME, 999, "feature/x", DAY, TOKEN)
        self.assertIn("branch=feature%2Fx", fake.calls[0][0])

    def _full_routes(self):
        return [
            (contains("/artifacts"), ({"total_count": 3, "artifacts": SOURCE_ARTIFACTS}, None)),
            (contains("/runs/654321/jobs"), ({"total_count": 1, "jobs": [SOURCE_JOBS[0]]}, None)),
            (contains("/jobs"), ({"total_count": 6, "jobs": SOURCE_JOBS}, None)),
            (contains("/workflows/999/runs"), ({"total_count": 2, "workflow_runs": [DAY_RUN_A, DAY_RUN_B]}, None)),
            (contains("/actions/runs/123456"), (SOURCE_RUN, None)),
        ]

    def test_generate_report_contents_and_artifact_links(self):
        fake = make_urlopen(self._full_routes())
        with mock.patch.object(reporter, "urlopen", fake):
            text = reporter.generate(REPO_NAME, RUN_ID, TOKEN)
        self.assertIn(str(RUN_ID), text)
        self.assertIn("attempt 2", text)
        self.assertIn("branch `mizu`", text)
        self.assertIn("https://github.com/o/r/actions/runs/123456", text)
        self.assertIn("qcg-test-time-ubuntu-latest-123456-1", text)
        self.assertIn("id 111", text)
        self.assertIn("https://github.com/o/r/actions/runs/123456/artifacts/111", text)
        self.assertIn("qcg-test-time-macos-latest-123456-1", text)
        self.assertIn("id 222", text)
        self.assertNotIn("unrelated-build", text)
        # Per-run and day totals are exact: source 2131.5 s = 35.52 min.
        self.assertIn("2131.500 s = 35.52 job-minutes", text)
        # Day cohort: 2131.5 + 812.5 = 2944.0 s = 49.07 min over 2 runs.
        self.assertIn("Day totals: 2 runs", text)
        self.assertIn("2944.000 s = 49.07 job-minutes", text)
        # Skipped/failed rows are auditable.
        self.assertIn("1 skipped", text)
        self.assertIn("1 failed", text)
        # Nothing was ever downloaded: no archive download URL requested.
        for url, _ in fake.calls:
            self.assertNotIn("artifacts/111", url)
            self.assertNotIn("zip", url)
        self.assertNotIn(TOKEN, text)

    def test_token_from_env_only_and_never_printed(self):
        fake = make_urlopen(self._full_routes())
        env = {"GH_TOKEN": TOKEN, "QCG_REPO": REPO_NAME, "QCG_RUN_ID": str(RUN_ID), "QCG_OUT": ""}
        out = io.StringIO()
        with mock.patch.object(reporter, "urlopen", fake), mock.patch.dict(os.environ, env, clear=False):
            with redirect_stdout(out):
                code = reporter.main([])
        self.assertEqual(code, 0)
        self.assertNotIn(TOKEN, out.getvalue())
        # Authorization header carries the token; it never reaches the report.
        _, headers = fake.calls[0]
        auth = {k.lower(): v for k, v in headers.items()}.get("authorization", "")
        self.assertIn(TOKEN, auth)

    def test_missing_token_fails_without_api_calls(self):
        fake = make_urlopen([])
        env = {k: v for k, v in os.environ.items() if k not in ("GH_TOKEN", "GITHUB_TOKEN")}
        err = io.StringIO()
        with mock.patch.object(reporter, "urlopen", fake), mock.patch.dict(os.environ, env, clear=True):
            with redirect_stderr(err):
                self.assertEqual(reporter.main(["--repo", REPO_NAME, "--run-id", str(RUN_ID)]), 2)
        self.assertIn("missing GH_TOKEN", err.getvalue())
        self.assertEqual(fake.calls, [])

    def test_source_branch_mismatch_fails_closed(self):
        fake = make_urlopen([(contains("/actions/runs/123456"), (SOURCE_RUN, None))])
        with mock.patch.object(reporter, "urlopen", fake):
            with self.assertRaises(reporter.ReportError):
                reporter.generate(REPO_NAME, RUN_ID, TOKEN, branch="main")
        self.assertEqual(len(fake.calls), 1)

    def test_incomplete_source_run_fails_closed(self):
        bad = dict(SOURCE_RUN, status="in_progress", conclusion=None)
        fake = make_urlopen([(contains("/actions/runs/"), (bad, None))])
        with mock.patch.object(reporter, "urlopen", fake):
            with self.assertRaises(reporter.ReportError):
                reporter.generate(REPO_NAME, RUN_ID, TOKEN)

    def test_workflow_uses_default_branch_and_read_only_permissions(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("workflows: [CI]", text)
        self.assertIn("branches: [main, mizu]", text)
        self.assertIn("ref: ${{ github.event.repository.default_branch }}", text)
        self.assertIn("actions: read", text)
        self.assertIn("contents: read", text)
        self.assertIn("persist-credentials: false", text)
        self.assertNotIn("id-token: write", text)


if __name__ == "__main__":
    unittest.main()
