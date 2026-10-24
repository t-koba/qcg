#!/usr/bin/env python3
"""Minimal CI job-minutes reporter (Python standard library only).

Reads one completed CI workflow run plus its UTC-day branch cohort from the
GitHub Actions REST API and writes a Markdown report with exact per-job and
per-day job-minutes.

Method (also printed into the report):
- Per-job durations come from exact ``started_at``/``completed_at`` stamps,
  summed over all attempts via ``GET .../runs/{id}/jobs?filter=all``.
- Never-started jobs (``skipped``, or ``cancelled`` / ``neutral`` with no
  timestamps) count as 0 s. Any other missing or
  inconsistent timestamp fails the report instead of guessing.
- The UTC-day cohort keeps each completed run once (deduped by run id),
  filters client-side to the source run's UTC ``created_at`` date and branch,
  and sums all job attempts per run.
- The source run's ``qcg-test-time-*`` artifacts are listed by name/ID as
  adjacent evidence; they are never downloaded or executed.
- The API token is read only from ``GH_TOKEN`` (fallback ``GITHUB_TOKEN``)
  and is never printed. API parameters are URL-encoded; list endpoints are
  paginated via ``Link: rel="next"`` with bounded retries on 429/5xx.

Usage:
  python3 scripts/ci-job-minutes.py --repo OWNER/REPO --run-id ID \\
      [--attempt N] [--branch BRANCH] [--day YYYY-MM-DD] [--out report.md]
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

API_BASE = "https://api.github.com"
USER_AGENT = "qcg-ci-job-minutes/1"
ARTIFACT_PREFIX = "qcg-test-time-"
PER_PAGE = 100
MAX_PAGES = 100
MAX_RETRIES = 3
API_TIMEOUT_S = 30
RETRY_STATUS = {429, 500, 502, 503, 504}
FAILURE_CONCLUSIONS = {"failure", "timed_out", "startup_failure", "action_required"}
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
LINK_NEXT_RE = re.compile(r'<([^>]+)>\s*;\s*rel="next"')


class ReportError(Exception):
    """Fatal reporter failure: fail closed, never guess."""


class _NoRedirectHandler(HTTPRedirectHandler):
    """Never forward the read-only Actions token to a redirect target."""

    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


# GitHub REST endpoints used below do not require redirects. Refuse them so
# urllib cannot forward Authorization to a different host.
urlopen = build_opener(_NoRedirectHandler).open


def parse_ts(value):
    """Parse a GitHub ISO-8601 timestamp into an aware UTC datetime."""
    if not isinstance(value, str) or not value.strip():
        raise ReportError("missing timestamp")
    text = value.strip()
    if text[-1:] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise ReportError("unparseable timestamp %r" % (value,))
    if parsed.tzinfo is None:
        raise ReportError("timestamp without offset %r" % (value,))
    return parsed.astimezone(timezone.utc)


def job_seconds(job):
    """Exact seconds for one job row (every attempt is its own row)."""
    name = job.get("name", "?")
    conclusion = job.get("conclusion")
    started = job.get("started_at")
    completed = job.get("completed_at")
    if conclusion == "skipped":
        if started is None and completed is None:
            return 0.0
        raise ReportError("skipped job %r unexpectedly has timestamps" % (name,))
    if started is None and completed is None and conclusion in ("cancelled", "neutral"):
        return 0.0  # never started: no runner time consumed
    if started is None or completed is None:
        raise ReportError(
            "job %r (conclusion %r): missing or inconsistent timestamps"
            % (name, conclusion)
        )
    delta = (parse_ts(completed) - parse_ts(started)).total_seconds()
    if delta < 0:
        raise ReportError("job %r: completed_at precedes started_at" % (name,))
    return delta

def _request(url, token):
    return Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer %s" % token,
            "User-Agent": USER_AGENT,
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )


def _retry_wait(exc, attempt):
    try:
        headers = getattr(exc, "headers", None)
        after = headers.get("Retry-After") if headers else None
        if after is not None:
            return min(max(float(after), 0.0), 60.0)
    except (TypeError, ValueError, AttributeError):
        pass
    return min(2.0**attempt, 8.0)


def api_get_json(url, token):
    """GET one JSON document, with bounded retries on 429/5xx."""
    last = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            with urlopen(_request(url, token), timeout=API_TIMEOUT_S) as resp:
                return json.loads(resp.read().decode("utf-8")), resp.headers
        except HTTPError as exc:
            last = exc
            if exc.code not in RETRY_STATUS or attempt == MAX_RETRIES:
                break
            time.sleep(_retry_wait(exc, attempt))
        except URLError as exc:
            last = exc
            break
        except OSError as exc:
            last = exc
            break
    raise ReportError("GitHub API request failed: %s (%s)" % (url, last))


def link_next(headers, current_url):
    try:
        link = headers.get("Link")
    except AttributeError:
        return None
    if not link:
        return None
    match = LINK_NEXT_RE.search(link)
    if not match:
        return None
    candidate = urljoin(current_url, match.group(1))
    parsed = urlsplit(candidate)
    current = urlsplit(current_url)
    if (
        parsed.scheme != "https"
        or parsed.netloc.lower() != "api.github.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != current.path
        or parsed.fragment
    ):
        raise ReportError("refusing unsafe GitHub API pagination URL")
    return candidate


def api_get_all(url, token, key):
    """Collect bounded pages from the same GitHub API endpoint."""
    items = []
    seen = set()
    for _ in range(MAX_PAGES):
        if not url:
            return items
        if url in seen:
            raise ReportError("pagination repeated a URL; refusing an infinite loop")
        seen.add(url)
        data, headers = api_get_json(url, token)
        page = data.get(key) if isinstance(data, dict) else None
        if not isinstance(page, list):
            raise ReportError("unexpected API shape at %s: no %r list" % (url, key))
        items.extend(page)
        url = link_next(headers, url)
    if url:
        raise ReportError("pagination exceeded %d pages" % MAX_PAGES)
    return items

def api_url(path, params=None):
    if params:
        return API_BASE + path + "?" + urlencode(params)
    return API_BASE + path


def check_repo(repo):
    if not repo or not REPO_RE.match(repo):
        raise ReportError("invalid repo %r; want OWNER/NAME" % (repo,))
    return repo


def check_run_id(run_id):
    text = str(run_id)
    if not text.isdigit():
        raise ReportError("invalid run id %r; want digits" % (run_id,))
    return text


def check_day(day):
    if not day or not DAY_RE.match(day):
        raise ReportError("invalid day %r; want YYYY-MM-DD" % (day,))
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        raise ReportError("invalid day %r; want YYYY-MM-DD" % (day,))
    return day


def get_run(repo, run_id, token):
    data, _ = api_get_json(api_url("/repos/%s/actions/runs/%s" % (repo, run_id)), token)
    if not isinstance(data, dict) or str(data.get("id")) != run_id:
        raise ReportError("unexpected run shape for run %s" % run_id)
    return data

def get_jobs(repo, run_id, token):
    run_id = check_run_id(run_id)
    return api_get_all(
        api_url(
            "/repos/%s/actions/runs/%s/jobs" % (repo, run_id),
            {"filter": "all", "per_page": str(PER_PAGE)},
        ),
        token,
        "jobs",
    )

def get_artifacts(repo, run_id, token):
    return api_get_all(
        api_url(
            "/repos/%s/actions/runs/%s/artifacts" % (repo, run_id),
            {"per_page": str(PER_PAGE)},
        ),
        token,
        "artifacts",
    )


def get_day_runs(repo, workflow_id, branch, day, token):
    """Completed runs on branch whose UTC created_at date is day, each once."""
    check_day(day)
    if not str(workflow_id).isdigit():
        raise ReportError("invalid workflow id %r" % (workflow_id,))
    runs = api_get_all(
        api_url(
            "/repos/%s/actions/workflows/%s/runs" % (repo, workflow_id),
            {
                "branch": branch,
                "status": "completed",
                "per_page": str(PER_PAGE),
                "created": "%s..%s" % (day, day),
            },
        ),
        token,
        "workflow_runs",
    )
    seen = set()
    kept = []
    for run in runs:
        rid = run.get("id")
        if rid in seen:
            continue
        seen.add(rid)
        if run.get("status") != "completed":
            continue
        created = run.get("created_at")
        if not created or parse_ts(created).date().isoformat() != day:
            continue
        if run.get("head_branch") != branch:
            continue
        kept.append(run)
    return kept

def summarize_jobs(jobs):
    rows = []
    for job in jobs:
        rows.append(
            {
                "name": job.get("name", "?"),
                "attempt": job.get("run_attempt"),
                "conclusion": job.get("conclusion"),
                "seconds": job_seconds(job),
            }
        )
    total = sum(row["seconds"] for row in rows)
    return {
        "rows": rows,
        "jobs": len(rows),
        "skipped": sum(1 for row in rows if row["conclusion"] == "skipped"),
        "failed": sum(1 for row in rows if row["conclusion"] in FAILURE_CONCLUSIONS),
        "cancelled": sum(1 for row in rows if row["conclusion"] == "cancelled"),
        "seconds": total,
        "minutes": total / 60.0,
    }

def fmt_s(value):
    return "%.3f" % value


def fmt_m(value):
    return "%.2f" % value


def render_markdown(ctx):
    src = ctx["source"]
    summary = ctx["summary"]
    lines = []
    lines.append(
        "# CI job minutes — run %s (attempt %s, branch `%s`)"
        % (src.get("id"), ctx["attempt"], ctx["branch"])
    )
    lines.append("")
    lines.append(
        "Source run: [%s #%s](%s) · branch `%s` · sha `%s` · status `%s` / conclusion `%s` · created `%s` (UTC run-created date `%s`)."
        % (
            src.get("name", "CI"),
            src.get("run_number", "?"),
            src.get("html_url", ""),
            ctx["branch"],
            (src.get("head_sha") or "?")[:12],
            src.get("status", "?"),
            src.get("conclusion", "?"),
            src.get("created_at", "?"),
            ctx["day"],
        )
    )
    lines.append("")
    lines.append("## Source run: per-job exact durations (all attempts, `filter=all`)")
    lines.append("")
    lines.append("| job | attempt | conclusion | duration_s | duration_min |")
    lines.append("|---|---|---|---|---|")
    for row in summary["rows"]:
        attempt = row["attempt"] if row["attempt"] is not None else "–"
        lines.append(
            "| %s | %s | %s | %s | %s |"
            % (
                row["name"],
                attempt,
                row["conclusion"],
                fmt_s(row["seconds"]),
                fmt_m(row["seconds"] / 60.0),
            )
        )
    lines.append("")
    lines.append(
        "Total: %d job rows · %d skipped (0 s) · %d failed · %d cancelled · %s s = %s job-minutes."
        % (
            summary["jobs"],
            summary["skipped"],
            summary["failed"],
            summary["cancelled"],
            fmt_s(summary["seconds"]),
            fmt_m(summary["minutes"]),
        )
    )
    lines.append("")
    lines.append(
        "## UTC-day cohort — branch `%s`, created `%s` (completed runs, each run once, all job attempts)"
        % (ctx["branch"], ctx["day"])
    )
    lines.append("")
    lines.append("| run | conclusion | jobs | skipped | failed | cancelled | seconds | job_minutes |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for entry in ctx["cohort"]:
        run = entry["run"]
        sub = entry["summary"]
        lines.append(
            "| [%s](%s) | %s | %d | %d | %d | %d | %s | %s |"
            % (
                run.get("id"),
                run.get("html_url", ""),
                run.get("conclusion", "?"),
                sub["jobs"],
                sub["skipped"],
                sub["failed"],
                sub["cancelled"],
                fmt_s(sub["seconds"]),
                fmt_m(sub["minutes"]),
            )
        )
    lines.append("")
    lines.append(
        "Day totals: %d runs · %d job rows · %s s = %s job-minutes."
        % (
            len(ctx["cohort"]),
            ctx["day_jobs"],
            fmt_s(ctx["day_seconds"]),
            fmt_m(ctx["day_seconds"] / 60.0),
        )
    )
    lines.append("")
    lines.append("## Adjacent test-time evidence (listed only — never downloaded or executed)")
    lines.append("")
    if ctx["artifacts"]:
        for artifact in ctx["artifacts"]:
            lines.append(
                "- [`%s`](%s) (id %s · %s bytes · expired: %s)"
                % (
                    artifact.get("name", "?"),
                    artifact.get("html_url", ""),
                    artifact.get("id", "?"),
                    artifact.get("size_in_bytes", "?"),
                    "yes" if artifact.get("expired") else "no",
                )
            )
        lines.append("")
        lines.append(
            "These `qcg-test-time-*` artifacts sit beside the [source run](%s); the reporter lists name/ID only and downloads nothing."
            % src.get("html_url", "")
        )
    else:
        lines.append("No `%s*` artifacts found on the [source run](%s)." % (ARTIFACT_PREFIX, src.get("html_url", "")))
    lines.append("")
    lines.append("## Method")
    lines.append("")
    lines.append("- Exact `started_at` → `completed_at` per job row across all attempts (`filter=all`); skipped / never-started jobs count 0 s; any other missing or inconsistent timestamp fails the report instead of guessing.")
    lines.append("- Day cohort: completed runs on the same branch whose UTC `created_at` date equals `%s`; each run counted once; all job attempts included." % ctx["day"])
    lines.append("- Token read only from `GH_TOKEN`/`GITHUB_TOKEN`; never printed. API parameters URL-encoded; list endpoints paginated via `Link: rel=\"next\"` with bounded retries on 429/5xx.")
    lines.append("- Generated at %s." % ctx["generated_at"])
    lines.append("")
    return "\n".join(lines)


def generate(repo, run_id, token, attempt=None, branch=None, day=None, workflow=None):
    repo = check_repo(repo)
    run_id = check_run_id(run_id)
    source = get_run(repo, run_id, token)
    if source.get("status") != "completed":
        raise ReportError("source run %s is not completed (status %r)" % (run_id, source.get("status")))
    if source.get("name") != "CI":
        raise ReportError("source run %s is not the CI workflow" % run_id)
    source_branch = source.get("head_branch")
    if not source_branch:
        raise ReportError("source run has no head_branch")
    if branch is not None and branch != source_branch:
        raise ReportError("requested branch %r does not match source run branch %r" % (branch, source_branch))
    resolved_branch = source_branch
    created_at = source.get("created_at")
    if not created_at:
        raise ReportError("source run has no created_at")
    resolved_day = check_day(day or parse_ts(created_at).date().isoformat())
    if parse_ts(created_at).date().isoformat() != resolved_day:
        raise ReportError("requested UTC day does not match source run created_at")
    workflow_id = workflow or source.get("workflow_id")
    if workflow_id is None or not str(workflow_id).isdigit():
        raise ReportError("cannot resolve workflow id; pass --workflow")
    resolved_attempt = attempt if attempt is not None else source.get("run_attempt", "unknown")
    if resolved_attempt is None:
        resolved_attempt = "unknown"
    summary = summarize_jobs(get_jobs(repo, run_id, token))
    source["html_url"] = "https://github.com/%s/actions/runs/%s" % (repo, run_id)
    artifacts = [
        item
        for item in get_artifacts(repo, run_id, token)
        if str(item.get("name", "")).startswith(ARTIFACT_PREFIX)
    ]
    for artifact in artifacts:
        artifact_id = artifact.get("id")
        if not str(artifact_id).isdigit():
            raise ReportError("test-time artifact has invalid ID %r" % (artifact_id,))
        artifact["html_url"] = "https://github.com/%s/actions/runs/%s/artifacts/%s" % (repo, run_id, artifact_id)
    cohort = []
    for run in get_day_runs(repo, workflow_id, resolved_branch, resolved_day, token):
        cohort_run_id = check_run_id(run.get("id"))
        run["html_url"] = "https://github.com/%s/actions/runs/%s" % (repo, cohort_run_id)
        if cohort_run_id == run_id:
            run_summary = summary
        else:
            run_summary = summarize_jobs(get_jobs(repo, cohort_run_id, token))
        cohort.append({"run": run, "summary": run_summary})
    cohort.sort(key=lambda entry: int(entry["run"].get("id")))
    return render_markdown(
        {
            "source": source,
            "attempt": resolved_attempt,
            "branch": resolved_branch,
            "day": resolved_day,
            "summary": summary,
            "artifacts": artifacts,
            "cohort": cohort,
            "day_jobs": sum(entry["summary"]["jobs"] for entry in cohort),
            "day_seconds": sum(entry["summary"]["seconds"] for entry in cohort),
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    )

def _opt_int(text, label):
    if text is None or str(text).strip() == "":
        return None
    try:
        return int(str(text).strip())
    except ValueError:
        raise ReportError("invalid %s %r; want an integer" % (label, text))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Minimal CI job-minutes reporter (stdlib only).")
    parser.add_argument("--repo", default=os.environ.get("QCG_REPO") or os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--run-id", default=os.environ.get("QCG_RUN_ID"))
    parser.add_argument("--attempt", default=os.environ.get("QCG_RUN_ATTEMPT"))
    parser.add_argument("--branch", default=os.environ.get("QCG_BRANCH"))
    parser.add_argument("--day", default=os.environ.get("QCG_DAY"))
    parser.add_argument("--workflow", default=os.environ.get("QCG_WORKFLOW"))
    parser.add_argument("--out", default=os.environ.get("QCG_OUT"))
    args = parser.parse_args(argv)
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        print(
            "error: missing GH_TOKEN (or GITHUB_TOKEN); the token comes only from the environment",
            file=sys.stderr,
        )
        return 2
    try:
        text = generate(
            args.repo,
            args.run_id,
            token,
            attempt=_opt_int(args.attempt, "attempt"),
            branch=args.branch or None,
            day=args.day or None,
            workflow=args.workflow or None,
        )
    except ReportError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 1
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
