#!/usr/bin/env node
// Generates the TypeScript and Python SDKs from docs/openapi.json.
//
// The operation names are pinned in OPERATION_NAMES so generated method
// names stay stable and reviewable; a spec operation without a pinned name
// fails generation instead of drifting into an accidental name. Every
// generated file is checked in and CI re-runs this script to prove it is
// current (scripts/check-sdk.sh).

import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const spec = JSON.parse(readFileSync(resolve(root, "docs/openapi.json"), "utf8"));

const OPERATION_NAMES = {
  "GET /healthz": "health",
  "GET /metrics": "metrics",
  "GET /api/openapi.json": "openapi",
  "GET /api/generators": "listGenerators",
  "GET /api/generators/{id}": "getGenerator",
  "GET /api/generators/{id}/assets/{path}": "readGeneratorAsset",
  "GET /api/llm/catalog": "llmCatalog",
  "GET /api/mcp/servers": "listMcpServers",
  "POST /api/mcp/servers/{id}/authorization": "startMcpAuthorization",
  "DELETE /api/mcp/servers/{id}/authorization": "clearMcpAuthorization",
  "DELETE /api/mcp/servers/{id}/authorization/pending": "cancelPendingMcpAuthorization",
  "GET /api/mcp/oauth/callback": "completeMcpAuthorization",
  "GET /api/runs": "listRuns",
  "POST /api/runs": "startRun",
  "GET /api/runs/{id}": "getRun",
  "DELETE /api/runs/{id}": "deleteRun",
  "POST /api/runs/{id}": "cancelRunAlias",
  "POST /api/runs/{id}:cancel": "cancelRun",
  "POST /api/runs/{id}/fork": "forkRun",
  "GET /api/runs/{id}/events": "runEvents",
  "GET /api/runs/{id}/artifacts": "listArtifacts",
  "GET /api/runs/{id}/artifacts.zip": "downloadArtifactsZip",
  "GET /api/runs/{id}/artifacts/{path}": "readArtifact",
  "GET /api/runs/{id}/bundle": "downloadRunBundle",
  "GET /api/runs/{id}/journal": "readRunJournal",
  "GET /api/runs/{id}/metrics": "readRunMetrics",
  "PUT /api/runs/{id}/questions/{qid}": "answerQuestion",
  "PUT /api/runs/{id}/confirmations/{cid}": "confirmRun",
};

const METHODS = ["get", "post", "put", "delete"];

function operations() {
  const found = [];
  for (const [path, entry] of Object.entries(spec.paths)) {
    for (const method of METHODS) {
      const operation = entry[method];
      if (!operation) continue;
      const key = `${method.toUpperCase()} ${path}`;
      const name = OPERATION_NAMES[key];
      if (!name) {
        throw new Error(`spec operation has no pinned SDK name: ${key}`);
      }
      found.push({ path, method, operation, name });
    }
  }
  const pinned = new Set(Object.keys(OPERATION_NAMES));
  for (const key of pinned) {
    if (!found.some(({ path, method }) => `${method.toUpperCase()} ${path}` === key)) {
      throw new Error(`pinned SDK operation is missing from the spec: ${key}`);
    }
  }
  return found;
}

function pathParams(path) {
  return [...path.matchAll(/\{([^}]+)\}/g)].map((match) => match[1]);
}

function jsonSchema(operation, section) {
  return operation[section]?.content?.["application/json"];
}

function responseType(entry) {
  const responses = entry.operation.responses ?? {};
  const codes = Object.keys(responses)
    .filter((code) => /^2\d\d$/.test(code))
    .sort((left, right) => Number(left) - Number(right));
  for (const code of codes) {
    const content = responses[code]?.content?.["application/json"];
    if (content) {
      return `paths[${JSON.stringify(entry.path)}][${JSON.stringify(entry.method)}]["responses"][${code}]["content"]["application/json"]`;
    }
  }
  return "void";
}

function tsOperation(entry) {
  const params = pathParams(entry.path);
  const args = params.map((name) => `${name}: string`);
  const hasQuery = Boolean(entry.operation.parameters?.some((parameter) => parameter.in === "query"));
  if (hasQuery) {
    args.push(
      `query?: paths[${JSON.stringify(entry.path)}][${JSON.stringify(entry.method)}]["parameters"]["query"]`,
    );
  }
  const bodySchema = jsonSchema(entry.operation, "requestBody");
  if (bodySchema) {
    args.push(
      `body: paths[${JSON.stringify(entry.path)}][${JSON.stringify(entry.method)}]["requestBody"]["content"]["application/json"]`,
    );
  }
  const template = entry.path.replace(/\{([^}]+)\}/g, "${encodeURIComponent($1)}");
  const call = [];
  if (hasQuery) call.push("query");
  if (bodySchema) call.push("body");
  const options = call.length > 0 ? `{ ${call.join(", ")} }` : "";
  return `  async ${entry.name}(${args.join(", ")}): Promise<${responseType(entry)}> {
    return this.request("${entry.method.toUpperCase()}", \`${template}\`${options ? `, ${options}` : ""});
  }`;
}

function snakeCase(name) {
  return name.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
}

function pyOperation(entry) {
  const params = pathParams(entry.path);
  const args = params.map((name) => snakeCase(name));
  const hasQuery = Boolean(entry.operation.parameters?.some((parameter) => parameter.in === "query"));
  if (hasQuery) args.push("query=None");
  const hasBody = Boolean(jsonSchema(entry.operation, "requestBody"));
  if (hasBody) args.push("body=None");
  const template = entry.path.replace(/\{([^}]+)\}/g, "{$1}");
  const path = params.length > 0 ? `f"${template}"` : JSON.stringify(entry.path);
  const call = [];
  if (hasQuery) call.push("query=query");
  if (hasBody) call.push("body=body");
  const signature = ["self", ...args].join(", ");
  return `    def ${snakeCase(entry.name)}(${signature}):
        return self._request("${entry.method.toUpperCase()}", ${path}${call.length ? ", " + call.join(", ") : ""})`;
}

const pinned = operations();

const ts = `// Generated by scripts/generate-sdk.mjs from docs/openapi.json; do not edit.
import type { paths } from "./types";

export type QcgClientOptions = {
  /** Origin for the API, for example \`http://127.0.0.1:8080\`. Defaults to same-origin. */
  baseUrl?: string;
  /** Bearer token for an authenticated instance; sent only as a header. */
  token?: string;
  /** Fetch implementation override (tests, Node runtimes). */
  fetch?: typeof fetch;
};

export class QcgError extends Error {
  constructor(
    public readonly status: number,
    public readonly problem: unknown,
    message: string,
  ) {
    super(message);
    this.name = "QcgError";
  }
}

export class QcgClient {
  private readonly baseUrl: string;
  private token?: string;
  private readonly fetchImpl: typeof fetch;

  constructor(options: QcgClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "").replace(/\\/$/, "");
    this.token = options.token;
    this.fetchImpl = options.fetch ?? globalThis.fetch;
  }

  /** Replaces the bearer token for subsequent requests. */
  setToken(token: string | undefined): void {
    this.token = token;
  }

  private url(path: string, query?: Record<string, unknown>): string {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(query ?? {})) {
      if (value !== undefined && value !== null) params.set(key, String(value));
    }
    const suffix = params.size > 0 ? \`?\${params.toString()}\` : "";
    return \`\${this.baseUrl}\${path}\${suffix}\`;
  }

  private headers(extra?: Record<string, string>): Record<string, string> {
    return {
      ...(this.token ? { authorization: \`Bearer \${this.token}\` } : {}),
      ...extra,
    };
  }

  async request<T>(
    method: string,
    path: string,
    options: { query?: Record<string, unknown>; body?: unknown } = {},
  ): Promise<T> {
    const headers: Record<string, string> = this.headers(
      options.body === undefined ? undefined : { "content-type": "application/json" },
    );
    const response = await this.fetchImpl(this.url(path, options.query), {
      method,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    });
    if (!response.ok) {
      const problem = await readProblem(response);
      throw new QcgError(response.status, problem, problemMessage(problem) || response.statusText);
    }
    if (response.status === 204) return undefined as T;
    return (await response.json()) as T;
  }

  private async text(path: string): Promise<string> {
    const response = await this.fetchImpl(this.url(path), { headers: this.headers() });
    if (!response.ok) {
      const problem = await readProblem(response);
      throw new QcgError(response.status, problem, problemMessage(problem) || response.statusText);
    }
    return response.text();
  }

  /** Reads run events as server-sent event JSON payloads, resuming from an id. */
  async *streamRunEvents(runId: string, lastEventId?: number): AsyncGenerator<unknown> {
    const response = await this.fetchImpl(
      this.url(\`/api/runs/\${encodeURIComponent(runId)}/events\`),
      {
        headers: this.headers(
          lastEventId === undefined ? undefined : { "last-event-id": String(lastEventId) },
        ),
      },
    );
    if (!response.ok || !response.body) {
      const problem = await readProblem(response);
      throw new QcgError(response.status, problem, problemMessage(problem) || response.statusText);
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      buffer += decoder.decode(value, { stream: true });
      let boundary = buffer.indexOf("\\n\\n");
      while (boundary >= 0) {
        const frame = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const data = frame
          .split("\\n")
          .filter((line) => line.startsWith("data:"))
          .map((line) => line.slice(5).trimStart())
          .join("");
        if (data) yield JSON.parse(data) as unknown;
        boundary = buffer.indexOf("\\n\\n");
      }
    }
  }

${pinned.map(tsOperation).join("\n\n")}
}

async function readProblem(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return { detail: text };
  }
}

function problemMessage(problem: unknown): string {
  if (problem && typeof problem === "object" && "detail" in problem) {
    const detail = (problem as { detail?: unknown }).detail;
    if (typeof detail === "string") return detail;
  }
  return "";
}
`;

const py = `"""Generated by scripts/generate-sdk.mjs from docs/openapi.json; do not edit."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Iterator
from urllib.parse import quote, urlencode


class QcgError(Exception):
    """Raised for non-2xx responses; carries the status and problem body."""

    def __init__(self, status: int, problem: Any, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.problem = problem


class QcgClient:
    """Dependency-free client for the qcg HTTP API.

    The bearer token is sent only as an Authorization header. Event streams
    are read incrementally from the response body.
    """

    def __init__(self, base_url: str = "", token: str | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def set_token(self, token: str | None) -> None:
        self.token = token

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = dict(extra or {})
        if self.token:
            headers["authorization"] = f"Bearer {self.token}"
        return headers

    def _url(self, path: str, query: dict[str, Any] | None = None) -> str:
        if not query:
            return f"{self.base_url}{path}"
        cleaned = {key: value for key, value in query.items() if value is not None}
        return f"{self.base_url}{path}?{urlencode(cleaned)}"

    def _request(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        data = None if body is None else json.dumps(body).encode("utf-8")
        extra = dict(headers or {})
        if body is not None:
            extra["content-type"] = "application/json"
        request = urllib.request.Request(
            self._url(path, query), data=data, method=method, headers=self._headers(extra)
        )
        try:
            with urllib.request.urlopen(request) as response:
                payload = response.read()
                if response.status == 204 or not payload:
                    return None
                return json.loads(payload.decode("utf-8"))
        except urllib.error.HTTPError as error:
            raw = error.read().decode("utf-8", errors="replace")
            try:
                problem = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                problem = {"detail": raw}
            detail = problem.get("detail") if isinstance(problem, dict) else None
            raise QcgError(error.code, problem, detail or error.reason or str(error.code)) from error

    def stream_run_events(self, run_id: str, last_event_id: int | None = None) -> Iterator[Any]:
        """Yields parsed SSE event payloads until the stream ends."""
        headers = self._headers()
        if last_event_id is not None:
            headers["last-event-id"] = str(last_event_id)
        request = urllib.request.Request(
            self._url(f"/api/runs/{quote(str(run_id), safe='')}/events"), headers=headers
        )
        with urllib.request.urlopen(request) as response:
            buffer = ""
            while True:
                chunk = response.read(4096)
                if not chunk:
                    return
                buffer += chunk.decode("utf-8", errors="replace")
                while "\\n\\n" in buffer:
                    frame, buffer = buffer.split("\\n\\n", 1)
                    data = "".join(
                        line[5:].lstrip() for line in frame.split("\\n") if line.startswith("data:")
                    )
                    if data:
                        yield json.loads(data)

${pinned.map(pyOperation).join("\n\n")}
`;

mkdirSync(resolve(root, "clients/ts"), { recursive: true });
mkdirSync(resolve(root, "clients/python"), { recursive: true });
execFileSync(
  // Windows spawns .cmd shims only through a shell.
  "npx",
  ["openapi-typescript", "../../docs/openapi.json", "-o", "../../clients/ts/types.ts"],
  {
    cwd: resolve(root, "frontend/generator"),
    stdio: "inherit",
    shell: process.platform === "win32",
  },
);
writeFileSync(resolve(root, "clients/ts/client.ts"), ts);
writeFileSync(resolve(root, "clients/python/qcg_client.py"), py);
writeFileSync(
  resolve(root, "clients/README.md"),
  `# qcg clients

Generated by \`scripts/generate-sdk.mjs\` from \`docs/openapi.json\`.

- \`ts/\` — dependency-free TypeScript client and generated OpenAPI types.
- \`python/qcg_client.py\` — dependency-free Python client (stdlib only).

Regenerate with \`node scripts/generate-sdk.mjs\`; \`scripts/check-sdk.sh\`
fails CI when the checked-in clients are stale. Both clients send the bearer
token only as an Authorization header and never place it in a URL.
`,
);
console.log(`generated ${pinned.length} SDK operations`);
