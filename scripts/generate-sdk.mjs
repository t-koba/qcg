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

function headerParams(operation) {
  return (operation.parameters ?? []).filter((p) => p.in === "header").map((p) => p.name);
}

// A path template parameter named `path` is a multi-segment wildcard
// (generator assets, run artifacts): it may contain slashes, so it is
// encoded segment by segment. Every other parameter is a single segment.
function isWildcardParam(name) {
  return name === "path";
}

// Success response for an operation: the lowest 2xx code plus its media
// types. Used to pick the runtime reader (JSON/text/bytes/stream/empty).
function successResponse(entry) {
  const responses = entry.operation.responses ?? {};
  const codes = Object.keys(responses)
    .filter((code) => /^2\d\d$/.test(code))
    .sort((left, right) => Number(left) - Number(right));
  for (const code of codes) {
    const content = responses[code]?.content ?? {};
    return { code, mediaTypes: Object.keys(content) };
  }
  return { code: codes[0] ?? "200", mediaTypes: [] };
}

function responseKind(entry) {
  const { code, mediaTypes } = successResponse(entry);
  if (code === "204" || mediaTypes.length === 0) return "empty";
  const primary = mediaTypes[0];
  if (primary === "application/json") return "json";
  if (primary === "application/x-ndjson") return "ndjson";
  if (primary === "text/event-stream") return "text";
  if (primary.startsWith("text/")) return "text";
  return "bytes";
}

function jsonTypeRef(entry) {
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

function tsReturnType(entry) {
  const kind = responseKind(entry);
  switch (kind) {
    case "json":
      return `${jsonTypeRef(entry)} | null`;
    case "ndjson":
      return `unknown[] | null`;
    case "text":
      return `string | null`;
    case "bytes":
      return `Uint8Array | null`;
    case "empty":
      return "void";
  }
}

function tsReader(entry) {
  const kind = responseKind(entry);
  switch (kind) {
    case "json":
      return "this.requestJson";
    case "ndjson":
      return "this.requestNdjson";
    case "text":
      return "this.requestText";
    case "bytes":
      return "this.requestBytes";
    case "empty":
      return "this.requestEmpty";
  }
}

function tsPathTemplate(path) {
  return path.replace(/\{([^}]+)\}/g, (_, name) =>
    isWildcardParam(name) ? "${encodePathParam(" + name + ")}" : "${encodeURIComponent(" + name + ")}",
  );
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
  const headers = headerParams(entry.operation);
  const hasIdempotency = headers.includes("Idempotency-Key");
  const needsOptions = hasQuery || bodySchema || headers.length > 0;
  if (needsOptions) {
    const parts = [];
    if (hasQuery) parts.push("query?: Record<string, unknown>");
    if (bodySchema) parts.push("body?: unknown");
    parts.push("headers?: Record<string, string>");
    if (hasIdempotency) parts.push("idempotencyKey?: string");
    args.push(`options?: { ${parts.join("; ")} }`);
  }
  const call = [];
  if (hasQuery) {
    call.push(hasIdempotency || headers.length > 0 || bodySchema ? "query: query ?? options?.query" : "query");
  }
  if (bodySchema) {
    call.push(headers.length > 0 || hasQuery ? "body: body ?? options?.body" : "body");
  }
  if (headers.length > 0 || (hasQuery && needsOptions) || (bodySchema && needsOptions)) {
    const headerExpr = hasIdempotency
      ? "this.withIdempotency(options?.headers, options?.idempotencyKey)"
      : "options?.headers";
    call.push(`headers: ${headerExpr}`);
  }
  const options = call.length > 0 ? `{ ${call.join(", ")} }` : "";
  const reader = tsReader(entry);
  const ret = tsReturnType(entry);
  const callExpr =
    reader === "this.requestJson"
      ? `${reader}<${ret}>("${entry.method.toUpperCase()}", \`${tsPathTemplate(entry.path)}\`${options ? `, ${options}` : ""})`
      : `${reader}("${entry.method.toUpperCase()}", \`${tsPathTemplate(entry.path)}\`${options ? `, ${options}` : ""})`;
  // Query/body shorthand: when the caller passes query/body positionally,
  // forward them; `options` carries headers and the idempotency key.
  return `  async ${entry.name}(${args.join(", ")}): Promise<${ret}> {
    return ${callExpr};
  }`;
}

function snakeCase(name) {
  return name.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
}

function pyResponseKind(entry) {
  return responseKind(entry);
}

function pyOperation(entry) {
  const params = pathParams(entry.path);
  const args = params.map((name) => snakeCase(name));
  const hasQuery = Boolean(entry.operation.parameters?.some((parameter) => parameter.in === "query"));
  if (hasQuery) args.push("query=None");
  const hasBody = Boolean(jsonSchema(entry.operation, "requestBody"));
  if (hasBody) args.push("body=None");
  const headers = headerParams(entry.operation);
  if (headers.length > 0) args.push("headers=None");
  if (headers.includes("Idempotency-Key")) args.push("idempotency_key=None");
  const kind = pyResponseKind(entry);
  const reader =
    kind === "json"
      ? "self._request_json"
      : kind === "ndjson"
        ? "self._request_ndjson"
        : kind === "text"
          ? "self._request_text"
          : kind === "bytes"
            ? "self._request_bytes"
            : "self._request_empty";
  const pathExpr = (name) =>
    isWildcardParam(name)
      ? `{_encode_path(${snakeCase(name)})}`
      : `{_encode_segment(${snakeCase(name)})}`;
  const template = entry.path.replace(/\{([^}]+)\}/g, (_, name) => pathExpr(name));
  const path = params.length > 0 ? `f"${template}"` : JSON.stringify(entry.path);
  const call = [];
  if (hasQuery) call.push("query=query");
  if (hasBody) call.push("body=body");
  if (headers.length > 0) call.push("headers=headers");
  if (headers.includes("Idempotency-Key")) call.push("idempotency_key=idempotency_key");
  const signature = ["self", ...args].join(", ");
  return `    def ${snakeCase(entry.name)}(${signature}):
        return ${reader}("${entry.method.toUpperCase()}", ${path}${call.length ? ", " + call.join(", ") : ""})`;
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
  /** Redirect policy for authenticated requests: same-origin only by default. */
  redirect?: "same-origin" | "follow";
};

export type RequestOptions = {
  query?: Record<string, unknown>;
  body?: unknown;
  headers?: Record<string, string>;
  idempotencyKey?: string;
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

/** Encodes one path segment (no slashes survive). */
function encodeSegment(value: string): string {
  return encodeURIComponent(value);
}

/** Encodes a multi-segment wildcard path, preserving slashes. */
function encodePathParam(value: string): string {
  return value
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

export class QcgClient {
  private readonly baseUrl: string;
  private token?: string;
  private readonly fetchImpl: typeof fetch;
  private readonly redirectPolicy: "same-origin" | "follow";

  constructor(options: QcgClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "").replace(/\\/$/, "");
    this.token = options.token;
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    this.redirectPolicy = options.redirect ?? "same-origin";
  }

  /** Replaces the bearer token for subsequent requests. */
  setToken(token: string | undefined): void {
    this.token = token;
  }

  private url(path: string, query?: Record<string, unknown>): string {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(query ?? {})) {
      if (value === undefined || value === null) continue;
      if (typeof value === "boolean") params.set(key, value ? "true" : "false");
      else params.set(key, String(value));
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

  private withIdempotency(
    extra?: Record<string, string>,
    idempotencyKey?: string,
  ): Record<string, string> | undefined {
    if (idempotencyKey === undefined) return extra;
    return { ...extra, "idempotency-key": idempotencyKey };
  }

  private sameOrigin(url: string): boolean {
    if (!this.baseUrl) return true;
    try {
      const base = new URL(this.baseUrl);
      const target = new URL(url, this.baseUrl);
      return base.origin === target.origin;
    } catch {
      return false;
    }
  }

  private async checkedFetch(url: string, init: RequestInit): Promise<Response> {
    // F04: never forward the bearer cross-origin or over a downgrade.
    // The token lives only in memory; cross-origin redirects are followed
    // without it (same-origin policy by default).
    const response = await this.fetchImpl(url, { ...init, redirect: "manual" });
    const location = response.headers.get("location");
    if (
      location !== null &&
      [301, 302, 303, 307, 308].includes(response.status)
    ) {
      const next = new URL(location, url);
      const current = new URL(url, this.baseUrl || undefined);
      const crossOrigin = next.origin !== current.origin;
      const downgrade = current.protocol === "https:" && next.protocol !== "https:";
      const initHeaders = new Headers(init.headers);
      if ((crossOrigin || downgrade) && initHeaders.has("authorization")) {
        initHeaders.delete("authorization");
      }
      if (crossOrigin && this.redirectPolicy === "same-origin") {
        // Follow same-origin redirects with the (possibly stripped)
        // headers; cross-origin is followed once without credentials.
        // Further hops re-enter this method via recursion below.
      }
      const nextInit: RequestInit = { ...init, headers: initHeaders };
      // 303 always becomes GET (except HEAD); 301/302 POST becomes GET.
      if (
        response.status === 303 ||
        ((response.status === 301 || response.status === 302) &&
          (init.method ?? "GET") === "POST")
      ) {
        nextInit.method = "GET";
        nextInit.body = undefined;
        initHeaders.delete("content-type");
        initHeaders.delete("content-length");
      }
      return this.checkedFetch(next.href, nextInit);
    }
    return response;
  }

  private async readProblem(response: Response): Promise<unknown> {
    const text = await response.text();
    if (!text) return {};
    try {
      return JSON.parse(text) as unknown;
    } catch {
      return { detail: text };
    }
  }

  private async throwForStatus(response: Response): Promise<never> {
    const problem = await this.readProblem(response);
    throw new QcgError(response.status, problem, problemMessage(problem) || response.statusText);
  }

  async request<T>(
    method: string,
    path: string,
    options: { query?: Record<string, unknown>; body?: unknown } = {},
  ): Promise<T> {
    return this.requestJson(method, path, options);
  }

  private async requestJson<T>(
    method: string,
    path: string,
    options: RequestOptions = {},
  ): Promise<T> {
    const headers = this.headers(
      options.body === undefined ? options.headers : { "content-type": "application/json", ...options.headers },
    );
    const response = await this.checkedFetch(this.url(path, options.query), {
      method,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    });
    if (response.status === 304) return null as T;
    if (!response.ok) await this.throwForStatus(response);
    if (response.status === 204) return undefined as T;
    const text = await response.text();
    if (!text) return undefined as T;
    return JSON.parse(text) as T;
  }

  private async requestText(
    method: string,
    path: string,
    options: RequestOptions = {},
  ): Promise<string | null> {
    const response = await this.checkedFetch(this.url(path, options.query), {
      method,
      headers: this.headers(options.headers),
    });
    if (response.status === 304) return null;
    if (!response.ok) await this.throwForStatus(response);
    return response.text();
  }

  private async requestBytes(
    method: string,
    path: string,
    options: RequestOptions = {},
  ): Promise<Uint8Array | null> {
    const response = await this.checkedFetch(this.url(path, options.query), {
      method,
      headers: this.headers(options.headers),
    });
    if (response.status === 304) return null;
    if (!response.ok) await this.throwForStatus(response);
    return new Uint8Array(await response.arrayBuffer());
  }

  private async requestNdjson(
    method: string,
    path: string,
    options: RequestOptions = {},
  ): Promise<unknown[] | null> {
    const text = await this.requestText(method, path, options);
    if (text === null) return null;
    return text
      .split("\\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .map((line) => JSON.parse(line) as unknown);
  }

  private async requestEmpty(method: string, path: string, options: RequestOptions = {}): Promise<void> {
    const headers = this.headers(
      options.body === undefined ? options.headers : { "content-type": "application/json", ...options.headers },
    );
    const response = await this.checkedFetch(this.url(path, options.query), {
      method,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    });
    if (!response.ok) await this.throwForStatus(response);
  }

  private async text(path: string): Promise<string> {
    const result = await this.requestText("GET", path, {});
    if (result === null) return "";
    return result;
  }

  /** Reads run events as server-sent event JSON payloads, resuming from an id. */
  async *streamRunEvents(runId: string, lastEventId?: number): AsyncGenerator<unknown> {
    const response = await this.checkedFetch(
      this.url(\`/api/runs/\${encodeURIComponent(runId)}/events\`),
      {
        headers: this.headers(
          lastEventId === undefined ? undefined : { "last-event-id": String(lastEventId) },
        ),
      },
    );
    if (!response.ok || !response.body) {
      await this.throwForStatus(response);
    }
    const reader = response.body!.getReader();
    try {
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          buffer += decoder.decode();
          if (buffer.trim().length > 0) {
            const payload = parseSseFrame(buffer);
            if (payload !== undefined) yield payload;
          }
          return;
        }
        buffer += decoder.decode(value, { stream: true });
        // Normalize CRLF/CR per the SSE spec before framing.
        buffer = buffer.replace(/\\r\\n/g, "\\n").replace(/\\r/g, "\\n");
        let boundary = buffer.indexOf("\\n\\n");
        while (boundary >= 0) {
          const frame = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const payload = parseSseFrame(frame);
          if (payload !== undefined) yield payload;
          boundary = buffer.indexOf("\\n\\n");
        }
      }
    } finally {
      try {
        await reader.cancel();
      } catch {
        // Cancel is best-effort: the connection may already be closed.
      }
      reader.releaseLock();
    }
  }

${pinned.map(tsOperation).join("\n\n")}
}

function parseSseFrame(frame: string): unknown | undefined {
  const data: string[] = [];
  for (const line of frame.split("\\n")) {
    if (line.startsWith(":")) continue;
    if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
  }
  if (data.length === 0) return undefined;
  const text = data.join("\\n");
  if (!text) return undefined;
  return JSON.parse(text) as unknown;
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

import codecs
import contextlib
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Iterator
from urllib.parse import quote, urlencode


class QcgError(Exception):
    """Raised for non-2xx responses; carries the status and problem body."""

    def __init__(self, status: int, problem: Any, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.problem = problem


def _encode_segment(value: Any) -> str:
    """Encodes one path segment (no slashes survive)."""
    return quote(str(value), safe="")


def _encode_path(value: Any) -> str:
    """Encodes a multi-segment wildcard path, preserving slashes."""
    return "/".join(quote(segment, safe="") for segment in str(value).split("/"))


def _normalize_query(query: dict[str, Any] | None) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in (query or {}).items():
        if value is None:
            continue
        if isinstance(value, bool):
            cleaned[key] = "true" if value else "false"
        elif isinstance(value, (list, tuple)):
            cleaned[key] = [
                "true" if item is True else "false" if item is False else item
                for item in value
            ]
        else:
            cleaned[key] = value
    return cleaned


class _NoCrossOriginAuthRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Strips Authorization on cross-origin redirects and downgrades (F04)."""

    def _strip_if_needed(self, req: urllib.request.Request, location: str) -> None:
        if req.get_header("Authorization") is None and req.get_header("authorization") is None:
            return
        try:
            current = urllib.parse.urlsplit(req.full_url)
            nxt = urllib.parse.urlsplit(urllib.parse.urljoin(req.full_url, location))
        except ValueError:
            req.remove_header("Authorization")
            req.remove_header("authorization")
            return
        current_origin = (current.scheme.lower(), current.hostname, current.port)
        next_origin = (nxt.scheme.lower(), nxt.hostname, nxt.port)
        downgrade = current.scheme.lower() == "https" and nxt.scheme.lower() != "https"
        if current_origin != next_origin or downgrade:
            req.remove_header("Authorization")
            req.remove_header("authorization")

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        self._strip_if_needed(req, newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)

    def http_error_308(self, req, fp, code, msg, headers):  # type: ignore[override]
        location = headers.get("Location") or headers.get("location")
        if location is not None:
            self._strip_if_needed(req, location)
        return super().http_error_307(req, fp, code, msg, headers)


def _opener() -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(_NoCrossOriginAuthRedirectHandler)


class QcgClient:
    """Dependency-free client for the qcg HTTP API.

    The bearer token is sent only as an Authorization header and is never
    forwarded cross-origin or over a TLS downgrade. Event streams are read
    incrementally from the response body.
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

    def _merge_headers(
        self,
        headers: dict[str, str] | None,
        idempotency_key: str | None,
    ) -> dict[str, str] | None:
        if idempotency_key is None:
            return headers
        merged = dict(headers or {})
        merged["idempotency-key"] = idempotency_key
        return merged

    def _url(self, path: str, query: dict[str, Any] | None = None) -> str:
        cleaned = _normalize_query(query)
        if not cleaned:
            return f"{self.base_url}{path}"
        return f"{self.base_url}{path}?{urlencode(cleaned, doseq=True)}"

    def _open(self, request: urllib.request.Request):
        return _opener().open(request)

    def _problem_from_error(self, error: urllib.error.HTTPError) -> tuple[Any, str]:
        if error.code == 304:
            return None, ""
        try:
            raw = error.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        try:
            problem = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            problem = {"detail": raw}
        detail = problem.get("detail") if isinstance(problem, dict) else None
        return problem, detail or error.reason or str(error.code)

    def _request_bytes_raw(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        extra = dict(headers or {})
        if body is not None:
            extra["content-type"] = "application/json"
        request = urllib.request.Request(
            self._url(path, query), data=data, method=method, headers=self._headers(extra)
        )
        try:
            with self._open(request) as response:
                payload = response.read()
                response_headers = {key.lower(): value for key, value in response.headers.items()}
                return response.status, response_headers, payload
        except urllib.error.HTTPError as error:
            if error.code == 304:
                return 304, {}, b""
            problem, detail = self._problem_from_error(error)
            raise QcgError(error.code, problem, detail) from error

    def _request_json(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        body: Any = None,
        headers: dict[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        headers = self._merge_headers(headers, idempotency_key)
        status, _, payload = self._request_bytes_raw(method, path, query, body, headers)
        if status == 204 or status == 304 or not payload:
            return None
        return json.loads(payload.decode("utf-8"))

    def _request_text(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str | None:
        status, _, payload = self._request_bytes_raw(method, path, query, None, headers)
        if status == 304:
            return None
        return payload.decode("utf-8")

    def _request_bytes(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> bytes | None:
        status, _, payload = self._request_bytes_raw(method, path, query, None, headers)
        if status == 304:
            return None
        return payload

    def _request_ndjson(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> list[Any] | None:
        text = self._request_text(method, path, query, headers)
        if text is None:
            return None
        events: list[Any] = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                events.append(json.loads(line))
        return events

    def _request_empty(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        body: Any = None,
        headers: dict[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        headers = self._merge_headers(headers, idempotency_key)
        self._request_bytes_raw(method, path, query, body, headers)
        return None

    # Back-compat JSON reader used by older call sites.
    def _request(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        return self._request_json(method, path, query, body, headers)

    def stream_run_events(self, run_id: str, last_event_id: int | None = None) -> Iterator[Any]:
        """Yields parsed SSE event payloads until the stream ends."""
        headers = self._headers()
        if last_event_id is not None:
            headers["last-event-id"] = str(last_event_id)
        request = urllib.request.Request(
            self._url(f"/api/runs/{_encode_segment(run_id)}/events"), headers=headers
        )
        try:
            raw = self._open(request)
        except urllib.error.HTTPError as error:
            problem, detail = self._problem_from_error(error)
            raise QcgError(error.code, problem, detail) from error
        with contextlib.closing(raw):
            decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
            text_buffer = ""
            byte_remainder = b""
            try:
                while True:
                    chunk = raw.read(1)
                    if not chunk:
                        break
                    byte_remainder += chunk
                    try:
                        text_buffer += decoder.decode(byte_remainder, final=False)
                        byte_remainder = b""
                    except UnicodeDecodeError:
                        # Incomplete multi-byte sequence: read more bytes.
                        if len(byte_remainder) > 4:
                            text_buffer += decoder.decode(byte_remainder, final=True)
                            byte_remainder = b""
                        continue
                    # Normalize CRLF/CR per the SSE spec before framing.
                    text_buffer = text_buffer.replace("\\r\\n", "\\n").replace("\\r", "\\n")
                    while "\\n\\n" in text_buffer:
                        frame, text_buffer = text_buffer.split("\\n\\n", 1)
                        data_lines = [
                            line[5:].lstrip() if line[5:6] == " " else line[5:]
                            for line in frame.split("\\n")
                            if line.startswith("data:")
                        ]
                        # Comment-only (: ...) and empty frames yield nothing.
                        if not data_lines:
                            continue
                        data = "\\n".join(data_lines)
                        if data:
                            yield json.loads(data)
            finally:
                try:
                    raw.close()
                except Exception:
                    pass

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
Authenticated redirects strip the bearer cross-origin and on TLS
downgrades; path wildcards encode segment by segment.
`,
);
console.log(`generated ${pinned.length} SDK operations`);
