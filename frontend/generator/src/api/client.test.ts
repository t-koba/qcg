import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClient, ApiProblemError, errorMessage } from "./client";

afterEach(() => vi.unstubAllGlobals());

describe("API problem responses", () => {
  it("formats RFC 9457 details and field errors", async () => {
    const response = new Response(JSON.stringify({
      type: "https://example.invalid/problems/invalid",
      title: "Invalid request",
      status: 422,
      detail: "Input validation failed",
      instance: "/api/runs",
      code: "invalid",
      errors: [{ field: "inputs.name", reason: "is required" }],
    }), { status: 422, headers: { "content-type": "application/problem+json" } });
    await expect(errorMessage(response)).resolves.toBe("Input validation failed — inputs.name: is required");
  });

  it("throws a typed API error", async () => {
    const error = new ApiProblemError({
      title: "Not found",
      status: 404,
      detail: "run does not exist",
      code: "not_found",
      type: "about:blank",
      instance: "/api/runs/missing",
      errors: [],
    }, 404, "Not Found");
    expect(error).toBeInstanceOf(ApiProblemError);
    expect(error).toMatchObject({ status: 404, message: "run does not exist" } satisfies Partial<ApiProblemError>);
  });
});


describe("package-reviewed run start", () => {
  it("sends the loaded package identity only on the start request", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(JSON.stringify({ run_id: "demo-1" }), { status: 201 }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const client = new ApiClient();
    await client.startRun("body" as unknown as Record<string, unknown>, "a".repeat(64), "key-1");
    const startHeaders = new Headers(fetchMock.mock.calls[0]?.[1]?.headers);
    expect(startHeaders.get("x-qcg-package-sha256")).toBe("a".repeat(64));
    expect(startHeaders.get("idempotency-key")).toBe("key-1");
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toBe("body");
    await client.post("/api/other", {}, "key-2");
    const otherHeaders = new Headers(fetchMock.mock.calls[1]?.[1]?.headers);
    expect(otherHeaders.has("x-qcg-package-sha256")).toBe(false);
  });
});

describe("continuing a failed run", () => {
  it("posts to the continue endpoint without a request body and with an idempotency key", async () => {
    const child = { run_id: "child", state: "queued" };
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(JSON.stringify(child), { status: 201 }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const controller = new AbortController();
    const client = new ApiClient();

    await expect(client.continueRun("run/1", "key-1", controller.signal)).resolves.toEqual(child);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/runs/run%2F1/continue");
    expect(init?.method).toBe("POST");
    expect(init?.body).toBeUndefined();
    expect(init?.signal).toBe(controller.signal);
    const headers = new Headers(init?.headers);
    expect(headers.get("idempotency-key")).toBe("key-1");
  });

  it("reports a rejected continuation as a typed API error", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(
      JSON.stringify({ detail: "only failed runs can be continued" }),
      { status: 409, headers: { "content-type": "application/problem+json" } },
    )));
    const client = new ApiClient();
    await expect(client.continueRun("test", "key-1")).rejects.toMatchObject({
      name: "ApiProblemError",
      status: 409,
      message: "only failed runs can be continued",
    });
  });
});
