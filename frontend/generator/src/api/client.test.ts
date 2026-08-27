import { describe, expect, it } from "vitest";
import { ApiProblemError, errorMessage } from "./client";

describe("API problem responses", () => {
  it("formats RFC 9457 details and field errors", async () => {
    const response = new Response(JSON.stringify({
      type: "https://qcg.dev/problems/invalid",
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
