import type { components } from "./types";

export type GeneratorSummary = components["schemas"]["GeneratorSummary"];
export type GeneratorDetail = components["schemas"]["GeneratorDetail"];
export type RunSnapshot = components["schemas"]["RunSnapshot"];
export type RunEvent = components["schemas"]["RunEvent"];
export type RunListItem = components["schemas"]["RunListItem"];
export type RunListResponse = components["schemas"]["RunListResponse"];
export type RunStatus = components["schemas"]["RunStatus"];
export type ProblemDetails = components["schemas"]["ProblemDetails"];
export type OutputArtifact = components["schemas"]["OutputManifest"]["artifacts"][number];
export type InputField = NonNullable<GeneratorDetail["inputs"]["stages"]>[number]["fields"] extends (infer Field)[]
  ? Field
  : never;
export type FormSpec = NonNullable<RunSnapshot["question"]>;
export type ConfirmSpec = NonNullable<RunSnapshot["confirm"]>;

export class ApiClient {
  readonly base = "";

  async get<T>(path: string, signal?: AbortSignal): Promise<T> {
    const response = await fetch(path, { signal });
    return parseResponse<T>(response);
  }

  async post<T>(path: string, body: unknown, idempotencyKey?: string): Promise<T> {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (idempotencyKey) headers["idempotency-key"] = idempotencyKey;
    const response = await fetch(`${this.base}${path}`, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
    return parseResponse<T>(response);
  }

  async put<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.base}${path}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    return parseResponse<T>(response);
  }

  artifactUrl(runId: string, path: string): string {
    return `${this.base}/api/runs/${encodeURIComponent(runId)}/artifacts/${encodePath(path)}`;
  }

  zipUrl(runId: string): string {
    return `${this.base}/api/runs/${encodeURIComponent(runId)}/artifacts.zip`;
  }
}

async function parseResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const problem = await readProblem(response);
    throw new ApiProblemError(problem, response.status, response.statusText);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export async function errorMessage(response: Response): Promise<string> {
  const problem = await readProblem(response);
  return formatProblem(problem) || `${response.status} ${response.statusText}`;
}

export class ApiProblemError extends Error {
  constructor(
    public readonly problem: Partial<ProblemDetails>,
    public readonly status: number,
    statusText: string,
  ) {
    super(formatProblem(problem) || `${status} ${statusText}`);
    this.name = "ApiProblemError";
  }
}

async function readProblem(response: Response): Promise<Partial<ProblemDetails>> {
  const text = await response.text();
  if (!text) return {};
  try {
    const parsed = JSON.parse(text) as unknown;
    return isRecord(parsed) ? parsed : { detail: text };
  } catch {
    return { detail: text };
  }
}

function formatProblem(problem: Partial<ProblemDetails>): string {
  const fields = Array.isArray(problem.errors)
    ? problem.errors.map((error) => `${error.field}: ${error.reason}`).join("; ")
    : "";
  return [problem.detail, fields].filter(Boolean).join(" — ");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function encodePath(path: string): string {
  return path.split("/").map(encodeURIComponent).join("/");
}
