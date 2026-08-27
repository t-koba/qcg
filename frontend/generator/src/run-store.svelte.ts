import { ApiClient, type ConfirmSpec, type FormSpec, type GeneratorDetail, type GeneratorSummary, type InputField, type OutputArtifact, type RunEvent, type RunSnapshot, type RunStatus } from "./api/client";
import { evalWhen } from "./expr/loader";
import { collectNodeProgress, record } from "./progress";

const MAX_FILE_INPUT_BYTES = 16 * 1024 * 1024;

export class RunStore {
  api = $state(new ApiClient());
  generators = $state<GeneratorSummary[]>([]);
  selected = $state("");
  detail = $state<GeneratorDetail | null>(null);
  activeFields = $state<InputField[]>([]);
  values = $state<Record<string, unknown>>({});
  currentRun = $state("");
  runState = $state<RunStatus | "idle" | "loading">("idle");
  events = $state<RunEvent[]>([]);
  artifacts = $state<OutputArtifact[]>([]);
  question = $state<FormSpec | null>(null);
  confirm = $state<ConfirmSpec | null>(null);
  errorText = $state("");
  nodeProgress = $derived(collectNodeProgress(this.events));

  #eventSource: EventSource | null = null;

  async initialize(): Promise<void> {
    await this.loadGenerators();
  }

  destroy(): void {
    this.#closeEventSource();
  }

  async loadGenerators(): Promise<void> {
    this.runState = "loading";
    this.generators = await this.api.get<GeneratorSummary[]>("/api/generators");
    if (this.generators.length > 0 && !this.selected) {
      await this.selectGenerator(this.generators[0].id);
    } else {
      this.runState = "idle";
    }
  }

  async selectGenerator(id: string): Promise<void> {
    this.destroy();
    this.selected = id;
    this.detail = await this.api.get<GeneratorDetail>(`/api/generators/${encodeURIComponent(id)}`);
    this.values = {};
    this.currentRun = "";
    this.events = [];
    this.question = null;
    this.confirm = null;
    this.artifacts = [];
    await this.refreshActiveFields();
    this.runState = "idle";
  }

  async refreshActiveFields(): Promise<void> {
    const fields: InputField[] = [];
    for (const stage of this.detail?.inputs?.stages || []) {
      if (await evalWhen(stage.when || undefined, { inputs: this.values })) fields.push(...stage.fields);
    }
    this.activeFields = fields;
    const values = { ...this.values };
    for (const field of fields) {
      if (values[field.id] === undefined && field.default !== undefined && field.default !== null) {
        values[field.id] = field.default;
      }
    }
    this.values = values;
  }

  setValue(id: string, value: unknown): void {
    this.values = { ...this.values, [id]: value };
    void this.withError(() => this.refreshActiveFields());
  }

  async setFileValue(id: string, file: File | undefined): Promise<void> {
    if (!file) {
      this.setValue(id, undefined);
      return;
    }
    if (file.size > MAX_FILE_INPUT_BYTES) {
      throw new Error(`file input exceeds the ${MAX_FILE_INPUT_BYTES} byte limit`);
    }
    if (!isSafeFileName(file.name)) {
      throw new Error(`file name must be one safe path component: ${file.name}`);
    }
    const bytes = new Uint8Array(await file.arrayBuffer());
    this.setValue(id, { name: file.name, content_base64: encodeBase64(bytes) });
  }

  async startRun(overrideInputs?: Record<string, unknown>): Promise<void> {
    const generatorId = this.selected || this.generators[0]?.id;
    if (!generatorId) return;
    this.destroy();
    this.errorText = "";
    this.runState = "running";
    this.events = [];
    this.question = null;
    this.confirm = null;
    this.artifacts = [];
    const inputs = overrideInputs ? { ...overrideInputs } : this.#collectInputs();
    const response = await this.api.post<RunSnapshot>("/api/runs", {
      generator_id: generatorId,
      inputs,
    }, crypto.randomUUID());
    this.applySnapshot(response);
    this.subscribe(response.run_id);
  }

  async answerQuestion(overrideValues?: Record<string, unknown>): Promise<void> {
    if (!this.currentRun || !this.question) return;
    const values = overrideValues || Object.fromEntries(this.question.fields.map((field) => [field.id, this.values[`question:${field.id}`] ?? field.default]));
    const snapshot = await this.api.put<RunSnapshot>(
      `/api/runs/${encodeURIComponent(this.currentRun)}/questions/${encodeURIComponent(this.question.id)}`,
      { values },
    );
    this.applySnapshot(snapshot);
  }

  async decideConfirmation(decision: "approve" | "deny"): Promise<void> {
    if (!this.currentRun || !this.confirm) return;
    const snapshot = await this.api.put<RunSnapshot>(
      `/api/runs/${encodeURIComponent(this.currentRun)}/confirmations/${encodeURIComponent(this.confirm.id)}`,
      { decision },
    );
    this.applySnapshot(snapshot);
  }

  async cancelRun(): Promise<void> {
    if (!this.currentRun) return;
    const snapshot = await this.api.post<RunSnapshot>(`/api/runs/${encodeURIComponent(this.currentRun)}:cancel`, {});
    this.applySnapshot(snapshot);
    this.destroy();
  }

  async withError(task: () => Promise<void>): Promise<void> {
    try {
      await task();
    } catch (error) {
      this.errorText = error instanceof Error ? error.message : String(error);
    }
  }

  async refreshRun(runId: string): Promise<void> {
    if (runId !== this.currentRun) return;
    const snapshot = await this.api.get<RunSnapshot>(`/api/runs/${encodeURIComponent(runId)}`);
    if (runId !== this.currentRun) return;
    this.applySnapshot(snapshot);
  }

  applySnapshot(snapshot: RunSnapshot): void {
    this.currentRun = snapshot.run_id;
    this.runState = snapshot.state;
    this.artifacts = snapshot.artifacts?.artifacts || [];
    this.question = snapshot.question || null;
    this.confirm = snapshot.confirm || null;
    if (!isActive(snapshot.state)) this.#closeEventSource();
  }

  subscribe(runId: string): void {
    this.#closeEventSource();
    const source = new EventSource(`/api/runs/${encodeURIComponent(runId)}/events`);
    source.onmessage = (event) => {
      try {
        this.#applyEvent(JSON.parse(event.data) as RunEvent);
      } catch {
        this.errorText = "The server sent an invalid run event.";
      }
    };
    source.onerror = () => {
      if (source.readyState === EventSource.CLOSED && this.#eventSource === source) {
        this.errorText = "The event stream closed unexpectedly.";
      }
    };
    this.#eventSource = source;
  }

  #collectInputs(): Record<string, unknown> {
    return Object.fromEntries(this.activeFields.flatMap((field) => {
      const value = this.values[field.id];
      return value === undefined || (Array.isArray(value) && value.length === 0 && !field.required) ? [] : [[field.id, value]];
    }));
  }

  #applyEvent(event: RunEvent): void {
    if (event.run_id !== this.currentRun) return;
    if (event.kind === "lagged") {
      void this.withError(() => this.refreshRun(event.run_id));
      return;
    }
    if (this.events.some((candidate) => candidate.seq === event.seq)) return;
    this.events = [...this.events, event].sort((left, right) => left.seq - right.seq);
    if (event.kind === "run_error") {
      const data = record(event.data);
      this.errorText = typeof data.error === "string" ? data.error : JSON.stringify(data);
    }
    if (["run_finished", "run_canceled", "run_failed", "run_waiting", "confirm_request"].includes(event.kind)) {
      void this.withError(() => this.refreshRun(event.run_id));
    }
  }

  #closeEventSource(): void {
    this.#eventSource?.close();
    this.#eventSource = null;
  }
}

function isActive(state: RunStatus | "idle" | "loading"): boolean {
  return state === "running" || state === "waiting" || state === "confirming" || state === "interrupted";
}

function isSafeFileName(name: string): boolean {
  return name.length > 0 && name !== "." && name !== ".." && !name.includes("/") && !name.includes("\\") && !name.includes("\0");
}

function encodeBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary);
}
