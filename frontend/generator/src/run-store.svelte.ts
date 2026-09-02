import { ApiClient, type ConfirmSpec, type FormSpec, type GeneratorDetail, type GeneratorSummary, type InputField, type OutputArtifact, type RunEvent, type RunSnapshot, type RunStatus } from "./api/client";
import { evalWhen } from "./expr/loader";
import { encodeBase64, validateFileInput } from "./field";
import { collectNodeProgress, record } from "./progress";

const CANCEL_REQUEST_TIMEOUT_MS = 15_000;
type PendingAction = "starting" | "answering" | "approving" | "denying" | "canceling";

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
  pendingAction = $state<PendingAction | null>(null);
  nodeProgress = $derived(collectNodeProgress(this.events));

  #eventSource: EventSource | null = null;
  #selectionController: AbortController | null = null;
  #cancelController: AbortController | null = null;
  #selectionVersion = 0;
  #snapshotVersion = 0;
  #lastSnapshotSeq = 0;
  #fieldVersion = 0;

  async initialize(): Promise<void> {
    await this.loadGenerators();
  }

  destroy(): void {
    this.#selectionController?.abort();
    this.#selectionController = null;
    this.#cancelController?.abort();
    this.#cancelController = null;
    this.#closeEventSource();
  }

  async loadGenerators(): Promise<void> {
    this.runState = "loading";
    try {
      this.generators = await this.api.get<GeneratorSummary[]>("/api/generators");
      if (this.generators.length > 0 && !this.selected) {
        await this.selectGenerator(this.generators[0].id);
      } else {
        this.runState = "idle";
      }
    } catch (error) {
      this.runState = "idle";
      throw error;
    }
  }

  async selectGenerator(id: string): Promise<void> {
    if (isActive(this.runState) && id !== this.selected) return;
    this.#selectionController?.abort();
    const controller = new AbortController();
    const version = ++this.#selectionVersion;
    this.#selectionController = controller;
    this.#closeEventSource();
    this.selected = id;
    this.runState = "loading";
    try {
      const detail = await this.api.get<GeneratorDetail>(
        `/api/generators/${encodeURIComponent(id)}`,
        controller.signal,
      );
      if (version !== this.#selectionVersion) return;
      this.detail = detail;
      this.values = {};
      this.currentRun = "";
      this.events = [];
      this.question = null;
      this.confirm = null;
      this.artifacts = [];
      this.#lastSnapshotSeq = 0;
      this.#snapshotVersion += 1;
      await this.refreshActiveFields();
      if (version === this.#selectionVersion) this.runState = "idle";
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (version === this.#selectionVersion) this.runState = "idle";
      throw error;
    } finally {
      if (this.#selectionController === controller) this.#selectionController = null;
    }
  }

  async refreshActiveFields(): Promise<void> {
    const version = ++this.#fieldVersion;
    const valuesSnapshot = { ...this.values };
    const fields: InputField[] = [];
    for (const stage of this.detail?.inputs?.stages || []) {
      if (await evalWhen(stage.when || undefined, { inputs: valuesSnapshot })) {
        fields.push(...stage.fields);
      }
    }
    if (version !== this.#fieldVersion) return;
    this.activeFields = fields;
    const values = { ...valuesSnapshot };
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
    validateFileInput(file);
    const bytes = new Uint8Array(await file.arrayBuffer());
    this.setValue(id, { name: file.name, content_base64: encodeBase64(bytes) });
  }

  async startRun(overrideInputs?: Record<string, unknown>): Promise<void> {
    const generatorId = this.selected || this.generators[0]?.id;
    if (!generatorId || this.pendingAction) return;
    this.destroy();
    this.errorText = "";
    this.pendingAction = "starting";
    this.#snapshotVersion += 1;
    this.runState = "running";
    this.events = [];
    this.question = null;
    this.confirm = null;
    this.artifacts = [];
    const inputs = overrideInputs ? { ...overrideInputs } : this.#collectInputs();
    try {
      const response = await this.api.post<RunSnapshot>("/api/runs", {
        generator_id: generatorId,
        inputs,
      }, crypto.randomUUID());
      this.applySnapshot(response);
      this.subscribe(response.run_id);
    } catch (error) {
      this.runState = "idle";
      throw error;
    } finally {
      this.pendingAction = null;
    }
  }

  async answerQuestion(overrideValues?: Record<string, unknown>): Promise<void> {
    if (!this.currentRun || !this.question || this.pendingAction) return;
    this.pendingAction = "answering";
    this.#snapshotVersion += 1;
    const values = overrideValues || Object.fromEntries(this.question.fields.map((field) => [field.id, this.values[`question:${field.id}`] ?? field.default]));
    try {
      const snapshot = await this.api.put<RunSnapshot>(
        `/api/runs/${encodeURIComponent(this.currentRun)}/questions/${encodeURIComponent(this.question.id)}`,
        { values },
      );
      this.applySnapshot(snapshot);
    } finally {
      this.pendingAction = null;
    }
  }

  async decideConfirmation(decision: "approve" | "deny"): Promise<void> {
    if (!this.currentRun || !this.confirm || this.pendingAction) return;
    this.pendingAction = decision === "approve" ? "approving" : "denying";
    this.#snapshotVersion += 1;
    try {
      const snapshot = await this.api.put<RunSnapshot>(
        `/api/runs/${encodeURIComponent(this.currentRun)}/confirmations/${encodeURIComponent(this.confirm.id)}`,
        { decision },
      );
      this.applySnapshot(snapshot);
    } finally {
      this.pendingAction = null;
    }
  }

  async cancelRun(): Promise<void> {
    if (!this.currentRun || this.pendingAction || !isCancelable(this.runState)) return;
    this.pendingAction = "canceling";
    this.#snapshotVersion += 1;
    const controller = new AbortController();
    this.#cancelController = controller;
    const timeout = setTimeout(() => controller.abort(), CANCEL_REQUEST_TIMEOUT_MS);
    try {
      const snapshot = await this.api.post<RunSnapshot>(
        `/api/runs/${encodeURIComponent(this.currentRun)}:cancel`,
        {},
        undefined,
        controller.signal,
      );
      this.applySnapshot(snapshot);
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error("The cancellation request timed out.");
      }
      throw error;
    } finally {
      clearTimeout(timeout);
      if (this.#cancelController === controller) this.#cancelController = null;
      this.pendingAction = null;
    }
  }

  async withError(task: () => Promise<void>): Promise<void> {
    this.errorText = "";
    try {
      await task();
    } catch (error) {
      this.errorText = error instanceof Error ? error.message : String(error);
    }
  }

  dismissError(): void {
    this.errorText = "";
  }

  resetRun(): void {
    this.destroy();
    this.currentRun = "";
    this.runState = "idle";
    this.events = [];
    this.artifacts = [];
    this.question = null;
    this.confirm = null;
    this.errorText = "";
    this.pendingAction = null;
    this.#lastSnapshotSeq = 0;
    this.#snapshotVersion += 1;
  }

  async refreshRun(runId: string): Promise<void> {
    if (runId !== this.currentRun) return;
    const version = ++this.#snapshotVersion;
    const snapshot = await this.api.get<RunSnapshot>(`/api/runs/${encodeURIComponent(runId)}`);
    if (runId !== this.currentRun || version !== this.#snapshotVersion) return;
    this.applySnapshot(snapshot);
  }

  applySnapshot(snapshot: RunSnapshot): void {
    if (snapshot.run_id === this.currentRun && snapshot.seq < this.#lastSnapshotSeq) return;
    if (snapshot.run_id !== this.currentRun) this.#lastSnapshotSeq = 0;
    this.currentRun = snapshot.run_id;
    this.#lastSnapshotSeq = Math.max(this.#lastSnapshotSeq, snapshot.seq);
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
      if (this.#eventSource === source && isActive(this.runState)) {
        void this.withError(() => this.refreshRun(runId));
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
    this.#lastSnapshotSeq = Math.max(this.#lastSnapshotSeq, event.seq);
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
  return state === "queued" || state === "running" || state === "waiting" || state === "confirming";
}

function isCancelable(state: RunStatus | "idle" | "loading"): boolean {
  return state === "queued" || state === "running" || state === "waiting" || state === "confirming";
}
