import { ApiClient, type ConfirmSpec, type FormSpec, type GeneratorDetail, type GeneratorSummary, type InputField, type OutputArtifact, type RunEvent, type RunListResponse, type RunSnapshot, type RunStatus } from "./api/client";
import { evalWhen } from "./expr/loader";
import { encodeBase64, validateFileInput } from "./field";
import { collectNodeProgress, record } from "./progress";
import {
  decodeView,
  encodeView,
  MAX_PERSISTED_TABS,
  VIEW_STATE_VERSION,
  VIEW_STORAGE_KEY,
} from "./view-persistence";

const CANCEL_REQUEST_TIMEOUT_MS = 15_000;
const RUN_HASH_PREFIX = "#/runs/";
type PendingAction = "starting" | "answering" | "approving" | "denying" | "canceling";

/** Per-run view backing one workspace tab. */
export interface RunTab {
  runId: string;
  generatorId: string;
  generatorName: string;
  runState: RunStatus | "idle" | "loading";
  events: RunEvent[];
  artifacts: OutputArtifact[];
  question: FormSpec | null;
  confirm: ConfirmSpec | null;
  /** `question:*` answer values owned by this run. */
  answers: Record<string, unknown>;
  queuePosition: number | null;
  queuedAt: string | null;
  pendingAction: PendingAction | null;
  lastSnapshotSeq: number;
  snapshotVersion: number;
  /** Stable idempotency key for the start request. */
  startKey: string | null;
  /** Stable idempotency key reused across cancel retries. */
  cancelKey: string | null;
}

function emptyTab(runId: string, generatorId: string, generatorName: string): RunTab {
  return {
    runId,
    generatorId,
    generatorName,
    runState: "running",
    events: [],
    artifacts: [],
    question: null,
    confirm: null,
    answers: {},
    queuePosition: null,
    queuedAt: null,
    pendingAction: null,
    lastSnapshotSeq: 0,
    snapshotVersion: 0,
    startKey: null,
    cancelKey: null,
  };
}

export class RunStore {
  api = $state(new ApiClient());
  generators = $state<GeneratorSummary[]>([]);
  selected = $state("");
  detail = $state<GeneratorDetail | null>(null);
  activeFields = $state<InputField[]>([]);
  values = $state<Record<string, unknown>>({});
  /** Open tabs keyed by run id. The selected tab mirrors into the fields below. */
  tabs = $state<Record<string, RunTab>>({});
  tabOrder = $state<string[]>([]);
  /** Run history from the server list API. */
  runs = $state<RunListResponse["items"]>([]);
  historyState = $state("");
  currentRun = $state("");
  runState = $state<RunStatus | "idle" | "loading">("idle");
  events = $state<RunEvent[]>([]);
  artifacts = $state<OutputArtifact[]>([]);
  question = $state<FormSpec | null>(null);
  confirm = $state<ConfirmSpec | null>(null);
  queuePosition = $state<number | null>(null);
  queuedAt = $state<string | null>(null);
  errorText = $state("");
  pendingAction = $state<PendingAction | null>(null);
  nodeProgress = $derived(collectNodeProgress(this.events));

  #sources: Record<string, EventSource> = {};
  #selectionController: AbortController | null = null;
  #cancelController: AbortController | null = null;
  #selectionVersion = 0;
  #fieldVersion = 0;
  #hashListener: ((event: HashChangeEvent) => void) | null = null;

  orderedTabs(): RunTab[] {
    return this.tabOrder
      .map((runId) => this.tabs[runId])
      .filter((tab): tab is RunTab => tab !== undefined);
  }

  async initialize(): Promise<void> {
    await this.loadGenerators();
    await this.withError(() => this.refreshRuns());
    await this.withError(() => this.restorePersistedView());
    const hashRunId = readHashRunId();
    if (hashRunId) {
      await this.withError(() => this.openRun(hashRunId));
    }
    if (!this.#hashListener && typeof window !== "undefined") {
      this.#hashListener = () => {
        const runId = readHashRunId();
        if (runId && runId !== this.currentRun) {
          void this.withError(() => this.openRun(runId));
        } else if (!runId && this.currentRun) {
          this.showForm();
        }
      };
      window.addEventListener("hashchange", this.#hashListener);
    }
  }

  destroy(): void {
    this.#selectionController?.abort();
    this.#selectionController = null;
    this.#cancelController?.abort();
    this.#cancelController = null;
    for (const source of Object.values(this.#sources)) source.close();
    this.#sources = {};
    if (this.#hashListener && typeof window !== "undefined") {
      window.removeEventListener("hashchange", this.#hashListener);
      this.#hashListener = null;
    }
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
    this.#selectionController?.abort();
    const controller = new AbortController();
    const version = ++this.#selectionVersion;
    this.#selectionController = controller;
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
      this.showForm();
      await this.refreshActiveFields();
      if (version === this.#selectionVersion && !this.currentRun) this.runState = "idle";
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
    this.persistView();
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

  async refreshRuns(): Promise<void> {
    const query = this.historyState ? { state: this.historyState } : undefined;
    const response = await this.api.listRuns(query);
    this.runs = response.items || [];
  }

  setHistoryFilter(state: string): void {
    this.historyState = state;
    void this.withError(() => this.refreshRuns());
  }

  /** Fork the selected run from its latest snapshot sequence. */
  async forkCurrentRun(): Promise<void> {
    const tab = this.currentTab();
    if (!tab || tab.runId.startsWith("pending-") || tab.lastSnapshotSeq === 0) return;
    this.saveCurrentView();
    const snapshot = await this.api.forkRun(tab.runId, tab.lastSnapshotSeq);
    this.applySnapshot(snapshot);
    this.selectTab(snapshot.run_id);
    void this.withError(() => this.refreshRuns());
  }

  async openRun(runId: string): Promise<void> {
    const snapshot = await this.api.get<RunSnapshot>(`/api/runs/${encodeURIComponent(runId)}`);
    this.applySnapshot(snapshot);
    this.selectTab(runId);
  }

  selectTab(runId: string): void {
    if (!this.tabs[runId]) return;
    const previous = this.currentRun;
    this.saveCurrentView();
    this.currentRun = runId;
    this.restoreView(this.tabs[runId]);
    if (previous && previous !== runId) {
      this.#closeEventSource(previous);
    }
    this.ensureSubscribed(runId);
    writeHashRunId(runId);
    this.persistView();
    // The newly visible tab may have missed events while backgrounded; resync its state.
    void this.withError(() => this.refreshRun(runId));
  }

  closeTab(runId: string): void {
    this.#closeEventSource(runId);
    delete this.tabs[runId];
    this.tabOrder = this.tabOrder.filter((id) => id !== runId);
    if (this.currentRun === runId) {
      const neighbor = this.tabOrder[this.tabOrder.length - 1];
      if (neighbor && this.tabs[neighbor]) {
        this.currentRun = neighbor;
        this.restoreView(this.tabs[neighbor]);
        this.ensureSubscribed(neighbor);
        writeHashRunId(neighbor);
        // The neighbor may have missed events while backgrounded; resync its state.
        void this.withError(() => this.refreshRun(neighbor));
      } else {
        this.showForm();
      }
    }
    this.persistView();
  }

  /** Leave the current tab open and return to the generator form. */
  showForm(): void {
    this.saveCurrentView();
    // No tab is visible; background streams have no consumer until a tab is selected.
    this.#closeEventSource();
    this.currentRun = "";
    this.runState = "idle";
    this.events = [];
    this.artifacts = [];
    this.question = null;
    this.confirm = null;
    this.queuePosition = null;
    this.queuedAt = null;
    this.pendingAction = null;
    writeHashRunId(null);
    this.persistView();
  }

  async startRun(overrideInputs?: Record<string, unknown>): Promise<void> {
    const generatorId = this.selected || this.generators[0]?.id;
    if (!generatorId) return;
    this.saveCurrentView();
    this.errorText = "";
    const inputs = overrideInputs ? { ...overrideInputs } : this.#collectInputs();
    const startKey = await sha256Hex(canonicalJson({ generator_id: generatorId, inputs }));
    const tab = emptyTab("", generatorId, this.generators.find((candidate) => candidate.id === generatorId)?.name || generatorId);
    tab.pendingAction = "starting";
    tab.startKey = startKey;
    // Publish an optimistic placeholder so the new tab is visible immediately.
    const placeholderId = `pending-${startKey.slice(0, 12)}`;
    tab.runId = placeholderId;
    this.tabs = { ...this.tabs, [placeholderId]: tab };
    this.tabOrder = [...this.tabOrder, placeholderId];
    this.currentRun = placeholderId;
    this.restoreView(tab);
    this.runState = "running";
    try {
      const response = await this.api.post<RunSnapshot>("/api/runs", {
        generator_id: generatorId,
        inputs,
      }, startKey);
      const started = this.tabs[placeholderId];
      if (started) {
        // The start request settled; progress now arrives via snapshots and
        // events, so the optimistic placeholder must not block actions.
        started.pendingAction = null;
      }
      if (this.currentRun === placeholderId) {
        this.pendingAction = null;
      }
      this.adoptTab(placeholderId, response);
      this.applySnapshot(response);
      this.selectTab(response.run_id);
      void this.withError(() => this.refreshRuns());
    } catch (error) {
      this.closeTab(placeholderId);
      throw error;
    }
  }

  async answerQuestion(overrideValues?: Record<string, unknown>): Promise<void> {
    const tab = this.currentTab();
    if (!tab || !this.question || tab.pendingAction) return;
    const values = overrideValues || Object.fromEntries(this.question.fields.map((field) => [field.id, this.values[`question:${field.id}`] ?? field.default]));
    const key = await sha256Hex(canonicalJson({ run: tab.runId, question: this.question.id, values }));
    tab.pendingAction = "answering";
    this.pendingAction = "answering";
    tab.snapshotVersion += 1;
    const version = tab.snapshotVersion;
    try {
      const snapshot = await this.api.put<RunSnapshot>(
        `/api/runs/${encodeURIComponent(tab.runId)}/questions/${encodeURIComponent(this.question.id)}`,
        { values },
        key,
      );
      if (version !== tab.snapshotVersion) return;
      this.applySnapshot(snapshot);
    } finally {
      if (tab.snapshotVersion === version) {
        tab.pendingAction = null;
        if (this.currentRun === tab.runId) this.pendingAction = null;
      }
    }
  }

  async decideConfirmation(decision: "approve" | "deny"): Promise<void> {
    const tab = this.currentTab();
    if (!tab || !this.confirm || tab.pendingAction) return;
    const action = decision === "approve" ? "approving" : "denying";
    const key = await sha256Hex(canonicalJson({ run: tab.runId, confirmation: this.confirm.id, decision }));
    tab.pendingAction = action;
    this.pendingAction = action;
    tab.snapshotVersion += 1;
    const version = tab.snapshotVersion;
    try {
      const snapshot = await this.api.put<RunSnapshot>(
        `/api/runs/${encodeURIComponent(tab.runId)}/confirmations/${encodeURIComponent(this.confirm.id)}`,
        { decision },
        key,
      );
      if (version !== tab.snapshotVersion) return;
      this.applySnapshot(snapshot);
    } finally {
      if (tab.snapshotVersion === version) {
        tab.pendingAction = null;
        if (this.currentRun === tab.runId) this.pendingAction = null;
      }
    }
  }

  async cancelRun(): Promise<void> {
    const tab = this.currentTab();
    if (!tab || tab.pendingAction || !isCancelable(tab.runState)) return;
    tab.pendingAction = "canceling";
    this.pendingAction = "canceling";
    tab.snapshotVersion += 1;
    const version = tab.snapshotVersion;
    if (!tab.cancelKey) {
      tab.cancelKey = randomId();
      this.tabs = { ...this.tabs };
    }
    const controller = new AbortController();
    this.#cancelController = controller;
    const timeout = setTimeout(() => controller.abort(), CANCEL_REQUEST_TIMEOUT_MS);
    try {
      const snapshot = await this.api.post<RunSnapshot>(
        `/api/runs/${encodeURIComponent(tab.runId)}:cancel`,
        {},
        tab.cancelKey,
        controller.signal,
      );
      if (version !== tab.snapshotVersion) return;
      this.applySnapshot(snapshot);
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error("The cancellation request timed out.");
      }
      throw error;
    } finally {
      clearTimeout(timeout);
      if (this.#cancelController === controller) this.#cancelController = null;
      if (tab.snapshotVersion === version) {
        tab.pendingAction = null;
        if (this.currentRun === tab.runId) this.pendingAction = null;
      }
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
    this.showForm();
    this.errorText = "";
  }

  async refreshRun(runId: string): Promise<void> {
    const tab = this.tabs[runId];
    if (!tab) return;
    const version = ++tab.snapshotVersion;
    this.tabs = { ...this.tabs };
    const snapshot = await this.api.get<RunSnapshot>(`/api/runs/${encodeURIComponent(runId)}`);
    const current = this.tabs[runId];
    if (!current || version !== current.snapshotVersion) return;
    this.applySnapshot(snapshot);
  }

  applySnapshot(snapshot: RunSnapshot): void {
    let tab = this.tabs[snapshot.run_id];
    if (!tab) {
      const generatorId = generatorIdFromRunId(snapshot.run_id);
      tab = emptyTab(
        snapshot.run_id,
        generatorId,
        this.generators.find((candidate) => candidate.id === generatorId)?.name || generatorId,
      );
      this.tabs = { ...this.tabs, [snapshot.run_id]: tab };
      this.tabOrder = [...this.tabOrder, snapshot.run_id];
    }
    if (snapshot.seq < tab.lastSnapshotSeq) return;
    tab.lastSnapshotSeq = Math.max(tab.lastSnapshotSeq, snapshot.seq);
    tab.runState = snapshot.state;
    tab.artifacts = snapshot.artifacts?.artifacts || [];
    tab.question = snapshot.question || null;
    tab.confirm = snapshot.confirm || null;
    tab.queuePosition = snapshot.queue_position ?? null;
    tab.queuedAt = snapshot.queued_at ?? null;
    this.tabs = { ...this.tabs };
    if (snapshot.run_id === this.currentRun) {
      this.restoreView(tab);
    }
    if (!isActive(snapshot.state) || snapshot.run_id !== this.currentRun) {
      this.#closeEventSource(snapshot.run_id);
    } else if (!snapshot.run_id.startsWith("pending-")) {
      this.ensureSubscribed(snapshot.run_id);
    }
  }

  subscribe(runId: string): void {
    this.ensureSubscribed(runId);
  }

  ensureSubscribed(runId: string): void {
    const tab = this.tabs[runId];
    if (!tab || !isActive(tab.runState) || this.#sources[runId]) return;
    const source = new EventSource(`/api/runs/${encodeURIComponent(runId)}/events`);
    source.onmessage = (event) => {
      try {
        this.#applyEvent(runId, JSON.parse(event.data) as RunEvent);
      } catch {
        if (runId === this.currentRun) this.errorText = "The server sent an invalid run event.";
      }
    };
    source.onerror = () => {
      const current = this.tabs[runId];
      if (this.#sources[runId] === source && current && isActive(current.runState)) {
        void this.withError(() => this.refreshRun(runId));
      }
    };
    this.#sources[runId] = source;
  }

  currentTab(): RunTab | null {
    return this.currentRun ? this.tabs[this.currentRun] || null : null;
  }

  #collectInputs(): Record<string, unknown> {
    return Object.fromEntries(this.activeFields.flatMap((field) => {
      const value = this.values[field.id];
      return value === undefined || (Array.isArray(value) && value.length === 0 && !field.required) ? [] : [[field.id, value]];
    }));
  }

  #applyEvent(runId: string, event: RunEvent): void {
    const tab = this.tabs[runId];
    if (!tab) return;
    if (event.kind === "lagged") {
      void this.withError(() => this.refreshRun(runId));
      return;
    }
    if (tab.events.some((candidate) => candidate.seq === event.seq)) return;
    tab.events = [...tab.events, event].sort((left, right) => left.seq - right.seq);
    tab.lastSnapshotSeq = Math.max(tab.lastSnapshotSeq, event.seq);
    this.tabs = { ...this.tabs };
    if (runId === this.currentRun) {
      this.events = tab.events;
      if (event.kind === "run_error") {
        const data = record(event.data);
        this.errorText = typeof data.error === "string" ? data.error : JSON.stringify(data);
      }
    }
    if (["run_finished", "run_canceled", "run_failed", "run_waiting", "confirm_request"].includes(event.kind)) {
      void this.withError(() => this.refreshRun(runId));
    }
  }

  #closeEventSource(runId?: string): void {
    if (runId) {
      this.#sources[runId]?.close();
      delete this.#sources[runId];
      return;
    }
    for (const source of Object.values(this.#sources)) source.close();
    this.#sources = {};
  }

  /** Persist the workspace screen so a revisit restores tabs and selection. */
  private persistView(): void {
    if (typeof localStorage === "undefined") return;
    try {
      const tabOrder = this.tabOrder
        .filter((id) => !id.startsWith("pending-"))
        .slice(-MAX_PERSISTED_TABS);
      const currentRun = tabOrder.includes(this.currentRun) ? this.currentRun : "";
      localStorage.setItem(VIEW_STORAGE_KEY, encodeView({
        version: VIEW_STATE_VERSION,
        selected: this.selected,
        tabOrder,
        currentRun,
      }));
    } catch {
      // Storage may be unavailable or full; the workspace still works for this session.
    }
  }

  /** Reopen the persisted screen. Runs are shared, so only existing runs are restored. */
  private async restorePersistedView(): Promise<void> {
    let raw: string | null = null;
    try {
      raw = localStorage.getItem(VIEW_STORAGE_KEY);
    } catch {
      return;
    }
    const persisted = decodeView(raw);
    if (!persisted) return;
    const knownIds = new Set(this.runs.map((run) => run.run_id));
    if (
      persisted.selected &&
      persisted.selected !== this.selected &&
      this.generators.some((generator) => generator.id === persisted.selected)
    ) {
      await this.selectGenerator(persisted.selected);
    }
    // Snapshots are independent per run, so fetch them concurrently.
    const pending = persisted.tabOrder.filter((runId) =>
      !(this.tabs as Record<string, RunTab | undefined>)[runId] && knownIds.has(runId)
    );
    const snapshots = await Promise.all(pending.map(async (runId) => {
      try {
        return await this.api.get<RunSnapshot>(`/api/runs/${encodeURIComponent(runId)}`);
      } catch {
        // The run was removed or is unreadable; skip it and continue with the rest.
        return null;
      }
    }));
    for (const snapshot of snapshots) {
      if (snapshot) this.applySnapshot(snapshot);
    }
    if (!readHashRunId() && persisted.currentRun && this.tabs[persisted.currentRun]) {
      this.selectTab(persisted.currentRun);
    }
    // Rewrite the payload in the current shape, purging any legacy secrets.
    this.persistView();
  }

  /** Persist the selected view back into its tab, including question answers. */
  private saveCurrentView(): void {
    const tab = this.currentRun ? this.tabs[this.currentRun] : undefined;
    if (!tab) return;
    tab.runState = this.runState;
    tab.events = this.events;
    tab.artifacts = this.artifacts;
    tab.question = this.question;
    tab.confirm = this.confirm;
    tab.queuePosition = this.queuePosition;
    tab.queuedAt = this.queuedAt;
    tab.pendingAction = this.pendingAction;
    tab.answers = Object.fromEntries(
      Object.entries(this.values).filter(([key]) => key.startsWith("question:")),
    );
    this.tabs = { ...this.tabs };
  }

  /** Mirror a tab into the selected top-level view fields. */
  private restoreView(tab: RunTab): void {
    this.runState = tab.runState;
    this.events = tab.events;
    this.artifacts = tab.artifacts;
    this.question = tab.question;
    this.confirm = tab.confirm;
    this.queuePosition = tab.queuePosition;
    this.queuedAt = tab.queuedAt;
    this.pendingAction = tab.pendingAction;
    const formValues = Object.fromEntries(
      Object.entries(this.values).filter(([key]) => !key.startsWith("question:")),
    );
    this.values = { ...formValues, ...tab.answers };
  }

  /** Replace the optimistic placeholder tab with the real run id. */
  private adoptTab(placeholderId: string, snapshot: RunSnapshot): void {
    const placeholder = this.tabs[placeholderId];
    delete this.tabs[placeholderId];
    this.tabOrder = this.tabOrder.filter((id) => id !== placeholderId);
    this.#closeEventSource(placeholderId);
    if (placeholder) {
      placeholder.runId = snapshot.run_id;
      placeholder.startKey = null;
      this.tabs = { ...this.tabs, [snapshot.run_id]: placeholder };
      this.tabOrder = [...this.tabOrder, snapshot.run_id];
    }
    if (this.currentRun === placeholderId) this.currentRun = snapshot.run_id;
  }
}

function isActive(state: RunStatus | "idle" | "loading"): boolean {
  return state === "queued" || state === "running" || state === "waiting" || state === "confirming";
}

function isCancelable(state: RunStatus | "idle" | "loading"): boolean {
  return state === "queued" || state === "running" || state === "waiting" || state === "confirming";
}

function generatorIdFromRunId(runId: string): string {
  const forkMarker = "-fork-";
  const forkIndex = runId.indexOf(forkMarker);
  const base = forkIndex >= 0 ? runId.slice(0, forkIndex) : runId;
  const dash = base.lastIndexOf("-");
  return dash > 0 ? base.slice(0, dash) : base;
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(sortValue(value));
}

function sortValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortValue);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
        .map(([key, entry]) => [key, sortValue(entry)]),
    );
  }
  return value;
}

async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function randomId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) return crypto.randomUUID();
  return `key-${Date.now()}-${Math.floor(Math.random() * 0xffffffff).toString(16)}`;
}

function readHashRunId(): string | null {
  if (typeof window === "undefined") return null;
  const hash = window.location.hash;
  if (!hash.startsWith(RUN_HASH_PREFIX)) return null;
  const runId = decodeURIComponent(hash.slice(RUN_HASH_PREFIX.length));
  return runId || null;
}

function writeHashRunId(runId: string | null): void {
  if (typeof window === "undefined" || typeof history === "undefined") return;
  const hash = runId ? `${RUN_HASH_PREFIX}${encodeURIComponent(runId)}` : "#";
  history.replaceState(null, "", hash);
}
