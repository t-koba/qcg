import type {
  McpAuthorizationResponse,
  McpServerSummary,
  McpServersResponse,
} from "./api/client";

export interface McpApi {
  listMcpServers(signal?: AbortSignal): Promise<McpServersResponse>;
  beginMcpAuthorization(serverId: string, signal?: AbortSignal): Promise<McpAuthorizationResponse>;
  cancelPendingMcpAuthorization(serverId: string, signal?: AbortSignal): Promise<void>;
  clearMcpAuthorization(serverId: string, signal?: AbortSignal): Promise<void>;
}

export interface McpPopup {
  readonly closed: boolean;
  readonly location: { replace(url: string): void };
  close(): void;
}

export interface McpPopupHost {
  open(url: string, target: string, features: string): McpPopup | null;
  setTimeout(handler: () => void, delayMs: number): ReturnType<typeof setTimeout>;
  clearTimeout(handle: ReturnType<typeof setTimeout>): void;
  now(): number;
}

export type McpNotice = "popup_blocked" | "canceled" | "timeout" | "disconnected";
export type McpPhase = "idle" | "loading" | "authorizing" | "canceling" | "disconnecting";

export interface McpSnapshot {
  servers: McpServerSummary[];
  phase: McpPhase;
  authorizingServerId: string | null;
  disconnectingServerId: string | null;
  notice: McpNotice | null;
  errorText: string;
}

export interface McpAuthorizationOptions {
  pollIntervalMs?: number;
  timeoutMs?: number;
  popupCloseGraceMs?: number;
}

const DEFAULT_POLL_INTERVAL_MS = 1_000;
const DEFAULT_TIMEOUT_MS = 120_000;
const DEFAULT_POPUP_CLOSE_GRACE_MS = 3_000;
const POPUP_CHECK_INTERVAL_MS = 250;
const CANCEL_REQUEST_TIMEOUT_MS = 10_000;
const POPUP_FEATURES = "popup,width=480,height=720,resizable=yes,scrollbars=yes";

type Listener = (snapshot: McpSnapshot) => void;
type TimerHandle = ReturnType<typeof setTimeout>;
type TerminationNotice = Exclude<McpNotice, "popup_blocked">;

interface AuthorizationAttempt {
  readonly serverId: string;
  readonly controller: AbortController;
  readonly popup: McpPopup;
  readonly deadline: number;
  popupClosedAt: number | null;
  pollTimer: TimerHandle | null;
  popupTimer: TimerHandle | null;
  timeoutTimer: TimerHandle | null;
}

export class McpConnectionController {
  #api: McpApi;
  #popupHost: McpPopupHost;
  #pollIntervalMs: number;
  #timeoutMs: number;
  #popupCloseGraceMs: number;
  #snapshot: McpSnapshot = {
    servers: [],
    phase: "idle",
    authorizingServerId: null,
    disconnectingServerId: null,
    notice: null,
    errorText: "",
  };
  #listeners = new Set<Listener>();
  #loadController: AbortController | null = null;
  #attempt: AuthorizationAttempt | null = null;
  #termination: Promise<unknown | null> | null = null;
  #disconnectController: AbortController | null = null;
  #destroyed = false;

  constructor(api: McpApi, popupHost: McpPopupHost = browserPopupHost(), options: McpAuthorizationOptions = {}) {
    this.#api = api;
    this.#popupHost = popupHost;
    this.#pollIntervalMs = Math.max(0, options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS);
    this.#timeoutMs = Math.max(1, options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
    this.#popupCloseGraceMs = Math.max(0, options.popupCloseGraceMs ?? DEFAULT_POPUP_CLOSE_GRACE_MS);
  }

  get snapshot(): McpSnapshot {
    return this.#snapshot;
  }

  subscribe(listener: Listener): () => void {
    this.#listeners.add(listener);
    listener(this.#snapshot);
    return () => this.#listeners.delete(listener);
  }

  async initialize(): Promise<void> {
    this.#loadController?.abort();
    const controller = new AbortController();
    this.#loadController = controller;
    this.#update({ phase: "loading", notice: null, errorText: "" });
    try {
      const response = await this.#api.listMcpServers(controller.signal);
      if (this.#destroyed || controller.signal.aborted) return;
      this.#update({ phase: "idle", servers: response.items, errorText: "" });
    } catch (error) {
      if (this.#destroyed || controller.signal.aborted || isAbortError(error)) return;
      this.#update({ phase: "idle", errorText: errorText(error) });
    } finally {
      if (this.#loadController === controller) this.#loadController = null;
    }
  }

  /**
   * Opens the blank popup before the first await so browser popup blockers see
   * the operation as part of the user's click gesture.
   */
  async authorize(serverId: string): Promise<void> {
    if (this.#destroyed || this.#termination || this.#disconnectController) return;
    const popup = this.#popupHost.open("", "_blank", POPUP_FEATURES);
    if (!popup) {
      this.#update({ notice: "popup_blocked", errorText: "" });
      return;
    }

    if (this.#termination) await this.#termination;
    const previous = this.#attempt;
    if (previous) await this.#terminate(previous, null, true);
    if (this.#destroyed) {
      closePopup(popup);
      return;
    }

    const attempt: AuthorizationAttempt = {
      serverId,
      controller: new AbortController(),
      popup,
      deadline: this.#popupHost.now() + this.#timeoutMs,
      popupClosedAt: null,
      pollTimer: null,
      popupTimer: null,
      timeoutTimer: null,
    };
    this.#attempt = attempt;
    this.#update({
      phase: "authorizing",
      authorizingServerId: serverId,
      notice: null,
      errorText: "",
    });
    attempt.timeoutTimer = this.#popupHost.setTimeout(() => {
      void this.#timeout(attempt);
    }, this.#timeoutMs);
    attempt.popupTimer = this.#popupHost.setTimeout(() => {
      void this.#watchPopup(attempt);
    }, Math.min(POPUP_CHECK_INTERVAL_MS, this.#timeoutMs));

    try {
      const response = await this.#api.beginMcpAuthorization(serverId, attempt.controller.signal);
      if (!this.#isCurrent(attempt)) {
        closePopup(popup);
        return;
      }
      if (!isAuthorizationUrl(response.authorization_url)) {
        throw new Error("The server returned an invalid authorization URL.");
      }
      if (isPopupClosed(popup)) {
        attempt.popupClosedAt = this.#popupHost.now();
      } else {
        popup.location.replace(response.authorization_url);
      }
      await this.#poll(attempt);
    } catch (error) {
      if (!this.#isCurrent(attempt) || isAbortError(error)) return;
      await this.#fail(attempt, error);
    }
  }

  async cancelAuthorization(): Promise<void> {
    const attempt = this.#attempt;
    if (!attempt) return;
    await this.#terminate(attempt, "canceled", true);
  }

  async disconnect(serverId: string): Promise<void> {
    if (this.#destroyed || this.#attempt || this.#termination || this.#disconnectController) return;
    const server = this.#snapshot.servers.find((candidate) => candidate.id === serverId);
    if (!server?.authorized || server.auth !== "oauth") return;

    const controller = new AbortController();
    this.#disconnectController = controller;
    this.#update({
      phase: "disconnecting",
      disconnectingServerId: serverId,
      notice: null,
      errorText: "",
    });
    try {
      await this.#api.clearMcpAuthorization(serverId, controller.signal);
      if (this.#destroyed || controller.signal.aborted) return;
      const response = await this.#api.listMcpServers(controller.signal);
      if (this.#destroyed || controller.signal.aborted) return;
      this.#update({
        phase: "idle",
        disconnectingServerId: null,
        servers: response.items,
        notice: "disconnected",
        errorText: "",
      });
    } catch (error) {
      if (this.#destroyed || controller.signal.aborted || isAbortError(error)) return;
      this.#update({ phase: "idle", disconnectingServerId: null, errorText: errorText(error) });
    } finally {
      if (this.#disconnectController === controller) this.#disconnectController = null;
    }
  }

  destroy(): void {
    this.#destroyed = true;
    this.#loadController?.abort();
    this.#loadController = null;
    this.#disconnectController?.abort();
    this.#disconnectController = null;
    const attempt = this.#attempt;
    if (!attempt) return;
    if (!this.#invalidate(attempt)) return;
    void this.#api.cancelPendingMcpAuthorization(attempt.serverId).catch(() => undefined);
  }

  async #poll(attempt: AuthorizationAttempt): Promise<void> {
    if (!this.#isCurrent(attempt)) return;
    if (isPopupClosed(attempt.popup) && attempt.popupClosedAt === null) {
      attempt.popupClosedAt = this.#popupHost.now();
    }

    try {
      const response = await this.#api.listMcpServers(attempt.controller.signal);
      if (!this.#isCurrent(attempt)) return;
      this.#update({ servers: response.items });
      const server = response.items.find((candidate) => candidate.id === attempt.serverId);
      if (server?.authorized) {
        this.#complete(attempt);
        return;
      }
    } catch (error) {
      if (!this.#isCurrent(attempt) || isAbortError(error)) return;
      await this.#fail(attempt, error);
      return;
    }

    if (!this.#isCurrent(attempt)) return;
    const now = this.#popupHost.now();
    if (now >= attempt.deadline) {
      await this.#terminate(attempt, "timeout", true);
      return;
    }
    if (attempt.popupClosedAt !== null && now >= attempt.popupClosedAt + this.#popupCloseGraceMs) {
      await this.#terminate(attempt, "canceled", true);
      return;
    }
    const remainingMs = Math.max(0, attempt.deadline - now);
    const closeGraceRemaining = attempt.popupClosedAt === null
      ? remainingMs
      : Math.max(0, attempt.popupClosedAt + this.#popupCloseGraceMs - now);
    attempt.pollTimer = this.#popupHost.setTimeout(() => {
      void this.#poll(attempt);
    }, Math.min(this.#pollIntervalMs, remainingMs, closeGraceRemaining));
  }

  async #watchPopup(attempt: AuthorizationAttempt): Promise<void> {
    if (!this.#isCurrent(attempt)) return;
    if (isPopupClosed(attempt.popup) && attempt.popupClosedAt === null) {
      attempt.popupClosedAt = this.#popupHost.now();
    }
    const now = this.#popupHost.now();
    if (attempt.popupClosedAt !== null && now >= attempt.popupClosedAt + this.#popupCloseGraceMs) {
      await this.#terminate(attempt, "canceled", true);
      return;
    }
    const remainingMs = Math.max(0, attempt.deadline - now);
    const closeGraceRemaining = attempt.popupClosedAt === null
      ? remainingMs
      : Math.max(0, attempt.popupClosedAt + this.#popupCloseGraceMs - now);
    attempt.popupTimer = this.#popupHost.setTimeout(() => {
      void this.#watchPopup(attempt);
    }, Math.min(POPUP_CHECK_INTERVAL_MS, remainingMs, closeGraceRemaining));
  }

  async #timeout(attempt: AuthorizationAttempt): Promise<void> {
    if (!this.#isCurrent(attempt)) return;
    await this.#terminate(attempt, "timeout", true);
  }

  async #fail(attempt: AuthorizationAttempt, error: unknown): Promise<void> {
    const message = errorText(error);
    const cancellationError = await this.#terminate(attempt, null, true);
    if (this.#destroyed) return;
    this.#update({ errorText: cancellationError ? `${message} (${errorText(cancellationError)})` : message });
  }

  async #terminate(
    attempt: AuthorizationAttempt,
    notice: TerminationNotice | null,
    cancelServer: boolean,
  ): Promise<unknown | null> {
    if (!this.#invalidate(attempt)) return null;
    this.#update({
      phase: cancelServer ? "canceling" : "idle",
      authorizingServerId: cancelServer ? attempt.serverId : null,
      disconnectingServerId: null,
      notice,
      errorText: "",
    });
    if (!cancelServer) return null;
    const cancellation = this.#cancelOnServer(attempt.serverId);
    this.#termination = cancellation;
    try {
      const cancellationError = await cancellation;
      if (!this.#destroyed) {
        this.#update({ phase: "idle", authorizingServerId: null });
      }
      return cancellationError;
    } finally {
      if (this.#termination === cancellation) this.#termination = null;
    }
  }

  async #cancelOnServer(serverId: string): Promise<unknown | null> {
    const controller = new AbortController();
    let timedOut = false;
    const timeout = this.#popupHost.setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, CANCEL_REQUEST_TIMEOUT_MS);
    try {
      await this.#api.cancelPendingMcpAuthorization(serverId, controller.signal);
      return null;
    } catch (error) {
      const reported = timedOut ? new Error("The cancellation request timed out.") : error;
      if (!this.#destroyed) this.#update({ errorText: errorText(reported) });
      return reported;
    } finally {
      this.#popupHost.clearTimeout(timeout);
    }
  }

  #complete(attempt: AuthorizationAttempt): void {
    if (!this.#invalidate(attempt)) return;
    closePopup(attempt.popup);
    this.#update({ phase: "idle", authorizingServerId: null, disconnectingServerId: null, notice: null, errorText: "" });
  }

  #invalidate(attempt: AuthorizationAttempt): boolean {
    if (this.#attempt !== attempt) return false;
    this.#attempt = null;
    attempt.controller.abort();
    if (attempt.pollTimer !== null) this.#popupHost.clearTimeout(attempt.pollTimer);
    if (attempt.popupTimer !== null) this.#popupHost.clearTimeout(attempt.popupTimer);
    if (attempt.timeoutTimer !== null) this.#popupHost.clearTimeout(attempt.timeoutTimer);
    attempt.pollTimer = null;
    attempt.popupTimer = null;
    attempt.timeoutTimer = null;
    closePopup(attempt.popup);
    return true;
  }

  #isCurrent(attempt: AuthorizationAttempt): boolean {
    return !this.#destroyed && this.#attempt === attempt && !attempt.controller.signal.aborted;
  }

  #update(patch: Partial<McpSnapshot>): void {
    if (this.#destroyed) return;
    this.#snapshot = { ...this.#snapshot, ...patch };
    for (const listener of this.#listeners) listener(this.#snapshot);
  }
}

export function browserPopupHost(): McpPopupHost {
  return {
    open: (url, target, features) => {
      if (typeof window === "undefined") return null;
      return window.open(url, target, features) as McpPopup | null;
    },
    setTimeout: (handler, delayMs) => globalThis.setTimeout(handler, delayMs),
    clearTimeout: (handle) => globalThis.clearTimeout(handle),
    now: () => Date.now(),
  };
}

function closePopup(popup: McpPopup): void {
  try {
    if (!popup.closed) popup.close();
  } catch {
    // A closed or inaccessible popup needs no further cleanup.
  }
}

function isPopupClosed(popup: McpPopup): boolean {
  try {
    return popup.closed;
  } catch {
    return true;
  }
}

function isAuthorizationUrl(value: unknown): value is string {
  if (typeof value !== "string" || value.length === 0) return false;
  try {
    const url = new URL(value);
    if (url.username || url.password || url.hash) return false;
    if (url.protocol === "https:") return true;
    if (url.protocol !== "http:") return false;
    const host = url.hostname.replace(/^\[|\]$/g, "").toLowerCase();
    return host === "localhost" || host === "::1" || /^127(?:\.\d{1,3}){3}$/.test(host);
  } catch {
    return false;
  }
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
