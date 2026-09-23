/** Codec for the persisted workspace view (screen restoration). No Svelte imports. */

export const VIEW_STORAGE_KEY = "qcg-view";
export const VIEW_STATE_VERSION = 1;
export const MAX_PERSISTED_TABS = 10;

/** Shared approval-scope gate for confirmation UI (Q1). Single predicate so
 * the component disabled condition and its test cannot diverge. Unknown
 * scopes fail closed (approve disabled).
 */
export function isApproveDisabled(scope: unknown, pendingAction: unknown): boolean {
  return pendingAction !== null || (scope !== "content" && scope !== "invocation");
}

const HEX64 = /^[0-9a-fA-F]{64}$/;

/** Decompose a confirmation id into its digest vs invocation-hash parts (Q1).
 * Content scope is `<node>:<kind>:<operation_digest>` (3 parts),
 * invocation scope appends `:<invocation_hash>` (4 parts). Hex lengths are
 * validated before splitting is trusted: a tail guess without a 64-hex
 * check could label an arbitrary suffix as a digest. Unknown scopes never
 * render empty: they return an explicit unknown-scope state so the panel
 * shows a fail-closed message instead of a blank line. Malformed ids for a
 * known scope return an explicit invalid-id state, never an empty string.
 */
export function describeConfirmId(id: unknown, scope: unknown): string {
  if (typeof id !== "string" || id === "") return "";
  const parts = id.split(":");
  if (scope === "content") {
    const digest = parts[parts.length - 1];
    if (parts.length >= 3 && HEX64.test(digest ?? "")) {
      return `digest ${digest}`;
    }
    return "invalid content id: expected <node>:<kind>:<64hex digest>";
  }
  if (scope === "invocation") {
    const digest = parts[parts.length - 2];
    const invocation = parts[parts.length - 1];
    if (parts.length >= 4 && HEX64.test(digest ?? "") && HEX64.test(invocation ?? "")) {
      return `digest ${digest} · invocation ${invocation}`;
    }
    return "invalid invocation id: expected <node>:<kind>:<64hex digest>:<64hex invocation>";
  }
  return "unknown scope: cannot decompose id";
}

// Only tab order and selection persist. Question answers and form drafts may
// carry secrets and already live server-side once submitted, so they are never
// written to localStorage.
export interface PersistedView {
  version: number;
  selected: string;
  tabOrder: string[];
  currentRun: string;
}

export function emptyPersistedView(): PersistedView {
  return {
    version: VIEW_STATE_VERSION,
    selected: "",
    tabOrder: [],
    currentRun: "",
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// Fail-closed schema: only these top-level keys persist. Any other key
// resets the whole view to defaults by returning null. There is no compat.
const ALLOWED_VIEW_KEYS = new Set(["version", "selected", "tabOrder", "currentRun"]);

function cleanStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const seen = new Set<string>();
  const result: string[] = [];
  for (const entry of value) {
    if (typeof entry !== "string" || entry === "" || entry.startsWith("pending-")) continue;
    if (seen.has(entry)) continue;
    seen.add(entry);
    result.push(entry);
    if (result.length >= MAX_PERSISTED_TABS) break;
  }
  return result;
}

/** Decode a stored view. Returns null when the payload is missing or invalid.
 *
 * Fail-closed contract (no compat):
 * - Unknown top-level fields reset the whole section to defaults by
 *   returning null. The caller treats null as "no persisted view" and starts
 *   from `emptyPersistedView()`.
 * - Per-entry sanitization inside `tabOrder` (drop non-strings, empty ids,
 *   `pending-` placeholders, duplicates, cap at `MAX_PERSISTED_TABS`) is
 *   intentional filtering of opaque tab ids, not schema compat, and is
 *   covered by tests below.
 * - A `currentRun` that is not an open tab returns null for the same reason:
 *   restoring a selection outside the tab order would fork the UI state.
 */
export function decodeView(raw: string | null | undefined): PersistedView | null {
  if (!raw) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!isRecord(parsed) || parsed.version !== VIEW_STATE_VERSION) return null;
  for (const key of Object.keys(parsed)) {
    if (!ALLOWED_VIEW_KEYS.has(key)) return null;
  }
  const view = emptyPersistedView();
  view.selected = typeof parsed.selected === "string" ? parsed.selected : "";
  view.tabOrder = cleanStringList(parsed.tabOrder);
  view.currentRun = typeof parsed.currentRun === "string" ? parsed.currentRun : "";
  if (view.currentRun.startsWith("pending-")) view.currentRun = "";
  if (view.currentRun !== "" && !view.tabOrder.includes(view.currentRun)) return null;
  return view;
}

/** Encode a view. Tab order is capped, so the payload stays far below quota. */
export function encodeView(view: PersistedView): string {
  const cleaned: PersistedView = {
    version: VIEW_STATE_VERSION,
    selected: view.selected,
    tabOrder: cleanStringList(view.tabOrder),
    currentRun: view.currentRun,
  };
  return JSON.stringify(cleaned);
}
