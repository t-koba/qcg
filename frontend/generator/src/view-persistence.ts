/** Codec for the persisted workspace view (screen restoration). No Svelte imports. */

export const VIEW_STORAGE_KEY = "qcg-view";
export const VIEW_STATE_VERSION = 1;
export const MAX_PERSISTED_TABS = 10;

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

/** Decode a stored view. Returns null when the payload is missing or invalid. */
export function decodeView(raw: string | null | undefined): PersistedView | null {
  if (!raw) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!isRecord(parsed) || parsed.version !== VIEW_STATE_VERSION) return null;
  const view = emptyPersistedView();
  view.selected = typeof parsed.selected === "string" ? parsed.selected : "";
  view.tabOrder = cleanStringList(parsed.tabOrder);
  view.currentRun = typeof parsed.currentRun === "string" ? parsed.currentRun : "";
  if (view.currentRun.startsWith("pending-")) view.currentRun = "";
  if (view.currentRun !== "" && !view.tabOrder.includes(view.currentRun)) return null;
  // Legacy answer and form maps are ignored; the trailing persist rewrites the
  // payload without them.
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
