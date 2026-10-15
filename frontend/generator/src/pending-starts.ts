/** Session-scoped retry records for ambiguous run starts. The request body and
 * reviewed package identity must stay paired with their idempotency key across
 * reloads; sessionStorage avoids retaining user inputs across browser sessions.
 */
export const PENDING_STARTS_STORAGE_KEY = "pending-starts";
const SHA256 = /^[0-9a-f]{64}$/;

export interface PendingStart {
  startKey: string;
  generatorId: string;
  inputs: Record<string, unknown>;
  packageSha256: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPendingStart(value: unknown): value is PendingStart {
  if (!isRecord(value)) return false;
  const keys = Object.keys(value).sort();
  return keys.length === 4 &&
    keys.join(",") === "generatorId,inputs,packageSha256,startKey" &&
    typeof value.startKey === "string" && value.startKey.length > 0 && value.startKey.length <= 256 &&
    typeof value.generatorId === "string" && value.generatorId.length > 0 &&
    isRecord(value.inputs) &&
    typeof value.packageSha256 === "string" && SHA256.test(value.packageSha256);
}

export function loadPendingStarts(): PendingStart[] {
  if (typeof sessionStorage === "undefined") return [];
  try {
    const raw = sessionStorage.getItem(PENDING_STARTS_STORAGE_KEY);
    if (raw === null) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed) || !parsed.every(isPendingStart)) {
      sessionStorage.removeItem(PENDING_STARTS_STORAGE_KEY);
      return [];
    }
    return parsed;
  } catch {
    return [];
  }
}

/** Returns false rather than sending a start that cannot be retried safely. */
export function savePendingStart(operation: PendingStart): boolean {
  if (typeof sessionStorage === "undefined" || !isPendingStart(operation)) return false;
  const operations = loadPendingStarts().filter((entry) => entry.startKey !== operation.startKey);
  operations.push(operation);
  try {
    sessionStorage.setItem(PENDING_STARTS_STORAGE_KEY, JSON.stringify(operations));
    return true;
  } catch {
    return false;
  }
}

export function removePendingStart(startKey: string): void {
  if (typeof sessionStorage === "undefined") return;
  try {
    const operations = loadPendingStarts().filter((entry) => entry.startKey !== startKey);
    if (operations.length === 0) sessionStorage.removeItem(PENDING_STARTS_STORAGE_KEY);
    else sessionStorage.setItem(PENDING_STARTS_STORAGE_KEY, JSON.stringify(operations));
  } catch {
    // In-memory completion still succeeds; a stale record can only retry the
    // original idempotency key and is therefore safe if storage is unavailable.
  }
}
