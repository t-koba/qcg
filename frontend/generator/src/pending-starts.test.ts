import { afterEach, describe, expect, it, vi } from "vitest";
import {
  loadPendingStarts,
  PENDING_STARTS_STORAGE_KEY,
  removePendingStart,
  savePendingStart,
} from "./pending-starts";

function memoryStorage(): Storage {
  const values = new Map<string, string>();
  return {
    get length() { return values.size; },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => [...values.keys()][index] ?? null,
    removeItem: (key) => { values.delete(key); },
    setItem: (key, value) => { values.set(key, String(value)); },
  };
}

afterEach(() => vi.unstubAllGlobals());

describe("pending start retry persistence", () => {
  it("keeps the original body and reviewed hash paired with the idempotency key", () => {
    vi.stubGlobal("sessionStorage", memoryStorage());
    const operation = {
      startKey: "operation-1",
      generatorId: "demo",
      inputs: { prompt: "draft" },
      packageSha256: "a".repeat(64),
    };
    expect(savePendingStart(operation)).toBe(true);
    expect(loadPendingStarts()).toEqual([operation]);
    removePendingStart(operation.startKey);
    expect(loadPendingStarts()).toEqual([]);
    expect(sessionStorage.getItem(PENDING_STARTS_STORAGE_KEY)).toBeNull();
  });

  it("discards malformed or unknown persisted records", () => {
    const storage = memoryStorage();
    vi.stubGlobal("sessionStorage", storage);
    storage.setItem(PENDING_STARTS_STORAGE_KEY, JSON.stringify([{
      startKey: "operation-1",
      generatorId: "demo",
      inputs: {},
      packageSha256: "not-a-sha",
    }]));
    expect(loadPendingStarts()).toEqual([]);
    expect(storage.getItem(PENDING_STARTS_STORAGE_KEY)).toBeNull();
  });
});
