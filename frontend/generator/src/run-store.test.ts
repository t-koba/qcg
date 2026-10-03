import { afterEach, expect, it, vi } from "vitest";
import { RunStore } from "./run-store.svelte";
import type { RunSnapshot } from "./api/client";
const snapshot = (seq: number, state: RunSnapshot["state"]): RunSnapshot => ({
  run_id: "test",
  generator_id: "demo",
  seq,
  state,
  queue_position: null,
  queue_position_quality: "unavailable",
  priority: 0,
  labels: {},
  contract_sha256: null,
  metrics: null,
  parent_run_id: null,
  queued_at: null,
});
afterEach(() => vi.useRealTimers());
it("snapshot requests coalesce and concurrent updates add one followup", async () => {
  const store = new RunStore();
  store.applySnapshot(snapshot(1, "running"));
  const resolvers: Array<(value: RunSnapshot) => void> = [];
  store.api.get = vi.fn(
    () => new Promise((resolve) => resolvers.push(resolve)),
  ) as typeof store.api.get;
  const first = store.refreshRun("test");
  await Promise.resolve();
  const others = Array.from({ length: 64 }, () => store.refreshRun("test"));
  expect(store.api.get).toHaveBeenCalledTimes(1);
  resolvers[0](snapshot(2, "running"));
  await vi.waitFor(() => expect(store.api.get).toHaveBeenCalledTimes(2));
  resolvers[1](snapshot(3, "succeeded"));
  await Promise.all([first, ...others]);
  expect(store.api.get).toHaveBeenCalledTimes(2);
  expect(store.tabs.test.runState).toBe("succeeded");
});
it("terminal notification settles immediately and recovers snapshot failure; reader releases", async () => {
  vi.useFakeTimers();
  const store = new RunStore();
  store.applySnapshot(snapshot(1, "running"));
  store.currentRun = "test";
  let reads = 0;
  const cancel = vi.fn(async () => {});
  const releaseLock = vi.fn();
  const reader = {
    cancel,
    releaseLock,
    read: vi.fn(async () =>
      ++reads === 1
        ? {
            done: false,
            value: new TextEncoder().encode(
              'data: {"seq":2,"kind":"run_finished","data":{"status":"success"}}\n\n',
            ),
          }
        : { done: true },
    ),
  };
  store.api.events = vi.fn(async () => ({
    body: { getReader: () => reader },
  })) as unknown as typeof store.api.events;
  store.api.get = vi
    .fn()
    .mockResolvedValueOnce(snapshot(1, "running"))
    .mockRejectedValueOnce(new Error("temporary snapshot failure"))
    .mockResolvedValue(snapshot(2, "succeeded")) as typeof store.api.get;
  store.ensureSubscribed("test");
  await vi.advanceTimersByTimeAsync(50);
  expect(store.tabs.test.runState).toBe("succeeded");
  await vi.advanceTimersByTimeAsync(5000);
  expect(cancel).toHaveBeenCalledTimes(1);
  expect(releaseLock).toHaveBeenCalledTimes(1);
  expect(store.tabs.test.lastSnapshotSeq).toBe(2);
  expect(vi.mocked(store.api.get).mock.calls.length).toBeGreaterThanOrEqual(3);
  expect(store.api.events).toHaveBeenCalledTimes(1);
});

it("an old subscription cleanup preserves the replacement subscription", async () => {
  const store = new RunStore();
  store.applySnapshot(snapshot(1, "running"));
  store.currentRun = "test";
  const completions: Array<(value: { done: boolean }) => void> = [];
  const readers = [0, 1].map(() => ({
    read: vi.fn(() => new Promise((resolve) => completions.push(resolve))),
    cancel: vi.fn(async () => {}),
    releaseLock: vi.fn(),
  }));
  let requests = 0;
  store.api.events = vi.fn(async () => ({
    body: { getReader: () => readers[requests++] },
  })) as unknown as typeof store.api.events;
  store.api.get = vi
    .fn()
    .mockResolvedValue(snapshot(1, "running")) as typeof store.api.get;
  store.ensureSubscribed("test");
  await vi.waitFor(() => expect(completions).toHaveLength(1));
  store.closeTab("test");
  store.applySnapshot(snapshot(1, "running"));
  store.currentRun = "test";
  store.ensureSubscribed("test");
  await vi.waitFor(() => expect(completions).toHaveLength(2));
  completions[0]({ done: true });
  await vi.waitFor(() => expect(readers[0].releaseLock).toHaveBeenCalledOnce());
  store.ensureSubscribed("test");
  expect(store.api.events).toHaveBeenCalledTimes(2);
  store.closeTab("test");
  completions[1]({ done: true });
  await vi.waitFor(() => expect(readers[1].releaseLock).toHaveBeenCalledOnce());
});
