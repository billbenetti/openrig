// The read worker's queue (T-010440, SH-1 of REVISAO-T-010374): every job has a deadline and a stuck worker is
// replaced, the request lane has a ceiling, the wake ladder's per-tick read goes ahead of the requests, a message that
// cannot be read is a lost worker, and the fallbacks are counted per site in the slow-operation log. Each case runs a
// stand-in worker script (one that never answers, answers late, or fails) against a real WAL file, so the queue is
// what is exercised, and the caller always gets the synchronous read when the worker does not answer.
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import {
  closeReadOffLoop,
  flushReadOffLoopFallbacks,
  readOffLoop,
  readOffLoopWorkerForTest,
  reopenReadOffLoopForTest,
  setReadOffLoopFallbackIntervalForTest,
  setReadOffLoopLimitsForTest,
  stopReadOffLoopWorker,
} from "../src/domain/read-off-loop.js";
import { configureSyncSiteRecorder } from "../src/domain/sync-site-wrap.js";
import { readPromptRefusalCandidatesOffLoop } from "../src/domain/queue-wake-ladder.js";

/** A worker script as a data: URL. */
function script(body: string): URL {
  return new URL(`data:text/javascript,${encodeURIComponent(`import { parentPort } from "node:worker_threads";\n${body}`)}`);
}
/** Never answers: a worker that hangs on a read. */
const STUCK = script(`parentPort.on("message", () => {});`);
/** Answers each job with its args.tag after args.ms. */
const LATE = script(`parentPort.on("message", ({ id, args }) => setTimeout(() => parentPort.postMessage({ id, result: args.tag }), args.ms ?? 0));`);
/** Hangs on the job tagged "hang", answers the others with their tag. */
const HANGS = script(`parentPort.on("message", ({ id, args }) => { if (args.tag !== "hang") parentPort.postMessage({ id, result: args.tag }); });`);
/** Fails every job. */
const FAILS = script(`parentPort.on("message", ({ id }) => parentPort.postMessage({ id, error: "boom" }));`);

type Read = Promise<unknown>;
/** One request-lane read through the queue (the job name only types the call; the stand-in worker ignores it). */
function read(db: Database.Database, args: Record<string, unknown>, sync: () => unknown, site = "test.read", lane?: "tick" | "request"): Read {
  return readOffLoop(db, "rigSummaries", args as never, sync as never, { site, record: "whenSlow", lane });
}
/** Resolves to "pending" if `p` has not settled within `ms`. */
function within<T>(p: Promise<T>, ms: number): Promise<T | "pending"> {
  return Promise.race([p, new Promise<"pending">((resolve) => setTimeout(() => resolve("pending"), ms))]);
}

describe("the read worker's queue", () => {
  let dir: string;
  let db: Database.Database;

  beforeAll(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "read-off-loop-queue-"));
    db = createDb(path.join(dir, "openrig.sqlite"));
    migrate(db, ALL_MIGRATIONS);
  });

  afterEach(async () => {
    await stopReadOffLoopWorker();
    reopenReadOffLoopForTest();
    setReadOffLoopLimitsForTest({ deadlineMs: null, maxWaiting: null, source: null });
    flushReadOffLoopFallbacks();
    setReadOffLoopFallbackIntervalForTest(null);
    configureSyncSiteRecorder(undefined);
    vi.restoreAllMocks();
  });

  afterAll(() => {
    db.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("a job the worker does not answer in time falls back to the synchronous read, and the stuck worker is replaced", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setReadOffLoopLimitsForTest({ source: STUCK, deadlineMs: 150 });
    const first = read(db, {}, () => "sync-1");
    // the ask dispatches at once: the worker is up before the deadline can run
    const stuck = readOffLoopWorkerForTest()!;
    expect(stuck).not.toBeNull();
    const exited = new Promise((resolve) => stuck.once("exit", resolve));
    expect(await within(first, 3000)).toBe("sync-1");
    // the stuck worker was terminated, not only forgotten
    expect(await within(exited, 3000)).not.toBe("pending");
    // the stuck worker is gone; the next read starts another
    expect(readOffLoopWorkerForTest()).not.toBe(stuck);
    const second = read(db, {}, () => "sync-2");
    const next = readOffLoopWorkerForTest();
    expect(next).not.toBeNull();
    expect(next).not.toBe(stuck);
    expect(await within(second, 3000)).toBe("sync-2");
  });

  it("after a stuck job's deadline, the job waiting behind it is answered by the new worker", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setReadOffLoopLimitsForTest({ source: HANGS, deadlineMs: 300 });
    const stuck = read(db, { tag: "hang" }, () => "sync-hang");
    // the job behind it has a long deadline of its own: only the restart can answer it, and its fallback throws
    setReadOffLoopLimitsForTest({ deadlineMs: 10_000 });
    const behind = read(db, { tag: "behind" }, () => { throw new Error("the job behind fell back"); });
    expect(await within(stuck, 3000)).toBe("sync-hang");
    expect(await within(behind, 5000)).toBe("behind");
  });

  it("a job still waiting behind a stuck one also gets its answer by the deadline: no promise stays pending", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setReadOffLoopLimitsForTest({ source: STUCK, deadlineMs: 200 });
    const reads = [read(db, {}, () => "a"), read(db, {}, () => "b"), read(db, {}, () => "c")];
    expect(await within(Promise.all(reads), 3000)).toEqual(["a", "b", "c"]);
  });

  it("a job that waits past its deadline leaves the queue alone: the worker running a healthy read is kept", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setReadOffLoopLimitsForTest({ source: LATE, deadlineMs: 5000 });
    const running = read(db, { tag: "w-0", ms: 400 }, () => "s-0");
    const worker = readOffLoopWorkerForTest()!;
    setReadOffLoopLimitsForTest({ deadlineMs: 100 });
    const behind = read(db, { tag: "w-1" }, () => "s-1");
    expect(await within(behind, 300)).toBe("s-1");
    expect(readOffLoopWorkerForTest()).toBe(worker);
    expect(await running).toBe("w-0");
  });

  it("above the ceiling, a new request reads synchronously at once, and the ones in the queue keep their place", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setReadOffLoopLimitsForTest({ source: LATE, deadlineMs: 5000, maxWaiting: 2 });
    const running = read(db, { tag: "w-0", ms: 300 }, () => "s-0");
    const queued = [read(db, { tag: "w-1" }, () => "s-1"), read(db, { tag: "w-2" }, () => "s-2")];
    let syncCalled = false;
    const extra = read(db, { tag: "w-3" }, () => { syncCalled = true; return "s-3"; });
    // the one over the ceiling answers before the worker even finishes the first job
    expect(await within(extra, 100)).toBe("s-3");
    expect(syncCalled).toBe(true);
    expect(await Promise.all([running, ...queued])).toEqual(["w-0", "w-1", "w-2"]);
  });

  it("the wake ladder's per-tick read runs on its own worker: never behind a request, not even one running, nor refused", async () => {
    setReadOffLoopLimitsForTest({ source: LATE, deadlineMs: 5000, maxWaiting: 2 });
    const order: string[] = [];
    const track = (p: Read) => p.then((value) => { order.push(String(value)); });
    const reads = [
      // the request runs long (1.5 s) so the tick's worker has room to start even on a busy runner
      track(read(db, { tag: "request-1", ms: 1500 }, () => "sync")),
      track(read(db, { tag: "request-2", ms: 10 }, () => "sync")),
      track(read(db, { tag: "request-3", ms: 10 }, () => "sync")),
      track(readOffLoop(db, "promptRefusalCandidates", { tag: "tick", ms: 10 } as never, (() => "sync") as never,
        { site: "test.tick", record: "always" })),
    ];
    expect(readOffLoopWorkerForTest("tick")).not.toBeNull();
    expect(readOffLoopWorkerForTest("tick")).not.toBe(readOffLoopWorkerForTest("request"));
    await Promise.all(reads);
    expect(order).toEqual(["tick", "request-1", "request-2", "request-3"]);
  });

  it("the wake ladder's prompt candidates go to the tick lane", async () => {
    setReadOffLoopLimitsForTest({ source: STUCK, deadlineMs: 10_000 });
    const candidates = readPromptRefusalCandidatesOffLoop(db);
    expect(readOffLoopWorkerForTest("tick")).not.toBeNull();
    expect(readOffLoopWorkerForTest("request")).toBeNull();
    await stopReadOffLoopWorker();
    expect(await candidates).toEqual([]);
  });

  it("a message from the worker that cannot be read is a lost worker: the running job reads synchronously", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setReadOffLoopLimitsForTest({ source: HANGS, deadlineMs: 10_000 });
    const pending = read(db, { tag: "hang" }, () => "sync-after-messageerror");
    const behind = read(db, { tag: "behind" }, () => { throw new Error("the job behind fell back"); });
    const worker = readOffLoopWorkerForTest()!;
    worker.emit("messageerror", new Error("could not deserialize"));
    expect(await within(pending, 1000)).toBe("sync-after-messageerror");
    // the job waiting goes to the next worker
    expect(await within(behind, 5000)).toBe("behind");
    expect(readOffLoopWorkerForTest()).not.toBe(worker);
  });

  it("the daemon's shutdown closes the reads: a stuck job reads synchronously at once, and no worker starts again", async () => {
    setReadOffLoopLimitsForTest({ source: STUCK, deadlineMs: 10_000 });
    const stuck = read(db, {}, () => "sync-at-shutdown");
    const waitingOne = read(db, {}, () => "sync-waiting-at-shutdown");
    expect(readOffLoopWorkerForTest()).not.toBeNull();
    await closeReadOffLoop();
    expect(await within(Promise.all([stuck, waitingOne]), 1000)).toEqual(["sync-at-shutdown", "sync-waiting-at-shutdown"]);
    expect(await read(db, {}, () => "sync-after-close")).toBe("sync-after-close");
    expect(readOffLoopWorkerForTest()).toBeNull();
  });

  it("the fallbacks are counted per site in the slow-operation log, and each site says so once on the console", async () => {
    const records: Array<Record<string, unknown>> = [];
    configureSyncSiteRecorder({ recordDiagnostic: (record: Record<string, unknown>) => { records.push(record); } } as never);
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const clock = vi.spyOn(performance, "now");
    setReadOffLoopLimitsForTest({ source: FAILS, deadlineMs: 5000 });
    clock.mockReturnValue(1_000_000);
    expect(await read(db, {}, () => "s", "test.count.a")).toBe("s");
    clock.mockReturnValue(1_001_000);
    await read(db, {}, () => "s", "test.count.a");
    await read(db, {}, () => "s", "test.count.a");
    await read(db, {}, () => "s", "test.count.b");
    clock.mockReturnValue(1_062_000);
    await read(db, {}, () => "s", "test.count.a");
    const fallbacks = records.filter((r) => String(r.site).startsWith("test.count."));
    expect(fallbacks.map((r) => [r.site, r.fallbacks])).toEqual([
      ["test.count.a.fallback", 1],
      ["test.count.b.fallback", 1],
      // the minute after: the three since the last record of this site
      ["test.count.a.fallback", 3],
    ]);
    const lines = errors.mock.calls.map((call) => String(call[0]));
    expect(lines.filter((line) => line.includes("test.count.a"))).toHaveLength(1);
    expect(lines.filter((line) => line.includes("test.count.b"))).toHaveLength(1);
  });

  it("the fallbacks inside the minute are recorded when it ends, so a burst followed by a recovery is still counted", async () => {
    const records: Array<Record<string, unknown>> = [];
    configureSyncSiteRecorder({ recordDiagnostic: (record: Record<string, unknown>) => { records.push(record); } } as never);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setReadOffLoopFallbackIntervalForTest(150);
    setReadOffLoopLimitsForTest({ source: FAILS, deadlineMs: 5000 });
    for (let i = 0; i < 4; i++) await read(db, {}, () => "s", "test.burst");
    // the first one at once, the other three are still inside the interval
    expect(records.filter((r) => r.site === "test.burst.fallback").map((r) => r.fallbacks)).toEqual([1]);
    // then the worker recovers: no more fallbacks, and the three are recorded when the interval ends
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(records.filter((r) => r.site === "test.burst.fallback").map((r) => r.fallbacks)).toEqual([1, 3]);
  });

  it("the daemon's shutdown records the fallbacks still pending", async () => {
    const records: Array<Record<string, unknown>> = [];
    configureSyncSiteRecorder({ recordDiagnostic: (record: Record<string, unknown>) => { records.push(record); } } as never);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    setReadOffLoopLimitsForTest({ source: FAILS, deadlineMs: 5000 });
    await read(db, {}, () => "s", "test.shutdown.count");
    await read(db, {}, () => "s", "test.shutdown.count");
    await closeReadOffLoop();
    expect(records.filter((r) => r.site === "test.shutdown.count.fallback").map((r) => r.fallbacks)).toEqual([1, 1]);
  });
});
