import { Worker } from "node:worker_threads";
import { createRequire } from "node:module";
import type Database from "better-sqlite3";
import { WAL_AUTOCHECKPOINT_PAGES } from "./connection.js";
import { cooperativeReadFile } from "../domain/execution-candidate-reader.js";
import { recordSlowDiagnostic } from "../domain/sync-site-wrap.js";

/** How often the worker checkpoints the WAL (PASSIVE). Short enough that the WAL stays near the size the commit-time
 *  autocheckpoint kept it at (1000 pages, ~4 MB). */
export const WAL_CHECKPOINT_INTERVAL_MS = 10_000;
/** A checkpoint still unanswered after this long gives the checkpoint back to the commits (see stall below). */
export const WAL_CHECKPOINT_STALL_MS = 60_000;
/** How long stop() waits for the worker to exit: a checkpoint in native code is not interrupted by terminate(), and a
 *  slow one (a saturated disk) must not spend the shutdown's whole budget before the recorder's drain. */
export const WAL_CHECKPOINTER_STOP_WAIT_MS = 1000;

// The checkpointer's thread: its own read-write connection to the daemon's database (a checkpoint writes the database
// file, so not readonly), never setting journal_mode (persistent, and it takes a lock). Only PASSIVE: it never waits
// for a writer and never calls the busy handler, so the daemon's commits keep appending to the WAL while it copies;
// RESTART and TRUNCATE would wait for the writer and make the daemon's own commit spin in its busy handler on the loop.
const CHECKPOINTER_SOURCE = String.raw`
  const { parentPort, workerData } = require("node:worker_threads");
  const Database = require(workerData.module);
  const db = new Database(workerData.file, { fileMustExist: true });
  db.pragma("busy_timeout = 0");
  parentPort.on("message", ({ id }) => {
    const started = performance.now();
    try {
      const row = db.pragma("wal_checkpoint(PASSIVE)")[0];
      parentPort.postMessage({ id, busy: row.busy, log: row.log, checkpointed: row.checkpointed,
        ms: performance.now() - started });
    } catch (error) {
      parentPort.postMessage({ id, error: error instanceof Error ? error.message : String(error) });
    }
  });
`;

type Answer = { id: number; busy: number; log: number; checkpointed: number; ms: number } | { id: number; error: string };

export interface WalCheckpointer {
  /** True while the worker checkpoints and the commits do not (wal_autocheckpoint = 0 on the daemon's connection). */
  readonly offLoop: boolean;
  stop(): Promise<void>;
}

/**
 * T-010230 (item 5 of the supervisor's ask): the WAL checkpoint off the daemon's event loop. better-sqlite3 runs the
 * autocheckpoint inside the commit that crosses WAL_AUTOCHECKPOINT_PAGES, on the loop, with its fsync. Here a worker
 * thread with its own connection checkpoints every intervalMs, and only after its first answer does the daemon's
 * connection stop checkpointing on commit (wal_autocheckpoint = 0). Any failure gives the checkpoint back to the
 * commits (WAL_AUTOCHECKPOINT_PAGES) before anything else, so the WAL is never left without a checkpointer: a failed
 * or busy checkpoint, the worker's error or exit, an answer owed for longer than WAL_CHECKPOINT_STALL_MS, and stop(). A good
 * answer later hands it to the worker again. Returns null, and changes nothing, for a memory or non-WAL database.
 */
export function startWalCheckpointer(db: Database.Database, opts: { intervalMs?: number; stallMs?: number } = {}): WalCheckpointer | null {
  const file = cooperativeReadFile(db);
  if (!file) return null;
  const intervalMs = opts.intervalMs ?? WAL_CHECKPOINT_INTERVAL_MS;
  const stallMs = opts.stallMs ?? WAL_CHECKPOINT_STALL_MS;
  const worker = new Worker(CHECKPOINTER_SOURCE, {
    eval: true, execArgv: [],
    workerData: { file, module: createRequire(import.meta.url).resolve("better-sqlite3") },
  });
  worker.unref();
  let offLoop = false;
  let stopped = false;
  let lastAsk = 0;
  let owedSince: number | null = null;

  const backToCommits = (why: string): void => {
    if (!offLoop) return;
    offLoop = false;
    try { db.pragma(`wal_autocheckpoint = ${WAL_AUTOCHECKPOINT_PAGES}`); } catch { /* the connection is gone */ }
    if (!stopped) console.error(`[wal-checkpointer] the commits checkpoint the WAL again: ${why}`);
  };

  worker.on("message", (answer: Answer) => {
    if (answer.id !== lastAsk) return;
    owedSince = null;
    if ("error" in answer) { backToCommits(`checkpoint failed: ${answer.error}`); return; }
    // busy = the checkpoint lock was held by another checkpointer (the commit's own autocheckpoint before the hand-over,
    // or another process): nothing was copied, so it is not an answer that can take the checkpoint off the commits
    if (answer.busy !== 0) { backToCommits("checkpoint busy (another checkpointer holds the lock)"); return; }
    if (stopped) return;
    if (!offLoop) {
      db.pragma("wal_autocheckpoint = 0");
      offLoop = true;
    }
    // Named in the slow-operation log when it took the slow threshold, with the pages a reader's snapshot left behind
    // (log above checkpointed) in the same record.
    recordSlowDiagnostic(answer.ms, { schema: "openrig.wal-checkpoint/v1", utc: new Date().toISOString(),
      phase: "wal_checkpoint", ms: Math.round(answer.ms), busy: answer.busy, log: answer.log,
      checkpointed: answer.checkpointed });
  });
  worker.on("error", (error) => backToCommits(`worker error: ${error.message}`));
  worker.on("exit", (code) => backToCommits(`worker exited (code ${code})`));

  const tick = (): void => {
    if (stopped) return;
    if (owedSince !== null) {
      // One checkpoint at a time; a stalled one gives the checkpoint back to the commits until it answers.
      if (performance.now() - owedSince >= stallMs) backToCommits(`checkpoint unanswered for ${Math.round(stallMs / 1000)} s`);
      return;
    }
    owedSince = performance.now();
    worker.postMessage({ id: ++lastAsk });
  };
  const timer = setInterval(tick, intervalMs);
  timer.unref();
  tick();

  return {
    get offLoop() { return offLoop; },
    async stop() {
      if (stopped) return;
      stopped = true;
      clearInterval(timer);
      backToCommits("stopped");
      // The commits checkpoint again before any wait; the worker's exit is waited for a bounded time only.
      let waitCap: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([worker.terminate(), new Promise<void>((resolve) => {
        waitCap = setTimeout(resolve, WAL_CHECKPOINTER_STOP_WAIT_MS);
        waitCap.unref();
      })]).finally(() => clearTimeout(waitCap));
    },
  };
}
