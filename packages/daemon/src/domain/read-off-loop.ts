import { Worker } from "node:worker_threads";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import type Database from "better-sqlite3";
import { cooperativeReadFile } from "./execution-candidate-reader.js";
import { recordSlowDiagnostic, runAsyncSite, runAsyncSiteWhenSlow } from "./sync-site-wrap.js";
import type { ReadJobArgs, ReadJobName, ReadJobResult } from "./read-off-loop-jobs.js";

// Heavy reads (the ps and rig-summary projections, the usage series, the wake ladder's prompt candidates) run here on
// a read-only connection in long-lived worker threads (one per lane, below), so a read that takes seconds under load
// holds that thread and never the daemon's event loop, as the process table's /proc reader does. The worker imports the same read
// functions the synchronous path calls, so both paths compute one projection from one code. Each job reads inside one
// read transaction, so a multi-query projection sees one snapshot, as the synchronous read on the main handle does.
//
// The read stays synchronous on the main handle when a second connection cannot serve it (cooperativeReadFile):
// a memory database (every test that does not opt in), a temporary database, a non-WAL file, or a read asked inside
// an open transaction, whose uncommitted writes only the main handle sees. It also falls back to the synchronous read
// when the worker cannot answer (it failed to start, to import, or to open the file, a job threw, or the queue below
// refused or timed out the job), so a failure here never changes what a caller gets; it only costs the loop the read
// it would have paid anyway.
const SOURCE = new URL(`data:text/javascript,${encodeURIComponent(`
import { parentPort, workerData } from "node:worker_threads";
let jobs;
parentPort.on("message", async ({ id, file, job, args }) => {
  try {
    // Assigned before any await, so reads that arrive while the module loads share one import.
    // A failed load is dropped, so the next read tries again rather than failing for the worker's whole life.
    jobs ??= (workerData.url.endsWith(".ts")
      ? import(workerData.loaderUrl).then((api) => api.tsImport(workerData.url, workerData.parent))
      : import(workerData.url)).catch((error) => { jobs = undefined; throw error; });
    parentPort.postMessage({ id, result: (await jobs).runReadJob(file, job, args) });
  } catch (error) {
    parentPort.postMessage({ id, error: error instanceof Error ? (error.stack ?? error.message) : String(error) });
  }
});
`)}`);

// T-010440 (SH-1 of REVISAO-T-010374): the jobs wait HERE, in the daemon's own queues, one queue and one worker per
// lane, and each worker gets one job at a time, so every queue has an order and every job a deadline:
// - lane "tick" (the wake ladder's per-tick prompt candidates, a read recorded "always") has its own worker, so the
//   ladder's tick never waits behind a request, not even one already running; lane "request" (/api/ps,
//   /api/rigs/summary, /usage/series) has the other;
// - above MAX_WAITING request jobs waiting, a new request job is refused at once and its caller reads synchronously on
//   the main handle, the path every read already has when the worker cannot answer (a 503 would change what the
//   routes answer; the ceiling bounds the queue, not the reads); the tick lane is one job per tick and never refused;
// - a job waits at most JOB_DEADLINE_MS in its queue (then it leaves the queue), and runs at most JOB_DEADLINE_MS
//   from the moment its worker gets it (then the worker is presumed stuck, is terminated, and the next job starts
//   another); either way its caller reads synchronously, and no promise stays pending on a worker that stopped
//   answering. The run's deadline counts from the dispatch, so a job that waited long never takes a healthy worker
//   down for the time it spent in the queue;
// - a message from the worker that cannot be deserialized (messageerror) is a lost worker, like an error or an exit.
const JOB_DEADLINE_MS = 15_000;
const MAX_WAITING = 16;

type Lane = "tick" | "request";
interface Job {
  id: number;
  file: string;
  job: ReadJobName;
  args: unknown;
  lane: Lane;
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout> | null;
  worker: Worker | null;
}
interface LaneState {
  reader: Worker | null;
  running: Job | null;
  waiting: Job[];
}

const lanes: Record<Lane, LaneState> = {
  tick: { reader: null, running: null, waiting: [] },
  request: { reader: null, running: null, waiting: [] },
};
let lastAsk = 0;
let enabled = true;
/** Set by the daemon's shutdown: every read from then on is synchronous, and no worker starts again. */
let closed = false;
let deadlineMs = JOB_DEADLINE_MS;
let maxWaiting = MAX_WAITING;
let source: URL = SOURCE;

/** Tests turn the worker off to compare against the synchronous read on the same file, and back on. */
export function setReadOffLoopEnabledForTest(next: boolean): void {
  enabled = next;
}

/** Tests shorten the deadline, lower the ceiling, or run another worker script (one that never answers, say); `null`
 *  restores the default. The source applies to the next worker started, the deadline to the next ask or dispatch,
 *  the ceiling to the next ask. */
export function setReadOffLoopLimitsForTest(next: { deadlineMs?: number | null; maxWaiting?: number | null; source?: URL | null }): void {
  if (next.deadlineMs !== undefined) deadlineMs = next.deadlineMs ?? JOB_DEADLINE_MS;
  if (next.maxWaiting !== undefined) maxWaiting = next.maxWaiting ?? MAX_WAITING;
  if (next.source !== undefined) source = next.source ?? SOURCE;
}

/** The daemon's shutdown, before it waits for the readers' consumers (the wake ladder's tick): the workers go, what
 *  they ran and what waited read synchronously, and every read after this is synchronous, so the shutdown never waits
 *  a job's deadline on a stuck worker nor starts another one. */
export async function closeReadOffLoop(): Promise<void> {
  closed = true;
  await stopReadOffLoopWorker();
  flushReadOffLoopFallbacks();
}

/** Tests reopen after closeReadOffLoop. */
export function reopenReadOffLoopForTest(): void {
  closed = false;
}

/** The worker serving a lane now, if any (tests). */
export function readOffLoopWorkerForTest(lane: Lane = "request"): Worker | null {
  return lanes[lane].reader;
}

function settle(job: Job, outcome: { value: unknown } | { error: Error }): void {
  if (job.timer !== null) clearTimeout(job.timer);
  job.timer = null;
  if ("error" in outcome) job.reject(outcome.error);
  else job.resolve(outcome.value);
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function arm(job: Job): void {
  if (job.timer !== null) clearTimeout(job.timer);
  const timer = setTimeout(() => expire(job), deadlineMs);
  timer.unref?.();
  job.timer = timer;
}

/** Ends the workers (tests, shutdown) and fails what they ran and what waited for them, so those callers read
 *  synchronously; the next read starts another. */
export async function stopReadOffLoopWorker(): Promise<void> {
  const stopped = new Error("the read worker was stopped");
  const workers: Worker[] = [];
  for (const state of Object.values(lanes)) {
    if (state.reader !== null) workers.push(state.reader);
    state.reader = null;
    for (const job of state.waiting.splice(0)) settle(job, { error: stopped });
    if (state.running !== null) {
      const job = state.running;
      state.running = null;
      settle(job, { error: stopped });
    }
  }
  await Promise.all(workers.map((worker) => worker.terminate()));
}

/** The lane's worker is gone (an error, an exit, a message that cannot be read): it fails the job it was running, and
 *  only that one; the jobs waiting go to the next worker. */
function lose(state: LaneState, worker: Worker, error: Error): void {
  if (state.reader === worker) state.reader = null;
  if (state.running !== null && state.running.worker === worker) {
    const job = state.running;
    state.running = null;
    settle(job, { error });
  }
  dispatch(state);
}

function startReader(state: LaneState): Worker {
  const extension = import.meta.url.endsWith(".ts") ? "ts" : "js";
  const worker = new Worker(source, {
    execArgv: [],
    workerData: {
      url: new URL(`./read-off-loop-jobs.${extension}`, import.meta.url).href,
      parent: import.meta.url,
      loaderUrl: extension === "ts" ? pathToFileURL(createRequire(import.meta.url).resolve("tsx/esm/api")).href : undefined,
    },
  });
  worker.unref();
  worker.on("message", ({ id, result, error }: { id: number; result?: unknown; error?: string }) => {
    const job = state.running;
    if (job === null || job.worker !== worker || job.id !== id) return;
    state.running = null;
    worker.unref();
    settle(job, error !== undefined ? { error: new Error(error) } : { value: result });
    dispatch(state);
  });
  worker.on("messageerror", (error: Error) => {
    void worker.terminate();
    lose(state, worker, new Error(`the read worker sent a message that could not be read: ${error.message}`));
  });
  worker.on("error", (error) => lose(state, worker, error));
  worker.on("exit", (code) => lose(state, worker, new Error(`the read worker exited with code ${code}`)));
  return worker;
}

/** Hands the lane's next waiting job to its worker, when none is running. */
function dispatch(state: LaneState): void {
  if (state.running !== null) return;
  if (closed) {
    const stopped = new Error("the read worker was closed");
    for (const job of state.waiting.splice(0)) settle(job, { error: stopped });
    return;
  }
  const job = state.waiting.shift();
  if (job === undefined) return;
  let worker: Worker;
  try {
    worker = state.reader ??= startReader(state);
  } catch (error) {
    settle(job, { error: asError(error) });
    dispatch(state);
    return;
  }
  state.running = job;
  job.worker = worker;
  // the run's own deadline, from now: the time the job spent waiting never counts against the worker
  arm(job);
  // Idle, the worker keeps no process up; while an answer is owed it does.
  worker.ref();
  try {
    worker.postMessage({ id: job.id, file: job.file, job: job.job, args: job.args });
  } catch (error) {
    state.running = null;
    worker.unref();
    settle(job, { error: asError(error) });
    dispatch(state);
  }
}

/** The job's deadline passed: out of the queue if it still waits; if it runs, the worker is presumed stuck and goes. */
function expire(job: Job): void {
  job.timer = null;
  const state = lanes[job.lane];
  const at = state.waiting.indexOf(job);
  if (at >= 0) {
    state.waiting.splice(at, 1);
    settle(job, { error: new Error(`the read job ${job.job} waited ${deadlineMs} ms for the read worker`) });
    return;
  }
  if (state.running !== job) return;
  const worker = job.worker!;
  state.running = null;
  if (state.reader === worker) state.reader = null;
  worker.unref();
  void worker.terminate();
  settle(job, { error: new Error(`the read worker did not answer ${job.job} in ${deadlineMs} ms; it was stopped`) });
  dispatch(state);
}

function askWorker<J extends ReadJobName>(file: string, job: J, args: ReadJobArgs<J>, lane: Lane): Promise<ReadJobResult<J>> {
  const state = lanes[lane];
  if (lane === "request" && state.waiting.length >= maxWaiting) {
    return Promise.reject(new Error(`the read worker's queue is full (${maxWaiting} requests waiting)`));
  }
  return new Promise<ReadJobResult<J>>((resolve, reject) => {
    const ask: Job = { id: ++lastAsk, file, job, args, lane, resolve: resolve as (value: unknown) => void, reject, timer: null, worker: null };
    // the wait's deadline; the dispatch arms the run's
    arm(ask);
    state.waiting.push(ask);
    dispatch(state);
  });
}

/** Sites that already said on the console that their read fell back: once per site, not once per process. */
const reportedSites = new Set<string>();
/** When each site last offered a fallback record (at most one a minute, so a worker that stays broken keeps showing in
 *  the slow-operation log without a record per request), and how many fallbacks it had since: the record carries that
 *  count, so the log says how often each site fell back, not only that it did. The fallbacks inside the minute are
 *  flushed when it ends (a trailing record), and at shutdown, so a burst followed by a recovery is still counted. */
const lastFallbackRecord = new Map<string, number>();
const fallbacksSinceRecord = new Map<string, { count: number; error: string }>();
const fallbackFlush = new Map<string, ReturnType<typeof setTimeout>>();
const FALLBACK_RECORD_INTERVAL_MS = 60_000;
let fallbackIntervalMs = FALLBACK_RECORD_INTERVAL_MS;

/** Tests shorten the minute between fallback records; `null` restores it. */
export function setReadOffLoopFallbackIntervalForTest(ms: number | null): void {
  fallbackIntervalMs = ms ?? FALLBACK_RECORD_INTERVAL_MS;
}

function recordFallbacks(site: string, at: number): void {
  const flush = fallbackFlush.get(site);
  if (flush !== undefined) clearTimeout(flush);
  fallbackFlush.delete(site);
  const pending = fallbacksSinceRecord.get(site);
  fallbacksSinceRecord.delete(site);
  if (pending === undefined || pending.count === 0) return;
  lastFallbackRecord.set(site, at);
  recordSlowDiagnostic(Number.POSITIVE_INFINITY, { site: `${site}.fallback`, fallbacks: pending.count, error: pending.error });
}

/** Offers every site's fallbacks not yet recorded (the daemon's shutdown, tests). */
export function flushReadOffLoopFallbacks(): void {
  for (const site of [...fallbacksSinceRecord.keys()]) recordFallbacks(site, performance.now());
}

function countFallback(site: string, message: string): void {
  const pending = fallbacksSinceRecord.get(site) ?? { count: 0, error: "" };
  pending.count += 1;
  pending.error = message.split("\n")[0]!.slice(0, 200);
  fallbacksSinceRecord.set(site, pending);
  const at = performance.now();
  const since = at - (lastFallbackRecord.get(site) ?? -Infinity);
  if (since >= fallbackIntervalMs) {
    recordFallbacks(site, at);
  } else if (!fallbackFlush.has(site)) {
    const timer = setTimeout(() => recordFallbacks(site, performance.now()), fallbackIntervalMs - since);
    timer.unref?.();
    fallbackFlush.set(site, timer);
  }
}

export interface ReadOffLoopOptions {
  /** The slow-operation site that names this read. */
  site: string;
  /** "always" records every read (a per-tick read); "whenSlow" only one at the recorder's slow threshold (a per-request
   *  read, where a record per call would crowd the bounded log). */
  record: "always" | "whenSlow";
  /** "tick" runs on its own worker, never behind a "request", and is never refused for a full queue. By default a read
   *  recorded "always" (a per-tick read) is "tick", and a "whenSlow" one is "request". */
  lane?: Lane;
}

/**
 * Runs one named read job off the event loop when the database allows it, else `sync` (the same read on the main
 * handle, with its own instrumentation). Resolves to what `sync` would return.
 */
export async function readOffLoop<J extends ReadJobName>(
  db: Database.Database,
  job: J,
  args: ReadJobArgs<J>,
  sync: () => ReadJobResult<J>,
  options: ReadOffLoopOptions,
): Promise<ReadJobResult<J>> {
  const file = enabled && !closed ? cooperativeReadFile(db) : null;
  if (file === null) return sync();
  try {
    const lane = options.lane ?? (options.record === "always" ? "tick" : "request");
    const run = () => askWorker(file, job, args, lane);
    return await (options.record === "always" ? runAsyncSite(options.site, run) : runAsyncSiteWhenSlow(options.site, run));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!reportedSites.has(options.site)) {
      reportedSites.add(options.site);
      console.error(`[read-off-loop] ${options.site}: the read worker could not answer; reading on the main thread instead (reported once per site): ${message}`);
    }
    countFallback(options.site, message);
    return sync();
  }
}
