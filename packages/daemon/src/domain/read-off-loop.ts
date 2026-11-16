import { Worker } from "node:worker_threads";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import type Database from "better-sqlite3";
import { cooperativeReadFile } from "./execution-candidate-reader.js";
import { recordSlowDiagnostic, runAsyncSite, runAsyncSiteWhenSlow } from "./sync-site-wrap.js";
import type { ReadJobArgs, ReadJobName, ReadJobResult } from "./read-off-loop-jobs.js";

// Heavy reads (the ps and rig-summary projections, the usage series, the wake ladder's prompt candidates) run here on
// a read-only connection in one long-lived worker thread, so a read that takes seconds under load holds that thread
// and never the daemon's event loop, as the process table's /proc reader does. The worker imports the same read
// functions the synchronous path calls, so both paths compute one projection from one code. Each job reads inside one
// read transaction, so a multi-query projection sees one snapshot, as the synchronous read on the main handle does.
//
// The read stays synchronous on the main handle when a second connection cannot serve it (cooperativeReadFile):
// a memory database (every test that does not opt in), a temporary database, a non-WAL file, or a read asked inside
// an open transaction, whose uncommitted writes only the main handle sees. It also falls back to the synchronous read
// when the worker cannot answer (it failed to start, to import, or to open the file, or a job threw), so a failure
// here never changes what a caller gets; it only costs the loop the read it would have paid anyway.
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

interface Owed { worker: Worker; resolve: (value: unknown) => void; reject: (error: Error) => void }

let reader: Worker | null = null;
let lastAsk = 0;
const owed = new Map<number, Owed>();
let enabled = true;

/** Tests turn the worker off to compare against the synchronous read on the same file, and back on. */
export function setReadOffLoopEnabledForTest(next: boolean): void {
  enabled = next;
}

/** Ends the worker (tests, shutdown); the next read starts another. */
export async function stopReadOffLoopWorker(): Promise<void> {
  const worker = reader;
  reader = null;
  await worker?.terminate();
}

function startReader(): Worker {
  const extension = import.meta.url.endsWith(".ts") ? "ts" : "js";
  const worker = new Worker(SOURCE, {
    execArgv: [],
    workerData: {
      url: new URL(`./read-off-loop-jobs.${extension}`, import.meta.url).href,
      parent: import.meta.url,
      loaderUrl: extension === "ts" ? pathToFileURL(createRequire(import.meta.url).resolve("tsx/esm/api")).href : undefined,
    },
  });
  worker.unref();
  worker.on("message", ({ id, result, error }: { id: number; result?: unknown; error?: string }) => {
    const ask = owed.get(id);
    if (!ask) return;
    owed.delete(id);
    if ([...owed.values()].every((other) => other.worker !== worker)) worker.unref();
    if (error !== undefined) ask.reject(new Error(error));
    else ask.resolve(result);
  });
  // A worker lost fails what it owed, and only that; the next read starts another.
  const lost = (error: Error) => {
    if (reader === worker) reader = null;
    for (const [id, ask] of owed) {
      if (ask.worker !== worker) continue;
      owed.delete(id);
      ask.reject(error);
    }
  };
  worker.on("error", lost);
  worker.on("exit", (code) => lost(new Error(`the read worker exited with code ${code}`)));
  return worker;
}

function askWorker<J extends ReadJobName>(file: string, job: J, args: ReadJobArgs<J>): Promise<ReadJobResult<J>> {
  const worker = reader ??= startReader();
  const id = ++lastAsk;
  // Idle, the worker keeps no process up; while an answer is owed it does.
  worker.ref();
  return new Promise<ReadJobResult<J>>((resolve, reject) => {
    owed.set(id, { worker, resolve: resolve as (value: unknown) => void, reject });
    try {
      worker.postMessage({ id, file, job, args });
    } catch (error) {
      owed.delete(id);
      if ([...owed.values()].every((other) => other.worker !== worker)) worker.unref();
      reject(error instanceof Error ? error : new Error(String(error)));
    }
  });
}

let reportedFailure = false;
/** When each site last offered a fallback record: at most one a minute, so a worker that stays broken keeps showing
 *  in the slow-operation log without a record per request. */
const lastFallbackRecord = new Map<string, number>();
const FALLBACK_RECORD_INTERVAL_MS = 60_000;

export interface ReadOffLoopOptions {
  /** The slow-operation site that names this read. */
  site: string;
  /** "always" records every read (a per-tick read); "whenSlow" only one at the recorder's slow threshold (a per-request
   *  read, where a record per call would crowd the bounded log). */
  record: "always" | "whenSlow";
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
  const file = enabled ? cooperativeReadFile(db) : null;
  if (file === null) return sync();
  try {
    const run = () => askWorker(file, job, args);
    return await (options.record === "always" ? runAsyncSite(options.site, run) : runAsyncSiteWhenSlow(options.site, run));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!reportedFailure) {
      reportedFailure = true;
      console.error(`[read-off-loop] ${options.site}: the read worker could not answer; reading on the main thread instead (reported once): ${message}`);
    }
    const at = performance.now();
    if (at - (lastFallbackRecord.get(options.site) ?? -Infinity) >= FALLBACK_RECORD_INTERVAL_MS) {
      lastFallbackRecord.set(options.site, at);
      recordSlowDiagnostic(Number.POSITIVE_INFINITY, { site: `${options.site}.fallback`, error: message.split("\n")[0]!.slice(0, 200) });
    }
    return sync();
  }
}
