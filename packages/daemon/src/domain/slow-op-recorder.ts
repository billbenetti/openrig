import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { Worker } from "node:worker_threads";
import type { Context, MiddlewareHandler } from "hono";
import { transportSenderSession } from "../routes/require-sender-identity.js";

export const SLOW_OPERATION_THRESHOLD_MS = 250;
// 16 MiB x (1 live + 6 rotated) keeps a day or more at the ~2 MB/h a busy daemon writes; 1 MiB x 3 kept ~2.5 h.
export const SLOW_OPERATION_ROTATION_BYTES = 16 * 1024 * 1024;
export const SLOW_OPERATION_ROTATION_COUNT = 6;
// A rotated file past the count goes only once its newest record is a day old (its mtime), so a burst can't cut the
// window under a day; the file ceiling bounds the disk (32 x 16 MiB) whatever the rate (T-009903).
export const SLOW_OPERATION_RETENTION_MS = 24 * 60 * 60 * 1000;
export const SLOW_OPERATION_MAX_FILES = 32;
export const SLOW_OPERATION_LOG_BASENAME = "slow-operations.jsonl";

export interface SlowOperationSnapshot {
  healthy: boolean;
  reason?: string;
  site?: string;
}

/**
 * OPR.0.4.3.21 (51elv2) — the named terminal-failure signal. A rejected
 * flush/close carries this error after the recorder Worker is lost, so a
 * caller can never mistake a settled-but-lost drain for a successful durable
 * one (see the terminal transition in {@link SlowOpRecorder}).
 */
export class SlowOpRecorderTerminatedError extends Error {
  constructor(reason: string) {
    super(`slow-operation recorder terminated: ${reason}`);
    this.name = "SlowOpRecorderTerminatedError";
  }
}

/**
 * OPR.0.4.3.21 (51elv2) — the named acknowledged-write-failure signal. The
 * Worker is still alive (distinct from {@link SlowOpRecorderTerminatedError}),
 * but it acknowledged that a record could not be durably written; flush()/close()
 * reject with this so a known-lost record can never read as a clean durable drain.
 */
export class SlowOpRecorderWriteError extends Error {
  constructor(reason: string) {
    super(`slow-operation recorder write failed: ${reason}`);
    this.name = "SlowOpRecorderWriteError";
  }
}

export interface SlowOperationInstrumentation {
  runSync?<T>(site: string, fn: () => T): T;
  runStage?<T>(
    site: string,
    fn: () => Promise<T>,
    classify?: (value: T) => "ok" | "failed",
  ): Promise<T>;
  recordRequest?(site: string, durationMs: number, detail?: RequestDetail): void;
  /** A synchronous site's duration, recorded only at or above the slow threshold (runSyncSiteWhenSlow). */
  recordSlowSync?(site: string, durationMs: number, outcome: "ok" | "failed"): void;
  /** Bounded nonblocking diagnostic append; false means observation was dropped. */
  recordDiagnostic?(record: Record<string, unknown>): boolean;
  snapshot?(): SlowOperationSnapshot;
  setDegradedHandler?(handler: (snapshot: Required<Pick<SlowOperationSnapshot, "reason" | "site">>) => void): void;
  // OPR.0.4.3.21 (51elv2) — optional graceful-shutdown lifecycle. A drain that
  // cannot prove durability rejects (never a silent success); see index.ts.
  flush?(): Promise<void>;
  close?(): Promise<void>;
}

/** What a slow request's record says about the request besides its route: the filters it asked for and who asked. */
export interface RequestDetail {
  /** The allowlisted query keys with their values (cut), a repeated key as a list. */
  query?: Record<string, string | string[]>;
  /** How many query keys were left out: neither their names nor their values are recorded. */
  queryOmitted?: number;
  /** The transport sender (X-OpenRig-Session), canonicalized, when the request named one. */
  sender?: string;
}

// The query keys a slow request's record keeps, with their values: the listing filters and the window. Any other
// key is only counted, never named, so a token or a row body passed in a query string never reaches the log.
const REQUEST_QUERY_KEYS_KEPT = new Set([
  "destinationSession", "sourceSession", "as", "rig", "state", "tag", "targetRepo", "limit", "compact", "activeOnly",
  "attention", "handedOffFrom", "updatedSince", "sort", "view", "full",
]);
const REQUEST_QUERY_VALUE_MAX = 120;
const REQUEST_QUERY_VALUES_PER_KEY_MAX = 10;

function cutQueryValue(value: string): string {
  return value.length > REQUEST_QUERY_VALUE_MAX ? `${value.slice(0, REQUEST_QUERY_VALUE_MAX)}…` : value;
}

/** The request's allowlisted query filters and transport sender, for a slow request's record. Never throws. */
export function describeRequest(c: Context): RequestDetail {
  const detail: RequestDetail = {};
  try {
    const params = new URL(c.req.url).searchParams;
    const query: Record<string, string | string[]> = {};
    let omitted = 0;
    for (const key of new Set(params.keys())) {
      if (!REQUEST_QUERY_KEYS_KEPT.has(key)) { omitted += 1; continue; }
      const values = params.getAll(key).slice(0, REQUEST_QUERY_VALUES_PER_KEY_MAX).map(cutQueryValue);
      query[key] = values.length === 1 ? values[0]! : values;
    }
    if (Object.keys(query).length > 0) detail.query = query;
    if (omitted > 0) detail.queryOmitted = omitted;
  } catch {
    // An unparsable URL leaves the record with its route only.
  }
  try {
    const sender = transportSenderSession(c);
    if (sender) detail.sender = cutQueryValue(sender);
  } catch {
    // Same: the sender is extra, never a reason to lose the timing.
  }
  return detail;
}

// The request-timing MIDDLEWARE, extracted as a wired seam (OPR request-observer). It is
// MEASUREMENT-ONLY: the observer is invoked in a `finally` and isolated in its own try/catch, so a
// throwing observer can NEVER replace the route's real status/body. `now` is injectable (default
// real wall-clock) so the recorded durations are DETERMINISTIC under test — the same injectable-clock
// discipline as the compaction-restore + mission-bucket seams. server.ts wires this via
// `app.use("*", createSlowOpRequestMiddleware(recorder))`; the startup-wiring pin proves that enable path.
export function createSlowOpRequestMiddleware(
  recorder: Pick<SlowOperationInstrumentation, "recordRequest">,
  now: () => number = () => Date.now(),
): MiddlewareHandler {
  return async (c, next) => {
    const startedAt = now();
    try {
      await next();
    } finally {
      try {
        recorder.recordRequest?.(`${c.req.method} ${c.req.path}`, now() - startedAt, describeRequest(c));
      } catch (error) {
        // A measurement throw must never re-enter Hono control flow (would turn the route into a 500).
        console.error("[slow-operation] request observer failed", error);
      }
    }
  };
}

interface SlowOpRecorderOptions {
  logPath: string;
  maxBytes?: number;
  rotationCount?: number;
  /** A rotated file past rotationCount stays until its newest record is this old (0: rotationCount only). */
  retentionMs?: number;
  /** The most rotated files kept, whatever their age. */
  maxFiles?: number;
  slowThresholdMs?: number;
}

interface Span {
  spanId: string;
  site: string;
  startedAt: number;
}

const WORKER_SOURCE = String.raw`
  const fs = require("node:fs");
  const path = require("node:path");
  const { parentPort } = require("node:worker_threads");

  // Shift every rotated file up by one, then drop, oldest first, the ones past rotationCount whose newest record (mtime)
  // is retentionMs old, and any past maxFiles; a younger file stops the drop (the files below it are younger still).
  function rotate(logPath, rotationCount, retentionMs, maxFiles) {
    let top = 0;
    while (top < maxFiles && fs.existsSync(logPath + "." + (top + 1))) top += 1;
    for (let index = top + 1; index >= 1; index -= 1) {
      const source = index === 1 ? logPath : logPath + "." + (index - 1);
      const target = logPath + "." + index;
      if (!fs.existsSync(source)) continue;
      if (fs.existsSync(target)) fs.rmSync(target, { force: true });
      fs.renameSync(source, target);
      fs.chmodSync(target, 0o600);
    }
    const now = Date.now();
    for (let index = top + 1; index > rotationCount; index -= 1) {
      const file = logPath + "." + index;
      let mtime = 0;
      try { mtime = fs.statSync(file).mtimeMs; } catch { continue; }
      if (index <= maxFiles && now - mtime < retentionMs) break;
      fs.rmSync(file, { force: true });
    }
  }

  function append(message) {
    const line = JSON.stringify(message.record) + "\n";
    fs.mkdirSync(path.dirname(message.logPath), { recursive: true, mode: 0o700 });
    let size = 0;
    try { size = fs.statSync(message.logPath).size; } catch {}
    if (size > 0 && size + Buffer.byteLength(line) > message.maxBytes) {
      rotate(message.logPath, message.rotationCount, message.retentionMs, message.maxFiles);
    }
    const fd = fs.openSync(message.logPath, "a", 0o600);
    try {
      fs.chmodSync(message.logPath, 0o600);
      fs.writeSync(fd, line);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
  }

  parentPort.on("message", (message) => {
    let ok = true;
    let error;
    try {
      if (message.type === "append") append(message);
    } catch (caught) {
      ok = false;
      error = caught instanceof Error ? caught.message : String(caught);
    }
    if (message.id) parentPort.postMessage({ id: message.id, ok, error });
  });
`;

export class SlowOpRecorder implements SlowOperationInstrumentation {
  private readonly worker: Worker;
  private readonly logPath: string;
  private readonly maxBytes: number;
  private readonly rotationCount: number;
  private readonly retentionMs: number;
  private readonly maxFiles: number;
  private readonly slowThresholdMs: number;
  private readonly pending = new Map<string, { resolve: () => void; reject: (error: unknown) => void }>();
  private degraded: SlowOperationSnapshot = { healthy: true };
  private degradedHandler?: (snapshot: Required<Pick<SlowOperationSnapshot, "reason" | "site">>) => void;
  private closed = false;
  // OPR.0.4.3.21 (51elv2) — set once the Worker is lost (error / unexpected
  // exit / messageerror / synchronous postMessage failure). Distinct from
  // `closed` (an expected caller-initiated teardown): a terminal recorder
  // rejects every future post/flush so nothing waits forever or reports a
  // false durable drain.
  private terminalReason: string | null = null;
  // OPR.0.4.3.21 (51elv2) — sticky latch set once the Worker acknowledges a
  // record it could not durably write (ok:false). The Worker stays alive, but
  // durability is lost, so flush()/close() must refuse to report a clean drain.
  private acknowledgedWriteFailure = false;

  private diagnosticIds = new Set<string>();
  private diagnostic = { offered: 0, enqueued: 0, dropped: 0, acknowledged: 0, failed: 0 };

  constructor(options: SlowOpRecorderOptions) {
    this.logPath = options.logPath;
    this.maxBytes = options.maxBytes ?? SLOW_OPERATION_ROTATION_BYTES;
    this.rotationCount = options.rotationCount ?? SLOW_OPERATION_ROTATION_COUNT;
    this.retentionMs = options.retentionMs ?? SLOW_OPERATION_RETENTION_MS;
    this.maxFiles = Math.max(options.maxFiles ?? SLOW_OPERATION_MAX_FILES, this.rotationCount);
    this.slowThresholdMs = options.slowThresholdMs ?? SLOW_OPERATION_THRESHOLD_MS;
    this.worker = new Worker(WORKER_SOURCE, { eval: true, execArgv: [] });
    this.worker.unref();
    this.worker.on("message", (message: { id?: string; ok?: boolean }) => {
      if (!message.id) return;
      if (this.diagnosticIds.delete(message.id)) {
        if (message.ok === false) this.diagnostic.failed++;
        else this.diagnostic.acknowledged++;
      }
      if (message.ok === false) {
        // Latch the lost durability BEFORE resolving this waiter (the write
        // attempt is done, just failed): keep the one-shot degraded signal and
        // waiter resolution, but flush()/close() will now refuse a clean drain.
        this.acknowledgedWriteFailure = true;
        this.markDegraded("recorder_write_failed", "recorder.worker");
      }
      this.pending.get(message.id)?.resolve();
      this.pending.delete(message.id);
    });
    // One terminal transition for every unrecoverable Worker-loss trigger.
    this.worker.on("error", () => this.handleTerminalFailure("recorder_worker_failed"));
    // An unexpected exit (any code — 0 included) while the recorder is still
    // open is a loss; a normal close() sets `closed` first, so we skip it.
    this.worker.on("exit", () => {
      if (!this.closed) this.handleTerminalFailure("recorder_worker_exited");
    });
    this.worker.on("messageerror", () => this.handleTerminalFailure("recorder_worker_message_error"));
  }

  setDegradedHandler(handler: (snapshot: Required<Pick<SlowOperationSnapshot, "reason" | "site">>) => void): void {
    this.degradedHandler = handler;
  }

  snapshot(): SlowOperationSnapshot {
    return { ...this.degraded };
  }

  beginSyncSpan(site: string): Span {
    const span: Span = { spanId: randomUUID(), site, startedAt: performance.now() };
    // Queue before the operation without waiting on worker startup or fsync. The
    // worker can write even while the main thread is wedged; only flush/close
    // acknowledge durability. A crash before the worker appends can lose begin.
    this.appendAsync({ v: 1, ts: new Date().toISOString(), spanId: span.spanId, phase: "begin", site });
    return span;
  }

  runSync<T>(site: string, fn: () => T): T {
    const span = this.beginSyncSpan(site);
    try {
      const value = fn();
      this.endSpan(span, "ok");
      return value;
    } catch (error) {
      this.endSpan(span, "failed");
      throw error;
    }
  }

  async runStage<T>(
    site: string,
    fn: () => Promise<T>,
    classify?: (value: T) => "ok" | "failed",
  ): Promise<T> {
    const startedAt = performance.now();
    try {
      const value = await fn();
      this.recordMeasurement(site, performance.now() - startedAt, classify?.(value) ?? "ok");
      return value;
    } catch (error) {
      this.recordMeasurement(site, performance.now() - startedAt, "failed");
      throw error;
    }
  }

  recordMeasurement(site: string, durationMs: number, outcome: "ok" | "failed" = "ok"): void {
    this.appendAsync({
      v: 1,
      ts: new Date().toISOString(),
      spanId: randomUUID(),
      phase: "end",
      site,
      durationMs: Math.max(0, durationMs),
      outcome,
    });
  }

  recordRequest(site: string, durationMs: number, detail?: RequestDetail): void {
    if (!Number.isFinite(durationMs) || durationMs < this.slowThresholdMs) return;
    this.appendAsync({
      v: 1,
      ts: new Date().toISOString(),
      spanId: randomUUID(),
      phase: "end",
      site: `request:${site}`,
      durationMs: Math.max(0, durationMs),
      outcome: "ok",
      ...(detail?.query ? { query: detail.query } : {}),
      ...(detail?.queryOmitted ? { queryOmitted: detail.queryOmitted } : {}),
      ...(detail?.sender ? { sender: detail.sender } : {}),
    });
  }

  recordSlowSync(site: string, durationMs: number, outcome: "ok" | "failed" = "ok"): void {
    if (!Number.isFinite(durationMs) || durationMs < this.slowThresholdMs) return;
    this.recordMeasurement(site, durationMs, outcome);
  }

  recordDiagnostic(record: Record<string, unknown>): boolean {
    this.diagnostic.offered++;
    const coverage = { ...this.diagnostic, pending: this.diagnosticIds.size,
      recorderHealthy: this.degraded.healthy, maxBytes: this.maxBytes, rotationCount: this.rotationCount };
    let bytes: number;
    try { bytes = Buffer.byteLength(JSON.stringify({ ...record, coverage })); }
    catch { this.diagnostic.dropped++; return false; }
    if (this.closed || this.terminalReason !== null || this.diagnosticIds.size >= 1024 || bytes > 1024) {
      this.diagnostic.dropped++; return false;
    }
    this.diagnostic.enqueued++;
    void this.postAndWait({ type: "append", logPath: this.logPath, maxBytes: this.maxBytes,
      rotationCount: this.rotationCount, retentionMs: this.retentionMs, maxFiles: this.maxFiles, record: { ...record, coverage } }, true).catch(() => {});
    return true;
  }

  async flush(): Promise<void> {
    if (this.closed) return;
    // Rejects with SlowOpRecorderTerminatedError when the Worker is lost —
    // finite, and never a false durable-drain success.
    await this.postAndWait({ type: "flush" });
    // The marker round-tripped, but if the Worker earlier acknowledged a write
    // it could not persist, durability is unproven — reject so flush()/close()
    // (and the production drain) never report a clean drain over a lost record.
    if (this.acknowledgedWriteFailure) {
      throw new SlowOpRecorderWriteError("recorder_write_failed");
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    // Best-effort drain that still tears the Worker down and finishes finitely,
    // but preserves a terminal drain failure for the caller (index.ts maps it
    // to a nonzero exit so a lost drain never looks like a clean shutdown).
    let drainError: unknown;
    try {
      await this.flush();
    } catch (error) {
      drainError = error;
    }
    this.closed = true;
    await this.worker.terminate();
    if (drainError) throw drainError;
  }

  private endSpan(span: Span, outcome: "ok" | "failed"): void {
    this.appendAsync({
      v: 1,
      ts: new Date().toISOString(),
      spanId: span.spanId,
      phase: "end",
      site: span.site,
      durationMs: Math.max(0, performance.now() - span.startedAt),
      outcome,
    });
  }

  private appendAsync(record: Record<string, unknown>): void {
    if (this.closed) return;
    // Fire-and-forget: a terminal Worker loss rejects this promise, so we
    // internalize that outcome — Worker loss must never surface as an
    // unhandled rejection on a measurement path.
    void this.postAndWait({
      type: "append",
      logPath: this.logPath,
      maxBytes: this.maxBytes,
      rotationCount: this.rotationCount,
      retentionMs: this.retentionMs,
      maxFiles: this.maxFiles,
      record,
    }).catch(() => {});
  }

  private postAndWait(message: Record<string, unknown>, diagnostic = false): Promise<void> {
    if (this.closed) return Promise.resolve();
    if (this.terminalReason !== null) return Promise.reject(this.terminalError());
    const id = randomUUID();
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      if (diagnostic) this.diagnosticIds.add(id);
      try {
        this.worker.postMessage({ ...message, id });
      } catch {
        // Synchronous postMessage failure — the Worker is gone. Same terminal
        // transition; this waiter is rejected along with any others.
        this.pending.delete(id);
        this.handleTerminalFailure("recorder_post_failed");
        reject(this.terminalError());
      }
    });
  }

  private terminalError(): SlowOpRecorderTerminatedError {
    return new SlowOpRecorderTerminatedError(this.terminalReason ?? "recorder_terminated");
  }

  /**
   * The single terminal-failure transition for every unrecoverable Worker-loss
   * trigger. Idempotent (settles pending exactly once), marks health degraded
   * with the one-shot high-urgency signal, and rejects every pending waiter so
   * no flush hangs or reports a false durable drain.
   */
  private handleTerminalFailure(reason: string, site = "recorder.worker"): void {
    if (this.terminalReason !== null) return;
    this.terminalReason = reason;
    this.markDegraded(reason, site);
    const error = this.terminalError();
    for (const waiter of this.pending.values()) waiter.reject(error);
    this.pending.clear();
    this.diagnostic.failed += this.diagnosticIds.size;
    this.diagnosticIds.clear();
  }

  private markDegraded(reason: string, site: string): void {
    if (!this.degraded.healthy) return;
    this.degraded = { healthy: false, reason, site };
    try {
      this.degradedHandler?.({ reason, site });
    } catch {}
  }
}
