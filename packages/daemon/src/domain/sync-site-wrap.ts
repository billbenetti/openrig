import { PerformanceObserver, constants as perfConstants } from "node:perf_hooks";
import { SLOW_OPERATION_THRESHOLD_MS, type SlowOperationInstrumentation } from "./slow-op-recorder.js";

let recorder: SlowOperationInstrumentation | undefined;

export function configureSyncSiteRecorder(next: SlowOperationInstrumentation | undefined): void {
  recorder = next;
}

/** T-010230: the sites that ran long enough to matter, kept on the same clock as the event-loop tick (performance.now),
 *  so an event_loop_gap can name what ran inside it. A synchronous site is never in flight when the tick runs (the tick
 *  only fires after the stall ended), so "in flight" is read as "ran within [previous tick, this tick]". The floor keeps
 *  the hot per-row sites from pushing the culprit out of the short trail. */
export const SYNC_SITE_TRAIL_FLOOR_MS = 50;
const SYNC_SITE_TRAIL_SIZE = 32;
const SITE_NAME_MAX = 48;
const trail: Array<{ site: string; startedAt: number; endedAt: number }> = [];

function remember(site: string, startedAt: number, endedAt: number): void {
  if (!(endedAt - startedAt >= SYNC_SITE_TRAIL_FLOOR_MS)) return;
  trail.push({ site, startedAt, endedAt });
  if (trail.length > SYNC_SITE_TRAIL_SIZE) trail.shift();
}

/** The sites that overlapped [from, to] (performance.now), longest overlap first, at most `max`, each with its own
 *  duration and its end relative to `origin`: the fields an event_loop_gap record carries (bounded so the record stays
 *  under the recorder's 1024-byte diagnostic cap). Empty when nothing ran long enough. */
export function syncSitesDuring(from: number, to: number, origin = 0, max = 5): Record<string, unknown> {
  const hits = trail
    .filter((entry) => entry.endedAt > from && entry.startedAt < to)
    .map((entry) => ({ entry, overlap: Math.min(entry.endedAt, to) - Math.max(entry.startedAt, from) }))
    .sort((a, b) => b.overlap - a.overlap);
  if (hits.length === 0) return {};
  return {
    sites: hits.slice(0, max).map(({ entry }) => ({ site: entry.site.slice(0, SITE_NAME_MAX),
      ms: Math.round(entry.endedAt - entry.startedAt), endMs: Math.round(entry.endedAt - origin) })),
    ...(hits.length > max ? { sitesOmitted: hits.length - max } : {}),
  };
}

/** Test seam: forget the trail. */
export function resetSyncSiteTrail(): void { trail.length = 0; }

export function runSyncSite<T>(site: string, fn: () => T): T {
  if (!recorder?.runSync) return fn();
  const startedAt = performance.now();
  try {
    return recorder.runSync(site, fn);
  } finally {
    remember(site, startedAt, performance.now());
  }
}

/**
 * runSyncSite for a site on a hot path (per request, per row), where a begin and an end record per call would crowd
 * the bounded log: it records only a call that took at least the recorder's slow threshold, and writes no begin
 * record, so a call that wedges the loop is named once it ends rather than while it runs.
 */
export function runSyncSiteWhenSlow<T>(site: string, fn: () => T): T {
  const record = recorder?.recordSlowSync?.bind(recorder);
  if (!record) return fn();
  const startedAt = performance.now();
  let outcome: "ok" | "failed" = "failed";
  try {
    const value = fn();
    outcome = "ok";
    return value;
  } finally {
    // Measurement only: a failing record never replaces fn's own result or error.
    const endedAt = performance.now();
    remember(site, startedAt, endedAt);
    try { record(site, endedAt - startedAt, outcome); } catch { /* the recorder reports its own health */ }
  }
}

/**
 * Async twin of runSyncSite: instruments a NON-blocking site through the
 * recorder's async `runStage` seam (falls back to a bare call when no recorder
 * is configured). Use this for sites whose work must NOT block the event loop
 * (e.g. process-table sampling via async execFile) — a synchronous `runSyncSite`
 * wrapping an `execFileSync` freezes the loop for the whole spawn, which is the
 * daemon-degradation defect this replaces.
 */
export async function runAsyncSite<T>(site: string, fn: () => Promise<T>): Promise<T> {
  return recorder?.runStage ? recorder.runStage(site, fn) : fn();
}

/**
 * Offers one diagnostic record to the configured recorder's bounded log, only when `elapsedMs` reached the slow
 * threshold, so a hot path adds no record per call. Measurement only: no recorder, a dropped record or a throwing
 * recorder changes nothing for the caller.
 */
export function recordSlowDiagnostic(elapsedMs: number, record: Record<string, unknown>): void {
  if (!(elapsedMs >= SLOW_OPERATION_THRESHOLD_MS)) return;
  try { recorder?.recordDiagnostic?.(record); } catch { /* the recorder reports its own health */ }
}

/**
 * runAsyncSite for a NON-blocking site on a hot path (per request): like runSyncSiteWhenSlow, it records only a call
 * that took at least the recorder's slow threshold, so a read moved off the event loop is still named when it is slow
 * without a record per call. The time is the caller's wait, not loop time.
 */
export async function runAsyncSiteWhenSlow<T>(site: string, fn: () => Promise<T>): Promise<T> {
  const record = recorder?.recordSlowSync?.bind(recorder);
  if (!record) return fn();
  const startedAt = performance.now();
  let outcome: "ok" | "failed" = "failed";
  try {
    const value = await fn();
    outcome = "ok";
    return value;
  } finally {
    try { record(site, performance.now() - startedAt, outcome); } catch { /* the recorder reports its own health */ }
  }
}

const GC_KIND: Record<number, string> = {
  [perfConstants.NODE_PERFORMANCE_GC_MAJOR]: "major",
  [perfConstants.NODE_PERFORMANCE_GC_MINOR]: "minor",
  [perfConstants.NODE_PERFORMANCE_GC_INCREMENTAL]: "incremental",
  [perfConstants.NODE_PERFORMANCE_GC_WEAKCB]: "weakcb",
};

/**
 * T-010230: a garbage collection of at least the slow threshold (250 ms) is a pause without a span: it is recorded as
 * the site `gc.<kind>` (major, minor, incremental, weakcb) and kept in the trail, so the event_loop_gap it caused names
 * it when its entry arrives before the tick (the entry is delivered after the pause, so a gap can still miss it; the
 * gc record itself stays, correlated by time). Returns the disconnect.
 */
export function startLongGcObserver(): () => void {
  const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      if (!(entry.duration >= SLOW_OPERATION_THRESHOLD_MS)) continue;
      const kind = GC_KIND[(entry as unknown as { detail?: { kind?: number } }).detail?.kind ?? -1] ?? "other";
      remember(`gc.${kind}`, entry.startTime, entry.startTime + entry.duration);
      try { recorder?.recordSlowSync?.(`gc.${kind}`, entry.duration, "ok"); } catch { /* the recorder reports its own health */ }
    }
  });
  observer.observe({ entryTypes: ["gc"] });
  return () => observer.disconnect();
}
