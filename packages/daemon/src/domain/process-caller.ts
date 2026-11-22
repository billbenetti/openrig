import { AsyncLocalStorage } from "node:async_hooks";

/**
 * Who asked for a process-table read, for telemetry only (T-018092, C1). The `codex.runtime.list_processes` site name
 * is older than most of its callers: Claude delivery and the periodic reconcilers read the same table under it, so the
 * slow-operations log could not tell them apart. The site name stays (its readers group by it); each record gains a
 * `caller` field when the read ran under one of these entry points.
 *
 * Telemetry only: nothing reads this value to decide anything. A read outside every entry point carries no `caller`.
 * A record counts one caller's wait, not one physical read: callers that share a read already in flight
 * (process-table.ts) each write their own record, with their own caller and their own wait.
 */
export type ProcessCaller = "claude_delivery" | "reconciler" | "codex_runtime";

const scope = new AsyncLocalStorage<ProcessCaller>();

/**
 * Runs `fn` with `caller` as the attributed caller of the process-table reads it makes. The outermost entry point
 * wins: a reconciler that reaches the Codex adapter's readiness check stays `reconciler`, because the question the
 * field answers is what triggered the read, not which helper made it.
 */
export function withProcessCaller<T>(caller: ProcessCaller, fn: () => T): T {
  return scope.getStore() === undefined ? scope.run(caller, fn) : fn();
}

/** The caller attributed to the current async context, if any. */
export function currentProcessCaller(): ProcessCaller | undefined {
  return scope.getStore();
}
