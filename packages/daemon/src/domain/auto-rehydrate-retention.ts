// P04-v2 PR-B: drains the backlog of old `auto-rehydrate` captures in bounded batches, after boot, through the
// repository's one retention method (`SnapshotRepository.pruneAutoRehydrate`). A tick deletes at most `batchSize` rows
// across all rigs, never rejects, and a rig whose prune fails never stops the others. `start` only schedules: nothing
// is deleted synchronously, so the daemon's boot never waits on a large DELETE.

import type Database from "better-sqlite3";
import { AUTO_REHYDRATE_RETENTION_DEFAULTS, type SnapshotRepository } from "./snapshot-repository.js";

/** How often the daemon runs one drain batch (one tick = at most `batchSize` deletions). */
export const AUTO_REHYDRATE_RETENTION_INTERVAL_MS = 60_000;

/** A rig whose prune deleted nothing (every old capture past `keep` is protected) sits out this many ticks: the
 *  rig still counts more old captures than `keep`, and without the pause every tick would redo its reads for nothing.
 *  Its own captures still prune at capture time meanwhile. */
export const AUTO_REHYDRATE_RETENTION_IDLE_TICKS = 10;

export interface AutoRehydrateRetentionSchedulerDeps {
  db: Database.Database;
  snapshotRepo: SnapshotRepository;
  keep?: number;
  batchSize?: number;
}

export class AutoRehydrateRetentionScheduler {
  private timer: ReturnType<typeof setInterval> | null = null;
  /** The tick in flight, which `stop()` hands to the shutdown to wait for. */
  private inFlight: Promise<number> | null = null;
  /** Set by `stop()`: the tick in flight deletes nothing more, and no tick starts until `start` again. */
  private stopping = false;
  private ticks = 0;
  /** Rig id → the tick number before which the drain skips it (it last deleted nothing). */
  private readonly idleUntil = new Map<string, number>();
  private readonly keep: number;
  private readonly batchSize: number;

  constructor(private readonly deps: AutoRehydrateRetentionSchedulerDeps) {
    this.keep = Math.max(1, Math.floor(deps.keep ?? AUTO_REHYDRATE_RETENTION_DEFAULTS.keep));
    this.batchSize = Math.max(1, Math.floor(deps.batchSize ?? AUTO_REHYDRATE_RETENTION_DEFAULTS.batchSize));
  }

  /** Schedules the ticks; deletes nothing here. */
  start(intervalMs: number): void {
    if (this.timer) return;
    this.stopping = false;
    this.timer = setInterval(() => { void this.tick(); }, intervalMs);
    this.timer.unref?.();
  }

  /** Ends the ticks, and the tick in flight before its next prune; resolves once that tick has returned, so the
   *  daemon's shutdown deletes nothing after this phase. */
  stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.stopping = true;
    return this.inFlight ? this.inFlight.then(() => undefined) : Promise.resolve();
  }

  get isActive(): boolean {
    return this.timer !== null;
  }

  /** One batch across the rigs with more old captures than `keep`; resolves to how many rows it deleted. */
  tick(): Promise<number> {
    if (this.inFlight || this.stopping) return Promise.resolve(0);
    const run = this.drainBatch().finally(() => { this.inFlight = null; });
    this.inFlight = run;
    return run;
  }

  private async drainBatch(): Promise<number> {
    this.ticks += 1;
    let budget = this.batchSize;
    let rigIds: string[];
    try {
      rigIds = this.deps.snapshotRepo.rigIdsWithAutoRehydrateOverKeep(this.keep);
    } catch (error) {
      console.warn(`Snapshot retention: auto-rehydrate drain could not list rigs: ${String(error)}`);
      return 0;
    }
    for (const rigId of rigIds) {
      if (budget <= 0 || this.stopping) break;
      if ((this.idleUntil.get(rigId) ?? 0) > this.ticks) continue;
      try {
        const deleted = this.deps.snapshotRepo.pruneAutoRehydrate(rigId, { keep: this.keep, batchSize: budget });
        if (deleted === 0) this.idleUntil.set(rigId, this.ticks + AUTO_REHYDRATE_RETENTION_IDLE_TICKS);
        else this.idleUntil.delete(rigId);
        budget -= deleted;
      } catch (error) {
        console.warn(`Snapshot retention: auto-rehydrate drain failed for ${rigId}: ${String(error)}`);
      }
      // Yield between rigs: the event loop serves requests between the batches of different rigs.
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    const deleted = this.batchSize - budget;
    if (deleted > 0) console.log(`Snapshot retention: auto-rehydrate drain deleted ${deleted} old capture(s)`);
    return deleted;
  }
}
