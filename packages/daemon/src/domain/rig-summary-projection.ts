import type Database from "better-sqlite3";
import { RigRepository, type RigArchiveFilter } from "./rig-repository.js";
import { getNodeInventoryForRigs } from "./node-inventory.js";
import { deriveRigLifecycleFromInventory, deriveRigLifecycleState } from "./ps-projection.js";
import type { RigLifecycleState } from "./types.js";

export type RigSummaryEntry = ReturnType<RigRepository["getRigSummaries"]>[number] & {
  lifecycleState: RigLifecycleState;
  restoreLifecycleState: RigLifecycleState;
  hasLiveAgents: boolean | null;
};

/**
 * GET /api/rigs/summary's whole read: the summaries plus the inventory fold, SCOPED to the rigs this read returns
 * (default active-only; archived variants pass their archived rig-ids), so the per-node fold never widens to excluded
 * rigs — one fleet startup + one restore scan. Live per call; no cache. Database reads only, so it runs on the main
 * handle or on the read worker's connection with the same result.
 */
export function readRigSummaryEntries(db: Database.Database, filter: RigArchiveFilter): RigSummaryEntry[] {
  const summaries = new RigRepository(db).getRigSummaries(filter);
  const invByRig = getNodeInventoryForRigs(db, new Set(summaries.map((s) => s.id)));
  return summaries.map((s) => {
    const inventory = invByRig.get(s.id) ?? [];
    const lifecycleState = deriveRigLifecycleFromInventory(inventory);
    // Restore eligibility still accounts for every seat, including intentional stops.
    const restoreLifecycleState = deriveRigLifecycleState(inventory.map((entry) => entry.lifecycleState));
    const agents = inventory.filter((e) => e.nodeKind === "agent");
    // Presence is separate from lifecycle/attention, and uses this same inventory fold.
    const hasLiveAgents = agents.some((e) => e.sessionStatus === "running" || e.sessionStatus === "idle")
      ? true : agents.every((e) => e.sessionStatus === null || e.sessionStatus === "stopped" || e.sessionStatus === "exited") ? false : null;
    return { ...s, lifecycleState, restoreLifecycleState, hasLiveAgents };
  });
}
