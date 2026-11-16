// The read worker's side of read-off-loop.ts: it runs here, on the worker thread, and never on the event loop. Each
// job is the same read function the synchronous path calls on the main handle, given a read-only connection instead;
// what it returns is plain data, so it crosses the thread by structured clone.
import Database from "better-sqlite3";
import type { RigArchiveFilter } from "./rig-repository.js";
import { readPsBaseAt, type PsBaseRig } from "./ps-projection.js";
import { readRigSummaryEntries, type RigSummaryEntry } from "./rig-summary-projection.js";
import { queryUsageSeries, type UsageSeriesQuery } from "./usage-series.js";
import { readPromptRefusalCandidates } from "./queue-wake-ladder.js";
import { AgentActivityStore } from "./agent-activity-store.js";
import { SessionRegistry } from "./session-registry.js";
import type { EventBus } from "./event-bus.js";

interface Jobs {
  psBase: {
    args: { filter: RigArchiveFilter | undefined; now: number | null; periodicSnapshotActive: boolean; hookFreshnessMs: number | null };
    result: { now: number; rigs: PsBaseRig[] };
  };
  rigSummaries: { args: { filter: RigArchiveFilter }; result: RigSummaryEntry[] };
  /** The rows already serialized: up to 10,000 of them would cost the loop more to clone and serialize than to read. */
  usageSeriesJson: { args: UsageSeriesQuery; result: string };
  promptRefusalCandidates: { args: Record<string, never>; result: Array<{ qitem_id: string; destination_session: string }> };
}

export type ReadJobName = keyof Jobs;
export type ReadJobArgs<J extends ReadJobName> = Jobs[J]["args"];
export type ReadJobResult<J extends ReadJobName> = Jobs[J]["result"];

// The hook reads publish nothing; a store needs a bus only to write.
const NO_EVENTS = { emit: () => undefined } as unknown as EventBus;

/** The daemon's hook store, wired as startup wires it (the occupant-generation gate from the session registry), on
 *  the worker's connection. */
function hookStore(db: Database.Database, freshnessMs: number): AgentActivityStore {
  const sessions = new SessionRegistry(db);
  return new AgentActivityStore({
    db,
    eventBus: NO_EVENTS,
    freshnessMs,
    resolveOccupantGeneration: (nodeId) => sessions.currentOccupantTenure(nodeId)?.generationUuid ?? null,
    isRegisteredOccupantGeneration: (nodeId, generation) => sessions.isOccupantGenerationRegistered(nodeId, generation),
  });
}

const JOBS: { [J in ReadJobName]: (db: Database.Database, args: ReadJobArgs<J>) => ReadJobResult<J> } = {
  psBase: (db, { filter, now, periodicSnapshotActive, hookFreshnessMs }) => readPsBaseAt(db, filter, {
    now, periodicSnapshotActive, hooks: hookFreshnessMs === null ? null : hookStore(db, hookFreshnessMs),
  }),
  rigSummaries: (db, { filter }) => readRigSummaryEntries(db, filter),
  usageSeriesJson: (db, query) => JSON.stringify(queryUsageSeries(db, query)),
  promptRefusalCandidates: (db) => readPromptRefusalCandidates(db),
};

// One read-only connection per database file, kept for the worker's life: opening one costs a schema read, and between
// jobs it holds no snapshot (each job ends its read transaction), so it never keeps the WAL from checkpointing.
const connections = new Map<string, Database.Database>();

function connection(file: string): Database.Database {
  let db = connections.get(file);
  if (!db) {
    db = new Database(file, { readonly: true, fileMustExist: true });
    connections.set(file, db);
  }
  return db;
}

/** Runs one job inside one read transaction, so every query of a projection reads the same snapshot. */
export function runReadJob<J extends ReadJobName>(file: string, job: J, args: ReadJobArgs<J>): ReadJobResult<J> {
  const run = JOBS[job];
  if (!run) throw new Error(`unknown read job ${JSON.stringify(job)}`);
  const db = connection(file);
  db.exec("BEGIN");
  try {
    return run(db, args);
  } finally {
    if (db.inTransaction) db.exec("ROLLBACK");
  }
}
