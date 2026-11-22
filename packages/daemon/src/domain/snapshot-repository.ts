import type Database from "better-sqlite3";
import { ulid } from "ulid";
import type { RestoreSnapshotSelection, RestoreSnapshotSummary, Snapshot, SnapshotData } from "./types.js";
import { parseSqliteUtcMs } from "./sqlite-time.js";
import { resolveActiveOccupantRow } from "./active-occupant.js";

/** Retention of `auto-rehydrate` captures (P04-v2 PR-B): every capture younger than `minAgeMs` stays, the `keep`
 *  newest older ones stay, and one call deletes at most `batchSize` rows. */
export const AUTO_REHYDRATE_RETENTION_DEFAULTS = Object.freeze({ keep: 5, batchSize: 25, minAgeMs: 24 * 60 * 60 * 1000 });

/** Operator off-switch without a rebuild: `OPENRIG_AUTO_REHYDRATE_RETENTION=off` (or `0`/`false`) in the daemon's
 *  environment turns the auto-rehydrate retention off, at capture and in the backlog drain. Read on every call. */
export function autoRehydrateRetentionEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const value = env.OPENRIG_AUTO_REHYDRATE_RETENTION?.trim().toLowerCase();
  return value !== "off" && value !== "0" && value !== "false";
}

export interface AutoRehydratePruneOptions {
  keep?: number;
  batchSize?: number;
  nowMs?: number;
}

/** SQLite pre-filter for "some session carries a non-empty string resume token": a superset of the occupant rule
 *  (`hasResolvedOccupantResumeToken`), so a row it drops can never be usable. Data SQLite can't parse passes, and the
 *  exact rule decides in JS. It spares parsing large captures that carry no token at all. */
const MAY_CARRY_RESUME_TOKEN = `(CASE WHEN json_valid(data) THEN EXISTS (
  SELECT 1 FROM json_each(data, '$.sessions') AS s
  WHERE CASE WHEN s.type = 'object'
    THEN json_type(s.value, '$.resumeToken') = 'text' AND json_extract(s.value, '$.resumeToken') <> '' ELSE 0 END
) ELSE 1 END)`;

export type RestoreSnapshotSelectionOutcome =
  | { ok: true; snapshot: Snapshot; selection: RestoreSnapshotSelection }
  | { ok: false; code: "snapshot_not_found" | "snapshot_wrong_rig" | "snapshot_unusable" | "no_usable_snapshot"; message: string };

interface ListOptions {
  kind?: string;
  limit?: number;
}

/** Per database: the snapshot ids an in-flight restore or launch names, with how many attempts hold each. Every
 *  repository on the same database sees them, so retention never deletes them whichever instance prunes. */
const heldForRestoreByDb = new WeakMap<object, Map<string, number>>();

/** Rows of the rig a prune reads at most, looking for a usable copy newer than the candidate it would keep. */
const RESUME_SCAN_ROWS_PER_PRUNE = 50;

/** Per database and rig: the resume-usable prune candidate kept back (`usableId`) and the newest of the rig's rows
 *  newer than it already read without a usable one (`throughAt`, `throughId`, in (created_at, id) order). Snapshot
 *  rows never change after insert, so each prune reads on from there, at most RESUME_SCAN_ROWS_PER_PRUNE rows. Until a
 *  newer usable row turns up the candidate stays, so a row the scan misses (inserted out of order, with an older
 *  `created_at`) only ever keeps it longer, never deletes a usable row. */
const resumeScanByDb = new WeakMap<object, Map<string, { usableId: string; throughAt: string; throughId: string }>>();

/** SQLite UTC stamp ("YYYY-MM-DD HH:MM:SS", the form `datetime('now')` stores in `snapshots.created_at`). */
function sqliteUtcStamp(ms: number): string {
  return new Date(ms).toISOString().replace("T", " ").slice(0, 19);
}

export class SnapshotRepository {
  readonly db: Database.Database;

  constructor(db: Database.Database) {
    this.db = db;
  }

  createSnapshot(rigId: string, kind: string, data: SnapshotData): Snapshot {
    const id = ulid();
    this.db
      .prepare(
        "INSERT INTO snapshots (id, rig_id, kind, data) VALUES (?, ?, ?, ?)"
      )
      .run(id, rigId, kind, JSON.stringify(data));

    return this.rowToSnapshot(
      this.db.prepare("SELECT * FROM snapshots WHERE id = ?").get(id) as SnapshotRow
    );
  }

  getSnapshot(id: string): Snapshot | null {
    const row = this.db
      .prepare("SELECT * FROM snapshots WHERE id = ?")
      .get(id) as SnapshotRow | undefined;
    return row ? this.rowToSnapshot(row) : null;
  }

  findLatestAutoPreDown(rigId: string): Snapshot | null {
    const row = this.db
      .prepare(
        "SELECT * FROM snapshots WHERE rig_id = ? AND kind = 'auto-pre-down' ORDER BY created_at DESC LIMIT 1"
      )
      .get(rigId) as SnapshotRow | undefined;
    return row ? this.rowToSnapshot(row) : null;
  }

  /**
   * Returns the latest snapshot whose persisted `data` carries the minimum
   * structural metadata `RestoreOrchestrator.restore`'s pre-validation requires.
   *
   * OPR.0.3.4.9 Option Y: prefers the most-recent of the crash-insurance tier
   * {auto-pre-down, auto-periodic, runtime-switch} -- the freshest wins. A newer
   * auto-periodic beats a stale auto-pre-down (the crash fix); a genuinely-
   * fresher auto-pre-down still wins (graceful-cycle preserved). Manual,
   * pre_restore, and auto-rehydrate remain below the tier (unchanged). Slice 16: a committed runtime switch
   * captures `runtime-switch` in the tier, so an older automatic snapshot can't restore the seat's previous runtime.
   *
   * The SQL query orders by `(kind IN ('auto-pre-down','auto-periodic','runtime-switch')) DESC,
   * created_at DESC, rowid DESC`. The in-memory loop validates each candidate and
   * skips snapshots with corrupted JSON or missing topology metadata, returning
   * the first usable row. Returns null when no usable snapshot exists.
   *
   * Distinct from `findLatestUsableSnapshot` (rig-repository.ts, L2): that
   * helper requires at least one persisted resume token and is consumed by
   * the lifecycle projection. This helper only requires structural metadata
   * `RestoreOrchestrator.restore` actually inspects, so terminal-only or
   * resume-tokenless rigs still resolve.
   */
  findLatestRestoreUsable(rigId: string): Snapshot | null {
    // Rank by id alone: each capture's data can be large, and only the first usable one is needed. A row removed
    // between the ranking and its read is skipped like an unusable one.
    const ids = this.db
      .prepare(
        "SELECT id FROM snapshots WHERE rig_id = ? ORDER BY (kind IN ('auto-pre-down', 'auto-periodic', 'runtime-switch')) DESC, created_at DESC, rowid DESC"
      )
      .all(rigId) as Array<{ id: string }>;

    for (const { id } of ids) {
      const row = this.rowById(id);
      if (!row) continue;
      let data: SnapshotData;
      try {
        data = decodeStoredSnapshotData(row.data);
      } catch {
        continue;
      }
      if (!isRestoreUsableSnapshotData(data)) continue;
      return { id: row.id, rigId: row.rig_id, kind: row.kind, status: row.status, data, createdAt: row.created_at };
    }
    return null;
  }

  /** Resolve an exact or policy-ranked restore source and return the evidence
   * needed to make that decision legible before any restore mutation. */
  selectRestoreUsable(rigId: string, snapshotId?: string, nowMs: number = Date.now()): RestoreSnapshotSelectionOutcome {
    let snapshot: Snapshot | null;
    if (snapshotId) {
      const row = this.db.prepare("SELECT * FROM snapshots WHERE id = ?").get(snapshotId) as SnapshotRow | undefined;
      if (!row) return { ok: false, code: "snapshot_not_found", message: `Snapshot ${snapshotId} not found` };
      if (row.rig_id !== rigId) {
        return { ok: false, code: "snapshot_wrong_rig", message: `Snapshot ${snapshotId} belongs to rig ${row.rig_id}, not ${rigId}` };
      }
      snapshot = this.restoreUsableRow(row);
      if (!snapshot) {
        return { ok: false, code: "snapshot_unusable", message: `Snapshot ${snapshotId} is not structurally restore-usable` };
      }
    } else {
      snapshot = this.findLatestRestoreUsable(rigId);
      if (!snapshot) return { ok: false, code: "no_usable_snapshot", message: `No usable snapshot for rig ${rigId}` };
    }

    // Equal second-resolution timestamps still have an insertion order. Scan
    // only newer rows, retaining restore's tolerant parse for damaged captures.
    const candidates = this.db.prepare("SELECT id, created_at FROM snapshots WHERE rig_id = ? ORDER BY created_at DESC, rowid DESC")
      .all(rigId) as Array<Pick<SnapshotRow, "id" | "created_at">>;
    const selectedIndex = candidates.findIndex((candidate) => candidate.id === snapshot!.id);
    // A concurrent pruner can remove the selected row after it was read. Its
    // insertion position is then unknown, so only strictly newer timestamps count.
    const newerRows = selectedIndex < 0
      ? candidates.filter((row) => parseSqliteUtcMs(row.created_at) > parseSqliteUtcMs(snapshot!.createdAt))
      : candidates.slice(0, selectedIndex);
    // Read newer captures' data only until the first usable one.
    let newer: Snapshot | undefined;
    for (const { id } of newerRows) {
      const row = this.rowById(id);
      const candidate = row ? this.restoreUsableRow(row) : null;
      if (candidate && isRestoreUsableSnapshotData(candidate.data)) { newer = candidate; break; }
    }
    const mode = snapshotId ? "explicit" as const : "automatic" as const;
    return {
      ok: true,
      snapshot,
      selection: {
        ...summarizeSnapshot(snapshot, nowMs),
        mode,
        rationale: mode === "explicit"
          ? "operator selected this exact restore-usable snapshot"
          : "automatic crash-insurance ranking prefers auto-pre-down/auto-periodic/runtime-switch, then newest usable",
        newerUsableAlternative: newer ? summarizeSnapshot(newer, nowMs) : null,
      },
    };
  }

  getLatestSnapshot(rigId: string): Snapshot | null {
    const row = this.db
      .prepare(
        "SELECT * FROM snapshots WHERE rig_id = ? ORDER BY created_at DESC LIMIT 1"
      )
      .get(rigId) as SnapshotRow | undefined;
    return row ? this.rowToSnapshot(row) : null;
  }

  listSnapshots(rigId: string, opts?: ListOptions): Snapshot[] {
    let sql = "SELECT * FROM snapshots WHERE rig_id = ?";
    const params: unknown[] = [rigId];

    if (opts?.kind) {
      sql += " AND kind = ?";
      params.push(opts.kind);
    }

    sql += " ORDER BY created_at DESC, rowid DESC";

    if (opts?.limit) {
      sql += " LIMIT ?";
      params.push(opts.limit);
    }

    const rows = this.db.prepare(sql).all(...params) as SnapshotRow[];
    return rows.map((r) => this.rowToSnapshot(r));
  }

  pruneSnapshots(rigId: string, keepCount: number): number {
    // Select the small keeper set in SQLite: expanding every discarded ID into
    // a parameter makes retention fail above SQLite's variable-number limit.
    return this.db.prepare(`DELETE FROM snapshots
      WHERE rig_id = ? AND id NOT IN (
        SELECT id FROM snapshots WHERE rig_id = ?
        ORDER BY created_at DESC, rowid DESC LIMIT ?
      )`).run(rigId, rigId, keepCount).changes;
  }

  /** OPR.0.3.4.9 — kind-scoped retention. Keeps the newest `keepCount` rows
   *  of the given kind and deletes only older rows of THAT kind. Never touches
   *  other kinds. Hard floor: keepCount >= 1 (never prune to zero). */
  pruneSnapshotsByKind(rigId: string, kind: string, keepCount: number): number {
    const effectiveKeep = Math.max(1, keepCount);
    return this.db.prepare(`DELETE FROM snapshots
      WHERE rig_id = ? AND kind = ? AND id NOT IN (
        SELECT id FROM snapshots WHERE rig_id = ? AND kind = ?
        ORDER BY created_at DESC, rowid DESC LIMIT ?
      )`).run(rigId, kind, rigId, kind, effectiveKeep).changes;
  }

  /** Hold a snapshot for an in-flight restore or launch: retention never deletes it until the returned function
   *  runs (idempotent). Holds count, so the id stays held until the last attempt that holds it returns. */
  holdForRestore(snapshotId: string): () => void {
    let held = heldForRestoreByDb.get(this.db);
    if (!held) {
      held = new Map();
      heldForRestoreByDb.set(this.db, held);
    }
    const map = held;
    map.set(snapshotId, (map.get(snapshotId) ?? 0) + 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const left = (map.get(snapshotId) ?? 1) - 1;
      if (left <= 0) map.delete(snapshotId);
      else map.set(snapshotId, left);
    };
  }

  /** The one retention policy of `auto-rehydrate` captures, shared by every capture origin (through
   *  `SnapshotCapture`) and by the backlog drain (`AutoRehydrateRetentionScheduler`). Deletes at most `batchSize` of
   *  the rig's `auto-rehydrate` rows, oldest first, and returns how many (`keep` and `batchSize` below 1 count as 1).
   *  Never deletes: a capture younger than 24 h (`created_at >= now - 24 h`, UTC), the `keep` newest older
   *  `auto-rehydrate` ones, another kind, the rig's newest snapshot whose occupant resolves with a resume token
   *  (`findLatestUsableSnapshot`'s rule), the one `findLatestRestoreUsable` returns (what `selectRestoreUsable` picks
   *  without an id), or one an in-flight restore or launch holds (`holdForRestore`). */
  pruneAutoRehydrate(rigId: string, opts?: AutoRehydratePruneOptions): number {
    if (!autoRehydrateRetentionEnabled()) return 0;
    const keep = Math.max(1, Math.floor(opts?.keep ?? AUTO_REHYDRATE_RETENTION_DEFAULTS.keep));
    const batchSize = Math.max(1, Math.floor(opts?.batchSize ?? AUTO_REHYDRATE_RETENTION_DEFAULTS.batchSize));
    const cutoff = sqliteUtcStamp((opts?.nowMs ?? Date.now()) - AUTO_REHYDRATE_RETENTION_DEFAULTS.minAgeMs);
    const older = this.db.prepare(
      "SELECT id, created_at FROM snapshots WHERE rig_id = ? AND kind = 'auto-rehydrate' AND created_at < ? ORDER BY created_at DESC, rowid DESC"
    ).all(rigId, cutoff) as Array<{ id: string; created_at: string }>;
    if (older.length <= keep) return 0;

    const held = heldForRestoreByDb.get(this.db);
    const restoreUsable = this.findLatestRestoreUsable(rigId)?.id;
    // Oldest first, without the held ones and the restore pick. At most one more row (the newest resume-usable) can be
    // held back below, so batchSize + 1 candidates are enough to fill a batch.
    const window = older.slice(keep).reverse()
      .filter(({ id }) => !held?.has(id) && id !== restoreUsable)
      .slice(0, batchSize + 1);
    if (window.length === 0) return 0;
    const resumeUsable = this.newestResumeUsableIn(rigId, window);
    const victims = window.map(({ id }) => id).filter((id) => id !== resumeUsable).slice(0, batchSize);
    if (victims.length === 0) return 0;
    return this.db.prepare(`DELETE FROM snapshots WHERE id IN (${victims.map(() => "?").join(",")})`).run(...victims).changes;
  }

  /** The rig ids whose `auto-rehydrate` captures older than 24 h outnumber `keep`: the drain's work list. */
  rigIdsWithAutoRehydrateOverKeep(keep: number, nowMs: number = Date.now()): string[] {
    const cutoff = sqliteUtcStamp(nowMs - AUTO_REHYDRATE_RETENTION_DEFAULTS.minAgeMs);
    return (this.db.prepare(
      "SELECT rig_id FROM snapshots WHERE kind = 'auto-rehydrate' AND created_at < ? GROUP BY rig_id HAVING COUNT(*) > ? ORDER BY rig_id"
    ).all(cutoff, Math.max(1, Math.floor(keep))) as Array<{ rig_id: string }>).map((row) => row.rig_id);
  }

  /** Which of the candidates is the rig's newest snapshot (any kind, in `findLatestUsableSnapshot`'s order) whose
   *  occupant resolves with a resume token, as far as the rows read so far tell, or null when none of the candidates
   *  is usable or a newer usable row was found. The candidates are read newest first until the first usable one, U;
   *  then, oldest first, at most RESUME_SCAN_ROWS_PER_PRUNE of the rig's rows newer than U that no earlier prune read
   *  (`resumeScanByDb`). U stays held back until one of them is usable: an unfinished scan never lets it go. A row's
   *  data is parsed in JS only when SQLite sees a token in it (`MAY_CARRY_RESUME_TOKEN`). */
  private newestResumeUsableIn(rigId: string, candidates: Array<{ id: string; created_at: string }>): string | null {
    const newestFirst = [...candidates].sort((a, b) =>
      a.created_at === b.created_at ? (a.id < b.id ? 1 : a.id > b.id ? -1 : 0) : (a.created_at < b.created_at ? 1 : -1));
    const usable = newestFirst.find(({ id }) => this.rowHasResumeToken(id));
    if (!usable) return null;
    let scans = resumeScanByDb.get(this.db);
    if (!scans) {
      scans = new Map();
      resumeScanByDb.set(this.db, scans);
    }
    const known = scans.get(rigId);
    let through = known?.usableId === usable.id ? { at: known.throughAt, id: known.throughId } : { at: usable.created_at, id: usable.id };
    // Ids and keys only (small rows), oldest first so the part already read stays contiguous, listed before reading any
    // data: better-sqlite3 refuses other statements while an iterator is open.
    const next = this.db.prepare(
      "SELECT id, created_at FROM snapshots WHERE rig_id = ? AND (created_at > ? OR (created_at = ? AND id > ?)) ORDER BY created_at ASC, id ASC LIMIT ?"
    ).all(rigId, through.at, through.at, through.id, RESUME_SCAN_ROWS_PER_PRUNE) as Array<{ id: string; created_at: string }>;
    for (const row of next) {
      if (this.rowHasResumeToken(row.id)) {
        scans.delete(rigId);
        return null;
      }
      through = { at: row.created_at, id: row.id };
    }
    scans.set(rigId, { usableId: usable.id, throughAt: through.at, throughId: through.id });
    return usable.id;
  }

  private rowHasResumeToken(id: string): boolean {
    const may = this.db.prepare(`SELECT ${MAY_CARRY_RESUME_TOKEN} AS may FROM snapshots WHERE id = ?`).get(id) as { may: number } | undefined;
    if (!may?.may) return false;
    const row = this.db.prepare("SELECT data FROM snapshots WHERE id = ?").get(id) as Pick<SnapshotRow, "data"> | undefined;
    if (!row) return false;
    try {
      return hasResolvedOccupantResumeToken(decodeStoredSnapshotData(row.data));
    } catch {
      return false;
    }
  }

  private rowById(id: string): SnapshotRow | undefined {
    return this.db.prepare("SELECT * FROM snapshots WHERE id = ?").get(id) as SnapshotRow | undefined;
  }

  private restoreUsableRow(row: SnapshotRow): Snapshot | null {
    let snapshot: Snapshot;
    try { snapshot = this.rowToSnapshot(row); }
    catch (err) {
      if (err instanceof SyntaxError) return null;
      throw err;
    }
    return isRestoreUsableSnapshotData(snapshot.data) ? snapshot : null;
  }

  private rowToSnapshot(row: SnapshotRow): Snapshot {
    return {
      id: row.id,
      rigId: row.rig_id,
      kind: row.kind,
      status: row.status,
      data: decodeStoredSnapshotData(row.data),
      createdAt: row.created_at,
    };
  }
}

/** Read historical selection provenance without rewriting saved snapshots or
 * exposing the old key. An explicit current key (including null) takes priority. */
export function decodeStoredSnapshotData(json: string): SnapshotData {
  const data = JSON.parse(json) as SnapshotData;
  if (Array.isArray(data?.sessions)) {
    for (const session of data.sessions) {
      if (!session || typeof session !== "object") continue;
      const stored = session as typeof session & { agentApp?: unknown };
      if (!("communityHarness" in stored) && typeof stored.agentApp === "string" && stored.agentApp.length > 0) {
        stored.communityHarness = stored.agentApp;
      }
      delete stored.agentApp;
    }
  }
  return data;
}

/** `findLatestUsableSnapshot`'s usability rule (OPR.0.5.7.1): at least one node RESOLVES to an active occupant
 *  carrying a non-empty resume token. A present-null, missing-key or dangling relation never counts. */
export function hasResolvedOccupantResumeToken(data: SnapshotData): boolean {
  const sessions = data.sessions ?? [];
  const nodeIds = [...new Set(sessions.map((s) => s.nodeId))];
  return nodeIds.some((nodeId) => {
    const resolution = resolveActiveOccupantRow(sessions, data.activeSessionIdByNode, nodeId);
    return resolution.kind === "resolved"
      && typeof resolution.session.resumeToken === "string"
      && resolution.session.resumeToken.length > 0;
  });
}

export function summarizeSnapshot(snapshot: Snapshot, nowMs: number = Date.now()): RestoreSnapshotSummary {
  return {
    snapshotId: snapshot.id,
    kind: snapshot.kind,
    createdAt: snapshot.createdAt,
    ageMs: Math.max(0, nowMs - parseSqliteUtcMs(snapshot.createdAt)),
  };
}

interface SnapshotRow {
  id: string;
  rig_id: string;
  kind: string;
  status: string;
  data: string;
  created_at: string;
}

// Validates `SnapshotData` carries the minimum structural metadata
// `RestoreOrchestrator.restore`'s pre-validation requires.
//
// Per the L3b orch amendment: validate against actual SnapshotData. There is
// NO `data.bindings[]` field and `Session` has NO `runtime` field (runtime
// lives on nodes). Resume tokens are NOT required (resume-tokenless rigs are
// still restorable).
//
// Required:
//   - rig with non-empty id
//   - nodes array (may be empty — restore handles empty topologies)
//   - edges array (may be empty)
//   - sessions array (may be empty — `validatePreRestore` accepts empty)
//   - checkpoints object
//
// When sessions is non-empty, each session must have a non-empty sessionName
// and nodeId so node linkage can be resolved during restore. We do NOT check
// session.runtime because that field doesn't exist on Session (orch amendment).
export function isRestoreUsableSnapshotData(data: unknown): data is SnapshotData {
  if (!data || typeof data !== "object") return false;
  const d = data as SnapshotData;
  if (!d.rig || typeof d.rig.id !== "string" || d.rig.id.length === 0) return false;
  if (!Array.isArray(d.nodes)) return false;
  if (!Array.isArray(d.edges)) return false;
  if (!Array.isArray(d.sessions)) return false;
  if (!d.checkpoints || typeof d.checkpoints !== "object") return false;
  const nodeIds = new Set<string>();
  for (const node of d.nodes) {
    if (!node || typeof node !== "object") return false;
    if (typeof node.id !== "string" || node.id.length === 0) return false;
    if (typeof node.logicalId !== "string" || node.logicalId.length === 0) return false;
    if (nodeIds.has(node.id)) return false;
    nodeIds.add(node.id);
  }
  for (const s of d.sessions) {
    if (!s || typeof s !== "object") return false;
    if (typeof s.sessionName !== "string" || s.sessionName.length === 0) return false;
    if (typeof s.nodeId !== "string" || s.nodeId.length === 0) return false;
  }
  if (d.topologyRoster !== undefined) {
    const roster = d.topologyRoster;
    if (!roster || typeof roster !== "object" || Array.isArray(roster)) return false;
    const allowedSources = new Set(["materialized_topology", "operator_explicit", "legacy_current_nodes"]);
    if (roster.version !== 1 || !allowedSources.has(roster.source) || !Array.isArray(roster.intendedNodeIds)) return false;
    if (!roster.intendedNodeIds.every((nodeId) => typeof nodeId === "string" && nodeId.length > 0)) return false;
    if (new Set(roster.intendedNodeIds).size !== roster.intendedNodeIds.length) return false;
    if (roster.intendedNodeIds.some((nodeId) => !nodeIds.has(nodeId))) return false;
  }
  if (d.activeOccupantsByNode !== undefined) {
    if (!d.activeOccupantsByNode || typeof d.activeOccupantsByNode !== "object" || Array.isArray(d.activeOccupantsByNode)) return false;
    const intendedNodeIds = d.topologyRoster?.intendedNodeIds ?? [...nodeIds];
    if (intendedNodeIds.some((nodeId) => !(nodeId in d.activeOccupantsByNode!))) return false;
    if (Object.keys(d.activeOccupantsByNode).some((nodeId) => !nodeIds.has(nodeId))) return false;
    const sessionsById = new Map(d.sessions.map((session) => [session.id, session]));
    for (const [nodeId, state] of Object.entries(d.activeOccupantsByNode)) {
      if (!state || typeof state !== "object") return false;
      if (state.kind === "absent") continue;
      if (state.kind === "resolved" && typeof state.sessionId === "string" && state.sessionId.length > 0) {
        if (sessionsById.get(state.sessionId)?.nodeId !== nodeId) return false;
        continue;
      }
      if (state.kind === "ambiguous" && Array.isArray(state.candidateIds)) {
        if (state.candidateIds.length < 2 || new Set(state.candidateIds).size !== state.candidateIds.length) return false;
        if (state.candidateIds.some((id) => typeof id !== "string" || id.length === 0 || sessionsById.get(id)?.nodeId !== nodeId)) return false;
        continue;
      }
      return false;
    }
  }
  return true;
}
