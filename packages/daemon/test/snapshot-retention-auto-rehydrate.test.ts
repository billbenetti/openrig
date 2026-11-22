// Retention of `auto-rehydrate` snapshots: one central method on the snapshot repository, a 24 h age floor on top of
// `keep`, the last usable copy and the snapshot an in-flight restore names are never pruned, and the backlog is drained
// in bounded batches by a scheduler that never prunes synchronously when it starts.
//
// Contract (the eval's LEIA-ME has the whole text):
//   - `AUTO_REHYDRATE_RETENTION_DEFAULTS` (snapshot-repository.ts): `{ keep, batchSize, minAgeMs }`, keep 5..10,
//     batchSize 20..50, minAgeMs 24 h.
//   - `SnapshotRepository.pruneAutoRehydrate(rigId, opts?: { keep?, batchSize?, nowMs? }): number` deletes
//     min(batchSize, eligible) `auto-rehydrate` rows of that rig, oldest first, and returns how many (keep and batchSize
//     below 1 count as 1). Never pruned: a row younger than 24 h (created_at >= now - 24 h), the `keep` newest of the
//     older ones, any other kind, the rig's newest snapshot with a resolved resume token (`findLatestUsableSnapshot`'s
//     rule), the snapshot `findLatestRestoreUsable` returns, and a snapshot an in-flight `RestoreOrchestrator.restore(id)`
//     or `launchSingleNode(…, { snapshotId })` names, seen by every repository instance on the same database.
//   - `SnapshotCapture.captureSnapshot(rigId, "auto-rehydrate")` applies that method (one batch, the defaults) to the
//     rig after the capture; a prune failure never fails the capture.
//   - `AutoRehydrateRetentionScheduler` (auto-rehydrate-retention.ts): `new (deps: { db, snapshotRepo, keep?,
//     batchSize? })` (omitted: the defaults), `start(intervalMs)`, `stop()`, `isActive`, `tick(): Promise<number>`; a
//     tick deletes at most `batchSize` rows across all rigs (every rig_id with auto-rehydrate rows, rig row or not),
//     only through `pruneAutoRehydrate`'s rule, never rejects, isolates a failing rig, and `start` prunes nothing
//     synchronously: the ticks come from the interval.
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import { RigRepository, findLatestUsableSnapshot, findLatestUsableSnapshotsForAllRigs } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";
import * as snapshotRepository from "../src/domain/snapshot-repository.js";
import { SnapshotRepository } from "../src/domain/snapshot-repository.js";
import { CheckpointStore } from "../src/domain/checkpoint-store.js";
import { SnapshotCapture } from "../src/domain/snapshot-capture.js";
import { NodeLauncher } from "../src/domain/node-launcher.js";
import { RestoreOrchestrator } from "../src/domain/restore-orchestrator.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";
import type { ClaudeResumeAdapter } from "../src/adapters/claude-resume.js";
import type { CodexResumeAdapter } from "../src/adapters/codex-resume.js";

const HOUR = 3_600_000;
const DAY = 24 * HOUR;
// A fixed "now" for the calls that take nowMs: the stored ages do not depend on the wall clock.
const NOW = Date.UTC(2026, 10, 22, 12, 0, 0);
const sqliteUtc = (ms: number) => new Date(ms).toISOString().replace("T", " ").slice(0, 19);
const OTHER_KINDS = ["auto-periodic", "auto-pre-down", "runtime-switch", "manual"];

type Retention = { keep: number; batchSize: number; minAgeMs: number };
type PruneOpts = { keep?: number; batchSize?: number; nowMs?: number };
type Scheduler = { start(intervalMs: number): void; stop(): void; readonly isActive: boolean; tick(): Promise<number> };

function defaults(): Retention {
  const value = (snapshotRepository as unknown as { AUTO_REHYDRATE_RETENTION_DEFAULTS?: Retention }).AUTO_REHYDRATE_RETENTION_DEFAULTS;
  expect(value, "AUTO_REHYDRATE_RETENTION_DEFAULTS is exported by snapshot-repository.ts").toBeTruthy();
  return value!;
}

function prune(repo: SnapshotRepository, rigId: string, opts?: PruneOpts): number {
  const method = (repo as unknown as { pruneAutoRehydrate?: (rigId: string, opts?: PruneOpts) => number }).pruneAutoRehydrate;
  expect(typeof method, "SnapshotRepository.pruneAutoRehydrate exists").toBe("function");
  return method!.call(repo, rigId, opts);
}

/** Prunes until nothing more goes, with a ceiling. */
function pruneAll(repo: SnapshotRepository, rigId: string, opts?: PruneOpts): void {
  for (let i = 0; i < 100; i++) if (prune(repo, rigId, opts) === 0) return;
  throw new Error("pruneAutoRehydrate never converged");
}

// Loaded when a test needs it, so the rest of the file still runs (and fails one by one) where the module is missing.
const RETENTION_MODULE = "../src/domain/auto-rehydrate-retention.js";
async function scheduler(deps: { db: Database.Database; snapshotRepo: SnapshotRepository; keep?: number; batchSize?: number }): Promise<Scheduler> {
  let retention: unknown = {};
  try { retention = await import(/* @vite-ignore */ RETENTION_MODULE); } catch { /* reported by the expect below */ }
  const Ctor = (retention as unknown as { AutoRehydrateRetentionScheduler?: new (d: typeof deps) => Scheduler }).AutoRehydrateRetentionScheduler;
  expect(typeof Ctor, "AutoRehydrateRetentionScheduler is exported by auto-rehydrate-retention.ts").toBe("function");
  return new Ctor!(deps);
}

/** Structurally restore-usable snapshot data; with a token, the occupant resolves to it (findLatestUsableSnapshot). */
function data(rigId: string, token: string | null): string {
  return JSON.stringify({
    rig: { id: rigId, name: rigId, createdAt: "2026-11-01 00:00:00", updatedAt: "2026-11-01 00:00:00" },
    nodes: [{ id: `${rigId}-n1`, rigId, logicalId: "dev.impl" }],
    edges: [],
    sessions: [{ id: `${rigId}-s1`, nodeId: `${rigId}-n1`, sessionName: `dev-impl@${rigId}`, status: "running",
      resumeType: token ? "claude_id" : null, resumeToken: token, restorePolicy: "resume_if_possible",
      lastSeenAt: null, createdAt: "2026-11-01 00:00:00", origin: "launched", startupStatus: "ready", startupCompletedAt: null }],
    activeSessionIdByNode: { [`${rigId}-n1`]: `${rigId}-s1` },
    activeOccupantsByNode: { [`${rigId}-n1`]: { kind: "resolved", sessionId: `${rigId}-s1` } },
    checkpoints: {},
  });
}
const CORRUPT = "{not json";

function insert(db: Database.Database, id: string, rigId: string, kind: string, ageMs: number, body: string, now = NOW): void {
  db.prepare("INSERT INTO snapshots (id, rig_id, kind, data, created_at) VALUES (?, ?, ?, ?, ?)")
    .run(id, rigId, kind, body, sqliteUtc(now - ageMs));
}

function ids(db: Database.Database, rigId: string, kind?: string): string[] {
  const rows = kind
    ? db.prepare("SELECT id FROM snapshots WHERE rig_id = ? AND kind = ? ORDER BY id").all(rigId, kind)
    : db.prepare("SELECT id FROM snapshots WHERE rig_id = ? ORDER BY id").all(rigId);
  return (rows as Array<{ id: string }>).map((row) => row.id);
}

function count(db: Database.Database, rigId: string, kind = "auto-rehydrate"): number {
  return (db.prepare("SELECT COUNT(*) AS n FROM snapshots WHERE rig_id = ? AND kind = ?").get(rigId, kind) as { n: number }).n;
}

function total(db: Database.Database): number {
  return (db.prepare("SELECT COUNT(*) AS n FROM snapshots WHERE kind = 'auto-rehydrate'").get() as { n: number }).n;
}

describe("auto-rehydrate retention", () => {
  let db: Database.Database;
  let rigRepo: RigRepository;
  let sessionRegistry: SessionRegistry;
  let eventBus: EventBus;
  let snapshotRepo: SnapshotRepository;
  let checkpointStore: CheckpointStore;
  let snapshotCapture: SnapshotCapture;

  beforeEach(() => {
    db = createFullTestDb();
    rigRepo = new RigRepository(db);
    sessionRegistry = new SessionRegistry(db);
    eventBus = new EventBus(db);
    snapshotRepo = new SnapshotRepository(db);
    checkpointStore = new CheckpointStore(db);
    snapshotCapture = new SnapshotCapture({ db, rigRepo, sessionRegistry, eventBus, snapshotRepo, checkpointStore });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    db.close();
  });

  describe("the defaults", () => {
    it("keep 5..10, batches of 20..50 and the 24 h floor", () => {
      const d = defaults();
      expect(d.minAgeMs).toBe(DAY);
      expect(Number.isInteger(d.keep) && d.keep >= 5 && d.keep <= 10).toBe(true);
      expect(Number.isInteger(d.batchSize) && d.batchSize >= 20 && d.batchSize <= 50).toBe(true);
    });
  });

  describe("(a) 15 auto-rehydrate captures above and below 24 h", () => {
    const recent = [1 * HOUR, 6 * HOUR, 12 * HOUR, 23 * HOUR, DAY - 1000, DAY];
    const old = [DAY + 1000, 26 * HOUR, 30 * HOUR, 40 * HOUR, 50 * HOUR, 60 * HOUR, 70 * HOUR, 80 * HOUR, 90 * HOUR];

    function seed(): void {
      recent.forEach((age, i) => insert(db, `recent-${i}`, "rig-a", "auto-rehydrate", age, data("rig-a", null)));
      old.forEach((age, i) => insert(db, `old-${i}`, "rig-a", "auto-rehydrate", age, data("rig-a", null)));
      // Other kinds, in excess, old and interleaved with the older auto-rehydrate ones: retention of auto-rehydrate
      // never touches them, and they never take an auto-rehydrate's place in keep.
      OTHER_KINDS.forEach((kind, k) => {
        insert(db, `${kind}-near`, "rig-a", kind, 25 * HOUR + k * HOUR, data("rig-a", `tok-${kind}-near`));
        for (let i = 0; i < 12; i++) insert(db, `${kind}-${i}`, "rig-a", kind, 200 * HOUR + i * HOUR, data("rig-a", `tok-${kind}-${i}`));
      });
      // Another rig's history is not this rig's.
      old.forEach((age, i) => insert(db, `other-rig-${i}`, "rig-other", "auto-rehydrate", age, data("rig-other", null)));
    }

    function expectKeepThreeOld(): void {
      expect(ids(db, "rig-a", "auto-rehydrate")).toEqual(
        ["old-0", "old-1", "old-2", "recent-0", "recent-1", "recent-2", "recent-3", "recent-4", "recent-5"]);
      for (const kind of OTHER_KINDS) expect(count(db, "rig-a", kind)).toBe(13);
      expect(count(db, "rig-other")).toBe(old.length);
    }

    it("keeps every capture under 24 h, the keep newest older ones, and every other kind", () => {
      seed();
      expect(count(db, "rig-a")).toBe(15);
      const deleted = prune(snapshotRepo, "rig-a", { keep: 3, batchSize: 50, nowMs: NOW });
      expect(deleted).toBe(6);
      expectKeepThreeOld();
      // Converged: nothing more to prune.
      expect(prune(snapshotRepo, "rig-a", { keep: 3, batchSize: 50, nowMs: NOW })).toBe(0);
    });

    it("the 24 h floor is the same in a timezone ahead of UTC (created_at is UTC)", () => {
      const tz = process.env.TZ;
      process.env.TZ = "Asia/Tokyo";
      try {
        seed();
        expect(prune(snapshotRepo, "rig-a", { keep: 3, batchSize: 50, nowMs: NOW })).toBe(6);
        expectKeepThreeOld();
      } finally {
        if (tz === undefined) delete process.env.TZ;
        else process.env.TZ = tz;
      }
    });

    it("a capture exactly 24 h old is still under the floor; one second older is not", () => {
      seed();
      prune(snapshotRepo, "rig-a", { keep: 1, batchSize: 50, nowMs: NOW });
      const left = ids(db, "rig-a", "auto-rehydrate");
      expect(left).toContain("recent-5");   // exactly 24 h
      expect(left).toContain("old-0");      // 24 h + 1 s: the newest older one, kept by keep = 1
      expect(left).not.toContain("old-1");
      expect(left.filter((id) => id.startsWith("old-"))).toEqual(["old-0"]);
    });

    it("keep never goes below 1", () => {
      seed();
      prune(snapshotRepo, "rig-a", { keep: 0, batchSize: 50, nowMs: NOW });
      expect(ids(db, "rig-a", "auto-rehydrate").filter((id) => id.startsWith("old-"))).toEqual(["old-0"]);
      insert(db, "old-x", "rig-a", "auto-rehydrate", 100 * HOUR, data("rig-a", null));
      prune(snapshotRepo, "rig-a", { keep: -5, batchSize: 50, nowMs: NOW });
      expect(ids(db, "rig-a", "auto-rehydrate").filter((id) => id.startsWith("old-"))).toEqual(["old-0"]);
    });

    it("without opts, keeps the default keep of the older ones", () => {
      for (let i = 0; i < 20; i++) insert(db, `d-${String(i).padStart(2, "0")}`, "rig-d", "auto-rehydrate", 30 * HOUR + i * HOUR, data("rig-d", null), Date.now());
      pruneAll(snapshotRepo, "rig-d");
      const keep = defaults().keep;
      expect(ids(db, "rig-d", "auto-rehydrate")).toEqual(
        Array.from({ length: keep }, (_, i) => `d-${String(i).padStart(2, "0")}`));
    });

    it("deletes min(batchSize, eligible) per call, oldest first, until it converges", () => {
      for (let i = 0; i < 30; i++) insert(db, `b-${String(i).padStart(2, "0")}`, "rig-b", "auto-rehydrate", 30 * HOUR + i * HOUR, data("rig-b", null));
      const steps: number[] = [];
      for (let i = 0; i < 8; i++) steps.push(prune(snapshotRepo, "rig-b", { keep: 1, batchSize: 7, nowMs: NOW }));
      expect(steps).toEqual([7, 7, 7, 7, 1, 0, 0, 0]);
      expect(ids(db, "rig-b", "auto-rehydrate")).toEqual(["b-00"]);
    });

    it("the first batch takes the oldest rows; a batch below 1 counts as 1", () => {
      for (let i = 0; i < 30; i++) insert(db, `b-${String(i).padStart(2, "0")}`, "rig-b", "auto-rehydrate", 30 * HOUR + i * HOUR, data("rig-b", null));
      expect(prune(snapshotRepo, "rig-b", { keep: 1, batchSize: 7, nowMs: NOW })).toBe(7);
      expect(ids(db, "rig-b", "auto-rehydrate")).toEqual(Array.from({ length: 23 }, (_, i) => `b-${String(i).padStart(2, "0")}`));
      expect(prune(snapshotRepo, "rig-b", { keep: 1, batchSize: 0, nowMs: NOW })).toBe(1);
      expect(prune(snapshotRepo, "rig-b", { keep: 1, batchSize: -3, nowMs: NOW })).toBe(1);
      expect(count(db, "rig-b")).toBe(21);
    });
  });

  describe("(b) the last usable copy", () => {
    function readers(rigId: string) {
      const selected = snapshotRepo.selectRestoreUsable(rigId, undefined, NOW);
      return {
        latestUsable: findLatestUsableSnapshot(db, rigId)?.id ?? null,
        latestUsableAll: findLatestUsableSnapshotsForAllRigs(db).get(rigId)?.id ?? null,
        restoreUsable: snapshotRepo.findLatestRestoreUsable(rigId)?.id ?? null,
        selected: selected.ok ? selected.snapshot.id : selected.code,
      };
    }

    it("10 recent captures without a resume token and 1 older valid one: the valid one survives, the readers answer as before", () => {
      insert(db, "valid-old", "rig-b", "auto-rehydrate", 100 * HOUR, data("rig-b", "tok-valid"));
      for (const age of [30, 28, 26]) insert(db, `tokenless-${age}h`, "rig-b", "auto-rehydrate", age * HOUR, data("rig-b", null));
      for (let i = 1; i <= 10; i++) insert(db, `recent-${String(i).padStart(2, "0")}`, "rig-b", "auto-rehydrate", i * HOUR, data("rig-b", null));
      // Another rig has a newer copy with a token: it is not rig-b's.
      insert(db, "z-valid-50h", "rig-z", "auto-rehydrate", 50 * HOUR, data("rig-z", "tok-z"));
      insert(db, "z-valid-40h", "rig-z", "auto-rehydrate", 40 * HOUR, data("rig-z", "tok-z2"));
      const before = readers("rig-b");
      const deleted = prune(snapshotRepo, "rig-b", { keep: 1, batchSize: 50, nowMs: NOW });
      expect(deleted).toBe(2);
      const left = ids(db, "rig-b", "auto-rehydrate");
      expect(left).toContain("valid-old");
      expect(left).toContain("tokenless-26h");
      expect(left).not.toContain("tokenless-28h");
      expect(left).not.toContain("tokenless-30h");
      expect(left.filter((id) => id.startsWith("recent-"))).toHaveLength(10);
      expect(readers("rig-b")).toEqual(before);
      // Converged with the valid copy still there.
      expect(prune(snapshotRepo, "rig-b", { keep: 1, batchSize: 50, nowMs: NOW })).toBe(0);
      expect(ids(db, "rig-b", "auto-rehydrate")).toContain("valid-old");
      expect(ids(db, "rig-z", "auto-rehydrate")).toEqual(["z-valid-40h", "z-valid-50h"]);
    });

    it("only the newest copy with a resume token is held back: an older valid one is pruned like any other", () => {
      insert(db, "valid-100h", "rig-b", "auto-rehydrate", 100 * HOUR, data("rig-b", "tok-100"));
      insert(db, "valid-90h", "rig-b", "auto-rehydrate", 90 * HOUR, data("rig-b", "tok-90"));
      for (const age of [30, 28, 26]) insert(db, `tokenless-${age}h`, "rig-b", "auto-rehydrate", age * HOUR, data("rig-b", null));
      for (let i = 1; i <= 10; i++) insert(db, `recent-${String(i).padStart(2, "0")}`, "rig-b", "auto-rehydrate", i * HOUR, data("rig-b", null));
      expect(prune(snapshotRepo, "rig-b", { keep: 1, batchSize: 50, nowMs: NOW })).toBe(3);
      const left = ids(db, "rig-b", "auto-rehydrate");
      expect(left).toContain("valid-90h");
      expect(left).not.toContain("valid-100h");
    });

    it("the snapshot selectRestoreUsable picks survives when the newer captures are not restore-usable", () => {
      insert(db, "valid-struct", "rig-c", "auto-rehydrate", 50 * HOUR, data("rig-c", null));
      insert(db, "corrupt-60h", "rig-c", "auto-rehydrate", 60 * HOUR, CORRUPT);
      insert(db, "corrupt-27h", "rig-c", "auto-rehydrate", 27 * HOUR, CORRUPT);
      insert(db, "corrupt-26h", "rig-c", "auto-rehydrate", 26 * HOUR, CORRUPT);
      for (let i = 1; i <= 10; i++) insert(db, `recent-${String(i).padStart(2, "0")}`, "rig-c", "auto-rehydrate", i * HOUR, CORRUPT);
      const before = readers("rig-c");
      expect(before.selected).toBe("valid-struct");
      expect(before.restoreUsable).toBe("valid-struct");
      expect(prune(snapshotRepo, "rig-c", { keep: 1, batchSize: 50, nowMs: NOW })).toBe(2);
      expect(ids(db, "rig-c", "auto-rehydrate")).toEqual(expect.arrayContaining(["valid-struct", "corrupt-26h"]));
      expect(ids(db, "rig-c", "auto-rehydrate")).not.toContain("corrupt-27h");
      expect(ids(db, "rig-c", "auto-rehydrate")).not.toContain("corrupt-60h");
      expect(readers("rig-c")).toEqual(before);
    });
  });

  describe("(c) a snapshot an in-flight restore names", () => {
    type Gate = { entered: Promise<void>; open: () => void };

    /** A rig with a live seat, an old auto-rehydrate target, two newer old ones, and an auto-pre-down holding the
     *  usable copies: nothing but the restore holds the target back. */
    function seedInFlight(name: string) {
      const rig = rigRepo.createRig(name);
      const node = rigRepo.addNode(rig.id, "worker", { role: "worker", runtime: "claude-code" });
      const session = sessionRegistry.registerSession(node.id, `worker@${name}`);
      sessionRegistry.updateStatus(session.id, "running");
      const now = Date.now();
      const target = snapshotCapture.captureSnapshot(rig.id, "auto-rehydrate");
      db.prepare("UPDATE snapshots SET created_at = ? WHERE id = ?").run(sqliteUtc(now - 100 * HOUR), target.id);
      const body = db.prepare("SELECT data FROM snapshots WHERE id = ?").get(target.id) as { data: string };
      insert(db, "old-40h", rig.id, "auto-rehydrate", 40 * HOUR, body.data, now);
      insert(db, "old-30h", rig.id, "auto-rehydrate", 30 * HOUR, body.data, now);
      const withToken = JSON.parse(body.data);
      for (const s of withToken.sessions) { s.resumeType = "claude_id"; s.resumeToken = "tok-pre-down"; }
      insert(db, "pre-down", rig.id, "auto-pre-down", 200 * HOUR, JSON.stringify(withToken), now);
      return { rig, target };
    }

    /** An orchestrator whose seat leases wait, one gate per call, until the test opens it. The seat is live, so a
     *  restore that read its snapshot again by id stops at rig_not_stopped. */
    function gatedOrchestrator() {
      const gates: Gate[] = [];
      let inside = 0;
      const nextGate = (): Gate & { wait: Promise<void>; enter: () => void } => {
        let enter!: () => void;
        const entered = new Promise<void>((resolve) => { enter = resolve; });
        let open!: () => void;
        const wait = new Promise<void>((resolve) => { open = resolve; });
        const gate = { entered, open, wait, enter };
        gates.push(gate);
        return gate;
      };
      const pending: Array<ReturnType<typeof nextGate>> = [];
      const gateFor = (i: number) => { while (pending.length <= i) pending.push(nextGate()); return pending[i]!; };
      let calls = 0;
      const tmux = {
        createSession: vi.fn(async () => ({ ok: true as const })),
        killSession: vi.fn(async () => ({ ok: true as const })),
        sendText: vi.fn(async () => ({ ok: true as const })),
        sendKeys: vi.fn(async () => ({ ok: true as const })),
        getPaneCommand: vi.fn(async () => "claude"),
        getPanePid: vi.fn(async () => 1234),
        capturePaneContent: vi.fn(async () => ""),
        listSessions: async () => [],
        listWindows: async () => [],
        listPanes: async () => [],
        hasSession: async () => true,
        probeSession: async () => ({ state: "present" as const }),
        deliveryGuard: {
          ownsLifecycle: () => inside > 0,
          ownsCustody: () => inside > 0,
          lifecycle: async <T>(_ids: string[], fn: () => Promise<T>): Promise<T> => {
            const gate = gateFor(calls++);
            gate.enter();
            await gate.wait;
            inside++;
            try { return await fn(); } finally { inside--; }
          },
        },
      } as unknown as TmuxAdapter;
      const nodeLauncher = new NodeLauncher({ db, rigRepo, sessionRegistry, eventBus, tmuxAdapter: tmux });
      const orchestrator = new RestoreOrchestrator({
        db, rigRepo, sessionRegistry, eventBus, snapshotRepo, snapshotCapture, checkpointStore, nodeLauncher, tmuxAdapter: tmux,
        claudeResume: { canResume: vi.fn(() => true), resume: vi.fn(async () => ({ ok: true as const })) } as unknown as ClaudeResumeAdapter,
        codexResume: { canResume: vi.fn(() => true), resume: vi.fn(async () => ({ ok: true as const })) } as unknown as CodexResumeAdapter,
      });
      return { orchestrator, gate: (i: number): Gate => gateFor(i) };
    }

    it("restore(id): survives a prune from its first read, a concurrent capture, the drain and another repository instance; prunable once it returns", async () => {
      const { rig, target } = seedInFlight("r-inflight");
      const { orchestrator, gate } = gatedOrchestrator();
      const outcome = orchestrator.restore(target.id);
      // Before any await: the restore has read its snapshot by id and is waiting on the automatic-restore check.
      prune(snapshotRepo, rig.id, { keep: 1, batchSize: 50 });
      expect(ids(db, rig.id, "auto-rehydrate")).toContain(target.id);
      await gate(0).entered;
      // While it waits for its seat leases: another slot's capture, a prune, the drain, and a second repository.
      snapshotCapture.captureSnapshot(rig.id, "auto-rehydrate");
      pruneAll(snapshotRepo, rig.id, { keep: 1, batchSize: 50 });
      const other = new SnapshotRepository(db);
      pruneAll(other, rig.id, { keep: 1, batchSize: 50 });
      await (await scheduler({ db, snapshotRepo: other, keep: 1, batchSize: 50 })).tick();
      expect(ids(db, rig.id, "auto-rehydrate")).toContain(target.id);
      expect(ids(db, rig.id, "auto-rehydrate")).not.toContain("old-40h");
      gate(0).open();
      const result = await outcome;
      expect(result.ok).toBe(false);
      expect((result as { code?: string }).code).toBe("rig_not_stopped");
      // The restore returned: its snapshot is an ordinary old capture again.
      pruneAll(snapshotRepo, rig.id, { keep: 1, batchSize: 50 });
      expect(ids(db, rig.id, "auto-rehydrate")).not.toContain(target.id);
      expect(ids(db, rig.id, "auto-rehydrate")).toContain("old-30h");
    });

    it("two restores of the same id: the snapshot stays held until the last one returns", async () => {
      const { rig, target } = seedInFlight("r-inflight-2");
      const { orchestrator, gate } = gatedOrchestrator();
      const first = orchestrator.restore(target.id);
      await gate(0).entered;
      const second = orchestrator.restore(target.id);
      await gate(1).entered;
      gate(0).open();
      expect((await first).ok).toBe(false);
      pruneAll(snapshotRepo, rig.id, { keep: 1, batchSize: 50 });
      expect(ids(db, rig.id, "auto-rehydrate")).toContain(target.id);
      gate(1).open();
      expect((await second).ok).toBe(false);
      pruneAll(snapshotRepo, rig.id, { keep: 1, batchSize: 50 });
      expect(ids(db, rig.id, "auto-rehydrate")).not.toContain(target.id);
    });

    it("launchSingleNode(…, { snapshotId }): the named snapshot survives a prune while the launch waits", async () => {
      const { rig, target } = seedInFlight("r-inflight-launch");
      const { orchestrator, gate } = gatedOrchestrator();
      const outcome = orchestrator.launchSingleNode(rig.id, "worker", { snapshotId: target.id });
      await gate(0).entered;
      pruneAll(snapshotRepo, rig.id, { keep: 1, batchSize: 50 });
      pruneAll(new SnapshotRepository(db), rig.id, { keep: 1, batchSize: 50 });
      expect(ids(db, rig.id, "auto-rehydrate")).toContain(target.id);
      gate(0).open();
      const result = await outcome;
      expect((result as { code?: string }).code).not.toBe("snapshot_not_found");
      pruneAll(snapshotRepo, rig.id, { keep: 1, batchSize: 50 });
      expect(ids(db, rig.id, "auto-rehydrate")).not.toContain(target.id);
    });
  });

  describe("(d) the backlog drained in bounded batches, outside boot", () => {
    function seedBacklog(rigId: string, n: number, now = Date.now()): void {
      const insertMany = db.transaction(() => {
        for (let i = 0; i < n; i++) insert(db, `${rigId}-old-${String(i).padStart(4, "0")}`, rigId, "auto-rehydrate", 25 * HOUR + i * 60_000, data(rigId, null), now);
        for (let i = 1; i <= 3; i++) insert(db, `${rigId}-recent-${i}`, rigId, "auto-rehydrate", i * HOUR, data(rigId, null), now);
        // The rig's newest copy with a resume token, older than the whole backlog: the drain keeps it too.
        insert(db, `${rigId}-tok`, rigId, "auto-rehydrate", 200 * HOUR, data(rigId, "tok-backlog"), now);
        for (const kind of OTHER_KINDS) insert(db, `${rigId}-${kind}`, rigId, kind, 300 * HOUR, data(rigId, `tok-${kind}`), now);
      });
      insertMany();
    }

    function expectConverged(rigId: string, keep: number): void {
      const left = ids(db, rigId, "auto-rehydrate");
      expect(left).toEqual([
        ...Array.from({ length: keep }, (_, i) => `${rigId}-old-${String(i).padStart(4, "0")}`),
        `${rigId}-recent-1`, `${rigId}-recent-2`, `${rigId}-recent-3`, `${rigId}-tok`,
      ]);
      for (const kind of OTHER_KINDS) expect(count(db, rigId, kind)).toBe(1);
    }

    it("700 old captures drain in ticks of at most batchSize until keep is left", async () => {
      seedBacklog("rig-700", 700);
      const s = await scheduler({ db, snapshotRepo, keep: 5, batchSize: 25 });
      const steps: number[] = [];
      for (let i = 0; i < 60; i++) {
        const before = count(db, "rig-700");
        const n = await s.tick();
        expect(before - count(db, "rig-700")).toBe(n);
        steps.push(n);
        if (n === 0) break;
      }
      expect(steps[0]).toBe(25);
      expect(Math.max(...steps)).toBeLessThanOrEqual(25);
      expect(steps.reduce((a, b) => a + b, 0)).toBe(695);
      expect(steps.filter((n) => n > 0)).toHaveLength(28);
      expectConverged("rig-700", 5);
    });

    it("a tick deletes at most batchSize across all rigs", async () => {
      for (const rigId of ["m1", "m2", "m3"]) seedBacklog(rigId, 100);
      const s = await scheduler({ db, snapshotRepo, keep: 5, batchSize: 20 });
      let sum = 0;
      for (let i = 0; i < 40; i++) {
        const before = total(db);
        const n = await s.tick();
        expect(before - total(db)).toBe(n);
        expect(n).toBeLessThanOrEqual(20);
        sum += n;
        if (n === 0) break;
      }
      expect(sum).toBe(3 * 95);
      for (const rigId of ["m1", "m2", "m3"]) expectConverged(rigId, 5);
    });

    it("without keep and batchSize, the scheduler uses the defaults", async () => {
      seedBacklog("rig-def", 120);
      const { keep, batchSize } = defaults();
      const s = await scheduler({ db, snapshotRepo });
      expect(await s.tick()).toBe(batchSize);
      for (let i = 0; i < 20; i++) if ((await s.tick()) === 0) break;
      expectConverged("rig-def", keep);
    });

    it("start prunes nothing synchronously; the interval drives the ticks; stop ends them", async () => {
      seedBacklog("rig-boot", 120);
      const s = await scheduler({ db, snapshotRepo, keep: 5, batchSize: 25 });
      vi.useFakeTimers();
      s.start(60_000);
      expect(count(db, "rig-boot")).toBe(124);
      await Promise.resolve();
      await Promise.resolve();
      expect(count(db, "rig-boot")).toBe(124);
      expect(s.isActive).toBe(true);
      let last = 124;
      for (let i = 0; i < 10 && last === 124; i++) {
        await vi.advanceTimersByTimeAsync(60_000);
        const now = count(db, "rig-boot");
        expect(last - now).toBeLessThanOrEqual(25);
        last = now;
      }
      expect(last).toBeLessThan(124);
      for (let i = 0; i < 20; i++) {
        await vi.advanceTimersByTimeAsync(60_000);
        const now = count(db, "rig-boot");
        expect(last - now).toBeLessThanOrEqual(25);
        last = now;
      }
      expectConverged("rig-boot", 5);
      s.stop();
      expect(s.isActive).toBe(false);
      insert(db, "rig-boot-late", "rig-boot", "auto-rehydrate", 400 * HOUR, data("rig-boot", null), Date.now());
      await vi.advanceTimersByTimeAsync(60_000 * 5);
      expect(ids(db, "rig-boot", "auto-rehydrate")).toContain("rig-boot-late");
    });

    it("a rig whose deletes fail does not stop the others, and is drained once deletes work again", async () => {
      seedBacklog("r1", 60);
      seedBacklog("r2", 60);
      db.exec("CREATE TEMP TRIGGER qa_fail_r1 BEFORE DELETE ON snapshots WHEN OLD.rig_id = 'r1' BEGIN SELECT RAISE(ABORT, 'simulated delete failure'); END;");
      const s = await scheduler({ db, snapshotRepo, keep: 5, batchSize: 20 });
      for (let i = 0; i < 12; i++) await expect(s.tick()).resolves.toEqual(expect.any(Number));
      expect(count(db, "r1")).toBe(64);
      expectConverged("r2", 5);
      db.exec("DROP TRIGGER qa_fail_r1;");
      for (let i = 0; i < 12; i++) await s.tick();
      expectConverged("r1", 5);
    });

    it("a restart in the middle of the drain picks up where it stopped", async () => {
      seedBacklog("rig-restart", 300);
      const first = await scheduler({ db, snapshotRepo, keep: 5, batchSize: 30 });
      for (let i = 0; i < 4; i++) await first.tick();
      expect(count(db, "rig-restart")).toBe(304 - 120);
      first.stop();
      const afterRestart = await scheduler({ db, snapshotRepo: new SnapshotRepository(db), keep: 5, batchSize: 30 });
      for (let i = 0; i < 20; i++) if ((await afterRestart.tick()) === 0) break;
      expectConverged("rig-restart", 5);
    });
  });

  describe("(e) every capture origin goes through the central method", () => {
    function seedRig(name: string) {
      const rig = rigRepo.createRig(name);
      const node = rigRepo.addNode(rig.id, "worker", { role: "worker", runtime: "claude-code" });
      const session = sessionRegistry.registerSession(node.id, `worker@${name}`);
      sessionRegistry.updateStatus(session.id, "running");
      return rig;
    }

    it("an auto-rehydrate capture prunes one batch of its own rig, oldest first, and no other rig", () => {
      const now = Date.now();
      const rig = seedRig("r-capture");
      const other = seedRig("r-capture-other");
      const tx = db.transaction(() => {
        for (let i = 0; i < 100; i++) {
          insert(db, `c-${String(i).padStart(3, "0")}`, rig.id, "auto-rehydrate", 30 * HOUR + i * HOUR, data(rig.id, null), now);
          insert(db, `o-${String(i).padStart(3, "0")}`, other.id, "auto-rehydrate", 30 * HOUR + i * HOUR, data(other.id, null), now);
        }
      });
      tx();
      const { batchSize } = defaults();
      const snap = snapshotCapture.captureSnapshot(rig.id, "auto-rehydrate");
      expect(ids(db, rig.id, "auto-rehydrate")).toContain(snap.id);
      expect(count(db, rig.id)).toBe(101 - batchSize);
      expect(count(db, other.id)).toBe(100);
      const left = ids(db, rig.id, "auto-rehydrate").filter((id) => id.startsWith("c-"));
      expect(left).toEqual(Array.from({ length: 100 - batchSize }, (_, i) => `c-${String(i).padStart(3, "0")}`));
    });

    it("a capture of another kind never deletes that kind", () => {
      const rig = seedRig("r-capture-kinds");
      for (const kind of OTHER_KINDS) {
        for (let i = 0; i < 30; i++) insert(db, `${kind}-${i}`, rig.id, kind, 30 * HOUR + i * HOUR, data(rig.id, null), Date.now());
      }
      for (const kind of OTHER_KINDS) {
        snapshotCapture.captureSnapshot(rig.id, kind);
        expect(count(db, rig.id, kind)).toBe(31);
      }
    });

    it("a prune that fails never fails the capture", () => {
      const rig = seedRig("r-capture-fail");
      for (let i = 0; i < 30; i++) insert(db, `f-${String(i).padStart(2, "0")}`, rig.id, "auto-rehydrate", 30 * HOUR + i * HOUR, data(rig.id, null), Date.now());
      db.exec("CREATE TEMP TRIGGER qa_fail_all BEFORE DELETE ON snapshots BEGIN SELECT RAISE(ABORT, 'simulated delete failure'); END;");
      const snap = snapshotCapture.captureSnapshot(rig.id, "auto-rehydrate");
      expect(snapshotRepo.getSnapshot(snap.id)?.kind).toBe("auto-rehydrate");
      const event = db.prepare("SELECT payload FROM events WHERE type = 'snapshot.created' ORDER BY seq DESC LIMIT 1").get() as { payload: string };
      expect(JSON.parse(event.payload).snapshotId).toBe(snap.id);
      expect(count(db, rig.id)).toBe(31);
    });

    it("the 7 origins capture through SnapshotCapture.captureSnapshot, and snapshot rows are written and deleted only by the repository", () => {
      const src = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../src");
      const files: string[] = [];
      const walk = (dir: string) => {
        for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
          const full = path.join(dir, entry.name);
          if (entry.isDirectory()) walk(full);
          else if (entry.name.endsWith(".ts") && !entry.name.endsWith(".d.ts")) files.push(full);
        }
      };
      walk(src);
      const rel = (file: string) => path.relative(src, file).split(path.sep).join("/");
      // Comments out: a comment that cites a call is not a call.
      const code = (text: string) => text.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`\\])\/\/.*$/gm, "$1");
      const text = new Map(files.map((file) => [rel(file), code(fs.readFileSync(file, "utf8"))]));
      const RETENTION_OWNERS = new Set(["domain/snapshot-repository.ts", "domain/snapshot-capture.ts", "domain/auto-rehydrate-retention.ts"]);
      const STATEMENT = new Set(["run", "all", "get", "iterate"]);
      const captures = new Map<string, number>();
      for (const [file, body] of text) {
        const n = (body.match(/\.captureSnapshot\(\s*[^,()]+(?:\([^()]*\))?[^,()]*,\s*"auto-rehydrate"\s*\)/g) ?? []).length;
        if (n) captures.set(file, n);
        // Outside the retention owners, a literal "auto-rehydrate" passed as a kind is a capture, with no options.
        if (file === "domain/snapshot-repository.ts" || file === "domain/auto-rehydrate-retention.ts") continue;
        for (const m of body.matchAll(/\b(\w+)\(\s*[^,()]+(?:\([^()]*\))?[^,()]*,\s*"auto-rehydrate"(\s*\))?/g)) {
          if (STATEMENT.has(m[1]!)) continue;
          expect(m[1], `${file}: ${m[0]}`).toBe("captureSnapshot");
          expect(m[2], `${file}: ${m[0]} (no options that skip retention)`).toBeTruthy();
        }
      }
      for (const [file, n] of [["startup.ts", 1], ["domain/existing-rig-restore.ts", 1], ["routes/rigs.ts", 1], ["routes/sessions.ts", 1], ["routes/startup.ts", 3]] as const) {
        expect(captures.get(file) ?? 0, file).toBeGreaterThanOrEqual(n);
      }
      for (const [file, body] of text) {
        if (file !== "domain/snapshot-capture.ts" && file !== "domain/snapshot-repository.ts") {
          expect(/\.createSnapshot\(/.test(body), `${file} calls createSnapshot`).toBe(false);
        }
        if (file !== "domain/snapshot-repository.ts") {
          expect(/DELETE\s+FROM\s+snapshots\b/i.test(body), `${file} deletes snapshot rows`).toBe(false);
          expect(/INSERT\s+INTO\s+snapshots\b/i.test(body), `${file} inserts snapshot rows`).toBe(false);
        }
        if (!RETENTION_OWNERS.has(file)) expect(/\bpruneAutoRehydrate\b/.test(body), `${file} calls the retention itself`).toBe(false);
        if (file.startsWith("routes/")) expect(/\bprune(?:AutoRehydrate|Snapshots\w*)\b/.test(body), `${file} prunes snapshots`).toBe(false);
      }
    });
  });
});
