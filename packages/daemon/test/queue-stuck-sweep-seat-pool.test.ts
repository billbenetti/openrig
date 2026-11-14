// T-007029 (mvschwarz/openrig#2988): a row waiting in an active seat pool's queue (addressed to the pool's model seat
// and carrying its dispatch_tag) is the pool's to move, not a stuck obligation: the stuck sweep files no
// unclaimed-obligation finding for it while the pool is active, not draining, and has had a live seat within
// no_seat_grace_s (default 1800 s). A pool that drains, a stopped pool, a row without the tag, and a pool with no live
// seat for longer than the grace still get the finding.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { migrate } from "../src/db/migrate.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository, type QueueItem } from "../src/domain/queue-repository.js";
import { runStuckSweep, createStuckSweepStatus } from "../src/domain/queue-stuck-sweep.js";

const MODEL = "impl@dev";
const TAG = "tarefa-nova";
/** Another address of the model seat (the pool's dispatch reads every address of the model). */
const ALIAS = "impl-physical@dev";

describe("stuck sweep: rows waiting in an active seat pool's queue (T-007029, #2988)", () => {
  let db: Database.Database;
  let rigs: RigRepository;
  let queue: QueueRepository;
  let nodeSeq = 0;
  let dir = "";
  /** A file database, so a test can close and open it again as a daemon restart does (T-007545 SD1). */
  function abrir(): void {
    db = new Database(join(dir, "openrig.sqlite"));
    migrate(db, ALL_MIGRATIONS);
    rigs = new RigRepository(db);
    queue = new QueueRepository(db, new EventBus(db), {
      validateRig: () => true, loadHumanRegistry: () => ({ ok: true, entities: [] }),
      resolveSessionAddresses: (name) => (name === MODEL || name === ALIAS ? [MODEL, ALIAS] : [name]),
    });
  }
  /** The daemon restarts: the same database file, a new connection (nothing kept in the old process). */
  function reabrir(): void {
    db.close();
    abrir();
  }
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "seat-pool-custody-"));
    abrir();
  });
  afterEach(() => {
    db.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const minutesAgo = (m: number) => new Date(Date.now() - m * 60_000).toISOString();

  function pool(context: Record<string, unknown>, state = "active", target = MODEL): string {
    const jobId = `pool-${++nodeSeq}`;
    const lines = Object.entries({ max: 4, cwd_template: "/w/{slot}", dispatch_tag: TAG, ...context })
      .map(([k, v]) => `  ${k}: ${JSON.stringify(v)}`).join("\n");
    db.prepare(`INSERT INTO watchdog_jobs (job_id, policy, spec_yaml, target_session, interval_seconds, state, registered_by_session, registered_at)
                VALUES (?, 'seat-pool', ?, ?, 60, ?, 'lead@dev', ?)`).run(jobId, `context:\n${lines}\n`, target, state, minutesAgo(600));
    return jobId;
  }
  /** A seat of the pool: `live` with its node present and its latest session `session` (default "running"), or retired
   *  `retiredMinutesAgo` ago; `hadNode` (default true): the retired seat got its node before it was retired (an add that
   *  failed before its node never had one); `adopted`: a seat the pool adopted (no Operation-Id). */
  function slot(jobId: string, opts: {
    live: boolean; retiredMinutesAgo?: number; hadNode?: boolean; neverLive?: boolean; session?: string; adopted?: boolean;
  }): void {
    const rig = rigs.listRigs().find((r) => r.name === "dev") ?? rigs.createRig("dev");
    const n = ++nodeSeq;
    const nodeId = opts.live ? rigs.addNode(rig.id, `impl-s${n}`, { runtime: "claude-code" }).id : (opts.hadNode ?? true) ? `node-gone-${n}` : null;
    if (opts.live) {
      db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, ?)")
        .run(`session-${n}`, nodeId, `impl-s${n}@dev`, opts.session ?? "running");
    }
    db.prepare(`INSERT INTO seat_pool_slots (pool_job_id, node_id, logical_id, phase, intent, op_id, created_at, retired_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(jobId, nodeId, `impl-s${n}`, opts.live ? "live" : "retired",
      opts.neverLive ? "never_live" : null, opts.adopted ? null : "op", minutesAgo(500),
      opts.live ? null : minutesAgo(opts.retiredMinutesAgo ?? 300));
  }
  async function work(tags: string[] = [TAG], destination = MODEL): Promise<QueueItem> {
    const row = await queue.create({ sourceSession: "lead@dev", destinationSession: destination, body: "work", tags, nudge: false } as never);
    const at = minutesAgo(120);
    db.prepare("UPDATE queue_items SET ts_created = ? WHERE qitem_id = ?").run(at, row.qitemId);
    db.prepare("UPDATE queue_transitions SET ts = ? WHERE qitem_id = ?").run(at, row.qitemId);
    return row;
  }
  async function sweep(now?: Date) {
    return runStuckSweep({ db, queueRepo: queue, status: createStuckSweepStatus(), unclaimedAgeMinutes: 60, now, log: () => {},
      resolveOrchestrator: () => "lead@dev", isRegisteredHost: () => false, resolveRecoveryAgentSeat: () => null, resolveOperatorSeat: () => null });
  }
  const findingFor = (row: QueueItem) => queue.list({ limit: 500 })
    .some((item) => (item.tags ?? []).includes(`recovery-for:${row.qitemId}`));

  it("an active pool with a live seat: no finding for its tagged row", async () => {
    slot(pool({}), { live: true });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(false);
  });

  it("the same pool draining: the finding is back", async () => {
    slot(pool({ drain: true }), { live: true });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("a row without the pool's dispatch_tag: finding", async () => {
    slot(pool({}), { live: true });
    const row = await work(["other"]);
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("no live seat for longer than no_seat_grace_s: finding; within it: none", async () => {
    slot(pool({ no_seat_grace_s: 3600 }), { live: false, retiredMinutesAgo: 30 });
    const within = await work();
    await sweep();
    expect(findingFor(within)).toBe(false);
    db.prepare("DELETE FROM watchdog_jobs").run();
    db.prepare("DELETE FROM seat_pool_slots").run();
    slot(pool({}), { live: false, retiredMinutesAgo: 45 });
    const past = await work();
    await sweep();
    expect(findingFor(past)).toBe(true);
  });

  it("a seat that failed before its node was added doesn't count as the pool having had one", async () => {
    // Retired 5 minutes ago, but before its node was added: the pool has had no seat since it was registered (600 min ago).
    slot(pool({}), { live: false, retiredMinutesAgo: 5, hadNode: false });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("a seat whose node was added but whose launch failed (retired never live) doesn't count either", async () => {
    slot(pool({}), { live: false, retiredMinutesAgo: 5, neverLive: true });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("SeatPoolSlots.retire keeps never_live for a seat retired while adding or added, and nothing for a live one", async () => {
    const { SeatPoolSlots } = await import("../src/domain/seat-pool-executor.js");
    const jobId = pool({});
    const slots = new SeatPoolSlots(db);
    const at = minutesAgo(1);
    const adding = slots.insert({ poolJobId: jobId, logicalId: "impl-a", nodeId: null, phase: "adding", intent: "add", opId: "op-a", at });
    const added = slots.insert({ poolJobId: jobId, logicalId: "impl-b", nodeId: "node-b", phase: "added", intent: "launch", opId: "op-b", at });
    const live = slots.insert({ poolJobId: jobId, logicalId: "impl-c", nodeId: "node-c", phase: "live", intent: null, opId: "op-c", at });
    for (const s of [adding, added, live]) slots.retire(s.slotId, at);
    const intents = db.prepare("SELECT logical_id, phase, intent FROM seat_pool_slots WHERE pool_job_id = ? ORDER BY logical_id").all(jobId);
    expect(intents).toEqual([
      { logical_id: "impl-a", phase: "retired", intent: "never_live" },
      { logical_id: "impl-b", phase: "retired", intent: "never_live" },
      { logical_id: "impl-c", phase: "retired", intent: null },
    ]);
  });

  // B1 of the review (REVISAO-v0713): only a seat the planner counts as live holds the queue: its session running, and
  // an adopted seat that stopped doesn't count. A seat that stopped holds it for no_seat_grace_s from when the sweep
  // first saw it stopped, then the finding comes.
  it("an adopted seat that stopped, with no other seat: no finding within the grace, finding once it passes", async () => {
    slot(pool({}), { live: true, adopted: true, session: "stopped" });
    const row = await work();
    const t0 = new Date();
    await sweep(t0);
    expect(findingFor(row)).toBe(false);
    await sweep(new Date(t0.getTime() + 31 * 60_000));
    expect(findingFor(row)).toBe(true);
  });

  it("a pool seat still live whose session stopped (its reap failing): finding once the grace passes", async () => {
    slot(pool({}), { live: true, session: "stopped" });
    const row = await work();
    const t0 = new Date();
    await sweep(t0);
    expect(findingFor(row)).toBe(false);
    await sweep(new Date(t0.getTime() + 31 * 60_000));
    expect(findingFor(row)).toBe(true);
  });

  // SD1 of the review (REVISAO-v0713, T-007545): the 'stopped since' of a live seat is kept on its slot, not in the
  // process, so a daemon restart after the grace doesn't give the stopped seat a whole grace again.
  it("SD1: a pool seat that stopped, the daemon restarting after the grace: the finding comes, the grace isn't renewed", async () => {
    slot(pool({}), { live: true, session: "exited" });
    const row = await work();
    const t0 = new Date();
    await sweep(t0);
    expect(findingFor(row)).toBe(false);
    reabrir();
    await sweep(new Date(t0.getTime() + 31 * 60_000));
    expect(findingFor(row)).toBe(true);
  });

  it("SD1: the same for an adopted fixed seat that was already stopped when the pool adopted it", async () => {
    slot(pool({}), { live: true, adopted: true, session: "detached" });
    const row = await work();
    const t0 = new Date();
    await sweep(t0);
    expect(findingFor(row)).toBe(false);
    reabrir();
    await sweep(new Date(t0.getTime() + 31 * 60_000));
    expect(findingFor(row)).toBe(true);
  });

  it("SD1: an adopted seat whose session's record says it stopped 40 min ago counts from then, not from the first sweep", async () => {
    slot(pool({}), { live: true, adopted: true, session: "exited" });
    db.prepare("UPDATE sessions SET last_seen_at = datetime('now', '-40 minutes')").run();
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("SD1: a seat seen stopped that runs again starts afresh: its slot no longer carries the 'stopped since'", async () => {
    const jobId = pool({});
    slot(jobId, { live: true, session: "exited" });
    const t0 = new Date();
    await sweep(t0);
    expect(db.prepare("SELECT stopped_since FROM seat_pool_slots WHERE pool_job_id = ?").get(jobId)).not.toEqual({ stopped_since: null });
    db.prepare("UPDATE sessions SET status = 'running'").run();
    await sweep(new Date(t0.getTime() + 60_000));
    expect(db.prepare("SELECT stopped_since FROM seat_pool_slots WHERE pool_job_id = ?").get(jobId)).toEqual({ stopped_since: null });
  });

  // B1 of the REVISAO-T-007545 (SD2): the crash loop through the reap's real path. The launch works (the slot goes live,
  // the pool records its start), the session dies on its own and stays 'running' in sessions (nothing records the death
  // at runtime), and the reap marks the stop and then cleans the seat, which writes its session exited now
  // (SessionRegistry.updateStatus, as cleanSeat does), takes the node and retires the slot; the next add starts. Three
  // turns two minutes apart, shorter than the grace: no seat ever ran a sweep period, so the grace is not renewed and the
  // finding comes within no_seat_grace_s of the first death.
  // The same loop through the executor's own reap is in seat-pool-executor.test.ts; this one runs it to the stuck sweep's
  // finding, with the session written exited by the method cleanSeat uses.
  it("B1: the crash loop in the reap's order, 3 turns shorter than the grace: the sweep's finding comes within the grace", async () => {
    const { SeatPoolSlots } = await import("../src/domain/seat-pool-executor.js");
    const { SessionRegistry } = await import("../src/domain/session-registry.js");
    const jobId = pool({});
    const slots = new SeatPoolSlots(db);
    const sessions = new SessionRegistry(db);
    const rig = rigs.listRigs().find((r) => r.name === "dev") ?? rigs.createRig("dev");
    const row = await work();
    const iso = (ms: number) => new Date(ms).toISOString();
    const t0 = Date.now() - 40 * 60_000;
    for (let turn = 0; turn < 3; turn++) {
      const t = t0 + turn * 2 * 60_000;
      const node = rigs.addNode(rig.id, `impl-loop${turn}`, { runtime: "claude-code" });
      const added = slots.insert({ poolJobId: jobId, logicalId: `impl-loop${turn}`, nodeId: node.id, phase: "added", intent: null, opId: `op-${turn}`, at: iso(t) });
      // the launch works at t + 20 s: live, with its start recorded (SeatPoolSlots.markLive)
      slots.update(added.slotId, { phase: "live" });
      db.prepare("UPDATE seat_pool_slots SET live_at = ? WHERE id = ?").run(iso(t + 20_000), added.slotId);
      db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`s-loop${turn}`, node.id, `impl-loop${turn}@dev`);
      // it dies at t + 30 s with no record; the reap at t + 90 s marks the stop, then the clean writes the session exited
      slots.markStopped(added.slotId, iso(t + 90_000));
      sessions.updateStatus(`s-loop${turn}`, "exited");
      db.prepare("DELETE FROM nodes WHERE id = ?").run(node.id);
      slots.retire(added.slotId, iso(t + 95_000));
    }
    await sweep(new Date(t0 + 30_000 + 30 * 60_000));   // the first death plus the grace
    expect(findingFor(row)).toBe(true);
  });

  it("B1: a pool seat that stopped a minute after its start (not reaped yet) doesn't hold the queue either", async () => {
    const jobId = pool({});
    slot(jobId, { live: true, session: "exited" });
    db.prepare("UPDATE seat_pool_slots SET live_at = ? WHERE pool_job_id = ?").run(minutesAgo(1), jobId);
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("B1: a pool seat that ran longer than a sweep period and then stopped holds the queue for the grace, as before", async () => {
    const jobId = pool({});
    slot(jobId, { live: true, session: "exited" });
    db.prepare("UPDATE seat_pool_slots SET live_at = ? WHERE pool_job_id = ?").run(minutesAgo(45), jobId);
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(false);
  });

  // The edges of SEAT_POOL_MIN_RUN_MS (10 min from the start to the stop as seen; the stop is seen up to a sweep period
  // after the death). A retired seat of the pool, its slot created long before its start (the start is live_at, not
  // created_at), started at `liveAt` and seen stopped (and reaped) at `stoppedAt`.
  function retiredPoolSeat(jobId: string, at: { createdAt: string; liveAt: string | null; stoppedAt: string }): void {
    const n = ++nodeSeq;
    db.prepare(`INSERT INTO seat_pool_slots (pool_job_id, node_id, logical_id, phase, intent, op_id, created_at, live_at, stopped_since, retired_at)
                VALUES (?, ?, ?, 'retired', NULL, 'op', ?, ?, ?, ?)`).run(jobId, `node-gone-${n}`, `impl-s${n}`, at.createdAt, at.liveAt, at.stoppedAt, at.stoppedAt);
  }

  it("B1: a database that already applied 159_seat_pool_slots_stopped_since gets live_at from its own migration", () => {
    const old = new Database(join(dir, "old.sqlite"));
    try {
      // the daemon of 4fda421f7: every migration up to 159_seat_pool_slots_stopped_since, which then added only stopped_since
      migrate(old, ALL_MIGRATIONS.filter((m) => m.name.localeCompare("159_seat_pool_slots_stopped_since.sql") < 0 && m.name !== "159_seat_pool_slots_live_at.sql"));
      old.exec("ALTER TABLE seat_pool_slots ADD COLUMN stopped_since TEXT");
      old.prepare("INSERT INTO schema_migrations (name) VALUES ('159_seat_pool_slots_stopped_since.sql')").run();
      migrate(old, ALL_MIGRATIONS);
      const columns = (old.prepare("PRAGMA table_info(seat_pool_slots)").all() as Array<{ name: string }>).map((c) => c.name);
      expect(columns).toEqual(expect.arrayContaining(["stopped_since", "live_at"]));
    } finally {
      old.close();
    }
  });

  it("B1: a seat that died 4 min after its start, its reap 4 min later (8 min from the start): doesn't count", async () => {
    const jobId = pool({});
    retiredPoolSeat(jobId, { createdAt: minutesAgo(60), liveAt: minutesAgo(12), stoppedAt: minutesAgo(4) });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("B1: a seat that ran 11 min before it was seen stopped counts, and holds the queue for the grace", async () => {
    const jobId = pool({});
    retiredPoolSeat(jobId, { createdAt: minutesAgo(60), liveAt: minutesAgo(21), stoppedAt: minutesAgo(10) });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(false);
  });

  it("B1: a seat seen stopped exactly SEAT_POOL_MIN_RUN_MS after its start counts", async () => {
    const { SEAT_POOL_MIN_RUN_MS } = await import("../src/domain/seat-pool-sweep-custody.js");
    const jobId = pool({});
    const iso = (ms: number) => new Date(ms).toISOString();
    // A stop about 5 min ago whose run of exactly SEAT_POOL_MIN_RUN_MS julianday makes a fraction of a ms short (about 1
    // in 9 such pairs): the case the 1 ms tolerance is for.
    const runMs = db.prepare("SELECT (julianday(?) - julianday(?)) * 86400000.0 AS ms");
    let stop = Date.now() - 5 * 60_000;
    for (let i = 0; (runMs.get(iso(stop), iso(stop - SEAT_POOL_MIN_RUN_MS)) as { ms: number }).ms >= SEAT_POOL_MIN_RUN_MS; i++) {
      if (i > 10_000) throw new Error("no stop whose run julianday makes short");
      stop -= 7;
    }
    retiredPoolSeat(jobId, { createdAt: minutesAgo(60), liveAt: iso(stop - SEAT_POOL_MIN_RUN_MS), stoppedAt: iso(stop) });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(false);
  });

  // The lenses on 00f93b4ae (L1): a seat of the pool whose start was never recorded (a path that leaves live_at unset)
  // is measured from its add, so a crash loop through such a path doesn't renew the grace either.
  it("B1: a pool seat without live_at, added 7 min before it was seen stopped and reaped: doesn't count", async () => {
    const jobId = pool({});
    retiredPoolSeat(jobId, { createdAt: minutesAgo(11), liveAt: null, stoppedAt: minutesAgo(4) });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("B1: a live pool seat without live_at, added 3 min ago and seen stopped: doesn't count", async () => {
    const jobId = pool({});
    slot(jobId, { live: true, session: "exited" });
    db.prepare("UPDATE seat_pool_slots SET created_at = ?, live_at = NULL WHERE pool_job_id = ?").run(minutesAgo(3), jobId);
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("B1: a live seat of the pool seen stopped 8 min after its start (not reaped yet) doesn't count", async () => {
    const jobId = pool({});
    slot(jobId, { live: true, session: "exited" });
    db.prepare("UPDATE seat_pool_slots SET live_at = ? WHERE pool_job_id = ?").run(minutesAgo(8), jobId);
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("SD2: a seat reaped while it still ran counts until its reap, as before", async () => {
    const { SeatPoolSlots } = await import("../src/domain/seat-pool-executor.js");
    const jobId = pool({});
    const rig = rigs.listRigs().find((r) => r.name === "dev") ?? rigs.createRig("dev");
    const node = rigs.addNode(rig.id, "impl-ok", { runtime: "claude-code" });
    db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES ('s-ok', ?, 'impl-ok@dev', 'running')").run(node.id);
    const slots = new SeatPoolSlots(db);
    const live = slots.insert({ poolJobId: jobId, logicalId: "impl-ok", nodeId: node.id, phase: "live", intent: null, opId: "op-1", at: minutesAgo(45) });
    db.prepare("UPDATE seat_pool_slots SET live_at = ? WHERE id = ?").run(minutesAgo(44), live.slotId);   // it ran 39 min
    slots.markStopped(live.slotId, minutesAgo(5));
    db.prepare("DELETE FROM nodes WHERE id = ?").run(node.id);
    slots.retire(live.slotId, minutesAgo(5));
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(false);
  });

  it("an adopted seat that runs holds the queue like the pool's own", async () => {
    slot(pool({}), { live: true, adopted: true, session: "running" });
    const row = await work();
    const t0 = new Date();
    await sweep(t0);
    await sweep(new Date(t0.getTime() + 31 * 60_000));
    expect(findingFor(row)).toBe(false);
  });

  it("a tagged row addressed to another address of the model: no finding", async () => {
    slot(pool({}), { live: true });
    const row = await work([TAG], ALIAS);
    await sweep();
    expect(findingFor(row)).toBe(false);
  });

  it("a stopped pool, or a pool of another seat: finding", async () => {
    slot(pool({}, "stopped"), { live: true });
    slot(pool({}, "active", "other@dev"), { live: true });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
  });

  it("refuses no_seat_grace_s out of range in the spec", async () => {
    const { parseSeatPoolSpec } = await import("../src/domain/policies/seat-pool.js");
    const bad = parseSeatPoolSpec({ max: 2, cwd_template: "/w/{slot}", no_seat_grace_s: 30 });
    expect("problem" in bad && bad.problem.field).toBe("no_seat_grace_s");
    const ok = parseSeatPoolSpec({ max: 2, cwd_template: "/w/{slot}", no_seat_grace_s: 3600 });
    expect("spec" in ok && ok.spec.noSeatGraceSeconds).toBe(3600);
    const none = parseSeatPoolSpec({ max: 2, cwd_template: "/w/{slot}" });
    expect("spec" in none && none.spec).not.toHaveProperty("noSeatGraceSeconds");
  });
});
