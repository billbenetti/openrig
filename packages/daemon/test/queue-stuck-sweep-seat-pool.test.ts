// T-007029 (mvschwarz/openrig#2988): a row waiting in an active seat pool's queue (addressed to the pool's model seat
// and carrying its dispatch_tag) is the pool's to move, not a stuck obligation: the stuck sweep files no
// unclaimed-obligation finding for it while the pool is active, not draining, and has had a live seat within
// no_seat_grace_s (default 1800 s). A pool that drains, a stopped pool, a row without the tag, and a pool with no live
// seat for longer than the grace still get the finding.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";
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
  beforeEach(() => {
    db = new Database(":memory:");
    migrate(db, ALL_MIGRATIONS);
    rigs = new RigRepository(db);
    queue = new QueueRepository(db, new EventBus(db), {
      validateRig: () => true, loadHumanRegistry: () => ({ ok: true, entities: [] }),
      resolveSessionAddresses: (name) => (name === MODEL || name === ALIAS ? [MODEL, ALIAS] : [name]),
    });
  });
  afterEach(() => db.close());

  const minutesAgo = (m: number) => new Date(Date.now() - m * 60_000).toISOString();

  function pool(context: Record<string, unknown>, state = "active", target = MODEL): string {
    const jobId = `pool-${++nodeSeq}`;
    const lines = Object.entries({ max: 4, cwd_template: "/w/{slot}", dispatch_tag: TAG, ...context })
      .map(([k, v]) => `  ${k}: ${JSON.stringify(v)}`).join("\n");
    db.prepare(`INSERT INTO watchdog_jobs (job_id, policy, spec_yaml, target_session, interval_seconds, state, registered_by_session, registered_at)
                VALUES (?, 'seat-pool', ?, ?, 60, ?, 'lead@dev', ?)`).run(jobId, `context:\n${lines}\n`, target, state, minutesAgo(600));
    return jobId;
  }
  /** A seat of the pool: `live` with its node present, or retired `retiredMinutesAgo` ago; `launched` (default true)
   *  records in the job's history that the pool launched it, as the executor does. */
  function slot(jobId: string, opts: { live: boolean; retiredMinutesAgo?: number; launched?: boolean }): void {
    const rig = rigs.listRigs().find((r) => r.name === "dev") ?? rigs.createRig("dev");
    const n = ++nodeSeq;
    const node = opts.live ? rigs.addNode(rig.id, `impl-s${n}`, { runtime: "claude-code" }) : null;
    db.prepare(`INSERT INTO seat_pool_slots (pool_job_id, node_id, logical_id, phase, op_id, created_at, retired_at)
                VALUES (?, ?, ?, ?, 'op', ?, ?)`).run(jobId, node?.id ?? null, `impl-s${n}`, opts.live ? "live" : "retired",
      minutesAgo(500), opts.live ? null : minutesAgo(opts.retiredMinutesAgo ?? 300));
    if (opts.launched ?? true) {
      db.prepare(`INSERT INTO watchdog_history (history_id, job_id, evaluated_at, outcome, skip_reason, evaluation_notes)
                  VALUES (?, ?, ?, 'skipped', 'seat_pool_launched', ?)`).run(`h-${n}`, jobId, minutesAgo(499), JSON.stringify({ slot: `impl-s${n}` }));
    }
  }
  async function work(tags: string[] = [TAG], destination = MODEL): Promise<QueueItem> {
    const row = await queue.create({ sourceSession: "lead@dev", destinationSession: destination, body: "work", tags, nudge: false } as never);
    const at = minutesAgo(120);
    db.prepare("UPDATE queue_items SET ts_created = ? WHERE qitem_id = ?").run(at, row.qitemId);
    db.prepare("UPDATE queue_transitions SET ts = ? WHERE qitem_id = ?").run(at, row.qitemId);
    return row;
  }
  async function sweep() {
    return runStuckSweep({ db, queueRepo: queue, status: createStuckSweepStatus(), unclaimedAgeMinutes: 60, log: () => {},
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
    db.prepare("DELETE FROM watchdog_history").run();
    db.prepare("DELETE FROM watchdog_jobs").run();
    db.prepare("DELETE FROM seat_pool_slots").run();
    slot(pool({}), { live: false, retiredMinutesAgo: 45 });
    const past = await work();
    await sweep();
    expect(findingFor(past)).toBe(true);
  });

  it("a seat that failed before it was launched doesn't count as the pool having had one", async () => {
    // Retired 5 minutes ago, but never launched: the pool has had no seat since it was registered (600 minutes ago).
    slot(pool({}), { live: false, retiredMinutesAgo: 5, launched: false });
    const row = await work();
    await sweep();
    expect(findingFor(row)).toBe(true);
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
