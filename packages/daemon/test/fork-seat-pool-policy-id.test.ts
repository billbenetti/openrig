// T-018881: the upstream's seat pool (#3495) took the `seat-pool` id, so the fork's pool runs as `fork-seat-pool`. The
// 183 moves the fork's jobs registered under the old id (the VM's pools: a person's registration, a JSON spec) to the
// new id, in the column and in the spec, and leaves the upstream's jobs (`daemon@kernel`, `context.pool`) as they are.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import { stringify as stringifyYaml } from "yaml";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { migrate } from "../src/db/migrate.js";
import { forkSeatPoolPolicyIdSchema } from "../src/db/migrations/183_fork_seat_pool_policy_id.js";
import { EventBus } from "../src/domain/event-bus.js";
import { FORK_SEAT_POOL_POLICY_NAME, makeSeatPoolPolicy as makeForkSeatPoolPolicy } from "../src/domain/policies/fork-seat-pool.js";
import { makeSeatPoolPolicy, SEAT_POOL_POLICY_NAME } from "../src/domain/policies/seat-pool.js";
import type { PolicyJob } from "../src/domain/policies/types.js";
import { WatchdogHistoryLog } from "../src/domain/watchdog-history-log.js";
import { updateWatchdogJob } from "../src/domain/watchdog-job-update.js";
import { WatchdogJobsRepository } from "../src/domain/watchdog-jobs-repository.js";
import { parseWatchdogSpec, WatchdogPolicyEngine } from "../src/domain/watchdog-policy-engine.js";

const HERE = dirname(fileURLToPath(import.meta.url));
/** The VM's build-dev pool spec as its adapter registers it (pretty-printed JSON, `"policy": "seat-pool"` on top). */
const VM_SPEC_TEXT = readFileSync(join(HERE, "fixtures", "seat-pool-max-adopted", "build-dev.vm.json"), "utf8");
const VM_SPEC = JSON.parse(VM_SPEC_TEXT) as { policy: string; context: Record<string, unknown> };
const before183 = ALL_MIGRATIONS.filter((m) => m.name !== forkSeatPoolPolicyIdSchema.name);

const UPSTREAM_YAML = stringifyYaml({
  policy: "seat-pool",
  target: { session: "dev-impl@pool-rig" },
  context: { model_node_id: "node-model", pool: { max: 3, min: 1, dispatchAfterSeconds: 45 } },
});
const FORK_YAML = "policy: seat-pool\ncontext:\n  max: 2\n  cwd_template: /work/{slot}\n";

interface JobRow { job_id: string; policy: string; spec_yaml: string; state: string; registered_by_session: string }
const jobRows = (db: Database.Database) =>
  db.prepare("SELECT job_id, policy, spec_yaml, state, registered_by_session FROM watchdog_jobs ORDER BY job_id").all() as JobRow[];
const jobRow = (db: Database.Database, jobId: string) => jobRows(db).find((r) => r.job_id === jobId)!;

describe("183_fork_seat_pool_policy_id (T-018881)", () => {
  let db: Database.Database;
  const insert = (jobId: string, specYaml: string, registeredBy: string, target: string, state = "active") =>
    db.prepare(`INSERT INTO watchdog_jobs (job_id, policy, spec_yaml, target_session, interval_seconds, state, registered_by_session, registered_at)
                VALUES (?, 'seat-pool', ?, ?, 60, ?, ?, '2026-11-10T12:00:00.000Z')`).run(jobId, specYaml, target, state, registeredBy);

  beforeEach(() => {
    db = new Database(":memory:");
    // A database the fork's pool already wrote under the old id, beside the upstream's pool jobs.
    migrate(db, before183);
    insert("fork-vm-build-dev", VM_SPEC_TEXT, "plataforma-supervisor@comando", "build-dev-1@fabrica");
    insert("fork-yaml-stopped", FORK_YAML, "ops@kernel", "dev-impl@r", "stopped");
    insert("upstream-daemon", UPSTREAM_YAML, "daemon@kernel", "dev-impl@pool-rig");
    insert("upstream-by-hand", JSON.stringify({ policy: "seat-pool", context: { model_node_id: "node-model", pool: { max: 2 } } }),
      "person@pool-rig", "dev-other@pool-rig");
  });
  afterEach(() => db.close());

  it("sorts after every other migration, the upstream's 182 included", () => {
    const names = ALL_MIGRATIONS.map((m) => m.name).sort((a, b) => a.localeCompare(b));
    expect(names.at(-1)).toBe(forkSeatPoolPolicyIdSchema.name);
    expect(ALL_MIGRATIONS.at(-1)).toBe(forkSeatPoolPolicyIdSchema);
  });

  it("(a) moves a VM pool job to fork-seat-pool in the column and the spec, and the fork's policy is the one that evaluates it", async () => {
    migrate(db, ALL_MIGRATIONS);
    const row = jobRow(db, "fork-vm-build-dev");
    expect(row.policy).toBe(FORK_SEAT_POOL_POLICY_NAME);
    const spec = JSON.parse(row.spec_yaml) as { policy: string; context: Record<string, unknown> };
    expect(spec.policy).toBe("fork-seat-pool");
    expect(spec.context).toEqual(VM_SPEC.context);
    expect(Object.keys(spec)).toEqual(Object.keys(VM_SPEC));
    // a YAML spec the CLI registered moves too, whatever the job's state, and keeps the rest of its text
    expect(jobRow(db, "fork-yaml-stopped")).toMatchObject({
      policy: "fork-seat-pool", state: "stopped", spec_yaml: "policy: fork-seat-pool\ncontext:\n  max: 2\n  cwd_template: /work/{slot}\n",
    });

    // The engine picks a job's policy by its id (resolvePolicy(job.policy) in evaluate): composed as startup composes
    // both pools, the migrated job resolves to the fork's pool, and the fork's evaluation reads the pool's state.
    const readState = vi.fn(async (_job: { jobId: string; targetSession: string }) => { throw new Error("read by the fork"); });
    const forkPolicy = makeForkSeatPoolPolicy({ readState, submit: () => true });
    const upstreamPolicy = makeSeatPoolPolicy({ readBlock: () => undefined, readSlots: () => [] });
    const repo = new WatchdogJobsRepository(db);
    const engine = new WatchdogPolicyEngine({
      jobsRepo: repo, historyLog: new WatchdogHistoryLog(db), eventBus: new EventBus(db), deliver: async () => ({ status: "ok" }),
      additionalPolicies: [forkPolicy, upstreamPolicy],
    });
    const job = repo.getByIdOrThrow("fork-vm-build-dev");
    expect(engine.resolvePolicy(job.policy)).toBe(forkPolicy);
    await expect(forkPolicy.evaluate({
      jobId: job.jobId, policy: job.policy, target: { session: job.targetSession }, intervalSeconds: job.intervalSeconds,
      context: parseWatchdogSpec(job.specYaml).context,
    } as PolicyJob)).rejects.toThrow("read by the fork");
    expect(readState).toHaveBeenCalledWith(expect.objectContaining({ jobId: "fork-vm-build-dev", targetSession: "build-dev-1@fabrica" }));
    // and the moved spec still updates: its policy names the job's policy
    expect(() => updateWatchdogJob(repo, job.jobId, { specYaml: job.specYaml })).not.toThrow();

    // the upstream's jobs resolve to the upstream's pool
    expect(engine.resolvePolicy(repo.getByIdOrThrow("upstream-daemon").policy)).toBe(upstreamPolicy);
  });

  it("(b) leaves the upstream's jobs as `seat-pool`, byte for byte: the daemon's registration and a spec with context.pool", () => {
    const before = jobRows(db).filter((r) => r.job_id.startsWith("upstream-"));
    migrate(db, ALL_MIGRATIONS);
    const after = jobRows(db).filter((r) => r.job_id.startsWith("upstream-"));
    expect(after).toEqual(before);
    expect(after.map((r) => r.policy)).toEqual([SEAT_POOL_POLICY_NAME, SEAT_POOL_POLICY_NAME]);
  });

  it("(c) run again, it changes nothing", () => {
    migrate(db, ALL_MIGRATIONS);
    const once = jobRows(db);
    db.exec(forkSeatPoolPolicyIdSchema.sql);
    expect(jobRows(db)).toEqual(once);
    migrate(db, ALL_MIGRATIONS);
    expect(jobRows(db)).toEqual(once);
  });
});
