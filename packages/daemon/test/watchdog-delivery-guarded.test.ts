// Real scheduler/transport with inert terminal effects: a receipt cannot suppress a current reminder.
import Database from "better-sqlite3";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { queueRecoveryOwnsWake } from "../src/domain/queue-wake-ladder.js";
import { ClaudeCompactionEnforcer } from "../src/domain/claude-compaction-enforcer.js";
import { makeParkedOwnerConsumerPolicy, makeRigAnchor, PARKED_OWNER_POLICY_NAME } from "../src/domain/policies/parked-owner-consumer.js";
import stringWidth from "string-width";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TmuxAdapter } from "../src/adapters/tmux.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { migrate } from "../src/db/migrate.js";
import type { ComposerSnapshot } from "../src/domain/composer-prompts.js";
import { OutboxHandler, OutboxHandlerError } from "../src/domain/outbox-handler.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { SessionTransport } from "../src/domain/session-transport.js";
import { makeWatchdogDeliver } from "../src/domain/watchdog-delivery.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { WatchdogJobsRepository } from "../src/domain/watchdog-jobs-repository.js";
import { WatchdogHistoryLog } from "../src/domain/watchdog-history-log.js";
import { WatchdogPolicyEngine } from "../src/domain/watchdog-policy-engine.js";
import { WatchdogScheduler } from "../src/domain/watchdog-scheduler.js";
import { updateWatchdogJob } from "../src/domain/watchdog-job-update.js";
import { WorkflowInstanceStore } from "../src/domain/workflow-instance-store.js";
import { makeWorkflowKeepalivePolicy } from "../src/domain/policies/workflow-keepalive.js";
import { makeIdleGateQitemPolicy } from "../src/domain/policies/idle-gate-qitem.js";

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); });

function frame(body: string): ComposerSnapshot {
  const rows = body.split("\n").flatMap(line => {
    const chunks: string[] = []; let row = "";
    for (const char of line) {
      if (stringWidth(row + char) > 238) { chunks.push(row); row = ""; }
      row += char;
    }
    return [...chunks, row];
  });
  return { screen: ["Ready", "────────────────────", `❯ ${rows[0]}`, ...rows.slice(1).map(row => `  ${row}`), "────────────────────", "? for shortcuts", ""].join("\n"),
    cursor: { x: 2 + stringWidth(rows.at(-1)!), y: 1 + rows.length, width: 240, height: 60 }, inMode: false };
}

/** One draft-aware seat on a fake tmux whose only effect is recording what was pasted and submitted. */
function fixture(saved?: Buffer) {
  const db = new Database(saved ?? ":memory:");
  if (!saved) {
    migrate(db, ALL_MIGRATIONS);
    db.exec(`INSERT INTO rigs(id,name) VALUES ('rig','test');
      INSERT INTO nodes(id,rig_id,logical_id,runtime) VALUES ('a','rig','worker','claude-code');
      INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('ba','a','worker@test','%1');
      INSERT INTO sessions(id,node_id,session_name,status) VALUES ('sa','a','worker@test','running');
      INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES ('ga','a',1,'g1','fresh');`);
  }
  const guard = new SeatDeliveryGuard(db, name => resolveGuardTarget(db, name));
  guard.recoverActivation();
  let body: string | null = "";
  let history = "";
  let staged = "";
  const submissions: string[] = [];
  const pastes: string[] = [];
  const hooks: { paste?: () => Promise<void>; stage?: () => Promise<void>; pendingStage?: (session: string) => boolean } = {};
  const tmux = new TmuxAdapter(async command => {
    if (command.includes("paste-buffer")) { await hooks.paste?.(); body = staged; pastes.push(staged); }
    if (command.includes("send-keys") && command.includes("Enter")) { submissions.push(body ?? ""); history += `${body}\n`; body = ""; }
    return "";
  }, { writeFile: async (_path, text) => { await hooks.stage?.(); staged = text; }, unlink: async () => {}, tmpName: () => "/fixture/draft-input", bufferName: () => "draft-input" });
  tmux.deliveryGuard = guard;
  vi.spyOn(tmux, "probeSession").mockResolvedValue({ state: "present" });
  vi.spyOn(tmux, "listPanes").mockImplementation(async name => [{ id: guard.target(name).pane! } as never]);
  vi.spyOn(tmux, "getPaneCommand").mockResolvedValue("node");
  vi.spyOn(tmux, "captureComposerSnapshot").mockImplementation(async () => body == null ? null : frame(body));
  vi.spyOn(tmux, "capturePaneContent").mockImplementation(async () => body == null ? null : history + frame(body).screen);
  const at = Date.now();
  const transport = new SessionTransport({ db, tmuxAdapter: tmux, rigRepo: new RigRepository(db), sessionRegistry: new SessionRegistry(db),
    now: () => new Date(at), sleep: async () => {},
    agentActivityStore: { getLatestForNode: () => ({ state: "idle", reason: "fixture idle hook", evidenceSource: "runtime_hook", sampledAt: new Date(at).toISOString(), evidence: null }) } as never });
  transport.guardedDelivery!.recover();
  const outbox = new OutboxHandler(db);
  const deliveryDeps = { sessionTransport: transport, hasPendingPostCompactStage: (session: string) => hooks.pendingStage?.(session) ?? false };
  const deliver = makeWatchdogDeliver(deliveryDeps);
  cleanup.push(async () => { await transport.guardedDelivery!.stop(); db.close(); });
  return { db, outbox, deliver, transport, submissions, pastes, hooks, hold: () => guard.set("a", { mode: "hold" }, "human@test", "hold input"), setInput: (text: string) => { body = text; }, enable: () => guard.set("a", { mode: "draft-aware" }, "human@test", "keep draft") };
}

const seat = "worker@test";

/** The queue, watchdog engine and scheduler on a fixture's store, wired as the daemon wires them. `withoutPreview`
 *  leaves out the park-entry lookup, so wakes go with their stored message, as a daemon before the preview sent them. */
function scheduled(f: ReturnType<typeof fixture>, { withoutPreview = false, parkedRows = false, queueWait = false } = {}) {
  const bus = new EventBus(f.db);
  const repo = new QueueRepository(f.db, bus, { transport: f.transport as never, loadHumanRegistry: () => ({ ok: true, entities: [], warnings: [] }) });
  repo.attachOutbox(f.outbox);
  const jobs = new WatchdogJobsRepository(f.db);
  repo.attachWatchdogJobsRepository(jobs);
  const history = new WatchdogHistoryLog(f.db);
  const engine = new WatchdogPolicyEngine({ jobsRepo: jobs, historyLog: history, eventBus: bus, deliver: f.deliver,
    resolvePreDeliveryTerminalReason: ({ jobId }) => repo.resolveWatchdogPreDeliveryTerminalReason(jobId),
    onWakeAttempt: ({ jobId, deliveryStatus, retryAt, queuedQitemId, message }: { jobId: string; deliveryStatus: string; retryAt?: string; queuedQitemId?: string; message?: string }) =>
      (repo.recordWatchdogWakeAttempt as (...a: unknown[]) => void)(jobId, deliveryStatus, retryAt, queuedQitemId, message),
    ...(withoutPreview ? {} : { resolveDigestEntry: ({ jobId, message }: { jobId: string; message: string }) => repo.parkReminderDigestEntry(jobId, message) }),
    // As startup wires them (T-007715). Optional chaining keeps the fixture loadable on a base without the method, so
    // the tests that need it fail there by behaviour (a wd- row), not by a missing function.
    ...(queueWait ? { resolveQueueWait: (input: { jobId: string }) => repo.evaluateWaitReminder(input) } : {}),
    ...(parkedRows ? { resolveParkedRows: ({ jobId, targetSession }: { jobId: string; targetSession: string }) =>
      (repo as unknown as { parkedRowsOwnedBy?: (j: string, o: string) => string[] }).parkedRowsOwnedBy?.(jobId, targetSession) ?? [] } : {}),
  } as never);
  const scheduler = new WatchdogScheduler({ jobsRepo: jobs, policyEngine: engine });
  const park = async (summary: string, transitionNote = "parked") => {
    const row = await repo.create({ sourceSession: "orch@test", destinationSession: seat, body: summary, summary, nudge: false } as never) as unknown as { qitemId: string };
    repo.update({ qitemId: row.qitemId, actorSession: seat, state: "blocked", blockedOn: "external:wait", transitionNote, wakeAfterSeconds: 600 } as never);
    return { qitemId: row.qitemId, jobId: repo.getParkWakeStatus(row.qitemId)!.ref };
  };
  const due = (jobId: string) => {
    const past = new Date(Date.now() - 3_600_000).toISOString();
    f.db.prepare("UPDATE watchdog_jobs SET last_evaluation_at = ?, registered_at = ? WHERE job_id = ?").run(past, past, jobId);
  };
  return { repo, jobs, history, scheduler, park, due };
}
const lone = (jobId: string, occurrenceId: string) => ({ jobId, policy: "periodic-reminder", occurrenceId });
const digest = (...members: Array<[string, string]>) => ({
  jobId: `digest-${members.map(([, o]) => o).join("+")}`, policy: "periodic-reminder", occurrenceId: `digest-${members.map(([, o]) => o).join("+")}`,
  members: members.map(([jobId, occurrenceId]) => ({ jobId, occurrenceId })),
});

describe("watchdog reminders reach the recipient despite earlier delivery records", () => {
  it("retries an unaccounted condition and still delivers a changed condition", async () => {
    const f = fixture();
    await f.enable();
    const s = scheduled(f);
    await s.repo.create({ sourceSession: seat, destinationSession: seat, body: "review", tags: ["gate:review"], nudge: false });
    const policy = (db: Database.Database) => makeIdleGateQitemPolicy({ db, seatActivity: {
      getSeatStateBySession: () => ({ activity: "idle-at-prompt", needsInput: { count: 0 } }),
    } as never });
    const history = new WatchdogHistoryLog(f.db);
    const engine = new WatchdogPolicyEngine({ jobsRepo: s.jobs, historyLog: history, eventBus: new EventBus(f.db),
      deliver: f.deliver, additionalPolicies: [policy(f.db)] });
    const job = s.jobs.register({ policy: "idle-gate-qitem", targetSession: seat, intervalSeconds: 60, registeredBySession: seat,
      specYaml: "message: Gate ready\n" });
    vi.spyOn(s.jobs, "recordConditionReceipt").mockImplementationOnce(() => { throw new Error("receipt interrupted"); });
    await expect(engine.evaluate(job)).rejects.toThrow("receipt interrupted");
    expect(f.submissions).toHaveLength(1);

    const g = fixture(f.db.serialize());
    const jobs = new WatchdogJobsRepository(g.db);
    const reopened = new WatchdogPolicyEngine({ jobsRepo: jobs, historyLog: new WatchdogHistoryLog(g.db),
      eventBus: new EventBus(g.db), deliver: g.deliver, additionalPolicies: [policy(g.db)] });
    expect((await reopened.evaluate(jobs.getByIdOrThrow(job.jobId))).delivery).toMatchObject({ status: "ok" });
    expect(g.submissions).toHaveLength(1);
    expect(g.submissions[0]).toContain("Gate ready");
    expect(g.submissions[0]).toMatch(/may (?:be repeated|repeat)/i);
    expect((await reopened.evaluate(jobs.getByIdOrThrow(job.jobId))).outcome).toMatchObject({ action: "skip", reason: "gate_condition_unchanged" });
    await scheduled(g).repo.create({ sourceSession: seat, destinationSession: seat, body: "another review", tags: ["gate:review"], nudge: false });
    await reopened.evaluate(jobs.getByIdOrThrow(job.jobId));
    expect(g.submissions).toHaveLength(2);
  });

  it.each(["sending", "indeterminate", "delivered", "failed"] as const)("delivers the current reminder despite an old %s digest record", async (state) => {
    const f = fixture();
    await f.enable();
    const id = "guard-watchdog-occ-a";
    f.outbox.record({ outboxId: id, senderSession: "watchdog@system", destinationSession: seat,
      body: "old body", tags: ["watchdog:digest"] });
    f.db.prepare("UPDATE outbox_entries SET delivery_state = ? WHERE outbox_id = ?").run(state, id);
    const result = await f.deliver({ targetSession: seat, message: "Current A and B" }, digest(["job-a", "occ-a"], ["job-b", "occ-b"]));
    expect(result).toMatchObject({ status: "ok" });
    expect(f.submissions).toHaveLength(1);
    expect(f.submissions[0]).toContain("Current A and B");
    expect(f.submissions[0]).toMatch(/may (?:be repeated|repeat)/i);
    expect(f.submissions[0]).not.toContain("old body");
  });

  it("delivers without requiring the watchdog's outbox bookkeeping", async () => {
    const f = fixture();
    await f.enable();
    vi.spyOn(f.outbox, "record").mockImplementation(() => { throw new Error("watchdog bookkeeping unavailable"); });
    expect(await f.deliver({ targetSession: seat, message: "Both waits are due" }, digest(["a", "a1"], ["b", "b1"]))).toMatchObject({ status: "ok" });
    expect(f.submissions).toHaveLength(1);
    expect(f.submissions[0]).toContain("Both waits are due");
  });

  it.each([false, true])("keeps recurring after accounting interruption: %s", async (interrupted) => {
    const f = fixture();
    await f.enable();
    const instance = new WorkflowInstanceStore(f.db).create({ workflowName: "ongoing", workflowVersion: "1", createdBySession: seat });
    let at = Date.now();
    const jobs = new WatchdogJobsRepository(f.db, () => new Date(at));
    const history = new WatchdogHistoryLog(f.db);
    const engine = new WatchdogPolicyEngine({ jobsRepo: jobs, historyLog: history, eventBus: new EventBus(f.db),
      deliver: f.deliver, now: () => new Date(at), additionalPolicies: [makeWorkflowKeepalivePolicy({ db: f.db })] });
    const job = jobs.register({ policy: "workflow-keepalive", targetSession: seat, intervalSeconds: 60, registeredBySession: seat,
      specYaml: `message: Continue the current step\ncontext:\n  workflow_instance_id: ${instance.instanceId}\n` });
    if (interrupted) {
      vi.spyOn(history, "record").mockImplementationOnce(() => { throw new Error("accounting interrupted"); });
      await expect(engine.evaluate(jobs.getByIdOrThrow(job.jobId))).rejects.toThrow("accounting interrupted");
    }
    expect((await engine.evaluate(jobs.getByIdOrThrow(job.jobId))).delivery).toMatchObject({ status: "ok" });
    expect(f.submissions).toHaveLength(interrupted ? 2 : 1);
    at += 60_000;
    await engine.evaluate(jobs.getByIdOrThrow(job.jobId));
    expect(f.submissions).toHaveLength(interrupted ? 3 : 2);
    expect(f.submissions.at(-1)).toContain("Continue the current step");
  });

  it("reopens an interrupted wake and submits all current reminders with their continuations", async () => {
    const f = fixture();
    await f.enable();
    const first = scheduled(f, { withoutPreview: true });
    const a = await first.park("A's wait", "continuation: hand the head to the planner");
    first.due(a.jobId);
    let reached!: () => void;
    const pasting = new Promise<void>((done) => { reached = done; });
    let unblock!: () => void;
    f.hooks.paste = () => { reached(); return new Promise((done) => { unblock = done; }); };
    const lost = first.scheduler.runTickNow();
    await pasting;
    const g = fixture(f.db.serialize());
    // Settle the first process before assertions, while retaining its interrupted store as the recovery input.
    unblock();
    await lost;
    const next = scheduled(g);
    const b = await next.park("B's wait");
    next.due(b.jobId);
    await next.scheduler.runTickNow();
    expect(g.submissions).toHaveLength(1);
    expect(g.submissions[0]).toContain(a.qitemId);
    expect(g.submissions[0]).toContain(b.qitemId);
    expect(g.submissions[0]).toContain("hand the head to the planner");
    expect(g.submissions[0]).toMatch(/may (?:be repeated|repeat)/i);
    for (const { jobId } of [a, b]) expect(next.history.listForJob(jobId, 1)[0]).toMatchObject({ deliveryStatus: "ok" });
  });

  it("still protects a person's draft instead of submitting the reminder", async () => {
    const f = fixture();
    await f.enable();
    f.setInput("a person's unfinished message");
    expect(await f.deliver({ targetSession: seat, message: "Reminder" }, lone("a", "a1"))).toMatchObject({ status: "retained" });
    expect(f.submissions).toEqual([]);
  });
});


/** Production queue/policy/engine/delivery/transport; only terminal input is inert and recorded. */
async function parkedReminder(f: ReturnType<typeof fixture>, noteFailure?: "false" | "throw") {
  const s = scheduled(f);
  const row = await s.repo.create({ sourceSession: seat, destinationSession: seat, body: "Current owned work", nudge: false });
  s.repo.claim({ qitemId: row.qitemId, destinationSession: seat });
  const policy = makeParkedOwnerConsumerPolicy({
    diagnoseRig: () => ({ seats: [{ sessionName: seat, parked: true,
      activity: { value: "idle-at-prompt", needsInput: { count: 0, reason: null } },
      obligations: { items: [{ qitemId: row.qitemId, state: "in-progress", summary: null }], held: [] } }] }),
    history: s.history, occupantGeneration: () => "g1",
    hasPendingPostCompactStage: session => f.hooks.pendingStage?.(session) ?? false,
    rows: {
      listTransitions: id => s.repo.listTransitions(id),
      appendNote: (id, note) => {
        if (noteFailure === "throw") throw new Error("receipt write unavailable");
        if (noteFailure === "false") return { ok: false };
        s.repo.update({ qitemId: id, actorSession: "watchdog@system", transitionNote: note });
        return { ok: true };
      },
      recordNudgeResult: (id, result) => s.repo.recordNudgeAttempt(id, result),
      listOpenIds: session => s.repo.listOpenObligations(session, 500).map(q => q.qitemId),
    },
  });
  const job = s.jobs.register({ policy: PARKED_OWNER_POLICY_NAME, targetSession: makeRigAnchor("test"), intervalSeconds: 120,
    registeredBySession: seat, specYaml: "interval_seconds: 120\n" });
  let at = Date.now();
  const engine = new WatchdogPolicyEngine({ jobsRepo: s.jobs, historyLog: s.history, eventBus: new EventBus(f.db),
    deliver: f.deliver, additionalPolicies: [policy], now: () => new Date(at) });
  const tick = () => { at += 120_000; return engine.evaluate(s.jobs.getByIdOrThrow(job.jobId)); };
  return { ...s, row, job, policy, tick };
}

function compactionFor(f: ReturnType<typeof fixture>) {
  const home = mkdtempSync(join(tmpdir(), "parked-input-stage-"));
  cleanup.push(async () => { rmSync(home, { recursive: true, force: true }); });
  const transport = { send: async (_name: string, _text: string, opts?: { beforeWrite?: () => void }) => {
    opts?.beforeWrite?.(); return { ok: true };
  }, waitUntilIdle: async () => ({ ok: true }) } as unknown as SessionTransport;
  const e = new ClaudeCompactionEnforcer({ resolveClaudeCompactionPolicy: () => ({ enabled: true, thresholdPercent: 80,
    preCompactInstruction: "Write a restore map.", compactInstruction: "", messageInline: "", messageFilePath: "", postRestoreAuditInstruction: "",
  }) } as never, transport, { openrigHome: home, resolveOccupantGeneration: () => "g1" });
  f.hooks.pendingStage = name => e.hasPendingPostCompactStage(name);
  const submit = async () => {
    expect(await e.triggerManualCompact({ sessionName: seat, runtime: "claude-code", usedPercentage: 90 },
      { operatorInitiated: true, skipMap: true })).toMatchObject({ triggered: true });
    expect(e.hasPendingPostCompactStage(seat)).toBe(true);
  };
  return { e, submit };
}

describe("current parked-owner reminders", () => {
  it.each(["reserved", "failed", "unconfirmed"])("parked current input survives an old %s episode record", async state => {
    const f = fixture();
    const s = await parkedReminder(f);
    // A previous process reserved this exact obligation; the current physical request is new.
    const prior = await s.policy.evaluate({ target: { session: makeRigAnchor("test") }, jobId: s.job.jobId } as never);
    expect(prior.action).toBe("send");
    if (state !== "reserved") s.history.record({ jobId: s.job.jobId, evaluatedAt: new Date().toISOString(), outcome: "sent",
      deliveryStatus: state === "delivered" ? "ok" : "failed", deliveryTargetSession: seat,
      evaluationNotes: { ...prior.notes, deliveryReason: state === "unconfirmed" ? "Text write result unconfirmed" : "transport failed" } });
    expect((await s.tick()).delivery).toMatchObject({ status: "ok" });
    expect(f.submissions).toHaveLength(1);
    expect(f.submissions[0]).toContain(s.row.qitemId);
    expect(f.submissions[0]).toMatch(/may repeat/i);
  });

  it.each(["false", "throw"] as const)("parked current input does not require a pre-delivery note: %s", async failure => {
    const f = fixture();
    const s = await parkedReminder(f, failure);
    expect((await s.tick()).delivery).toMatchObject({ status: "ok" });
    expect(f.submissions).toHaveLength(1);
    expect(f.submissions[0]).toContain(s.row.qitemId);
  });

  it("parked current input survives unavailable watchdog telemetry", async () => {
    const f = fixture(); const s = await parkedReminder(f);
    vi.spyOn(s.history, "listForJob").mockImplementation(() => { throw new Error("telemetry read unavailable"); });
    expect((await s.tick()).delivery).toMatchObject({ status: "ok" });
    expect(f.submissions).toHaveLength(1);
  });

  describe.each([false, true])("late compaction with draft-aware=%s", draftAware => {
  it.each(["cancel", "invalidate"] as const)("parked compaction beginning during buffer setup prevents input, then retries after %s", async end => {
    const f = fixture();
    if (draftAware) await f.enable();
    const compact = compactionFor(f);
    const s = await parkedReminder(f);
    f.hooks.stage = compact.submit; // after policy admission, while the real adapter awaits its buffer
    const first = await s.tick();
    expect(f.pastes).toEqual([]);
    expect(f.submissions).toEqual([]);
    expect(first.delivery?.status).not.toBe("ok");
    expect(s.history.listForJob(s.job.jobId, 1)[0]!.evaluationNotes?.["deliveryReason"]).toContain("[managed-compaction-pending]");
    await s.tick();
    expect(s.repo.getById(s.row.qitemId)?.lastNudgeResult ?? "").not.toMatch(/^failed:/);
    expect(queueRecoveryOwnsWake(f.db, s.repo.getById(s.row.qitemId))).toBe(false);
    expect(s.repo.listTransitions(s.row.qitemId).some(t => t.transitionNote?.startsWith("parked-owner wake delivery failed:"))).toBe(false);
    f.hooks.stage = undefined;
    if (end === "cancel") compact.e.cancelPreparation(seat); else compact.e.invalidateOccupant(seat);
    expect(compact.e.hasPendingPostCompactStage(seat)).toBe(false);
    expect((await s.tick()).delivery).toMatchObject({ status: "ok" });
    expect(f.submissions).toHaveLength(1);
  });

  it("parked compaction beginning after paste keeps its actual partial-input outcome", async () => {
    const f = fixture();
    if (draftAware) await f.enable();
    const compact = compactionFor(f); const s = await parkedReminder(f);
    f.hooks.paste = compact.submit;
    const first = await s.tick();
    expect(f.pastes).toHaveLength(1);
    expect(f.submissions).toEqual([]);
    expect(first.delivery).toMatchObject({ status: "failed", error: expect.stringContaining("Text was written") });
    expect(s.history.listForJob(s.job.jobId, 1)[0]!.evaluationNotes?.["deliveryInputPhase"]).toBe("written");
    expect(s.history.listForJob(s.job.jobId, 1)[0]!.evaluationNotes?.["deliveryReason"]).toContain("[managed-compaction-pending]");
    await s.tick();
    expect(s.repo.getById(s.row.qitemId)?.lastNudgeResult ?? "").not.toMatch(/^failed:/);
    expect(queueRecoveryOwnsWake(f.db, s.repo.getById(s.row.qitemId))).toBe(false);
    expect(s.repo.listTransitions(s.row.qitemId).some(t => t.transitionNote?.startsWith("parked-owner wake delivery failed:"))).toBe(false);
  });

  });

  it("parked compaction hold is scoped: peer sends and unrelated watchdog policies still input", async () => {
    const f = fixture();
    const compact = compactionFor(f); await compact.submit();
    expect(await f.transport.send(seat, "Peer context")).toMatchObject({ ok: true });
    expect(await f.deliver({ targetSession: seat, message: "Unrelated reminder" }, lone("other", "occ"))).toMatchObject({ status: "ok" });
    expect(f.submissions).toHaveLength(2);
  });
});


describe("watchdog positive no-input outcomes", () => {
  it("preserves a real held-quota rejection through transport and engine history", async () => {
    const f = fixture();
    await f.hold();
    const target = resolveGuardTarget(f.db, seat)!;
    // Distinct senders fill the quota: a seat holds one message per sender (#2430).
    for (let i = 0; i < 100; i++) f.outbox.retain({ outboxId: `held-${i}`, senderSession: `filler-${i}@test`, destinationSession: seat, body: "held" }, target);
    const jobs = new WatchdogJobsRepository(f.db);
    const history = new WatchdogHistoryLog(f.db);
    const engine = new WatchdogPolicyEngine({ jobsRepo: jobs, historyLog: history, eventBus: new EventBus(f.db), deliver: f.deliver });
    const job = jobs.register({ policy: "periodic-reminder", targetSession: seat, intervalSeconds: 60, registeredBySession: seat, specYaml: "message: Still owed\n" });
    await engine.evaluate(job);
    expect(history.listForJob(job.jobId, 1)[0]).toMatchObject({ deliveryStatus: "failed", evaluationNotes: { deliveryCode: "retained_quota_full", deliveryInputPhase: "not_reached" } });
    expect(f.outbox.retentionUsage("a").count).toBe(100);
    expect(f.pastes).toHaveLength(0);
    expect(f.submissions).toHaveLength(0);
  });

  it.each([undefined, "attempted", "written"] as const)("does not promote a quota-shaped result with phase %s", async inputPhase => {
    const deliver = makeWatchdogDeliver({ sessionTransport: { send: async () => ({ ok: false, sessionName: seat, sent: false, reason: "retained_quota_full", error: "quota-shaped error", ...(inputPhase ? { inputPhase } : {}) }) } });
    const result = await deliver({ targetSession: seat, message: "Still owed" }, { jobId: "quota-result-control", policy: PARKED_OWNER_POLICY_NAME, occurrenceId: "one" });
    expect(result).not.toHaveProperty("code");
    if (inputPhase) expect(result).toHaveProperty("inputPhase", inputPhase);
    else expect(result).not.toHaveProperty("inputPhase");
  });

  it("does not infer no input from an untyped error with the same text", async () => {
    const deliver = makeWatchdogDeliver({ sessionTransport: { send: async () => { throw new Error("Held-message quota is full; no input was written."); } } });
    const result = await deliver({ targetSession: seat, message: "Still owed" }, { jobId: "quota-control", policy: PARKED_OWNER_POLICY_NAME, occurrenceId: "one" });
    expect(result).not.toHaveProperty("code");
    expect(result).not.toHaveProperty("inputPhase");
  });

  it("excludes bound terminal-only parked reminders while preserving other deliberate deliveries", async () => {
    const sends = vi.fn().mockResolvedValue({ ok: true });
    const deliver = makeWatchdogDeliver({ sessionTransport: { send: sends }, isTerminalSeat: () => true } as Parameters<typeof makeWatchdogDeliver>[0]);
    const refused = await deliver({ targetSession: seat, message: "Parked wake" }, { jobId: "terminal-policy", policy: PARKED_OWNER_POLICY_NAME, occurrenceId: "one" });
    expect(refused).toMatchObject({ status: "failed", code: "terminal_seat", inputPhase: "not_reached" });
    expect(sends).not.toHaveBeenCalled();
    const deliberate = await deliver({ targetSession: seat, message: "Explicitly requested terminal text" }, { jobId: "terminal-explicit", policy: "periodic-reminder", occurrenceId: "two" });
    expect(deliberate.status).toBe("ok");
    expect(sends).toHaveBeenCalledTimes(1);
  });

  it("T-006631: at a queue-only seat, a reminder with no parked row of origin is one cold queue row, never typed", async () => {
    const sends = vi.fn().mockResolvedValue({ ok: true });
    const rows: Array<{ qitemId: string; destinationSession: string; body: string; summary: string; tags: string[] }> = [];
    const deliver = makeWatchdogDeliver({ sessionTransport: { send: sends }, isQueueOnlySeat: s => s === seat,
      createQueueRow: async row => { if (!rows.some(r => r.qitemId === row.qitemId)) rows.push(row); } });
    const wake = { jobId: "wake-1", policy: "periodic-reminder", occurrenceId: "occ-1" };
    expect(await deliver({ targetSession: seat, message: "Check the deploy." }, wake)).toMatchObject({ status: "ok", code: "queued_as_row" });
    expect(await deliver({ targetSession: seat, message: "Check the deploy." }, wake)).toMatchObject({ status: "ok", code: "queued_as_row" });
    const digest = { jobId: "digest", policy: "periodic-reminder", members: [{ jobId: "a", occurrenceId: "o-a" }, { jobId: "b", occurrenceId: "o-b" }] };
    expect(await deliver({ targetSession: seat, message: "Two reminders" }, digest)).toMatchObject({ status: "ok" });
    expect(sends).not.toHaveBeenCalled();
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ destinationSession: seat, tags: ["watchdog", "queue-only", "policy:periodic-reminder"] });
    expect(rows[0]!.body).toContain("[OpenRig watchdog scheduler · policy: periodic-reminder · job: wake-1]");
    expect(rows[0]!.body).toContain("Check the deploy.");
    // Another seat is unchanged: typed as before.
    expect(await deliver({ targetSession: "other@test", message: "Typed" }, { jobId: "x", policy: "periodic-reminder", occurrenceId: "o-x" })).toMatchObject({ status: "ok" });
    expect(sends).toHaveBeenCalledTimes(1);
    // A row that can't be created leaves the occurrence owed, with no input.
    const failing = makeWatchdogDeliver({ sessionTransport: { send: sends }, isQueueOnlySeat: () => true, createQueueRow: async () => { throw new Error("queue down"); } });
    expect(await failing({ targetSession: seat, message: "m" }, { jobId: "f", policy: "periodic-reminder", occurrenceId: "o-f" }))
      .toMatchObject({ status: "failed", sent: false, inputPhase: "not_reached" });
  });

  it("T-006857: a queue-only seat's park timer is recorded on the parked row, with no second wd- row", async () => {
    const f = fixture();
    let repo!: QueueRepository;
    const deliver = makeWatchdogDeliver({ sessionTransport: f.transport, outbox: f.outbox, isQueueOnlySeat: name => name === seat,
      createQueueRow: row => repo.create({ qitemId: row.qitemId, sourceSession: "watchdog@system", destinationSession: row.destinationSession,
        body: row.body, summary: row.summary, tags: row.tags, nudge: false } as never).then(() => {}) });
    const s = scheduled({ ...f, deliver });
    repo = s.repo;
    const wdRows = () => (f.db.prepare("SELECT qitem_id FROM queue_items WHERE qitem_id LIKE 'wd-%'").all() as Array<{ qitem_id: string }>).map(r => r.qitem_id);
    // One parked row alone, then two parked rows in one pass (the digest): each timer is its row's own reminder.
    const a = await s.park("A's wait");
    s.due(a.jobId);
    await s.scheduler.runTickNow();
    const b = await s.park("B's wait");
    const c = await s.park("C's wait");
    s.due(b.jobId);
    s.due(c.jobId);
    await s.scheduler.runTickNow();
    expect(wdRows()).toEqual([]);
    for (const { qitemId, jobId } of [a, b, c]) {
      expect(s.repo.getById(qitemId)!.state).toBe("blocked");
      const notes = s.repo.listTransitions(qitemId).map(t => t.transitionNote ?? "");
      expect(notes.some(n => n.includes(`park wake noted on this row: watchdog ${jobId}; delivery=noted_on_parked_row; nothing typed`))).toBe(true);
      expect(notes.some(n => n.includes("delivery=ok"))).toBe(false);
      expect(s.repo.getParkWakeStatus(qitemId)).toMatchObject({ deliveryStatus: "noted_on_parked_row", unconsumed: true });
      expect(s.history.listForJob(jobId, 1)[0]).toMatchObject({ outcome: "sent", deliveryStatus: "noted_on_parked_row" });
      expect(s.jobs.getByIdOrThrow(jobId).state).toBe("terminal");
    }
    // The outbox closes custody for each occurrence (no retry, never indeterminate) and says it was noted on the row.
    const receipts = f.db.prepare("SELECT delivery_state, guard_delivery FROM outbox_entries WHERE outbox_id LIKE 'watchdog-occurrence-%'").all() as Array<{ delivery_state: string; guard_delivery: string }>;
    expect(receipts.length).toBeGreaterThan(0);
    for (const r of receipts) {
      expect(r.delivery_state).toBe("delivered");
      expect(JSON.parse(r.guard_delivery).watchdog).toMatchObject({ notedOnParkedRow: true });
    }
    expect(f.pastes).toEqual([]);
    expect(f.submissions).toEqual([]);
  });
});


describe("T-007715 (SH-A of the REVISAO-openrig-5): a park timer outside the digest is noted on its own parked row", () => {
  const other = "other@test";
  function queueOnly() {
    const f = fixture();
    let repo!: QueueRepository;
    const deliver = makeWatchdogDeliver({ sessionTransport: f.transport, outbox: f.outbox, isQueueOnlySeat: name => name === seat || name === other,
      createQueueRow: row => repo.create({ qitemId: row.qitemId, sourceSession: "watchdog@system", destinationSession: row.destinationSession,
        body: row.body, summary: row.summary, tags: row.tags, nudge: false } as never).then(() => {}) });
    const s = scheduled({ ...f, deliver }, { parkedRows: true, queueWait: true });
    repo = s.repo;
    const wdRows = () => (f.db.prepare("SELECT qitem_id, destination_session FROM queue_items WHERE qitem_id LIKE 'wd-%'").all() as
      Array<{ qitem_id: string; destination_session: string }>);
    const parkWith = async (summary: string, wake: Record<string, unknown>) => {
      const row = await repo.create({ sourceSession: "orch@test", destinationSession: seat, body: summary, summary, nudge: false } as never) as unknown as { qitemId: string };
      repo.update({ qitemId: row.qitemId, actorSession: seat, state: "blocked", blockedOn: "external:wait", transitionNote: "parked", ...wake } as never);
      return { qitemId: row.qitemId, jobId: repo.getParkWakeStatus(row.qitemId)!.ref };
    };
    const notas = (qitemId: string, jobId: string) => s.repo.listTransitions(qitemId).map(t => t.transitionNote ?? "")
      .filter(n => n.includes(`park wake noted on this row: watchdog ${jobId}; delivery=noted_on_parked_row; nothing typed`));
    const noted = (qitemId: string, jobId: string) => notas(qitemId, jobId).length > 0;
    return { f, s, wdRows, parkWith, noted, notas };
  }

  it("a park timer with its own message (rig watchdog update --message)", async () => {
    const { f, s, wdRows, notas } = queueOnly();
    const a = await s.park("A's wait");
    updateWatchdogJob(s.jobs, a.jobId, { message: "Check the release notes first." });
    s.due(a.jobId);
    await s.scheduler.runTickNow();
    expect(wdRows()).toEqual([]);
    // B1 of the REVISAO-openrig-9: the reminder's own text is on the note (the wd- row used to carry it)
    expect(notas(a.qitemId, a.jobId).some(n => n.includes("Check the release notes first."))).toBe(true);
    expect(s.history.listForJob(a.jobId, 1)[0]).toMatchObject({ outcome: "sent", deliveryStatus: "noted_on_parked_row" });
    expect(s.repo.getById(a.qitemId)!.state).toBe("blocked");
    expect(f.pastes).toEqual([]);
    expect(f.submissions).toEqual([]);
  });

  it("a workflow's park (its re-presentation message, never the default one)", async () => {
    const { f, s, wdRows, parkWith, notas } = queueOnly();
    const w = await parkWith("work step", { wakeAfterSeconds: 60, wakeMessage:
      "Workflow wait re-presentation (blocker progress or reminder): authored-wait@1 instance wf-x step work.\nYou still own the same frontier packet." });
    s.due(w.jobId);
    await s.scheduler.runTickNow();
    expect(wdRows()).toEqual([]);
    expect(notas(w.qitemId, w.jobId).some(n => n.includes("You still own the same frontier packet."))).toBe(true);
    expect(s.history.listForJob(w.jobId, 1)[0]).toMatchObject({ outcome: "sent", deliveryStatus: "noted_on_parked_row" });
    expect(f.submissions).toEqual([]);
  });

  it("a repeating wait (wakeMaxSeconds): noted on the row, and the wait keeps its cadence", async () => {
    const { f, s, wdRows, parkWith, notas } = queueOnly();
    const r = await parkWith("long wait", { wakeAfterSeconds: 600, wakeMaxSeconds: 2400 });
    s.due(r.jobId);
    await s.scheduler.runTickNow();
    expect(wdRows()).toEqual([]);
    // the wait's reason (backstop or source changed) is on the note too
    expect(notas(r.qitemId, r.jobId).some(n => /Waiting (backstop|source changed)/.test(n))).toBe(true);
    expect(s.history.listForJob(r.jobId, 1)[0]).toMatchObject({ outcome: "sent", deliveryStatus: "noted_on_parked_row" });
    expect(s.jobs.getByIdOrThrow(r.jobId).state).toBe("active");
    expect(f.submissions).toEqual([]);
  });

  it("a shared --wake-watchdog: both parked rows are noted, and no wd- row", async () => {
    const { f, s, wdRows, parkWith, noted } = queueOnly();
    const a = await s.park("A's wait");
    const b = await parkWith("B attached", { wakeWatchdogId: a.jobId });
    s.due(a.jobId);
    await s.scheduler.runTickNow();
    expect(wdRows()).toEqual([]);
    expect(noted(a.qitemId, a.jobId)).toBe(true);
    expect(noted(b.qitemId, a.jobId)).toBe(true);
    expect(s.history.listForJob(a.jobId, 1)[0]).toMatchObject({ outcome: "sent", deliveryStatus: "noted_on_parked_row" });
    expect(f.submissions).toEqual([]);
  });

  it("S1 of the REVISAO-openrig-9: an operator's periodic reminder attached by --wake-watchdog notes the parked row on every fire", async () => {
    const { f, s, wdRows, parkWith, notas } = queueOnly();
    const job = s.jobs.register({ policy: "periodic-reminder", targetSession: seat, intervalSeconds: 600, registeredBySession: "orch@test",
      specYaml: `policy: periodic-reminder\ntarget:\n  session: ${JSON.stringify(seat)}\nmessage: "check your rows"\n` });
    const b = await parkWith("bound to the operator's reminder", { wakeWatchdogId: job.jobId });
    for (let i = 0; i < 2; i++) {
      s.due(job.jobId);
      await s.scheduler.runTickNow();
    }
    expect(wdRows()).toEqual([]);
    expect(notas(b.qitemId, job.jobId).filter(n => n.includes("check your rows"))).toHaveLength(2);
    expect(s.jobs.getByIdOrThrow(job.jobId).state).toBe("active");
    expect(s.repo.getById(b.qitemId)!.state).toBe("blocked");
    expect(f.submissions).toEqual([]);
  });

  it("unchanged: a reminder whose target is another seat than the parked row's owner is still that seat's row", async () => {
    const { s, wdRows, parkWith } = queueOnly();
    const job = s.jobs.register({ policy: "periodic-reminder", targetSession: seat, intervalSeconds: 600, registeredBySession: "orch@test",
      specYaml: `policy: periodic-reminder\ntarget:\n  session: ${JSON.stringify(seat)}\nmessage: "check your rows"\n` });
    await parkWith("bound to a shared job", { wakeWatchdogId: job.jobId });
    // the job's target moves to another seat after the park: its fire is not the parked row's own reminder
    const moved = `policy: periodic-reminder\ntarget:\n  session: ${JSON.stringify(other)}\nmessage: "check your rows"\n`;
    s.jobs.updateInPlace(job.jobId, moved, 600, s.jobs.getByIdOrThrow(job.jobId).lastEvaluationAt);
    s.due(job.jobId);
    await s.scheduler.runTickNow();
    expect(wdRows().map(r => r.destination_session)).toEqual([other]);
  });
});

describe("T-006857 SH-2: a queue-only reminder is recorded as the row it became", () => {
  it("records queued_as_row with the row id in the outbox receipt and the history, never ok", async () => {
    const f = fixture();
    let repo!: QueueRepository;
    const deliver = makeWatchdogDeliver({ sessionTransport: f.transport, outbox: f.outbox, isQueueOnlySeat: s => s === seat,
      createQueueRow: row => repo.create({ qitemId: row.qitemId, sourceSession: "watchdog@system", destinationSession: row.destinationSession,
        body: row.body, summary: row.summary, tags: row.tags, nudge: false } as never).then(() => {}) });
    const s = scheduled({ ...f, deliver });
    repo = s.repo;
    // A reminder with no parked row of origin: the one case that still becomes a wd- row after SH-1.
    const job = s.jobs.register({ policy: "periodic-reminder", targetSession: seat, intervalSeconds: 600, registeredBySession: "orch@test",
      specYaml: `policy: periodic-reminder\ntarget:\n  session: ${JSON.stringify(seat)}\nmessage: "Check the deploy"\n` });
    s.due(job.jobId);
    await s.scheduler.runTickNow();
    expect(f.pastes).toEqual([]);
    expect(f.submissions).toEqual([]);
    const last = s.history.listForJob(job.jobId, 1)[0]!;
    expect(last).toMatchObject({ outcome: "sent", deliveryStatus: "queued_as_row",
      evaluationNotes: expect.objectContaining({ deliveryCode: "queued_as_row" }) });
    const wd = (last.evaluationNotes as { queuedQitemId: string }).queuedQitemId;
    expect(wd).toMatch(/^wd-/);
    expect(s.repo.getById(wd)).toMatchObject({ destinationSession: seat, state: "pending" });
    expect(s.repo.getById(wd)!.body).toContain("Check the deploy");
    const receipt = f.db.prepare("SELECT delivery_state, guard_delivery FROM outbox_entries WHERE outbox_id LIKE 'watchdog-occurrence-%'").get() as { delivery_state: string; guard_delivery: string };
    expect(receipt.delivery_state).toBe("delivered");
    expect(JSON.parse(receipt.guard_delivery).watchdog).toMatchObject({ queuedAs: wd });
    // The fired event names the row too.
    const fired = (f.db.prepare("SELECT payload FROM events WHERE type = 'watchdog.evaluation_fired'").all() as Array<{ payload: string }>)
      .map(e => JSON.parse(e.payload)).filter(e => e.jobId === job.jobId);
    expect(fired.at(-1)).toMatchObject({ deliveryStatus: "queued_as_row", queuedQitemId: wd });
  });
});

describe("blocked parked-owner no-input recovery on the real repository", () => {
  it.each(["quota", "terminal"].flatMap(kind => ["before", "after"].map(noteTiming => ({ kind, noteTiming }))))("keeps $kind recovery across another seat note $noteTiming creation", async ({ kind, noteTiming }) => {
    const f = fixture();
    if (kind === "quota") {
      await f.hold();
      const target = resolveGuardTarget(f.db, seat)!;
      // Distinct senders fill the quota: a seat holds one message per sender (#2430).
      for (let i = 0; i < 100; i++) f.outbox.retain({ outboxId: `blocked-held-${i}`, senderSession: `filler-${i}@test`, destinationSession: seat, body: "held" }, target);
    } else f.db.prepare("UPDATE nodes SET runtime='terminal' WHERE id='a'").run();
    const { repo, jobs, history } = scheduled(f);
    const row = await repo.create({ sourceSession: "orch@test", destinationSession: seat, body: "Current obligation", nudge: false });
    repo.update({ qitemId: row.qitemId, actorSession: seat, state: "blocked", blockedOn: "external:fixture", transitionNote: "waiting without a backstop" });
    const { isBoundTerminalSeat } = await import("../src/domain/terminal-seat.js");
    const { runStuckSweep } = await import("../src/domain/queue-stuck-sweep.js");
    const { findQueueRecovery } = await import("../src/domain/queue-recovery.js");
    const policy = makeParkedOwnerConsumerPolicy({
      diagnoseRig: () => ({ seats: [{ sessionName: seat, parked: true, activity: { value: "idle-at-prompt", needsInput: { count: 0, reason: null } }, obligations: { items: [], held: [{ qitemId: row.qitemId, healthy: false }] } }] }),
      history: { listForJob: (id, limit) => history.listForJob(id, limit), countForJob: id => history.countForJob(id) },
      rows: {
        listTransitions: id => repo.listTransitions(id).map(t => ({ ...t, ts: t.ts, transitionNote: t.transitionNote ?? null })),
        appendNote: (qitemId, transitionNote) => { repo.update({ qitemId, actorSession: "watchdog@system", transitionNote }); return { ok: true }; },
        recordNudgeResult: (id, result) => repo.recordNudgeAttempt(id, result),
        listOpenIds: destination => repo.listOpenObligations(destination, 500).map(r => r.qitemId),
        recoveryOwnsWake: id => queueRecoveryOwnsWake(f.db, repo.getById(id)),
      },
      retentionState: () => JSON.stringify(f.outbox.retentionUsage("a")),
      isTerminalSeat: session => isBoundTerminalSeat(f.db, session), occupantGeneration: () => "g1",
    });
    const job = jobs.register({ policy: PARKED_OWNER_POLICY_NAME, targetSession: makeRigAnchor("test"), intervalSeconds: 60, registeredBySession: seat, specYaml: "{}" });
    const engine = new WatchdogPolicyEngine({ jobsRepo: jobs, historyLog: history, eventBus: new EventBus(f.db), deliver: f.deliver, additionalPolicies: [policy] });
    await engine.evaluate(job);
    await engine.evaluate(jobs.getByIdOrThrow(job.jobId));
    let writes = repo.listTransitions(row.qitemId).length;
    const nudge = repo.getById(row.qitemId)!.lastNudgeAttempt;
    for (let i = 0; i < 10; i++) expect((await engine.evaluate(jobs.getByIdOrThrow(job.jobId))).outcome.action).toBe("skip");
    expect(repo.listTransitions(row.qitemId)).toHaveLength(writes);
    expect(repo.getById(row.qitemId)!.lastNudgeAttempt).toBe(nudge);
    const sweep = () => runStuckSweep({ db: f.db, queueRepo: repo, resolveOrchestrator: () => "orch@test", sessionConfirmedMissing: () => false, log: () => {} });
    if (noteTiming === "before") {
      repo.update({ qitemId: row.qitemId, actorSession: "observer@test", transitionNote: "Additional diagnostic context; owner has not responded" });
      writes++;
    }
    await sweep();
    const recovery = findQueueRecovery(f.db, row.qitemId);
    expect(recovery).not.toBeNull();
    const finding = repo.getById(recovery!.qitemId)!;
    expect(finding.destinationSession).toBe("orch@test");
    expect(repo.waitingView(finding.qitemId)?.nextBackstop).toMatchObject({ owner: "orch@test", mechanism: "queue-stuck-sweep:unclaimed", dueAt: expect.any(String) });
    if (noteTiming === "after") {
      repo.update({ qitemId: row.qitemId, actorSession: "observer@test", transitionNote: "More context; owner still has not responded" });
      writes++;
      await sweep();
      expect(repo.getById(finding.qitemId)?.state).toBe("pending");
    }
    const count = f.db.prepare("SELECT count(*) AS n FROM queue_items").get() as { n: number };
    for (let i = 0; i < 10; i++) { await sweep(); expect((await engine.evaluate(jobs.getByIdOrThrow(job.jobId))).outcome.action).toBe("skip"); }
    expect((f.db.prepare("SELECT count(*) AS n FROM queue_items").get() as { n: number }).n).toBe(count.n);
    expect(findQueueRecovery(f.db, row.qitemId)?.qitemId).toBe(finding.qitemId);
    expect(repo.listTransitions(row.qitemId)).toHaveLength(writes);
    repo.update({ qitemId: row.qitemId, actorSession: seat, transitionNote: "Owner response: I am handling this obligation" });
    await sweep();
    expect(repo.getById(finding.qitemId)?.state).toBe("done");
    expect(f.pastes).toHaveLength(0);
    expect(f.submissions).toHaveLength(0);
  });
});
