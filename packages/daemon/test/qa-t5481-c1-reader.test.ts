// Frozen eval of T-005497 (e-c1 of T-005481): what the reader reads of a pool slot (C2). Layers R (readSeatPoolState
// with fake deps, as seat-pool-executor.test.ts:1995-2027 builds it) and Q (QueueRepository.listPoolSeatRowFacts over the
// real queue in a temporary sqlite, as the F3 harness of T-004870 builds it; the clock is faked for Date only).
// Contract: ARQUITETURA.md r1 C2 and section 3, FATIAS.md r1 "e-c1", eval/c1.qa/INTERFACE.md sections 2, 4 and 7.
//
// The Q golden replays the real capture (fixtures/qa-t5481-c1/real/, 2026-11-11T05:02:55Z, the production daemon 7433 at
// 902f51ac7) on the real queue: each captured transition is applied again at its own timestamp (create; the
// system:queue-fallback reroute by routeToFallback; "claimed" by claim; another in-progress by update without a claim;
// each blocked that armed a wake by update with --wake-after; each fired wake by recordWatchdogWakeAttempt; any other
// transition as a note). Earlier parks are armed with 1800 s and the current one with the captured job's interval, so
// only the current wake's job can give the captured interval. listPoolSeatRowFacts must then give the facts the adapter
// reads from the capture (qa-t5481-c1-real.ts). The s9 stays out of the replay (blocked on another daemon row) and is
// in layer G only.
//
// MUTANTE → it() (the full map is at the top of qa-t5481-c1-policy.test.ts); in this file:
//   M2  → "negativo: blocked em gate:x (fila real)"
//   M3  → "caso 3"
//   M10 → "negativo: a chave ausente (reader)"
//   M13 → "negativo: o timer já disparou (fila real, disparo único)"
//   M15 → "caso 2", "golden do real (fila real): replay de qualidade-qa-s4"
//   M3b  → "negativo: o wake atual armado por outra cadeira (fila real)"
//   M15b → "takenAt pelo destino da linha (fila real): a in-progress de outra cadeira antes do slot não conta"

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { PodRepository } from "../src/domain/pod-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository, FALLBACK_ACTOR, type PoolSeatRowFacts } from "../src/domain/queue-repository.js";
import { WatchdogJobsRepository } from "../src/domain/watchdog-jobs-repository.js";
import { readSeatPoolState, SeatPoolSlots } from "../src/domain/seat-pool-executor.js";
import {
  parseSeatPoolSpec,
  seatPoolCanReceive,
  type ParkFacts,
  type SeatPoolSlotView,
  type SeatPoolSpec,
} from "../src/domain/policies/seat-pool.js";
import {
  CAPTURE_NOW_MS,
  armedTransition,
  captureFacts,
  captureView,
  loadCapture,
  loadTerminalS9,
  type Capture,
  type CapturedSlot,
} from "./qa-t5481-c1-real.js";

const JOB = "qa-t5481-c1-job";
const MODEL = "dev-impl@r";
const SLOT = "dev-impl-s1@r";
const OTHER_SEAT = "coordenacao-coord-4@fabrica";
const ON = { dispatch_to_parked: {} };
const EARLIER_INTERVAL_S = 1800;

function spec(overrides: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 3, cwd_template: "/work/{rig}/{slot}", ...overrides });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

// ---------------------------------------------------------------------------------------------------------------
// Layer R: readSeatPoolState with fake deps.
// ---------------------------------------------------------------------------------------------------------------
describe("qa T-005481 e-c1: the reader (C2) with fake deps", () => {
  const NOW = Date.parse("2026-11-11T05:00:00.000Z");
  const changedAt = "2026-11-11T04:30:00.000Z";
  const FACTS: ParkFacts = {
    qitemId: "a-1", blockedOn: "external:x", timerIntervalS: 2700, timerArmedLive: true, parkActor: SLOT,
    parkedAt: "2026-11-11T04:30:00.000Z", takenAt: "2026-11-11T04:00:00.000Z",
  };
  let db: Database.Database | undefined;
  let pooledId = "";
  afterEach(() => {
    if (db?.open) db.close();
    db = undefined;
  });

  function read(
    s: SeatPoolSpec,
    seat: { activity?: string; needsInput?: number; background?: boolean },
    openRows: Array<{ state: string }>,
    facts: PoolSeatRowFacts | null,
  ): SeatPoolSlotView {
    if (db?.open) db.close();
    const created = createFullTestDb();
    db = created;
    const rigRepo = new RigRepository(created);
    const rig = rigRepo.createRig("r");
    const pod = new PodRepository(created).createPod(rig.id, "dev", "Dev");
    const add = (logicalId: string, session: string) => {
      const node = rigRepo.addNode(rig.id, logicalId, { runtime: "claude-code", cwd: "/tmp", podId: pod.id });
      created.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`s-${logicalId}`, node.id, session);
      return node;
    };
    add("dev.impl", MODEL);
    const pooled = add("dev.impl-s1", SLOT);
    pooledId = pooled.id;
    const slots = new SeatPoolSlots(created);
    slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: pooled.id, phase: "live", intent: null, opId: "op-1", at: "2026-11-11T03:00:00.000Z" });
    const seatState = {
      activity: seat.activity ?? "idle-at-prompt",
      needsInput: { count: seat.needsInput ?? 0 },
      backgroundWork: seat.background ? { rung: "self-report", observedAt: changedAt } : null,
      changedAt,
    };
    const state = readSeatPoolState({
      db: created, slots,
      openRows: (session) => (session === SLOT ? openRows : []),
      sessionAddresses: (session) => [session],
      heldMessages: () => 0,
      seatStopped: () => false,
      seatState: (nodeId) => (nodeId === pooled.id ? seatState : { activity: "running", needsInput: { count: 0 }, backgroundWork: null, changedAt }) as never,
      restoring: () => false,
      demand: () => null,
      ...(facts ? { rowFacts: (session: string) => (session === SLOT ? facts : { rows: [], parked: null }) } : {}),
    }, { jobId: JOB, targetSession: MODEL, spec: s });
    expect(state.slots).toHaveLength(1);
    return state.slots[0]!;
  }

  const TODAY_KEYS = ["adopted", "cause", "heldMessages", "idleSince", "logicalId", "nodeExists", "nodeId", "openRows", "phase", "sessionName", "slotId"];

  it("negativo: a chave ausente (reader): a view tem exatamente a forma de hoje, sem atPrompt, backgroundWork nem parked", () => {
    const view = read(spec(), { background: true }, [{ state: "blocked" }], { rows: [{ qitemId: "a-1", state: "blocked" }], parked: FACTS });
    expect(view).toEqual({
      slotId: 1, logicalId: "dev.impl-s1", sessionName: SLOT, nodeId: pooledId, phase: "live", adopted: false, nodeExists: true,
      openRows: 1, heldMessages: 0, idleSince: null, cause: "working",
    });
    expect(Object.keys(view).sort()).toEqual(TODAY_KEYS);
    expect(seatPoolCanReceive(spec(), view, NOW)).toBeNull();
    // An idle seat with no row: today's view, and today's "free".
    const idle = read(spec(), {}, [], { rows: [], parked: null });
    expect(Object.keys(idle).sort()).toEqual(TODAY_KEYS);
    expect(idle).toMatchObject({ openRows: 0, idleSince: changedAt, cause: "idle" });
  });

  it("caso 1 (reader): com a chave, atPrompt e backgroundWork saem do mesmo seatState e parked dos fatos da fila", () => {
    const view = read(spec(ON), { background: true }, [{ state: "blocked" }], { rows: [{ qitemId: "a-1", state: "blocked" }], parked: FACTS });
    expect(view).toEqual({
      slotId: 1, logicalId: "dev.impl-s1", sessionName: SLOT, nodeId: pooledId, phase: "live", adopted: false, nodeExists: true,
      openRows: 1, heldMessages: 0, idleSince: null, cause: "working", atPrompt: true, backgroundWork: true, parked: FACTS,
    });
    expect(seatPoolCanReceive(spec(ON), view, NOW)).toBe("parked");
  });

  it("negativo: 1 in-progress (reader): sem fatos de park, a view não tem parked", () => {
    const view = read(spec(ON), { background: true }, [{ state: "in-progress" }], { rows: [{ qitemId: "a-1", state: "in-progress" }], parked: null });
    expect(view).not.toHaveProperty("parked");
    expect(view).toMatchObject({ openRows: 1, atPrompt: true, backgroundWork: true });
    expect(seatPoolCanReceive(spec(ON), view, NOW)).toBeNull();
  });

  it("negativo: needsInput > 0 (reader): atPrompt false e cause needs_input", () => {
    const view = read(spec(ON), { needsInput: 1, background: true }, [{ state: "blocked" }], { rows: [{ qitemId: "a-1", state: "blocked" }], parked: FACTS });
    expect(view).toMatchObject({ atPrompt: false, cause: "needs_input", idleSince: null });
    expect(seatPoolCanReceive(spec(ON), view, NOW)).toBeNull();
  });

  it("negativo: fora do prompt (reader): atPrompt false", () => {
    const view = read(spec(ON), { activity: "running" }, [{ state: "blocked" }], { rows: [{ qitemId: "a-1", state: "blocked" }], parked: FACTS });
    expect(view).toMatchObject({ atPrompt: false, backgroundWork: false, cause: "working" });
    expect(seatPoolCanReceive(spec(ON), view, NOW)).toBeNull();
  });

  it("nodeExists: false (reader): o slot sem nó não traz atPrompt, backgroundWork nem parked, mesmo com a chave e os fatos", () => {
    const created = createFullTestDb();
    db = created;
    const rigRepo = new RigRepository(created);
    const rig = rigRepo.createRig("r");
    const pod = new PodRepository(created).createPod(rig.id, "dev", "Dev");
    const model = rigRepo.addNode(rig.id, "dev.impl", { runtime: "claude-code", cwd: "/tmp", podId: pod.id });
    created.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run("s-dev.impl", model.id, MODEL);
    const slots = new SeatPoolSlots(created);
    // The slot's node is gone (node_id null): the pool still has its row.
    slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: null, phase: "live", intent: null, opId: "op-1", at: "2026-11-11T03:00:00.000Z" });
    const seatState = { activity: "idle-at-prompt", needsInput: { count: 0 }, backgroundWork: { rung: "self-report", observedAt: changedAt }, changedAt };
    const state = readSeatPoolState({
      db: created, slots,
      openRows: () => [{ state: "blocked" }],
      sessionAddresses: (session) => [session],
      heldMessages: () => 0,
      seatStopped: () => false,
      seatState: () => seatState as never,
      restoring: () => false,
      demand: () => null,
      rowFacts: () => ({ rows: [{ qitemId: "a-1", state: "blocked" }], parked: FACTS }),
    }, { jobId: JOB, targetSession: MODEL, spec: spec(ON) });
    expect(state.slots).toHaveLength(1);
    const view = state.slots[0]!;
    expect(view).toMatchObject({ nodeExists: false, openRows: 0, cause: "stopped" });
    expect(view).not.toHaveProperty("atPrompt");
    expect(view).not.toHaveProperty("backgroundWork");
    expect(view).not.toHaveProperty("parked");
    expect(Object.keys(view).sort()).toEqual(TODAY_KEYS);
    expect(seatPoolCanReceive(spec(ON), view, NOW)).toBeNull();
  });

  it("com a chave e sem a dependência rowFacts: openRows de openRows e sem parked", () => {
    const view = read(spec(ON), { background: true }, [{ state: "blocked" }], null);
    expect(view).toMatchObject({ openRows: 1, atPrompt: true, backgroundWork: true });
    expect(view).not.toHaveProperty("parked");
    expect(seatPoolCanReceive(spec(ON), view, NOW)).toBeNull();
  });
});

// ---------------------------------------------------------------------------------------------------------------
// Layer Q: listPoolSeatRowFacts over the real queue.
// ---------------------------------------------------------------------------------------------------------------
describe("qa T-005481 e-c1: listPoolSeatRowFacts over the real queue (C2)", () => {
  const T0 = Date.parse("2026-11-11T04:00:00.000Z");
  let db: Database.Database;
  let queue: QueueRepository;
  let jobs: WatchdogJobsRepository;
  const at = (ms: number) => vi.setSystemTime(ms);

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    at(T0);
    db = createDb();
    migrate(db, ALL_MIGRATIONS);
    queue = new QueueRepository(db, new EventBus(db));
    jobs = new WatchdogJobsRepository(db);
  });
  afterEach(() => {
    db.close();
    vi.useRealTimers();
  });

  /** Applies the captured history again on the real queue (see the header). Returns the captured → replayed job ids. */
  async function replay(cap: Capture): Promise<Map<string, string>> {
    const { show, transitions, watchdog } = cap;
    const current = armedTransition(transitions)!;
    const origin = (show.chainOfRecord ?? []).find((c) => c.startsWith("fallback-from:"))?.slice("fallback-from:".length) ?? show.destinationSession;
    const jobsOf = new Map<string, string>();
    for (const t of transitions) {
      const claim = t.state === "in-progress" && t.transitionNote === "claimed" && t.actorSession === show.destinationSession;
      // claimed_at is the daemon's own stamp of the claim (it may sit a millisecond before the transition's).
      at(Date.parse(claim && show.claimedAt ? show.claimedAt : t.ts));
      const row = queue.getById(show.qitemId);
      if (!row) {
        if (t.transitionNote !== "created") throw new Error(`${cap.slot}: the history does not start with the create`);
        await queue.create({
          qitemId: show.qitemId, sourceSession: t.actorSession, destinationSession: origin, body: show.body, summary: show.summary,
          tags: show.tags ?? [], nudge: false,
        } as never);
      } else if (t.actorSession === FALLBACK_ACTOR) {
        const m = /^fallback-routed: (\S+) → (\S+) \((.*)\)$/.exec(t.transitionNote ?? "");
        if (!m) throw new Error(`${cap.slot}: not a reroute note: ${t.transitionNote}`);
        queue.routeToFallback(show.qitemId, m[2]!, m[3]!, { expectState: t.state, expectDestination: m[1]! });
      } else if (t.wake?.phase === "armed") {
        const gate = /^parked on (\S+)$/.exec(t.transitionNote ?? "")?.[1] ?? show.blockedOn!;
        queue.update({
          qitemId: show.qitemId, actorSession: t.actorSession, state: "blocked", blockedOn: gate,
          wakeAfterSeconds: t === current ? watchdog.intervalSeconds : EARLIER_INTERVAL_S, transitionNote: t.transitionNote ?? undefined,
        } as never);
        jobsOf.set(t.wake.ref, queue.getParkWakeStatus(show.qitemId)!.ref);
      } else if (t.wake?.phase === "fired") {
        const job = jobsOf.get(t.wake.ref);
        if (!job) throw new Error(`${cap.slot}: a fire of a wake the replay did not arm: ${t.wake.ref}`);
        queue.recordWatchdogWakeAttempt(job, t.wake.deliveryStatus ?? "ok");
      } else if (claim) {
        queue.claim({ qitemId: show.qitemId, destinationSession: t.actorSession });
      } else if (t.state === "in-progress" && row.state !== "in-progress") {
        queue.update({ qitemId: show.qitemId, actorSession: t.actorSession, state: "in-progress", transitionNote: t.transitionNote ?? undefined } as never);
      } else {
        queue.update({ qitemId: show.qitemId, actorSession: t.actorSession, transitionNote: t.transitionNote ?? "note" } as never);
      }
    }
    at(CAPTURE_NOW_MS);
    return jobsOf;
  }

  const REPLAYED: CapturedSlot[] = ["build-dev-1-s6", "build-dev-1-s8", "build-dev-1-s10", "build-dev-1-s11", "qualidade-qa-s4", "qualidade-qa-s6"];

  it.each(REPLAYED)("golden do real (fila real): replay de %s dá os fatos da captura", async (slot) => {
    const cap = loadCapture(slot);
    await replay(cap);
    const row = queue.getById(cap.show.qitemId)!;
    expect(row).toMatchObject({ state: "blocked", blockedOn: cap.show.blockedOn, destinationSession: cap.show.destinationSession });
    const facts = queue.listPoolSeatRowFacts(cap.show.destinationSession);
    expect(facts.rows).toEqual([{ qitemId: cap.show.qitemId, state: "blocked" }]);
    expect(facts.parked).toEqual(captureFacts(cap));
    expect(facts.parked!.timerArmedLive).toBe(true);
    // The view built from the replayed facts decides as the capture's does.
    const expected = slot === "qualidade-qa-s4" || slot === "qualidade-qa-s6" ? null : "parked";
    expect(seatPoolCanReceive(spec(ON), captureView(cap, 1, facts.parked!), CAPTURE_NOW_MS)).toBe(expected);
  });

  it("caso 2: assumida por update --state in-progress, sem claim (claimedAt nulo, takenAt pela transição) → \"parked\"", async () => {
    const cap = loadCapture("qualidade-qa-s4");
    await replay(cap);
    expect(queue.getById(cap.show.qitemId)!.claimedAt).toBeNull();
    const facts = queue.listPoolSeatRowFacts(cap.show.destinationSession);
    expect(facts.parked!.takenAt).toBe("2026-11-11T03:22:51.327Z");
    expect(facts.parked!.parkedAt).toBe("2026-11-11T04:24:11.073Z");
    // At 05:02:55Z the qa-s4 was working; the same slot at its prompt (with a background shell) would receive.
    const atPrompt = { ...captureView(cap, 1, facts.parked!), atPrompt: true, cause: "working" as const };
    expect(seatPoolCanReceive(spec(ON), atPrompt, CAPTURE_NOW_MS)).toBe("parked");
  });

  it("caso 3: nota de outra cadeira na linha depois do park: continua \"parked\" (o ator é o da transição que armou)", async () => {
    const cap = loadCapture("build-dev-1-s6");
    await replay(cap);
    at(Date.parse("2026-11-11T05:00:00.000Z"));
    queue.update({ qitemId: cap.show.qitemId, actorSession: OTHER_SEAT, transitionNote: "coord-4: a linha segue estacionada no pronto" } as never);
    at(CAPTURE_NOW_MS);
    const last = queue.listTransitions(cap.show.qitemId).at(-1)!;
    expect(last).toMatchObject({ actorSession: OTHER_SEAT, state: "blocked" });
    const facts = queue.listPoolSeatRowFacts(cap.show.destinationSession);
    expect(facts.parked).toMatchObject({ parkActor: "build-dev-1-s6@fabrica", parkedAt: "2026-11-11T04:28:24.119Z", timerArmedLive: true });
    expect(seatPoolCanReceive(spec(ON), captureView(cap, 1, facts.parked!), CAPTURE_NOW_MS)).toBe("parked");
  });

  // ---- negatives on the real queue ----------------------------------------------------------------------------
  const view = (facts: PoolSeatRowFacts): SeatPoolSlotView => ({
    slotId: 1, logicalId: "dev.impl-s1", sessionName: SLOT, nodeId: "node-1", phase: "live", adopted: false, nodeExists: true,
    openRows: facts.rows.length, heldMessages: 0, idleSince: null, cause: "working", atPrompt: true, backgroundWork: true,
    ...(facts.parked ? { parked: facts.parked } : {}),
  });
  async function taken(): Promise<string> {
    const row = await queue.create({ sourceSession: MODEL, destinationSession: SLOT, body: "A", tags: ["tarefa-nova"], nudge: false } as never);
    queue.claim({ qitemId: row.qitemId, destinationSession: SLOT });
    return row.qitemId;
  }
  function park(qitemId: string, blockedOn: string, wakeAfterSeconds: number): void {
    at(T0 + 600_000);
    queue.update({ qitemId, actorSession: SLOT, state: "blocked", blockedOn, wakeAfterSeconds, transitionNote: `parked on ${blockedOn}` } as never);
    at(T0 + 1_200_000);
  }

  it("negativo: 1 in-progress (fila real)", async () => {
    const a = await taken();
    const facts = queue.listPoolSeatRowFacts(SLOT);
    expect(facts).toEqual({ rows: [{ qitemId: a, state: "in-progress" }], parked: null });
    expect(seatPoolCanReceive(spec(ON), view(facts), T0 + 1_200_000)).toBeNull();
  });

  it("negativo: 1 pending direta ao slot (fila real)", async () => {
    const row = await queue.create({ sourceSession: OTHER_SEAT, destinationSession: SLOT, body: "direta", nudge: false } as never);
    const facts = queue.listPoolSeatRowFacts(SLOT);
    expect(facts).toEqual({ rows: [{ qitemId: row.qitemId, state: "pending" }], parked: null });
    expect(seatPoolCanReceive(spec(ON), view(facts), T0)).toBeNull();
  });

  it("negativo: 2 linhas abertas (fila real)", async () => {
    const a = await taken();
    park(a, "external:x", 2700);
    const b = await queue.create({ sourceSession: MODEL, destinationSession: SLOT, body: "B", nudge: false } as never);
    const facts = queue.listPoolSeatRowFacts(SLOT);
    expect(facts).toEqual({ rows: [{ qitemId: b.qitemId, state: "pending" }, { qitemId: a, state: "blocked" }], parked: null });
    expect(seatPoolCanReceive(spec(ON), view(facts), T0 + 1_200_000)).toBeNull();
    // The single park on its own (B closed) qualifies again.
    queue.update({ qitemId: b.qitemId, actorSession: SLOT, state: "done", closureReason: "no-follow-on", transitionNote: "B feita" } as never);
    const alone = queue.listPoolSeatRowFacts(SLOT);
    expect(alone.parked).toMatchObject({ qitemId: a, blockedOn: "external:x", timerIntervalS: 2700, timerArmedLive: true, parkActor: SLOT });
    expect(seatPoolCanReceive(spec(ON), view(alone), T0 + 1_200_000)).toBe("parked");
  });

  it("negativo: blocked em gate:x (fila real)", async () => {
    const a = await taken();
    park(a, "gate:x", 2700);
    const facts = queue.listPoolSeatRowFacts(SLOT);
    expect(facts.parked).toEqual({
      qitemId: a, blockedOn: "gate:x", timerIntervalS: 2700, timerArmedLive: true, parkActor: SLOT,
      parkedAt: new Date(T0 + 600_000).toISOString(), takenAt: new Date(T0).toISOString(),
    });
    expect(seatPoolCanReceive(spec(ON), view(facts), T0 + 1_200_000)).toBeNull();
  });

  it("negativo: o wake atual armado por outra cadeira (fila real)", async () => {
    // (a) The slot took the row, but another seat parked it with its own timer: the current wake is that seat's.
    const a = await taken();
    at(T0 + 600_000);
    queue.update({ qitemId: a, actorSession: OTHER_SEAT, state: "blocked", blockedOn: "external:x", wakeAfterSeconds: 2700, transitionNote: "parked on external:x" } as never);
    at(T0 + 1_200_000);
    const facts = queue.listPoolSeatRowFacts(SLOT);
    expect(facts.parked).toMatchObject({
      qitemId: a, blockedOn: "external:x", timerIntervalS: 2700, timerArmedLive: true, parkActor: OTHER_SEAT,
      parkedAt: new Date(T0 + 600_000).toISOString(), takenAt: new Date(T0).toISOString(),
    });
    expect(seatPoolCanReceive(spec(ON), view(facts), T0 + 1_200_000)).toBeNull();
    // (b) The slot parked it itself first, then another seat armed a new wake on the same blocked row: the current arm
    // is the other seat's (the first blocked was the slot's).
    const b = await queue.create({ sourceSession: MODEL, destinationSession: "dev-impl-s2@r", body: "B", nudge: false } as never);
    queue.claim({ qitemId: b.qitemId, destinationSession: "dev-impl-s2@r" });
    at(T0 + 1_300_000);
    queue.update({ qitemId: b.qitemId, actorSession: "dev-impl-s2@r", state: "blocked", blockedOn: "external:y", wakeAfterSeconds: 2700, transitionNote: "parked on external:y" } as never);
    at(T0 + 1_400_000);
    queue.update({ qitemId: b.qitemId, actorSession: OTHER_SEAT, state: "blocked", blockedOn: "external:y", wakeAfterSeconds: 1800, transitionNote: "parked on external:y" } as never);
    at(T0 + 1_500_000);
    const rearmed = queue.listPoolSeatRowFacts("dev-impl-s2@r");
    expect(rearmed.parked).toMatchObject({
      qitemId: b.qitemId, blockedOn: "external:y", timerIntervalS: 1800, timerArmedLive: true, parkActor: OTHER_SEAT,
      parkedAt: new Date(T0 + 1_400_000).toISOString(),
    });
    const v2 = { ...view(rearmed), sessionName: "dev-impl-s2@r", logicalId: "dev.impl-s2" };
    expect(seatPoolCanReceive(spec(ON), v2, T0 + 1_500_000)).toBeNull();
  });

  it("takenAt pelo destino da linha (fila real): a in-progress de outra cadeira antes do slot não conta", async () => {
    // The row is addressed to the slot; 7 h earlier another seat moved it to in-progress (no claim) and parked it on a
    // session; now the slot takes it by update (no claim) and parks it on its own timer.
    const SEVEN_H = 7 * 3600_000;
    at(T0 - SEVEN_H - 60_000);
    const row = await queue.create({ sourceSession: MODEL, destinationSession: SLOT, body: "A", nudge: false } as never);
    at(T0 - SEVEN_H);
    queue.update({ qitemId: row.qitemId, actorSession: OTHER_SEAT, state: "in-progress", transitionNote: "coord pegou" } as never);
    at(T0 - SEVEN_H + 60_000);
    queue.update({ qitemId: row.qitemId, actorSession: OTHER_SEAT, state: "blocked", blockedOn: "lead@r", transitionNote: "espera o lead" } as never);
    at(T0);
    queue.update({ qitemId: row.qitemId, actorSession: SLOT, state: "in-progress", transitionNote: "o slot assume" } as never);
    park(row.qitemId, "external:x", 2700);
    expect(queue.getById(row.qitemId)!.claimedAt).toBeNull();
    const facts = queue.listPoolSeatRowFacts(SLOT);
    expect(facts.parked).toMatchObject({
      qitemId: row.qitemId, blockedOn: "external:x", timerIntervalS: 2700, timerArmedLive: true, parkActor: SLOT,
      parkedAt: new Date(T0 + 600_000).toISOString(), takenAt: new Date(T0).toISOString(),
    });
    expect(seatPoolCanReceive(spec(ON), view(facts), T0 + 1_200_000)).toBe("parked");
  });

  it("negativo: o timer já disparou (fila real, disparo único)", async () => {
    const terminal = loadTerminalS9();
    const a = await taken();
    park(a, "external:x", terminal.intervalSeconds);
    const ref = queue.getParkWakeStatus(a)!.ref;
    at(T0 + 600_000 + terminal.intervalSeconds * 1000);
    queue.recordWatchdogWakeAttempt(ref, "ok");
    // The job ends as the real one did (terminal-s9, 04:44:51Z): park_timer_fired_once.
    expect(jobs.getById(ref)).toMatchObject({
      state: terminal.state, terminalReason: terminal.terminalReason, intervalSeconds: terminal.intervalSeconds, policy: terminal.policy,
    });
    expect(queue.getById(a)!.state).toBe("blocked");
    expect(queue.getParkWakeStatus(a)).toMatchObject({ kind: "timer", phase: "fired", live: false });
    const facts = queue.listPoolSeatRowFacts(SLOT);
    expect(facts.parked).toMatchObject({ qitemId: a, blockedOn: "external:x", timerIntervalS: 3600, timerArmedLive: false, parkActor: SLOT });
    const now = T0 + 600_000 + terminal.intervalSeconds * 1000 + 1000;
    expect(seatPoolCanReceive(spec(ON), view(facts), now)).toBeNull();
    // The same facts with the timer live would qualify: the fire alone keeps it out.
    expect(seatPoolCanReceive(spec(ON), { ...view(facts), parked: { ...facts.parked!, timerArmedLive: true } }, now)).toBe("parked");
  });
});
