// Frozen eval of T-005497 (e-c1 of T-005481): a pool slot whose only open row is parked on its own wait receives a 2nd
// row. Layers A (the spec, the predicate seatPoolCanReceive and the planner, pure) and G (the golden of the real capture).
// Contract: ARQUITETURA.md r1 (section 0, C1-C4 of section 2, sections 3, 4 and 10), FATIAS.md r1 section "e-c1", and
// eval/c1.qa/INTERFACE.md (the names the reference implements). Decision D-3 (coord-4): max_wake_s 3600 by default; the
// 75-minute timer with max_wake_s 3600 is a negative.
//
// MUTANTE → it() (every mutant of FATIAS e-c1 is killed by at least one it(); the file is in brackets)
//   M1  aceita openRows === 2            → [policy] "negativo: 2 linhas abertas"; "caso 4 (plano)"; [executor] "caso 4"
//   M2  aceita blocked fora de external: → [policy] "negativo: blocked em outra linha (fc-…)", "negativo: blocked em gate:x",
//                                          "negativo: blocked em sessão", "golden do real: build-dev-1-s9 …"; [reader] "negativo: blocked em gate:x (fila real)"
//   M3  ator da última blocked           → [reader] "caso 3"
//   M4  ignora o max_wake_s              → [policy] "negativo: timer de 75 min com max_wake_s 3600", "golden do real: qualidade-qa-s6 …"
//   M5  exige cause === "idle"           → [policy] "golden do real (caso 1): <s6|s8|s10|s11> …", "caso 1"; [executor] "caso 1"
//   M6  o predicado entra no idle()      → [policy] "invariante: o parked nunca entra no free nem no dreno"
//   M7  a ordem só por id                → [policy] "caso 5"
//   M8  o recheck usa o predicado antigo → [executor] "caso 1", "caso 6", "caso 8"
//   M9  o adotado recebe                 → [policy] "negativo: slot adotado (uma fixa)"
//   M10 a chave ausente liga             → [policy] "negativo: a chave ausente (spec e predicado)", "golden do real: sem a chave, nenhum slot recebe";
//                                          [reader] "negativo: a chave ausente (reader)"; [executor] "negativo: a chave ausente (executor)"
//   M11 ignora o max_claim_age_s         → [policy] "negativo: takenAt de 7 h"
//   M12 o receives calculado à parte     → [executor] "caso 7"
//   M13 ignora o timerArmedLive          → [policy] "negativo: o timer já disparou", "golden do real: o job terminal do s9 …";
//                                          [reader] "negativo: o timer já disparou (fila real, disparo único)"
//   M14 aceita usage_limit               → [policy] "negativo: cause usage_limit no prompt"
//   M15 exige claimedAt                  → [reader] "caso 2", "golden do real (fila real): replay de qualidade-qa-s4"
//   M16 o comum do C3 sem phase/nodeExists (extra, cético) → [policy] "negativo: a parte comum do C3 no modo parked …";
//                                          [executor] "caso 8 (recheck): o slot passou a draining …"
//   M3b parkActor = destino da linha (extra, cético)       → [reader] "negativo: o wake atual armado por outra cadeira (fila real)"
//   M15b takenAt sem o filtro de ator (extra, cético)      → [reader] "takenAt pelo destino da linha (fila real) …"
//
// The asserts fail by rule, not by import: the names are imported statically; on the base (902f51ac7) they do not exist
// and this file fails, which is expected until the c1 lands.

import { describe, it, expect } from "vitest";
import {
  parseSeatPoolSpec,
  planSeatPool,
  seatPoolCanReceive,
  seatPoolSpecProblem,
  type ParkFacts,
  type SeatPoolIntent,
  type SeatPoolPendingRow,
  type SeatPoolSlotView,
  type SeatPoolSpec,
  type SeatPoolState,
} from "../src/domain/policies/seat-pool.js";
import {
  CAPTURE_NOW_MS,
  SLOT_DIRS,
  armedTransition,
  captureFacts,
  captureView,
  loadCapture,
  loadTerminalS9,
  type CapturedSlot,
} from "./qa-t5481-c1-real.js";

const NOW = Date.parse("2026-11-11T05:00:00.000Z");
const iso = (ms: number) => new Date(ms).toISOString();
const ago = (seconds: number) => iso(NOW - seconds * 1000);
const SAMPLE_MAX_AGE = 120_000;
const MODEL = "dev-impl@r";
const ON = { dispatch_to_parked: {} };
const OTHER_SEAT = "coordenacao-coord-4@fabrica";

function spec(overrides: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 3, cwd_template: "/work/{rig}/{slot}", cooldown_s: 60, idle_grace_s: 300, ...overrides });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

let nextSlot = 1;
/** A free seat, as today's tests build it. */
function free(overrides: Partial<SeatPoolSlotView> = {}): SeatPoolSlotView {
  const slotId = overrides.slotId ?? nextSlot++;
  return {
    slotId, logicalId: `dev.impl-s${slotId}`, sessionName: `dev-impl-s${slotId}@r`, nodeId: `node-${slotId}`, phase: "live", adopted: false,
    nodeExists: true, openRows: 0, heldMessages: 0, idleSince: ago(600), cause: "idle", ...overrides,
  };
}

/**
 * Case 1 of FATIAS (the base case): one row blocked on external:x, a 45-minute timer armed and live, armed by the slot
 * itself 30 min ago, after it took the row 1 h ago by claim; at its prompt, cause "working" because of a background shell.
 */
function parked(overrides: Partial<SeatPoolSlotView> = {}, park: Partial<ParkFacts> = {}): SeatPoolSlotView {
  const slotId = overrides.slotId ?? nextSlot++;
  const sessionName = overrides.sessionName ?? `dev-impl-s${slotId}@r`;
  return {
    slotId, logicalId: `dev.impl-s${slotId}`, sessionName, nodeId: `node-${slotId}`, phase: "live", adopted: false, nodeExists: true,
    openRows: 1, heldMessages: 0, idleSince: null, cause: "working", atPrompt: true, backgroundWork: true,
    parked: {
      qitemId: `a-${slotId}`, blockedOn: "external:x", timerIntervalS: 2700, timerArmedLive: true, parkActor: sessionName,
      parkedAt: ago(1800), takenAt: ago(3600), ...park,
    },
    ...overrides,
  };
}

let nextRow = 1;
function rows(count: number, destination = MODEL, ageSeconds = 120): SeatPoolPendingRow[] {
  return Array.from({ length: count }, () => ({ qitemId: `q-${nextRow++}`, destination, waitingSince: ago(ageSeconds), tags: ["tarefa-nova"] }));
}

function state(overrides: Partial<SeatPoolState> = {}): SeatPoolState {
  return {
    model: { rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model", sessionName: MODEL },
    modelCause: "working", restoring: false, slots: [], adoptable: [], pending: [], demand: null, lastScaleAt: null, ...overrides,
  };
}

const plan = (s: SeatPoolSpec, st: SeatPoolState) => planSeatPool(s, st, NOW, SAMPLE_MAX_AGE);
const can = (s: SeatPoolSpec, slot: SeatPoolSlotView) => seatPoolCanReceive(s, slot, NOW);
const dispatches = (intents: SeatPoolIntent[]) => intents.filter((i) => i.kind === "dispatch");

// ---------------------------------------------------------------------------------------------------------------
// The spec (C1)
// ---------------------------------------------------------------------------------------------------------------
describe("qa T-005481 e-c1: the spec (C1)", () => {
  it("spec: dispatch_to_parked: {} liga com os padrões (allow_background_work: true)", () => {
    const on = parseSeatPoolSpec({ max: 2, cwd_template: "/w/{slot}", dispatch_to_parked: {} });
    expect("spec" in on).toBe(true);
    expect((on as { spec: SeatPoolSpec }).spec.dispatchToParked).toEqual({
      maxWakeSeconds: 3600, maxClaimAgeSeconds: 21600, allowBackgroundWork: true, parkedMaxLoadPerCpuPercent: 0,
    });
    // The rest of the spec is today's.
    const { dispatchToParked: _ignored, ...rest } = (on as { spec: SeatPoolSpec }).spec;
    expect(rest).toEqual((parseSeatPoolSpec({ max: 2, cwd_template: "/w/{slot}" }) as { spec: SeatPoolSpec }).spec);
    const set = parseSeatPoolSpec({
      max: 2, cwd_template: "/w/{slot}",
      dispatch_to_parked: { max_wake_s: 5400, max_claim_age_s: 600, allow_background_work: false, parked_max_load_per_cpu_percent: 80 },
    });
    expect((set as { spec: SeatPoolSpec }).spec.dispatchToParked).toEqual({
      maxWakeSeconds: 5400, maxClaimAgeSeconds: 600, allowBackgroundWork: false, parkedMaxLoadPerCpuPercent: 80,
    });
    for (const maxWake of [60, 86400]) {
      expect(seatPoolSpecProblem({ context: { max: 2, cwd_template: "/w/{slot}", dispatch_to_parked: { max_wake_s: maxWake } } } as never)).toBeNull();
    }
  });

  it("spec: max_wake_s: 30 é inválido", () => {
    const base = { max: 2, cwd_template: "/w/{slot}" };
    const bad = parseSeatPoolSpec({ ...base, dispatch_to_parked: { max_wake_s: 30 } });
    expect("problem" in bad).toBe(true);
    expect((bad as { problem: { field: string } }).problem.field).toBe("dispatch_to_parked.max_wake_s");
    expect(seatPoolSpecProblem({ context: { ...base, dispatch_to_parked: { max_wake_s: 30 } } } as never)).toMatchObject({ field: "dispatch_to_parked.max_wake_s" });
    for (const value of [59, 86401, 3600.5, "3600", null]) {
      const parsed = parseSeatPoolSpec({ ...base, dispatch_to_parked: { max_wake_s: value } });
      expect((parsed as { problem?: { field: string } }).problem?.field, `max_wake_s ${JSON.stringify(value)}`).toBe("dispatch_to_parked.max_wake_s");
    }
    expect((parseSeatPoolSpec({ ...base, dispatch_to_parked: { max_claim_age_s: 599 } }) as { problem?: { field: string } }).problem?.field)
      .toBe("dispatch_to_parked.max_claim_age_s");
    expect((parseSeatPoolSpec({ ...base, dispatch_to_parked: { allow_background_work: "yes" } }) as { problem?: { field: string } }).problem?.field)
      .toBe("dispatch_to_parked.allow_background_work");
    expect((parseSeatPoolSpec({ ...base, dispatch_to_parked: { parked_max_load_per_cpu_percent: -1 } }) as { problem?: { field: string } }).problem?.field)
      .toBe("dispatch_to_parked.parked_max_load_per_cpu_percent");
    // Not an object (the empty YAML key reads null): the key itself is wrong.
    for (const value of [null, [], 3, "on", true]) {
      const parsed = parseSeatPoolSpec({ ...base, dispatch_to_parked: value });
      expect((parsed as { problem?: { field: string } }).problem?.field, `dispatch_to_parked ${JSON.stringify(value)}`).toBe("dispatch_to_parked");
    }
  });

  it("spec: campo desconhecido no objeto é inválido", () => {
    const base = { max: 2, cwd_template: "/w/{slot}" };
    for (const inner of [{ max_wake: 60 }, { max_wake_s: 3600, maxWakeSeconds: 3600 }, { drain: true }]) {
      const parsed = parseSeatPoolSpec({ ...base, dispatch_to_parked: inner });
      expect((parsed as { problem?: { field: string } }).problem?.field, JSON.stringify(inner)).toBe("dispatch_to_parked");
      expect(seatPoolSpecProblem({ context: { ...base, dispatch_to_parked: inner } } as never)).not.toBeNull();
    }
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The golden of the real capture (layer G): the view the reader would build at 05:02:55Z, from the capture only.
// ---------------------------------------------------------------------------------------------------------------
describe("qa T-005481 e-c1: golden of the real capture (2026-11-11T05:02:55Z, daemon 7433 at 902f51ac7)", () => {
  const ALL = Object.keys(SLOT_DIRS) as CapturedSlot[];
  const view = (slot: CapturedSlot) => captureView(loadCapture(slot), ALL.indexOf(slot) + 1);
  const at = (s: SeatPoolSpec, slot: SeatPoolSlotView) => seatPoolCanReceive(s, slot, CAPTURE_NOW_MS);

  it("golden do real: a captura é coerente com o que o adaptador lê", () => {
    for (const slot of ALL) {
      const cap = loadCapture(slot);
      expect(cap.ps.canonicalSessionName, slot).toBe(cap.parked.seat.sessionName);
      expect(cap.show.destinationSession, slot).toBe(cap.ps.canonicalSessionName);
      expect(cap.show.state, slot).toBe("blocked");
      const held = cap.parked.seat.obligations.held;
      expect(held.map((h) => h.qitemId), slot).toEqual([cap.show.qitemId]);
      const armed = armedTransition(cap.transitions);
      expect(armed, slot).not.toBeNull();
      expect(held[0]!.wake?.ref, slot).toBe(armed!.wake!.ref);
      expect(cap.watchdog.jobId, slot).toBe(armed!.wake!.ref);
      expect(cap.watchdog.policy, slot).toBe("periodic-reminder");
    }
  });

  // s6, s10 and s11: dev slots at their prompt with a background shell, their only row blocked on external:*, a live
  // 3600 s periodic-reminder armed by the slot itself after it claimed the row. s8 (checked in the data): the same, claimed
  // at 04:31:27Z and parked by itself at 05:02:07Z on external:ci-upstream-pr-2946 with a live 3600 s timer: it qualifies too.
  it.each(["build-dev-1-s6", "build-dev-1-s8", "build-dev-1-s10", "build-dev-1-s11"] as const)(
    "golden do real (caso 1): %s às 05:02:55Z → \"parked\"",
    (slot) => {
      const v = view(slot);
      expect(v).toMatchObject({ openRows: 1, atPrompt: true, backgroundWork: true, cause: "working", idleSince: null });
      expect(v.parked).toMatchObject({ timerIntervalS: 3600, timerArmedLive: true, parkActor: v.sessionName });
      expect(v.parked!.blockedOn.startsWith("external:")).toBe(true);
      expect(at(spec(ON), v)).toBe("parked");
      // With a 3600 s timer the D-3 default is the edge: inclusive.
      expect(at(spec({ dispatch_to_parked: { max_wake_s: 3599 } }), v)).toBeNull();
    },
  );

  it("golden do real: build-dev-1-s9 blocked numa linha (fc-…) → null", () => {
    const v = view("build-dev-1-s9");
    expect(v.parked!.blockedOn).toBe("fc-bf7770ab233d-1794367215");
    expect(v.parked).toMatchObject({ timerIntervalS: 3600, timerArmedLive: true, parkActor: v.sessionName });
    expect(at(spec(ON), v)).toBeNull();
    // Everything else about s9 qualifies: only the blocker keeps it out.
    expect(at(spec(ON), { ...v, parked: { ...v.parked!, blockedOn: "external:x" } })).toBe("parked");
  });

  it("golden do real: qualidade-qa-s6 com park de 4500 s → null com max_wake_s 3600 (D-3); \"parked\" com 5400", () => {
    const v = view("qualidade-qa-s6");
    expect(v.parked).toMatchObject({ timerIntervalS: 4500, timerArmedLive: true, parkActor: v.sessionName });
    expect(at(spec(ON), v)).toBeNull();
    expect(at(spec({ dispatch_to_parked: { max_wake_s: 5400 } }), v)).toBe("parked");
    expect(at(spec({ dispatch_to_parked: { max_wake_s: 4500 } }), v)).toBe("parked");
  });

  // qa-s4 (checked in the data): activity "working" (ps: running), so it is not at its prompt and does not receive. Its
  // row was taken without a claim (claimedAt null): takenAt comes from its in-progress transition at 03:22:51.327Z.
  it("golden do real: qualidade-qa-s4 fora do prompt → null (claimedAt nulo, takenAt pela transição)", () => {
    const cap = loadCapture("qualidade-qa-s4");
    expect(cap.show.claimedAt).toBeNull();
    const v = view("qualidade-qa-s4");
    expect(v).toMatchObject({ atPrompt: false, cause: "working" });
    expect(v.parked!.takenAt).toBe("2026-11-11T03:22:51.327Z");
    expect(at(spec(ON), v)).toBeNull();
  });

  it("golden do real: sem a chave, nenhum slot recebe", () => {
    for (const slot of ALL) expect(at(spec(), view(slot)), slot).toBeNull();
  });

  it("golden do real: com allow_background_work: false, os 4 positivos dão null", () => {
    const off = spec({ dispatch_to_parked: { allow_background_work: false } });
    for (const slot of ["build-dev-1-s6", "build-dev-1-s8", "build-dev-1-s10", "build-dev-1-s11"] as const) {
      expect(at(off, view(slot)), slot).toBeNull();
    }
  });

  it("golden do real: o job terminal do s9 (park_timer_fired_once, phase fired, live false) → null", () => {
    const cap = loadCapture("build-dev-1-s9");
    const terminal = loadTerminalS9();
    expect(terminal).toMatchObject({ state: "terminal", terminalReason: "park_timer_fired_once", intervalSeconds: 3600, policy: "periodic-reminder" });
    // The row's history up to that fire: the park armed at 03:44:51Z, and its single fire at 04:44:51Z.
    const fire = cap.transitions.find((t) => t.wake?.phase === "fired" && t.wake.ref === terminal.jobId)!;
    expect(fire).toBeDefined();
    const cut = cap.transitions.filter((t) => t.transitionId <= fire.transitionId);
    const facts = captureFacts(cap, { transitions: cut, watchdog: terminal });
    expect(facts).toMatchObject({ timerIntervalS: 3600, timerArmedLive: false, parkActor: cap.ps.canonicalSessionName });
    const atFire = Date.parse(fire.ts);
    const v = captureView(cap, 1, facts);
    expect(seatPoolCanReceive(spec(ON), v, atFire)).toBeNull();
    // Even on an external: gate the fired timer keeps it out; the same facts with the timer live would qualify.
    const gated = { ...v, parked: { ...facts, blockedOn: "external:x" } };
    expect(seatPoolCanReceive(spec(ON), gated, atFire)).toBeNull();
    expect(seatPoolCanReceive(spec(ON), { ...gated, parked: { ...gated.parked, timerArmedLive: true } }, atFire)).toBe("parked");
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The positive cases (layer A)
// ---------------------------------------------------------------------------------------------------------------
describe("qa T-005481 e-c1: positive cases (policy)", () => {
  it("caso 1 (o caso base): 1 blocked em external:x, timer de 45 min vivo armado pelo próprio slot depois do claim, há 1 h, no prompt com backgroundWork → \"parked\"", () => {
    const p = parked();
    expect(can(spec(ON), p)).toBe("parked");
    // At its prompt with no background shell (cause idle, the reader's idleSince set) it qualifies too.
    expect(can(spec(ON), parked({ backgroundWork: false, cause: "idle", idleSince: ago(60) }))).toBe("parked");
    // A free seat stays free.
    expect(can(spec(ON), free())).toBe("free");
    // It receives one row from the model once that row has waited dispatch_after_s, not before.
    const early = plan(spec(ON), state({ slots: [p], pending: rows(1, MODEL, 30) }));
    expect(dispatches(early.intents)).toEqual([]);
    const pending = rows(1);
    const late = plan(spec(ON), state({ slots: [p], pending }));
    expect(dispatches(late.intents)).toEqual([{ kind: "dispatch", qitemId: pending[0]!.qitemId, from: MODEL, slotId: p.slotId, mode: "parked" }]);
  });

  it("caso 4 (plano): depois de receber B, o slot não recebe C, nem no mesmo plano nem no seguinte", () => {
    const p = parked();
    const both = rows(2);
    const same = plan(spec(ON), state({ slots: [p], pending: both }));
    expect(dispatches(same.intents)).toEqual([{ kind: "dispatch", qitemId: both[0]!.qitemId, from: MODEL, slotId: p.slotId, mode: "parked" }]);
    // The next plan: B is at the slot (2 open rows), so the reader gives no park facts; C stays at the model.
    const next = plan(spec(ON), state({ slots: [{ ...p, openRows: 2, parked: undefined }], pending: [both[1]!] }));
    expect(dispatches(next.intents)).toEqual([]);
    // Even with the facts of A still in the view, 2 open rows are the ceiling.
    const ceiling = plan(spec(ON), state({ slots: [{ ...p, openRows: 2 }], pending: [both[1]!] }));
    expect(dispatches(ceiling.intents)).toEqual([]);
  });

  it("caso 5 (ordem): um \"free\" de id alto e um \"parked\" de id baixo; com 1 linha ela vai ao free, com 2 uma a cada", () => {
    const p = parked({ slotId: 2 });
    const f = free({ slotId: 9 });
    // The parked one first in the array: the order is not the array's nor the id's.
    const one = rows(1);
    expect(dispatches(plan(spec(ON), state({ slots: [p, f], pending: one })).intents))
      .toEqual([{ kind: "dispatch", qitemId: one[0]!.qitemId, from: MODEL, slotId: 9 }]);
    const two = rows(2);
    expect(dispatches(plan(spec(ON), state({ slots: [p, f], pending: two })).intents)).toEqual([
      { kind: "dispatch", qitemId: two[0]!.qitemId, from: MODEL, slotId: 9 },
      { kind: "dispatch", qitemId: two[1]!.qitemId, from: MODEL, slotId: 2, mode: "parked" },
    ]);
    // Parked slots among themselves go by id.
    const p5 = parked({ slotId: 5 });
    const p3 = parked({ slotId: 3 });
    const both = rows(2);
    expect(dispatches(plan(spec(ON), state({ slots: [p5, p3], pending: both })).intents).map((i) => (i as { slotId: number }).slotId)).toEqual([3, 5]);
  });

  it("caso 9: sem demand_cmd e abaixo do MAX, o target com a regra é o sem ela menos os despachos \"parked\" (piso em min e occupied+starting); no MAX é igual", () => {
    const s1 = parked({ slotId: 1 });
    const s2 = free({ slotId: 2, openRows: 1, cause: "working", idleSince: null });
    const st = () => state({ slots: [s1, s2], pending: rows(1) });
    const off = plan(spec({ max: 4 }), st());
    expect(off).toMatchObject({ target: 3, reason: "scale_up", current: 2 });
    expect(off.intents).toContainEqual({ kind: "add", count: 1 });
    expect(dispatches(off.intents)).toEqual([]);
    const on = plan(spec({ max: 4, ...ON }), st());
    expect(on).toMatchObject({ target: 2, reason: "steady", current: 2 });
    expect(on.intents.some((i) => i.kind === "add")).toBe(false);
    expect(dispatches(on.intents)).toHaveLength(1);
    // At the MAX: the same target.
    expect(plan(spec({ max: 2 }), st()).target).toBe(2);
    expect(plan(spec({ max: 2, ...ON }), st()).target).toBe(2);
    // The floor at min.
    expect(plan(spec({ max: 4, min: 3 }), st()).target).toBe(3);
    expect(plan(spec({ max: 4, min: 3, ...ON }), st()).target).toBe(3);
    // The floor at occupied + starting: a seat still starting keeps the target up with the rule on.
    const starting = free({ slotId: 3, phase: "added", nodeId: null, nodeExists: false, idleSince: null, cause: "stopped" });
    const withStarting = () => state({ slots: [s1, s2, starting], pending: rows(1) });
    expect(plan(spec({ max: 4 }), withStarting()).target).toBe(3);
    expect(plan(spec({ max: 4, ...ON }), withStarting()).target).toBe(3);
    // occupied is the same with and without the rule: with a demand answer of 0 the target is occupied + starting.
    const demand = { desired: 0, sampledAt: iso(NOW) };
    const withDemand = (extra: Record<string, unknown>) =>
      plan(spec({ max: 4, demand_cmd: { argv: ["demand"] }, ...extra }), state({ slots: [s1, s2], pending: rows(1), demand }));
    expect(withDemand({}).target).toBe(2);
    expect(withDemand(ON).target).toBe(2);
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The negative cases (layer A): the slot does not receive.
// ---------------------------------------------------------------------------------------------------------------
describe("qa T-005481 e-c1: negative cases (policy)", () => {
  it("negativo: a chave ausente (spec e predicado)", () => {
    const parsed = parseSeatPoolSpec({ max: 2, cwd_template: "/w/{slot}" });
    expect(parsed).toEqual({ spec: {
      min: 0, max: 2, cooldownSeconds: 60, idleGraceSeconds: 300, dispatchAfterSeconds: 60, dispatchTag: null, drain: false, drainDeadlineSeconds: 900, adopt: [], cwdTemplate: "/w/{slot}",
      demandCmd: null, prepareCmd: null, releaseCmd: null, reapCmd: null, removeCmd: null, escalationOwner: null,
    } });
    expect((parsed as { spec: SeatPoolSpec }).spec).not.toHaveProperty("dispatchToParked");
    expect(can(spec(), parked())).toBeNull();
    expect(can(spec(), parked({ backgroundWork: false, cause: "idle", idleSince: ago(60) }))).toBeNull();
    expect(can(spec(), free())).toBe("free");
    const p = parked();
    const off = plan(spec(), state({ slots: [p], pending: rows(1) }));
    expect(dispatches(off.intents)).toEqual([]);
    // A free seat's dispatch is today's intent, with no mode.
    const f = free();
    const pending = rows(1);
    expect(dispatches(plan(spec(), state({ slots: [f], pending })).intents))
      .toEqual([{ kind: "dispatch", qitemId: pending[0]!.qitemId, from: MODEL, slotId: f.slotId }]);
  });

  it("negativo: drain: true", () => {
    const p = parked();
    expect(can(spec({ ...ON, drain: true }), p)).toBeNull();
    expect(dispatches(plan(spec({ ...ON, drain: true }), state({ slots: [p], pending: rows(1) })).intents)).toEqual([]);
  });

  it("negativo: slot adotado (uma fixa)", () => {
    expect(can(spec(ON), parked({ adopted: true }))).toBeNull();
  });

  it("negativo: a parte comum do C3 no modo parked (phase live, nodeExists, fora do reap), com os fatos do park válidos", () => {
    // Control: the same seat, live and present, qualifies.
    expect(can(spec(ON), parked())).toBe("parked");
    // A seat that became draining (or is still being added) between the plan and the move: the executor's recheck calls
    // this same function, so it must say null here.
    expect(can(spec(ON), parked({ phase: "draining" }))).toBeNull();
    expect(can(spec(ON), parked({ phase: "added" }))).toBeNull();
    expect(can(spec(ON), parked({ phase: "adding" }))).toBeNull();
    // A seat whose node is gone.
    expect(can(spec(ON), parked({ nodeExists: false }))).toBeNull();
    // A reapable seat (added by the pool, live, present, its session stopped).
    const reapable = parked({ cause: "stopped", atPrompt: true });
    expect(reapable.adopted).toBe(false);
    expect(can(spec(ON), reapable)).toBeNull();
    // The planner never sends a row to any of them.
    const st = state({ slots: [parked({ phase: "draining" }), parked({ nodeExists: false }), parked({ cause: "stopped" })], pending: rows(1) });
    expect(dispatches(plan(spec(ON), st).intents)).toEqual([]);
  });

  it("negativo: heldMessages > 0", () => {
    expect(can(spec(ON), parked({ heldMessages: 1 }))).toBeNull();
  });

  it("negativo: needsInput > 0", () => {
    expect(can(spec(ON), parked({ atPrompt: false, cause: "needs_input" }))).toBeNull();
  });

  it("negativo: cause usage_limit no prompt", () => {
    expect(can(spec(ON), parked({ cause: "usage_limit" }))).toBeNull();
    expect(can(spec(ON), parked({ cause: "stopped" }))).toBeNull();
  });

  it("negativo: fora do prompt", () => {
    expect(can(spec(ON), parked({ atPrompt: false }))).toBeNull();
    expect(can(spec(ON), parked({ atPrompt: false, backgroundWork: false }))).toBeNull();
  });

  it("negativo: allow_background_work: false com backgroundWork", () => {
    const strict = spec({ dispatch_to_parked: { allow_background_work: false } });
    expect(can(strict, parked())).toBeNull();
    // Without a background shell the same slot qualifies.
    expect(can(strict, parked({ backgroundWork: false, cause: "idle", idleSince: ago(60) }))).toBe("parked");
  });

  it("negativo: 2 linhas abertas", () => {
    expect(can(spec(ON), parked({ openRows: 2 }))).toBeNull();
    expect(can(spec(ON), parked({ openRows: 2, parked: undefined }))).toBeNull();
  });

  it("negativo: 1 in-progress (predicado)", () => {
    // The reader gives no park facts for a row that is not blocked.
    expect(can(spec(ON), parked({ parked: undefined }))).toBeNull();
  });

  it("negativo: 1 pending direta ao slot (predicado)", () => {
    expect(can(spec(ON), parked({ parked: undefined, cause: "idle", backgroundWork: false, idleSince: ago(60) }))).toBeNull();
  });

  it("negativo: blocked em outra linha (fc-…)", () => {
    expect(can(spec(ON), parked({}, { blockedOn: "fc-bf7770ab233d-1794367215" }))).toBeNull();
    expect(can(spec(ON), parked({}, { blockedOn: "qitem-01M74BAQ16995V194YC1FPCF4D" }))).toBeNull();
  });

  it("negativo: blocked em gate:x", () => {
    expect(can(spec(ON), parked({}, { blockedOn: "gate:x" }))).toBeNull();
    expect(can(spec(ON), parked({}, { blockedOn: "fold:x" }))).toBeNull();
  });

  it("negativo: blocked em sessão", () => {
    expect(can(spec(ON), parked({}, { blockedOn: "lead@r" }))).toBeNull();
    expect(can(spec(ON), parked({}, { blockedOn: OTHER_SEAT }))).toBeNull();
  });

  it("negativo: external: sem timer", () => {
    expect(can(spec(ON), parked({}, { timerIntervalS: null, timerArmedLive: false }))).toBeNull();
    // A watchdog wake (--wake-watchdog) has no interval: never "parked".
    expect(can(spec(ON), parked({}, { timerIntervalS: null, timerArmedLive: true }))).toBeNull();
  });

  it("negativo: o timer já disparou (job terminal, phase fired, live false), com A ainda blocked", () => {
    expect(can(spec(ON), parked({}, { timerIntervalS: 2700, timerArmedLive: false }))).toBeNull();
    expect(can(spec(ON), parked({}, { timerIntervalS: 3600, timerArmedLive: false }))).toBeNull();
  });

  it("negativo: timer de 75 min com max_wake_s 3600", () => {
    expect(can(spec(ON), parked({}, { timerIntervalS: 4500 }))).toBeNull();
    expect(can(spec({ dispatch_to_parked: { max_wake_s: 3600 } }), parked({}, { timerIntervalS: 4500 }))).toBeNull();
    // The edge is inclusive, and a wider max_wake_s lets it in.
    expect(can(spec(ON), parked({}, { timerIntervalS: 3600 }))).toBe("parked");
    expect(can(spec({ dispatch_to_parked: { max_wake_s: 5400 } }), parked({}, { timerIntervalS: 4500 }))).toBe("parked");
  });

  it("negativo: o wake atual armado por outra cadeira", () => {
    expect(can(spec(ON), parked({}, { parkActor: OTHER_SEAT }))).toBeNull();
    expect(can(spec(ON), parked({}, { parkActor: "watchdog@system" }))).toBeNull();
    expect(can(spec(ON), parked({}, { parkActor: null }))).toBeNull();
  });

  it("negativo: takenAt de 7 h", () => {
    expect(can(spec(ON), parked({}, { takenAt: ago(7 * 3600), parkedAt: ago(1800) }))).toBeNull();
    // The edge (6 h) is inclusive, and a wider max_claim_age_s lets 7 h in.
    expect(can(spec(ON), parked({}, { takenAt: ago(21600), parkedAt: ago(1800) }))).toBe("parked");
    expect(can(spec({ dispatch_to_parked: { max_claim_age_s: 28800 } }), parked({}, { takenAt: ago(7 * 3600), parkedAt: ago(1800) }))).toBe("parked");
  });

  it("negativo: park anterior ao takenAt", () => {
    expect(can(spec(ON), parked({}, { takenAt: ago(1800), parkedAt: ago(3600) }))).toBeNull();
    expect(can(spec(ON), parked({}, { takenAt: null }))).toBeNull();
    expect(can(spec(ON), parked({}, { parkedAt: null }))).toBeNull();
    // Parked in the same instant it was taken: at or after counts.
    expect(can(spec(ON), parked({}, { takenAt: ago(1800), parkedAt: ago(1800) }))).toBe("parked");
  });
});

// ---------------------------------------------------------------------------------------------------------------
// Invariants (layer A)
// ---------------------------------------------------------------------------------------------------------------
describe("qa T-005481 e-c1: invariants (policy)", () => {
  it("invariante: o parked nunca entra no free nem no dreno", () => {
    // A parked slot at its prompt with no background shell: the reader gives it cause idle and an old idleSince.
    const p = parked({ slotId: 8, cause: "idle", backgroundWork: false, idleSince: ago(7200) });
    const f = free({ slotId: 3, idleSince: ago(7200) });
    expect(can(spec(ON), p)).toBe("parked");
    // max below the current: one seat too many; only the free one drains, the parked one (higher id) never.
    const lowered = plan(spec({ ...ON, max: 1, idle_grace_s: 0, cooldown_s: 0 }), state({ slots: [p, f] }));
    expect(lowered.intents.filter((i) => i.kind === "drain")).toEqual([{ kind: "drain", slotId: 3, idleSince: f.idleSince }]);
    // Idle grace with no rows waiting: the same.
    const grace = plan(spec({ ...ON, max: 3, idle_grace_s: 0, cooldown_s: 0 }), state({ slots: [p, f] }));
    expect(grace.intents.some((i) => i.kind === "drain" && i.slotId === 8)).toBe(false);
    // The kill switch: nothing to the parked slot, no drain of it.
    const drained = plan(spec({ ...ON, drain: true }), state({ slots: [p, f], pending: rows(1) }));
    expect(drained.intents.some((i) => (i.kind === "drain" || i.kind === "dispatch") && i.slotId === 8)).toBe(false);
  });
});
