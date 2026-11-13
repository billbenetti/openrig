// On 0.7.13 (#2917, a7b8a27eb) the wake ladder parks a usage-limited seat until the reset its message states (hours),
// but the provider's reactive signal the pool reads is fresh for 5 minutes only. The pool remembers, per seat, the
// latest reset it saw, so a seat stays usage-limited for the pool until that reset, as for the ladder.
import { describe, it, expect } from "vitest";
import { SeatPoolUsageLimitMemory } from "../src/domain/seat-pool-usage-limit.js";

const T0 = Date.parse("2026-11-13T12:00:00.000Z");
const min = (m: number) => T0 + m * 60_000;
const pool = (sessions: string[], expiresAtMs: number) => ({ seatSessions: sessions, expiresAt: new Date(expiresAtMs).toISOString() });

describe("SeatPoolUsageLimitMemory", () => {
  it("keeps a seat limited after the signal goes stale, until the reset it stated", () => {
    const memory = new SeatPoolUsageLimitMemory();
    expect([...memory.limited([pool(["s1@r"], min(180))], min(0))]).toEqual(["s1@r"]);
    // 6 minutes later the reactive signal is stale: no pool is derived any more.
    expect([...memory.limited([], min(6))]).toEqual(["s1@r"]);
    expect([...memory.limited([], min(179))]).toEqual(["s1@r"]);
    expect([...memory.limited([], min(181))]).toEqual([]);
  });

  it("keeps the latest reset seen for a seat, and a seat seen again without a reset in the future is dropped at its time", () => {
    const memory = new SeatPoolUsageLimitMemory();
    memory.limited([pool(["s1@r", "s2@r"], min(60))], min(0));
    memory.limited([pool(["s1@r"], min(120))], min(10));
    expect([...memory.limited([], min(90))].sort()).toEqual(["s1@r"]);
    expect([...memory.limited([], min(121))]).toEqual([]);
  });

  it("an unreadable reset limits the seat only for this read", () => {
    const memory = new SeatPoolUsageLimitMemory();
    expect([...memory.limited([{ seatSessions: ["s1@r"], expiresAt: "not a time" }], min(0))]).toEqual(["s1@r"]);
    expect([...memory.limited([], min(1))]).toEqual([]);
  });
});
