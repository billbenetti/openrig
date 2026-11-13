// T-006251: the follow-up of T-006090's review (REVISAO-T-006182: B1, S1, S2, S3). The busy read now covers a free seat
// too (B1: the planner, the executor's recheck and the demand command, tested in seat-pool-t6090-busy.test.ts with the
// executor's harness); this file holds the reader's own cases:
// - S2: a matching process that left the pane's tree (found by the TMUX_PANE it inherited) holds the seat only while it
//   is younger than dispatch_to_parked.busy_orphan_max_age_s and not older than the pane's current occupant (a fresh
//   launch starts a new runtime in the same pane; the orphan of the occupant before it is not this one's work). One
//   still under the pane holds it whatever its age.
// - S1: the environment is read off the event loop with a deadline; "no TMUX_PANE" and "process gone" free the seat,
//   a read that fails holds it.
// - S3: the server's panes in one read; a seat whose pane cannot be told is held alone, not the whole pool; the
//   composition the daemon runs (readSeatPoolBusyCandidates) is tested here instead of only as text.

import { describe, it, expect, vi, afterEach } from "vitest";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { TmuxAdapter } from "../src/adapters/tmux.js";
import { execArgvCommand, execCommand } from "../src/adapters/tmux-exec.js";
import { readSeatPoolBusyCandidates } from "../src/domain/seat-pool-executor.js";
import {
  readSeatPoolBusySessions,
  readProcTmuxPane,
  LIBUV_DEFAULT_THREADPOOL_SIZE,
  SEAT_POOL_BUSY_ENV_CONCURRENCY,
  SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S,
  seatPoolBusyCommand,
  seatPoolUnreadInTree,
  seatPoolBusyFromProcess,
  seatPoolPaneOccupantMs,
  type BusyProcessRow,
  type SeatPoolBusyReadDeps,
  type SeatPoolPane,
} from "../src/domain/seat-pool-busy.js";
import {
  parseSeatPoolSpec, planSeatPool, seatPoolSpecProblem, type SeatPoolSlotView, type SeatPoolSpec, type SeatPoolState,
} from "../src/domain/policies/seat-pool.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const REAL = join(HERE, "fixtures", "t6090", "real");
const PROGRAMS = ["suite-flock", "congelar-eval"];
const NOW = Date.parse("2026-11-12T19:00:00.000Z");

/** ps's lstart (local time) of `ms`, as process-table.ts writes a row's startedAt. */
function lstart(ms: number): string {
  const d = new Date(ms);
  const days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const two = (n: number) => String(n).padStart(2, "0");
  return `${days[d.getDay()]} ${months[d.getMonth()]} ${String(d.getDate()).padStart(2, " ")} ${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())} ${d.getFullYear()}`;
}
const agoS = (seconds: number) => lstart(NOW - seconds * 1000);

/** One seat's pane (%24, pid 100: a login shell) and its runtime (claude, pid 101) started `occupantAgoS` ago; an
 *  orphaned suite-flock (pid 500, reparented to 1) with TMUX_PANE %24 started `orphanAgoS` ago. */
function scene(opts: { occupantAgoS: number; orphanAgoS: number; shellAgoS?: number }) {
  const rows: BusyProcessRow[] = [
    { pid: 100, ppid: 1, command: "/bin/bash -l", startedAt: agoS(opts.shellAgoS ?? 90_000) },
    { pid: 101, ppid: 100, command: "/home/bruno/.local/bin/claude --dangerously-skip-permissions", startedAt: agoS(opts.occupantAgoS) },
    { pid: 500, ppid: 1, command: "bash /home/bruno/.local/bin/suite-flock nice -n 10 python3 -I t.py", startedAt: agoS(opts.orphanAgoS) },
  ];
  const deps: SeatPoolBusyReadDeps = {
    processes: async () => rows,
    panes: async () => [{ paneId: "%24", panePid: 100, session: "qa-s7@r", active: true }],
    paneOf: () => ({ paneId: "%24", tmuxSession: "qa-s7@r" }),
    processPane: async (pid) => (pid === 500 ? "%24" : null),
    nowMs: () => NOW,
  };
  return { rows, deps };
}

describe("T-006251 S2: the age of a process that left the pane's tree", () => {
  it("an orphan of the current occupant, younger than the ceiling, holds the seat", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, deps, { orphanMaxAgeSeconds: 21_600 });
    expect(read.get("qa-s7@r")).toBe("suite-flock (pid 500, TMUX_PANE %24)");
  });

  it("an orphan older than busy_orphan_max_age_s holds nothing (the real: suite-flock left 4 h 50 in a slot's queue)", async () => {
    const { deps } = scene({ occupantAgoS: 30_000, orphanAgoS: 17_400 });
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, deps, { orphanMaxAgeSeconds: 14_400 })).get("qa-s7@r")).toBeNull();
    // The same orphan under a higher ceiling (the default, 12 h since S8) still holds it.
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, deps)).get("qa-s7@r")).toBe("suite-flock (pid 500, TMUX_PANE %24)");
    expect(SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S).toBe(43_200);
  });

  it("an orphan of the occupant before a fresh launch (started before the pane's current runtime) holds nothing", async () => {
    const { deps } = scene({ occupantAgoS: 600, orphanAgoS: 1800 });
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, deps)).get("qa-s7@r")).toBeNull();
  });

  it("a pane that runs the runtime itself: a newer tool shell under it does not make the current orphan the previous occupant's (RC S1)", async () => {
    // The reviewer's repro: the runtime is the pane's process (2 h), the orphan 1 h old, a tool shell 10 s old.
    const rows: BusyProcessRow[] = [
      { pid: 100, ppid: 1, command: "/home/bruno/.local/bin/claude --dangerously-skip-permissions", startedAt: agoS(7200) },
      { pid: 103, ppid: 100, command: "/bin/bash -c source /x/snapshot.sh && eval 'git status'", startedAt: agoS(10) },
      { pid: 500, ppid: 1, command: "bash /home/bruno/.local/bin/suite-flock nice -n 10 python3 -I t.py", startedAt: agoS(3600) },
    ];
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => rows });
    expect(read.get("qa-s7@r")).toBe("suite-flock (pid 500, TMUX_PANE %24)");
    expect(seatPoolPaneOccupantMs(rows, 100)).toBe(Date.parse(agoS(7200)));
    // The orphan of an earlier runtime in that pane (older than the pane's process) still holds nothing.
    const older = rows.map((row) => (row.pid === 500 ? { ...row, startedAt: agoS(9000) } : row));
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => older })).get("qa-s7@r")).toBeNull();
  });

  it("a login shell (`-bash`) pane: the occupant is its newest child, the runtime a fresh launch started", () => {
    const rows: BusyProcessRow[] = [
      { pid: 100, ppid: 1, command: "-bash", startedAt: agoS(90_000) },
      { pid: 101, ppid: 100, command: "claude", startedAt: agoS(600) },
    ];
    expect(seatPoolPaneOccupantMs(rows, 100)).toBe(Date.parse(agoS(600)));
    expect(seatPoolPaneOccupantMs([], 100)).toBeNull();
  });

  it("a process still under the pane holds the seat whatever its age", async () => {
    const rows: BusyProcessRow[] = [
      { pid: 100, ppid: 1, command: "/bin/bash -l", startedAt: agoS(90_000) },
      { pid: 101, ppid: 100, command: "claude", startedAt: agoS(600) },
      { pid: 102, ppid: 101, command: "bash /x/suite-flock true", startedAt: agoS(80_000) },
    ];
    const { deps } = scene({ occupantAgoS: 600, orphanAgoS: 1 });
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => rows }, { orphanMaxAgeSeconds: 60 });
    expect(read.get("qa-s7@r")).toBe("suite-flock (pid 102)");
  });

  it("an orphan whose start cannot be read holds the seat (the read leans to busy)", async () => {
    const { rows, deps } = scene({ occupantAgoS: 600, orphanAgoS: 1800 });
    const blind = rows.map((row) => (row.pid === 500 ? { ...row, startedAt: "not a date" } : row));
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => blind })).get("qa-s7@r"))
      .toBe("suite-flock (pid 500, TMUX_PANE %24)");
  });

  it("busy_orphan_max_age_s: read only with busy_processes, default 43200 (S8: the suite-flock waits up to 12 h), bounds 60..172800; {} reads as before", () => {
    const parse = (inner: Record<string, unknown>) => parseSeatPoolSpec({ max: 2, cwd_template: "/w/{slot}", dispatch_to_parked: inner });
    const specOf = (inner: Record<string, unknown>) => (parse(inner) as { spec: SeatPoolSpec }).spec.dispatchToParked;
    expect(specOf({})).not.toHaveProperty("busyOrphanMaxAgeSeconds");
    expect(specOf({ refuse_gates: ["congelar"] })).not.toHaveProperty("busyOrphanMaxAgeSeconds");
    expect(specOf({ busy_processes: PROGRAMS })).toMatchObject({ busyProcesses: PROGRAMS, busyOrphanMaxAgeSeconds: 43_200 });
    expect(SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S).toBe(43_200);
    expect(specOf({ busy_processes: PROGRAMS, busy_orphan_max_age_s: 3600 })).toMatchObject({ busyOrphanMaxAgeSeconds: 3600 });
    for (const bad of [59, 172_801, 1.5, "3600", null]) {
      expect(seatPoolSpecProblem({ context: { max: 2, cwd_template: "/w/{slot}", dispatch_to_parked: { busy_processes: PROGRAMS, busy_orphan_max_age_s: bad } } }))
        .toMatchObject({ field: "dispatch_to_parked.busy_orphan_max_age_s" });
    }
    expect(seatPoolSpecProblem({ context: { max: 2, cwd_template: "/w/{slot}", dispatch_to_parked: { busy_orphan_max_age_s: 3600 } } }))
      .toMatchObject({ field: "dispatch_to_parked.busy_orphan_max_age_s", message: expect.stringContaining("only with") });
  });
});

describe("T-006251 S1: the environment read off the event loop, failure apart from absence", () => {
  it("no TMUX_PANE and a process gone (null) free the seat; a read that fails holds it", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processPane: async () => null })).get("qa-s7@r")).toBeNull();
    const failed = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, {
      ...deps, processPane: async () => { throw Object.assign(new Error("EMFILE: too many open files"), { code: "EMFILE" }); },
    });
    expect(failed.get("qa-s7@r")).toBe("environment of pid 500 not read: EMFILE: too many open files");
  });

  it("an environment read that never answers counts as failed after the timeout, and holds the seat", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processPane: () => new Promise<never>(() => {}) }, { timeoutMs: 20 });
    expect(read.get("qa-s7@r")).toBe("environment of pid 500 not read: the environment of pid 500 took over 20 ms");
  });

  it("environments that never answer: at most the limit pending, no new read after the one deadline, and the seat held (RC delta)", async () => {
    const { rows, deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const orphans: BusyProcessRow[] = Array.from({ length: 10 }, (_, i) => ({ pid: 600 + i, ppid: 1, command: "bash /x/suite-flock true", startedAt: agoS(60) }));
    let started = 0;
    const t0 = Date.now();
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, {
      ...deps, processes: async () => [...rows.filter((r) => r.pid !== 500), ...orphans],
      processPane: () => { started += 1; return new Promise<never>(() => {}); },
    }, { timeoutMs: 40 });
    expect(started).toBe(SEAT_POOL_BUSY_ENV_CONCURRENCY);
    expect(Date.now() - t0).toBeLessThan(400);
    expect(read.get("qa-s7@r")).toMatch(/^environment of pid 600 not read: /);
  });

  it("only matching processes have their environment read, each once for every seat, in parallel", async () => {
    const { rows, deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const more = [...rows, { pid: 501, ppid: 1, command: "nice -n 10 /x/congelar-eval /d", startedAt: agoS(60) }, { pid: 502, ppid: 1, command: "sleep 9" }];
    let inFlight = 0;
    let peak = 0;
    const processPane = vi.fn(async (pid: number) => {
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 5));
      inFlight -= 1;
      return pid === 500 ? "%24" : "%99";
    });
    const panes = async (): Promise<SeatPoolPane[]> => [
      { paneId: "%24", panePid: 100, session: "a@r", active: true }, { paneId: "%30", panePid: 300, session: "b@r", active: true },
    ];
    const read = await readSeatPoolBusySessions(["a@r", "b@r"], PROGRAMS, {
      ...deps, processes: async () => more, panes, paneOf: (s) => ({ paneId: s === "a@r" ? "%24" : "%30", tmuxSession: s }), processPane,
    });
    expect(processPane.mock.calls.map(([pid]) => pid).sort()).toEqual([500, 501]);
    expect(peak).toBe(2);
    expect(read.get("a@r")).toBe("suite-flock (pid 500, TMUX_PANE %24)");
    expect(read.get("b@r")).toBeNull();
  });

  it("readProcTmuxPane: absence (none, gone, another user's) is null; the daemon's own unreadable process rejects", async () => {
    const err = (code: string) => Object.assign(new Error(`${code}: x`), { code });
    const io = (read: () => Promise<string>, owner: () => Promise<number> = async () => 1000) => ({ readFile: read, ownerUid: owner, uid: 1000 });
    expect(await readProcTmuxPane(9, io(async () => "A=1\0TMUX_PANE=%24\0B=2\0"))).toBe("%24");
    expect(await readProcTmuxPane(9, io(async () => "A=1\0B=2\0"))).toBeNull();
    expect(await readProcTmuxPane(9, io(async () => { throw err("ENOENT"); }))).toBeNull();
    expect(await readProcTmuxPane(9, io(async () => { throw err("ESRCH"); }))).toBeNull();
    // EACCES on another user's process: no seat's; on the daemon's own: a failed read.
    expect(await readProcTmuxPane(9, io(async () => { throw err("EACCES"); }, async () => 1001))).toBeNull();
    await expect(readProcTmuxPane(9, io(async () => { throw err("EACCES"); }, async () => 1000))).rejects.toThrow(/EACCES/);
    await expect(readProcTmuxPane(9, io(async () => { throw err("EPERM"); }, async () => 1000))).rejects.toThrow(/EPERM/);
    // Gone between the read and the owner's stat: null.
    expect(await readProcTmuxPane(9, io(async () => { throw err("EACCES"); }, async () => { throw err("ENOENT"); }))).toBeNull();
    await expect(readProcTmuxPane(9, io(async () => { throw err("EMFILE"); }))).rejects.toThrow(/EMFILE/);
    const paths: string[] = [];
    await readProcTmuxPane(42, { readFile: async (path) => { paths.push(path); return ""; }, ownerUid: async () => 0, uid: 0 }, "/fake/proc");
    expect(paths).toEqual(["/fake/proc/42/environ"]);
  });

  it("the daemon reads it with fs.promises through readProcTmuxPane (startup.ts), never readFileSync", () => {
    const startup = readFileSync(join(HERE, "..", "src", "startup.ts"), "utf8");
    expect(startup).toContain("readFile: (path: string) => fs.promises.readFile(path, \"latin1\"),");
    expect(startup).toContain("process.platform === \"linux\" ? readProcTmuxPane(pid, procEnvironmentIo) : null;");
    expect(startup).not.toContain("readFileSync(`/proc/${pid}/environ`");
  });

  it("only the matching processes outside every pane's tree have their environment read; a failure under another pane holds nothing", async () => {
    const { rows, deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    // Another seat's pane (%30, pid 300) with a suite-flock under it (301): that one is found by the tree, not read.
    const more: BusyProcessRow[] = [...rows, { pid: 300, ppid: 1, command: "/bin/bash -l", startedAt: agoS(90_000) },
      { pid: 301, ppid: 300, command: "bash /x/suite-flock true", startedAt: agoS(60) }];
    const asked: number[] = [];
    const read = await readSeatPoolBusySessions(["qa-s7@r", "b@r"], PROGRAMS, {
      ...deps, processes: async () => more,
      panes: async () => [{ paneId: "%24", panePid: 100, session: "qa-s7@r", active: true }, { paneId: "%30", panePid: 300, session: "b@r", active: true }],
      paneOf: (s) => ({ paneId: s === "qa-s7@r" ? "%24" : "%30", tmuxSession: s }),
      processPane: async (pid) => { asked.push(pid); if (pid === 301) throw new Error("EMFILE"); return pid === 500 ? "%24" : null; },
    });
    expect(asked).toEqual([500]);
    expect(read.get("qa-s7@r")).toBe("suite-flock (pid 500, TMUX_PANE %24)");
    expect(read.get("b@r")).toBe("suite-flock (pid 301)");
  });
});

describe("T-006251 S3: one pane read; a seat that cannot be told is held alone", () => {
  it("a paneOf that throws (the database read) holds that seat only; the others are read", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const panes = vi.fn(async (): Promise<SeatPoolPane[]> => [
      { paneId: "%24", panePid: 100, session: "a@r", active: true }, { paneId: "%30", panePid: 300, session: "c@r", active: true },
    ]);
    const read = await readSeatPoolBusySessions(["a@r", "b@r", "c@r"], PROGRAMS, {
      ...deps, panes,
      paneOf: (s) => {
        if (s === "b@r") throw new Error("SQLITE_BUSY: database is locked");
        return { paneId: s === "a@r" ? "%24" : "%30", tmuxSession: s };
      },
    });
    expect(panes).toHaveBeenCalledTimes(1);
    expect(read.get("a@r")).toBe("suite-flock (pid 500, TMUX_PANE %24)");
    expect(read.get("b@r")).toBe("pane not read: SQLITE_BUSY: database is locked");
    expect(read.get("c@r")).toBeNull();
  });

  it("a seat with no bound pane is its tmux session's active pane (as `display-message -t =session:` resolved it)", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const panes = async (): Promise<SeatPoolPane[]> => [
      { paneId: "%23", panePid: 999, session: "qa-s7@r", active: false }, { paneId: "%24", panePid: 100, session: "qa-s7@r", active: true },
    ];
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, panes, paneOf: () => ({ paneId: null, tmuxSession: "qa-s7@r" }) });
    expect(read.get("qa-s7@r")).toBe("suite-flock (pid 500, TMUX_PANE %24)");
  });

  it("listAllPanePids: one `tmux list-panes -a` for the whole server, parsed by the adapter's printable separator (B2); a `|` in the session name stays; no server reads as none", async () => {
    const seen: string[][] = [];
    const out = "%0|2620|11|advisor-lead@kernel\n%310|2214571|11|build-dev-1-s6@fabrica\n%311|2214600|10|build-dev-1-s6@fabrica\n%312|7|11|a|b\nlixo\n%0_2620_11_x\n";
    const adapter = new TmuxAdapter(async () => "", undefined, async (argv) => { seen.push(argv); return out; });
    expect(await adapter.listAllPanePids()).toEqual([
      { paneId: "%0", panePid: 2620, session: "advisor-lead@kernel", active: true },
      { paneId: "%310", panePid: 2214571, session: "build-dev-1-s6@fabrica", active: true },
      { paneId: "%311", panePid: 2214600, session: "build-dev-1-s6@fabrica", active: false },
      { paneId: "%312", panePid: 7, session: "a|b", active: true },
    ]);
    expect(seen).toEqual([["tmux", "list-panes", "-a", "-F", "#{pane_id}|#{pane_pid}|#{window_active}#{pane_active}|#{session_name}"]]);
    const none = new TmuxAdapter(async () => "", undefined, async () => { throw new Error("no server running on /tmp/tmux-1000/default"); });
    expect(await none.listAllPanePids()).toEqual([]);
    const broken = new TmuxAdapter(async () => "", undefined, async () => { throw new Error("permission denied"); });
    await expect(broken.listAllPanePids()).rejects.toThrow(/permission denied/);
  });
});

describe("T-006251: the daemon's composition (readSeatPoolBusyCandidates, as startup.ts's busyPoolSessions)", () => {
  const view = (slotId: number, overrides: Partial<SeatPoolSlotView>): SeatPoolSlotView => ({
    slotId, logicalId: `dev.impl-s${slotId}`, sessionName: `dev-impl-s${slotId}@r`, nodeId: `node-${slotId}`, phase: "live", adopted: false,
    nodeExists: true, openRows: 0, heldMessages: 0, idleSince: new Date(NOW - 600_000).toISOString(), cause: "idle", atPrompt: true,
    backgroundWork: false, ...overrides,
  });
  const specOf = (inner: Record<string, unknown>) =>
    (parseSeatPoolSpec({ max: 4, cwd_template: "/w/{slot}", dispatch_to_parked: inner }) as { spec: SeatPoolSpec }).spec;

  it("reads the free and the parked seats (B1), not a working one; undefined without busy_processes", async () => {
    const free = view(1, {});
    const working = view(2, { idleSince: null, cause: "working", atPrompt: false });
    const parked = view(3, {
      openRows: 1, idleSince: null, cause: "working", backgroundWork: true,
      parked: { qitemId: "a-3", blockedOn: "external:ci-x", timerIntervalS: 2700, timerArmedLive: true, parkActor: "dev-impl-s3@r",
        parkedAt: new Date(NOW - 1_800_000).toISOString(), takenAt: new Date(NOW - 3_600_000).toISOString() },
    });
    const asked: string[] = [];
    const deps: SeatPoolBusyReadDeps = {
      processes: async () => [], panes: async () => [], processPane: async () => null,
      paneOf: (s) => { asked.push(s); return { paneId: "%1", tmuxSession: s }; },
    };
    const read = await readSeatPoolBusyCandidates(specOf({ busy_processes: PROGRAMS }), [free, working, parked], NOW, deps);
    expect(asked).toEqual(["dev-impl-s1@r", "dev-impl-s3@r"]);
    expect([...read!.keys()]).toEqual(["dev-impl-s1@r", "dev-impl-s3@r"]);
    expect(readSeatPoolBusyCandidates(specOf({ refuse_gates: ["congelar"] }), [free], NOW, deps)).toBeUndefined();
  });

  it("passes the spec's busy_orphan_max_age_s to the read", async () => {
    const { deps } = scene({ occupantAgoS: 30_000, orphanAgoS: 7200 });
    const slot = view(1, { sessionName: "qa-s7@r" });
    expect((await readSeatPoolBusyCandidates(specOf({ busy_processes: PROGRAMS }), [slot], NOW, deps))!.get("qa-s7@r")).toMatch(/^suite-flock/);
    expect((await readSeatPoolBusyCandidates(specOf({ busy_processes: PROGRAMS, busy_orphan_max_age_s: 3600 }), [slot], NOW, deps))!.get("qa-s7@r")).toBeNull();
  });

  it("the planner counts a free seat held by a busy process as occupied: the pool grows for the row, and the seat is not drained", () => {
    // Lente de lógica (wf_fff3f5f2): with the held seat counted as idle capacity the plan stayed steady (no dispatch, no
    // add) and the row waited for the orphan to end.
    const sp = (parseSeatPoolSpec({ max: 3, min: 0, cwd_template: "/w/{slot}", cooldown_s: 0, idle_grace_s: 60, dispatch_after_s: 60,
      dispatch_to_parked: { busy_processes: PROGRAMS } }) as { spec: SeatPoolSpec }).spec;
    const state = (busyProcess: string | null): SeatPoolState => ({
      model: { rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model", sessionName: "dev-impl@r" },
      modelCause: "working", restoring: false, adoptable: [], demand: null, lastScaleAt: null,
      slots: [view(1, { busyProcess, idleSince: new Date(NOW - 3_600_000).toISOString() })],
      pending: [{ qitemId: "q1", destination: "dev-impl@r", waitingSince: new Date(NOW - 120_000).toISOString(), tags: ["tarefa-nova"] }],
    });
    const quiet = planSeatPool(sp, state(null), NOW, 120_000);
    expect(quiet.intents).toEqual([{ kind: "dispatch", qitemId: "q1", from: "dev-impl@r", slotId: 1 }]);
    const held = planSeatPool(sp, state("suite-flock (pid 9, TMUX_PANE %24)"), NOW, 120_000);
    expect(held.intents.filter((i) => i.kind === "dispatch")).toEqual([]);
    expect(held.intents.some((i) => i.kind === "drain")).toBe(false);
    expect(held).toMatchObject({ reason: "scale_up", target: 2, current: 1 });
  });

  it("golden of the real (REVISAO-T-006182): the orphan of qualidade-qa-s8 by its TMUX_PANE, under the default ceiling", async () => {
    const orphans = JSON.parse(readFileSync(join(REAL, "orfao-tmux-pane.json"), "utf8")) as Array<{
      process: BusyProcessRow; tmuxPane: string; paneSessionNow: string;
    }>;
    const o = orphans.find((x) => x.paneSessionNow === "qualidade-qa-s8@fabrica")!;
    // The captured row has no start time (process-table.ts's startedAt came later to this read): its age is unknown,
    // which holds the seat.
    const rows: BusyProcessRow[] = [{ pid: 4_000_000, ppid: 1, command: "/bin/bash -l" }, { pid: 4_000_001, ppid: 4_000_000, command: "claude" }, o.process];
    const read = await readSeatPoolBusyCandidates(specOf({ busy_processes: PROGRAMS }), [view(1, { sessionName: "qualidade-qa-s8@fabrica" })], NOW, {
      processes: async () => rows,
      panes: async () => [{ paneId: o.tmuxPane, panePid: 4_000_000, session: "qualidade-qa-s8@fabrica", active: true }],
      paneOf: () => ({ paneId: o.tmuxPane, tmuxSession: "qualidade-qa-s8@fabrica" }),
      processPane: async (pid) => (pid === o.process.pid ? o.tmuxPane : null),
      nowMs: () => NOW,
    });
    expect(read!.get("qualidade-qa-s8@fabrica")).toBe(`suite-flock (pid ${o.process.pid}, TMUX_PANE ${o.tmuxPane})`);
  });
});

const nativeTmux = process.platform !== "win32" && (() => {
  try { execFileSync("tmux", ["-V"], { stdio: "ignore" }); return true; } catch { return false; }
})();

describe.skipIf(!nativeTmux)("T-006251 B2: listAllPanePids against a real tmux server, outside tmux, with LC_ALL=C", { timeout: 30_000 }, () => {
  let root = "";
  afterEach(() => {
    if (root) {
      try { execFileSync("tmux", ["-S", join(root, "s.sock"), "kill-server"], { stdio: "ignore" }); } catch { /* not started */ }
      rmSync(root, { recursive: true, force: true });
      root = "";
    }
    vi.unstubAllEnvs();
  });

  it("reads the pane of each session (a non-UTF-8 locale turns a tab into `_`, which dropped every pane)", async () => {
    root = mkdtempSync(join(tmpdir(), "t"));
    const sock = join(root, "s.sock");
    // The daemon's systemd --user unit may run without a UTF-8 locale and outside tmux: then tmux sanitizes a literal tab
    // in -F output (as 3.3a and 3.6 always do).
    vi.stubEnv("LC_ALL", "C");
    vi.stubEnv("LC_CTYPE", "C");
    vi.stubEnv("LANG", "C");
    vi.stubEnv("TMUX", undefined);
    vi.stubEnv("TMUX_PANE", undefined);
    vi.stubEnv("SHELL", "/bin/sh");
    execFileSync("tmux", ["-S", sock, "-f", "/dev/null", "new-session", "-d", "-s", "pool-seat@r", "sleep 120"], { stdio: "ignore" });
    const pid = Number(execFileSync("tmux", ["-S", sock, "display-message", "-p", "-t", "=pool-seat@r:", "#{pane_pid}"], { encoding: "utf8" }).trim());
    const adapter = new TmuxAdapter(execCommand, undefined, execArgvCommand, ["-S", sock]);
    const panes = await adapter.listAllPanePids();
    expect(panes).toEqual([expect.objectContaining({ session: "pool-seat@r", panePid: pid, active: true })]);
    expect(panes[0]!.paneId).toMatch(/^%\d+$/);
  });
});

describe("T-006251 volta 1: B3, S6, S9, S10 and the S11 boundaries", () => {
  const err = (code: string) => Object.assign(new Error(`${code}: x`), { code });

  it("B3: the owner's stat failing with anything but ENOENT/ESRCH is a failed read (it rejects, which holds the seat), not 'gone'", async () => {
    const io = (owner: () => Promise<number>) => ({ readFile: async (): Promise<string> => { throw err("EACCES"); }, ownerUid: owner, uid: 1000 });
    await expect(readProcTmuxPane(9, io(async () => { throw err("EIO"); }))).rejects.toThrow(/EACCES/);
    await expect(readProcTmuxPane(9, io(async () => { throw err("EMFILE"); }))).rejects.toThrow(/EACCES/);
    expect(await readProcTmuxPane(9, io(async () => { throw err("ENOENT"); }))).toBeNull();
    expect(await readProcTmuxPane(9, io(async () => { throw err("ESRCH"); }))).toBeNull();
  });

  it("B3 (the negative case): a free seat whose orphan's environ is unreadable (EACCES, not dumpable) and whose stat fails (EIO) is held", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const io = { readFile: async (): Promise<string> => { throw err("EACCES"); }, ownerUid: async (): Promise<number> => { throw err("EIO"); }, uid: 1000 };
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processPane: (pid) => readProcTmuxPane(pid, io) });
    expect(read.get("qa-s7@r")).toBe("environment of pid 500 not read: EACCES: x");
  });

  it("seatPoolBusyFromProcess: only the read's process reasons are a process; every failure the read writes is not", () => {
    for (const reason of ["suite-flock (pid 9)", "congelar-eval (pid 77, TMUX_PANE %9)", "suite-flock (pid 781461, TMUX_PANE %24)", "run(1) (pid 9)"]) {
      expect(seatPoolBusyFromProcess(reason)).toBe(true);
    }
    for (const reason of ["pane not found", "pane not read: db", "panes not read: x", "process table not read: EACCES",
      "environment of pid 500 not read: EMFILE: x", "busy processes not read: no /proc", "busy processes not read for this seat",
      "environment of pid 5 not read: environment reads still pending (4) at the deadline", "panes not read: boom (pid 3)", null, undefined, ""]) {
      expect(seatPoolBusyFromProcess(reason)).toBe(false);
    }
  });

  it("S6: a free seat held only because its processes could not be read takes no row, is not drained, holds its place and grows the pool by one seat at most", () => {
    const sp = (parseSeatPoolSpec({ max: 8, min: 0, cwd_template: "/w/{slot}", cooldown_s: 0, idle_grace_s: 60, dispatch_after_s: 60,
      dispatch_to_parked: { busy_processes: PROGRAMS } }) as { spec: SeatPoolSpec }).spec;
    const slot = (slotId: number, busyProcess: string | null): SeatPoolSlotView => ({
      slotId, logicalId: `dev.impl-s${slotId}`, sessionName: `dev-impl-s${slotId}@r`, nodeId: `node-${slotId}`, phase: "live", adopted: false,
      nodeExists: true, openRows: 0, heldMessages: 0, idleSince: new Date(NOW - 3_600_000).toISOString(), cause: "idle", atPrompt: true,
      backgroundWork: false, busyProcess,
    });
    const state = (reasons: Array<string | null>): SeatPoolState => ({
      model: { rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model", sessionName: "dev-impl@r" },
      modelCause: "working", restoring: false, adoptable: [], demand: null, lastScaleAt: null,
      slots: reasons.map((reason, i) => slot(i + 1, reason)),
      pending: ["q1", "q2"].map((qitemId) => ({ qitemId, destination: "dev-impl@r", waitingSince: new Date(NOW - 120_000).toISOString(), tags: ["tarefa-nova"] })),
    });
    // The lens's simulation: max 8, 2 pending, both seats unreadable. Before: 2 -> 4 -> 6 -> 8 with no dispatch. Now one
    // seat more (it takes a row when the failure was the seat's own), and a seat added that fails the same read grows
    // nothing more.
    const failed = "environment of pid 500 not read: the environments took over 5000 ms";
    const unread = planSeatPool(sp, state([failed, "pane not found"]), NOW, 120_000);
    expect(unread.intents.filter((i) => i.kind === "dispatch" || i.kind === "drain")).toEqual([]);
    expect(unread).toMatchObject({ reason: "scale_up", target: 3, current: 2 });
    expect(unread.intents).toContainEqual({ kind: "add", count: 1 });
    const next = planSeatPool(sp, state([failed, "pane not found", failed]), NOW, 120_000);
    expect(next.intents.filter((i) => i.kind === "dispatch" || i.kind === "drain" || i.kind === "add")).toEqual([]);
    expect(next).toMatchObject({ reason: "steady", target: 3, current: 3 });
    // No rows waiting: nothing grows, and the unread seats are neither drained nor counted away.
    const quietPool = planSeatPool(sp, { ...state([failed, failed]), pending: [] }, NOW, 120_000);
    expect(quietPool.intents.filter((i) => i.kind === "drain" || i.kind === "add" || i.kind === "remove")).toEqual([]);
    expect(quietPool).toMatchObject({ target: 2, current: 2 });
    // A real process still counts as occupied: the pool grows for the rows it can't take (T-006251 B1).
    const real = planSeatPool(sp, state(["suite-flock (pid 9, TMUX_PANE %24)", "congelar-eval (pid 7)"]), NOW, 120_000);
    expect(real.intents.filter((i) => i.kind === "dispatch" || i.kind === "drain")).toEqual([]);
    expect(real).toMatchObject({ reason: "scale_up", target: 4, current: 2 });
  });

  it("S6 (lens review): a seat starting is not removed when a seat's read flips from a process to a failure", () => {
    const sp = (parseSeatPoolSpec({ max: 8, min: 0, cwd_template: "/w/{slot}", cooldown_s: 0, idle_grace_s: 60, dispatch_after_s: 60,
      dispatch_to_parked: { busy_processes: PROGRAMS } }) as { spec: SeatPoolSpec }).spec;
    const view = (slotId: number, over: Partial<SeatPoolSlotView>): SeatPoolSlotView => ({
      slotId, logicalId: `dev.impl-s${slotId}`, sessionName: `dev-impl-s${slotId}@r`, nodeId: `node-${slotId}`, phase: "live", adopted: false,
      nodeExists: true, openRows: 0, heldMessages: 0, idleSince: new Date(NOW - 3_600_000).toISOString(), cause: "idle", atPrompt: true,
      backgroundWork: false, ...over,
    });
    const state = (slots: SeatPoolSlotView[]): SeatPoolState => ({
      model: { rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model", sessionName: "dev-impl@r" },
      modelCause: "working", restoring: false, adoptable: [], demand: null, lastScaleAt: null, slots,
      pending: [{ qitemId: "q1", destination: "dev-impl@r", waitingSince: new Date(NOW - 120_000).toISOString(), tags: ["tarefa-nova"] }],
    });
    const starting = view(2, { phase: "added", idleSince: null, cause: "idle" });
    const flipped = planSeatPool(sp, state([view(1, { busyProcess: "environment of pid 9 not read: x" }), starting]), NOW, 120_000);
    expect(flipped.intents.some((i) => i.kind === "remove")).toBe(false);
    expect(flipped.intents).toContainEqual({ kind: "launch", slotId: 2 });
    expect(flipped).toMatchObject({ target: 2, current: 2 });
    // A failure of one seat only (its pane gone): the pool adds a seat, which takes the row.
    const single = planSeatPool(sp, state([view(1, { busyProcess: "pane not found" })]), NOW, 120_000);
    expect(single).toMatchObject({ reason: "scale_up", target: 2, current: 1 });
  });

  it("S9: a read waits its turn while the limit of environment reads is running (a concurrent read is served once they end)", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const rows: BusyProcessRow[] = [
      { pid: 100, ppid: 1, command: "/bin/bash -l" }, { pid: 101, ppid: 100, command: "claude" },
      ...[501, 502, 503, 504].map((pid) => ({ pid, ppid: 1, command: "suite-flock nice x" })),
    ];
    let running = 0;
    let most = 0;
    const slow = async (pid: number): Promise<string | null> => {
      running += 1; most = Math.max(most, running);
      await new Promise((resolve) => setTimeout(resolve, 30));
      running -= 1;
      return pid === 501 ? "%24" : null;
    };
    const base = { ...deps, processes: async () => rows, processPane: slow };
    const [a, b] = await Promise.all([
      readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, base, { timeoutMs: 2000 }),
      readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, base, { timeoutMs: 2000 }),
    ]);
    expect(most).toBe(SEAT_POOL_BUSY_ENV_CONCURRENCY);
    expect(a.get("qa-s7@r")).toBe("suite-flock (pid 501, TMUX_PANE %24)");
    expect(b.get("qa-s7@r")).toBe("suite-flock (pid 501, TMUX_PANE %24)");
  });

  it("S9: environment reads past the deadline of one read still count against the next read: no new read starts while the limit is pending", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const rows: BusyProcessRow[] = [
      { pid: 100, ppid: 1, command: "/bin/bash -l" }, { pid: 101, ppid: 100, command: "claude" },
      ...[501, 502, 503, 504, 505].map((pid) => ({ pid, ppid: 1, command: "suite-flock nice x" })),
    ];
    let calls = 0;
    const hang = (): Promise<string | null> => { calls += 1; return new Promise<never>(() => {}); };
    const base = { ...deps, processes: async () => rows, processPane: hang };
    const first = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, base, { timeoutMs: 20 });
    expect(first.get("qa-s7@r")).toMatch(/not read/);
    expect(calls).toBe(SEAT_POOL_BUSY_ENV_CONCURRENCY);
    const second = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, base, { timeoutMs: 20 });
    expect(calls).toBe(SEAT_POOL_BUSY_ENV_CONCURRENCY);
    expect(second.get("qa-s7@r")).toMatch(/still pending/);
    expect(seatPoolBusyFromProcess(second.get("qa-s7@r"))).toBe(false);
  });

  it("S10: the occupant of a shell pane is its runtime (claude), not a newer sibling the shell started; an orphan of this runtime holds", async () => {
    const rows: BusyProcessRow[] = [
      { pid: 100, ppid: 1, command: "/bin/bash -l", startedAt: agoS(90_000) },
      { pid: 101, ppid: 100, command: "/home/bruno/.local/bin/claude --dangerously-skip-permissions", startedAt: agoS(7200) },
      { pid: 102, ppid: 100, command: "sleep 100", startedAt: agoS(60) },
      { pid: 500, ppid: 1, command: "bash /home/bruno/.local/bin/suite-flock nice -n 10 python3 -I t.py", startedAt: agoS(3600) },
    ];
    expect(seatPoolPaneOccupantMs(rows, 100)).toBe(Date.parse(agoS(7200)));
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => rows });
    expect(read.get("qa-s7@r")).toBe("suite-flock (pid 500, TMUX_PANE %24)");
    // With no runtime among the children, the newest child, as before.
    expect(seatPoolPaneOccupantMs(rows.filter((r) => r.pid !== 101), 100)).toBe(Date.parse(agoS(60)));
    // Claude run by Node from the npm package (Sol on the delta) and the native installer's versioned file are runtimes.
    for (const command of ["node /usr/lib/node_modules/@anthropic-ai/claude-code/cli.js --resume x",
      "/home/u/.local/share/claude/versions/2.1.3 --dangerously-skip-permissions", "codex resume abc"]) {
      const viaNode = rows.map((r) => (r.pid === 101 ? { ...r, command } : r));
      expect(seatPoolPaneOccupantMs(viaNode, 100)).toBe(Date.parse(agoS(7200)));
    }
  });

  it("S11: the boundaries of the ceiling and of the occupant (each comparison, both sides)", async () => {
    const at = async (occupantAgoS: number, orphanAgoS: number, ceiling: number) => {
      const { deps } = scene({ occupantAgoS, orphanAgoS });
      return (await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, deps, { orphanMaxAgeSeconds: ceiling })).get("qa-s7@r");
    };
    expect(await at(50_000, 3600, 3600)).toMatch(/^suite-flock/); // exactly the ceiling: held
    expect(await at(50_000, 3601, 3600)).toBeNull(); // one second older: free
    expect(await at(3600, 3600, 43_200)).toMatch(/^suite-flock/); // started with the occupant: held
    expect(await at(3600, 3601, 43_200)).toBeNull(); // one second before the occupant: free
    expect(await at(3599, 3600, 43_200)).toBeNull(); // the occupant one second newer: free
  });
});

describe("T-007205: S12 (half of libuv's pool), S13 (the turn passed on past a deadline), S14 (R + 1) and S15 ([comm])", () => {
  it("S12: the environment reads never take more than half of libuv's default thread pool, even with every read stuck", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const rows: BusyProcessRow[] = [
      { pid: 100, ppid: 1, command: "/bin/bash -l" }, { pid: 101, ppid: 100, command: "claude" },
      ...[601, 602, 603, 604, 605].map((pid) => ({ pid, ppid: 1, command: "suite-flock nice x" })),
    ];
    let calls = 0;
    const stuck = (): Promise<string | null> => { calls += 1; return new Promise<never>(() => {}); };
    const base = { ...deps, processes: async () => rows, processPane: stuck };
    await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, base, { timeoutMs: 20 });
    await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, base, { timeoutMs: 20 });
    expect(calls).toBe(SEAT_POOL_BUSY_ENV_CONCURRENCY);
    expect(calls * 2).toBeLessThanOrEqual(LIBUV_DEFAULT_THREADPOOL_SIZE);
  });

  it("S13: a waiter woken past its own deadline hands its turn on, so the next waiter reads now instead of at its deadline", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const holders: BusyProcessRow[] = [701, 702].map((pid) => ({ pid, ppid: 1, command: "suite-flock nice x" }));
    const base: BusyProcessRow[] = [{ pid: 100, ppid: 1, command: "/bin/bash -l" }, { pid: 101, ppid: 100, command: "claude" }];
    const finish: Array<() => void> = [];
    const pane = (pid: number): Promise<string | null> => {
      if (pid === 701 || pid === 702) return new Promise((resolve) => { finish.push(() => resolve(null)); });
      return Promise.resolve(pid === 800 ? "%24" : null);
    };
    const at = (rows: BusyProcessRow[], timeoutMs: number) =>
      readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => [...base, ...rows], processPane: pane }, { timeoutMs });
    // The two holders take every turn; then a short call waits first in line and a long one behind it.
    const holding = at(holders, 10_000);
    await vi.waitFor(() => expect(finish).toHaveLength(SEAT_POOL_BUSY_ENV_CONCURRENCY));
    const short = at([{ pid: 799, ppid: 1, command: "suite-flock nice y" }], 100);
    await new Promise((resolve) => setTimeout(resolve, 10));
    const long = at([{ pid: 800, ppid: 1, command: "suite-flock nice z" }], 5_000);
    await new Promise((resolve) => setTimeout(resolve, 10));
    // A holder ends just as the short call's deadline has passed, before its own timer runs: its turn goes to the short
    // call, which can no longer read and must hand the turn on.
    const realNow = Date.now.bind(Date);
    const clock = vi.spyOn(Date, "now").mockImplementation(() => realNow() + 1_000);
    const started = realNow();
    let read: Map<string, string | null>;
    try {
      finish.shift()!();
      read = await long;
    } finally {
      clock.mockRestore();
    }
    expect(read.get("qa-s7@r")).toBe("suite-flock (pid 800, TMUX_PANE %24)");
    expect(realNow() - started).toBeLessThan(1_000);
    expect((await short).get("qa-s7@r")).toMatch(/not read/);
    for (const end of finish) end();
    await holding;
  });

  it("N13: a waiter whose wait runs out in the same tick its turn is handed over does not drop the turn (fake timers)", async () => {
    vi.useFakeTimers();
    try {
      const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
      const base: BusyProcessRow[] = [{ pid: 100, ppid: 1, command: "/bin/bash -l" }, { pid: 101, ppid: 100, command: "claude" }];
      const started: number[] = [];
      const pane = (pid: number): Promise<string | null> => {
        started.push(pid);
        if (pid === 701) return new Promise((resolve) => setTimeout(() => resolve(null), 100));
        if (pid === 702) return new Promise<never>(() => {});
        return Promise.resolve(pid === 800 ? "%24" : null);
      };
      const at = (pids: number[], timeoutMs: number) => readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS,
        { ...deps, processes: async () => [...base, ...pids.map((pid) => ({ pid, ppid: 1, command: "suite-flock nice x" }))], processPane: pane },
        { timeoutMs });
      const flush = async () => { for (let k = 0; k < 20; k += 1) await Promise.resolve(); };
      void at([701, 702], 10_000);
      await flush();
      const w = at([799], 100);
      await flush();
      const x = at([800], 60_000);
      await flush();
      // 701's read ends and w's wait runs out in the same synchronous advance: the turn 701 frees goes to w, whose own
      // timer then rejects; w must not drop it, and x reads.
      vi.advanceTimersByTime(100);
      await flush();
      expect(started).toContain(800);
      expect((await x).get("qa-s7@r")).toBe("suite-flock (pid 800, TMUX_PANE %24)");
      expect((await w).get("qa-s7@r")).toMatch(/not read/);
    } finally {
      vi.useRealTimers();
    }
  });

  it("S14: with every read failing and more rows waiting than seats (R > N + 1), the pool asks R + 1 seats, up to max, with nothing dispatched", () => {
    const sp = (parseSeatPoolSpec({ max: 20, min: 0, cwd_template: "/w/{slot}", cooldown_s: 0, idle_grace_s: 60, dispatch_after_s: 60,
      dispatch_to_parked: { busy_processes: PROGRAMS } }) as { spec: SeatPoolSpec }).spec;
    const failed = "environment of pid 500 not read: the environments took over 5000 ms";
    const state = (seats: number, rows: number): SeatPoolState => ({
      model: { rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model", sessionName: "dev-impl@r" },
      modelCause: "working", restoring: false, adoptable: [], demand: null, lastScaleAt: null,
      slots: Array.from({ length: seats }, (_, i) => ({
        slotId: i + 1, logicalId: `dev.impl-s${i + 1}`, sessionName: `dev-impl-s${i + 1}@r`, nodeId: `node-${i + 1}`, phase: "live" as const,
        adopted: false, nodeExists: true, openRows: 0, heldMessages: 0, idleSince: new Date(NOW - 3_600_000).toISOString(), cause: "idle" as const,
        atPrompt: true, backgroundWork: false, busyProcess: failed,
      })),
      pending: Array.from({ length: rows }, (_, i) => ({
        qitemId: `q${i}`, destination: "dev-impl@r", waitingSince: new Date(NOW - 120_000).toISOString(), tags: ["tarefa-nova"],
      })),
    });
    const eight = planSeatPool(sp, state(2, 8), NOW, 120_000);
    expect(eight.intents.filter((i) => i.kind === "dispatch" || i.kind === "drain")).toEqual([]);
    expect(eight).toMatchObject({ reason: "scale_up", target: 9, current: 2 });
    const nineteen = planSeatPool(sp, state(2, 19), NOW, 120_000);
    expect(nineteen.intents.filter((i) => i.kind === "dispatch" || i.kind === "drain")).toEqual([]);
    expect(nineteen).toMatchObject({ reason: "scale_up", target: 20, current: 2 });
    // A seat holding a row (O = 1) adds itself: rows + occupied + 1 (R + O + 1), not R + 1.
    const withRow = state(3, 4);
    withRow.slots[0] = { ...withRow.slots[0]!, openRows: 1, idleSince: null, cause: "working", busyProcess: null };
    expect(planSeatPool(sp, withRow, NOW, 120_000)).toMatchObject({ reason: "scale_up", target: 6, current: 3 });
    // Once grown, the pool holds: the seats added fail the same read and ask nothing more.
    expect(planSeatPool(sp, state(9, 8), NOW, 120_000)).toMatchObject({ reason: "steady", target: 9, current: 9 });
  });

  it("S15: a process whose command line could not be read ([comm], as ps shows it on Linux): its own name matches, an interpreter's name under the pane is a failed read", async () => {
    expect(seatPoolBusyCommand("[congelar-eval]", PROGRAMS)).toBe("congelar-eval");
    expect(seatPoolBusyCommand("[suite-flock]", PROGRAMS)).toBe("suite-flock");
    // The kernel keeps 15 bytes of the name: a longer program matches by its first 15.
    expect(seatPoolBusyCommand("[hillclimb-avali]", ["hillclimb-avaliador"])).toBe("hillclimb-avaliador");
    expect(seatPoolBusyCommand("[hillclimb]", ["hillclimb-avaliador"])).toBeNull();
    expect(seatPoolBusyCommand("[congelar-eval] <defunct>", PROGRAMS)).toBeNull();
    expect(seatPoolBusyCommand("[kworker/0:1]", PROGRAMS)).toBeNull();
    expect(seatPoolBusyCommand("[bash]", PROGRAMS)).toBeNull();
    // On the VM (measured by the lens of T-007205), suite-flock is `#!/usr/bin/env bash`: its task name is `bash`, so with
    // its command line unreadable it is `[bash]`. Under the seat's pane, that is a failed read: held, not an occupant.
    const { rows, deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const noOrphan = rows.filter((r) => r.pid !== 500);
    const with_ = (row: BusyProcessRow) => ({ ...deps, processes: async () => [...noOrphan, row] });
    const unreadable = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, with_({ pid: 501, ppid: 101, command: "[bash]", startedAt: agoS(60) }));
    expect(unreadable.get("qa-s7@r")).toBe("command line of pid 501 not read ([bash])");
    expect(seatPoolBusyFromProcess(unreadable.get("qa-s7@r"))).toBe(false);
    const direct = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, with_({ pid: 501, ppid: 101, command: "[congelar-eval]", startedAt: agoS(60) }));
    expect(direct.get("qa-s7@r")).toBe("congelar-eval (pid 501)");
    const zombie = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, with_({ pid: 501, ppid: 101, command: "[bash] <defunct>", startedAt: agoS(60) }));
    expect(zombie.get("qa-s7@r")).toBeNull();
    // A real process under the pane still wins over an unreadable one.
    const both = [...noOrphan, { pid: 501, ppid: 101, command: "[bash]" }, { pid: 502, ppid: 101, command: "suite-flock nice -n 10 x" }];
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => both })).get("qa-s7@r"))
      .toBe("suite-flock (pid 502)");
    expect(seatPoolUnreadInTree(both, 100, PROGRAMS)).toBe("command line of pid 501 not read ([bash])");
    // An orphan (outside the pane) whose own name matches still holds by its TMUX_PANE.
    const orphan = rows.map((r) => (r.pid === 500 ? { ...r, command: "[congelar-eval]" } : r));
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => orphan })).get("qa-s7@r"))
      .toBe("congelar-eval (pid 500, TMUX_PANE %24)");
  });
});

describe("T-007477: S16 (an orphan [bash]/[sh] whose command line could not be read, outside the pane)", () => {
  // The suite-flock reparented to 1 (nohup, setsid) with its command line unreadable is `[bash]`: the environment of a
  // young orphan interpreter is read too, inside the same ceiling of reads and the same deadline (S12), and its
  // TMUX_PANE holds that seat as a failed read (held, not an occupant).
  const unreadOrphan = (cmd = "[bash]", ageS = 3600) => {
    const { rows, deps } = scene({ occupantAgoS: 7200, orphanAgoS: ageS });
    return { rows: rows.map((r) => (r.pid === 500 ? { ...r, command: cmd } : r)), deps };
  };

  it("a young orphan [bash] with the seat's TMUX_PANE holds it as a failed read, not as an occupant", async () => {
    for (const cmd of ["[bash]", "[sh]", "[dash]", "[zsh]"]) {
      const { rows, deps } = unreadOrphan(cmd);
      const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => rows });
      expect(read.get("qa-s7@r")).toBe(`command line of pid 500 not read (${cmd}, TMUX_PANE %24)`);
      expect(seatPoolBusyFromProcess(read.get("qa-s7@r"))).toBe(false);
    }
  });

  it("an orphan [bash] of another pane, with no TMUX_PANE, older than the ceiling or before the occupant holds nothing", async () => {
    const { rows, deps } = unreadOrphan();
    const other = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => rows, processPane: async (pid) => (pid === 500 ? "%31" : null) });
    expect(other.get("qa-s7@r")).toBeNull();
    const none = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => rows, processPane: async () => null });
    expect(none.get("qa-s7@r")).toBeNull();
    const old = unreadOrphan("[bash]", 50_000);
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...old.deps, processes: async () => old.rows })).get("qa-s7@r")).toBeNull();
    const before = unreadOrphan("[bash]", 7300); // started before the occupant (7200 s ago)
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...before.deps, processes: async () => before.rows })).get("qa-s7@r")).toBeNull();
  });

  it("an orphan [bash] whose environment can't be read holds no seat (it may be no seat's work), unlike a matching orphan", async () => {
    const { rows, deps } = unreadOrphan();
    const failing = { ...deps, processes: async () => rows, processPane: async () => { throw Object.assign(new Error("EIO"), { code: "EIO" }); } };
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, failing)).get("qa-s7@r")).toBeNull();
    // the matching orphan with an unreadable environment still holds every seat it could belong to (S1)
    const { rows: named } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...failing, processes: async () => named })).get("qa-s7@r"))
      .toMatch(/^environment of pid 500 not read/);
  });

  it("a named orphan wins over an unread one, and the unread orphans share the S12 ceiling of reads", async () => {
    const { rows, deps } = unreadOrphan();
    const both = [...rows, { pid: 502, ppid: 1, command: "suite-flock nice -n 10 y", startedAt: agoS(60) }];
    const pane = async (pid: number) => (pid === 500 || pid === 502 ? "%24" : null);
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => both, processPane: pane })).get("qa-s7@r"))
      .toBe("suite-flock (pid 502, TMUX_PANE %24)");
    let calls = 0;
    const stuck = (): Promise<string | null> => { calls += 1; return new Promise<never>(() => {}); };
    const many = [...rows.filter((r) => r.pid !== 500), ...[601, 602, 603, 604].map((pid) => ({ pid, ppid: 1, command: "[bash]", startedAt: agoS(60) }))];
    await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => many, processPane: stuck }, { timeoutMs: 20 });
    expect(calls).toBe(SEAT_POOL_BUSY_ENV_CONCURRENCY);
  });

  it("the named orphans are read before the unread shells, whatever their pids", async () => {
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const order: number[] = [];
    const rows: BusyProcessRow[] = [
      { pid: 100, ppid: 1, command: "/bin/bash -l", startedAt: agoS(90_000) },
      { pid: 101, ppid: 100, command: "claude", startedAt: agoS(7200) },
      ...[300, 301, 302].map((pid) => ({ pid, ppid: 1, command: "[bash]", startedAt: agoS(60) })),
      { pid: 9000, ppid: 1, command: "bash /x/suite-flock nice -n 10 y", startedAt: agoS(60) },
    ];
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => rows,
      processPane: async (pid) => { order.push(pid); return pid === 9000 ? "%24" : null; } });
    expect(order[0]).toBe(9000);
    expect(read.get("qa-s7@r")).toBe("suite-flock (pid 9000, TMUX_PANE %24)");
  });

  it("an orphan of a program that is not an interpreter ([kworker/0:1], [sleep]) has no environment read", async () => {
    const seen: number[] = [];
    const { deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const rows: BusyProcessRow[] = [
      { pid: 100, ppid: 1, command: "/bin/bash -l", startedAt: agoS(90_000) },
      { pid: 101, ppid: 100, command: "claude", startedAt: agoS(7200) },
      { pid: 700, ppid: 2, command: "[kworker/0:1]", startedAt: agoS(60) },
      { pid: 701, ppid: 1, command: "[sleep]", startedAt: agoS(60) },
      { pid: 702, ppid: 1, command: "[bash] <defunct>", startedAt: agoS(60) },
    ];
    await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, { ...deps, processes: async () => rows, processPane: async (pid) => { seen.push(pid); return null; } });
    expect(seen).toEqual([]);
  });
});
