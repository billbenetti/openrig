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

import { describe, it, expect, vi } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { TmuxAdapter } from "../src/adapters/tmux.js";
import { readSeatPoolBusyCandidates } from "../src/domain/seat-pool-executor.js";
import {
  readSeatPoolBusySessions,
  readProcTmuxPane,
  SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S,
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
    // The same orphan under a higher ceiling (the default, 6 h) still holds it.
    expect((await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, deps)).get("qa-s7@r")).toBe("suite-flock (pid 500, TMUX_PANE %24)");
    expect(SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S).toBe(21_600);
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

  it("busy_orphan_max_age_s: read only with busy_processes, default 21600, bounds 60..172800; {} reads as before", () => {
    const parse = (inner: Record<string, unknown>) => parseSeatPoolSpec({ max: 2, cwd_template: "/w/{slot}", dispatch_to_parked: inner });
    const specOf = (inner: Record<string, unknown>) => (parse(inner) as { spec: SeatPoolSpec }).spec.dispatchToParked;
    expect(specOf({})).not.toHaveProperty("busyOrphanMaxAgeSeconds");
    expect(specOf({ refuse_gates: ["congelar"] })).not.toHaveProperty("busyOrphanMaxAgeSeconds");
    expect(specOf({ busy_processes: PROGRAMS })).toMatchObject({ busyProcesses: PROGRAMS, busyOrphanMaxAgeSeconds: 21_600 });
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

  it("environments that never answer: at most 4 pending, no new read after the one deadline, and the seat held (RC delta)", async () => {
    const { rows, deps } = scene({ occupantAgoS: 7200, orphanAgoS: 3600 });
    const orphans: BusyProcessRow[] = Array.from({ length: 10 }, (_, i) => ({ pid: 600 + i, ppid: 1, command: "bash /x/suite-flock true", startedAt: agoS(60) }));
    let started = 0;
    const t0 = Date.now();
    const read = await readSeatPoolBusySessions(["qa-s7@r"], PROGRAMS, {
      ...deps, processes: async () => [...rows.filter((r) => r.pid !== 500), ...orphans],
      processPane: () => { started += 1; return new Promise<never>(() => {}); },
    }, { timeoutMs: 40 });
    expect(started).toBe(4);
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

  it("listAllPanePids: one `tmux list-panes -a` for the whole server, parsed by tab; no server reads as none", async () => {
    const seen: string[][] = [];
    const out = "%0\t2620\t11\tadvisor-lead@kernel\n%310\t2214571\t11\tbuild-dev-1-s6@fabrica\n%311\t2214600\t10\tbuild-dev-1-s6@fabrica\nlixo\n";
    const adapter = new TmuxAdapter(async () => "", undefined, async (argv) => { seen.push(argv); return out; });
    expect(await adapter.listAllPanePids()).toEqual([
      { paneId: "%0", panePid: 2620, session: "advisor-lead@kernel", active: true },
      { paneId: "%310", panePid: 2214571, session: "build-dev-1-s6@fabrica", active: true },
      { paneId: "%311", panePid: 2214600, session: "build-dev-1-s6@fabrica", active: false },
    ]);
    expect(seen).toEqual([["tmux", "list-panes", "-a", "-F", "#{pane_id}\t#{pane_pid}\t#{window_active}#{pane_active}\t#{session_name}"]]);
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
