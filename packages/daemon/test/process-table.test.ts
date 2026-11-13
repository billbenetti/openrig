import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { invalidateProcessTable, processTableReadsForTest, readProcessTable, readProcTable } from "../src/domain/process-table.js";
import { claudeCommandSessionToken, listNativeProcesses, processArgv, verifyCodexPaneProcess, verifyPaneIdleShell } from "../src/domain/native-process-lineage.js";
import { defaultListProcessesStrict } from "../src/domain/resume-metadata-refresher.js";
import { restoreProc, serveProcRoot, writeFakeProc } from "./helpers/fake-proc.js";
import { createFullTestDb } from "./helpers/test-app.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";
import { SeatLifecycleService } from "../src/domain/seat-lifecycle-service.js";
import { defaultListProcesses } from "../src/adapters/codex-runtime-adapter.js";

let scratch: string | undefined;
afterEach(() => {
  restoreProc();
  vi.unstubAllEnvs();
  if (scratch) fs.rmSync(scratch, { recursive: true, force: true });
  scratch = undefined;
});

/** A /proc of its own: `btime`, then each pid's stat line and cmdline bytes. */
function fakeProc(btime: number, processes: Record<number, { stat: string | Buffer; cmdline: string }>): string {
  scratch = fs.mkdtempSync(path.join(os.tmpdir(), "process-table-"));
  fs.writeFileSync(path.join(scratch, "stat"), `cpu  1 2 3\nintr 0\nbtime ${btime}\nprocesses 9\n`);
  fs.mkdirSync(path.join(scratch, "self"));
  for (const [pid, { stat, cmdline }] of Object.entries(processes)) {
    fs.mkdirSync(path.join(scratch, pid));
    fs.writeFileSync(path.join(scratch, pid, "stat"), stat);
    fs.writeFileSync(path.join(scratch, pid, "cmdline"), cmdline);
  }
  return scratch;
}

/** A stat line: the fields after comm, from state to the start time (field 22, in ticks). */
const stat = (pid: number, comm: string, state: string, ppid: number, pgrp: number, tpgid: number, ticks: number) =>
  `${pid} (${comm}) ${state} ${ppid} ${pgrp} ${pgrp} 34816 ${tpgid} 4194560 1 0 0 0 0 0 0 0 20 0 1 0 ${ticks} 1000 200 18446744073709551615\n`;

describe("readProcTable", () => {
  it("carries exact Codex argv from the same fake /proc sample, including a spaced value and empty arg", async () => {
    const token = "00000000-0000-7000-8000-000000000001";
    const root = fakeProc(0, {
      41: { stat: stat(41, "codex", "S", 1, 41, 41, 0),
        cmdline: `codex\0-p\0managed work\0resume\0${token}\0\0` },
    });
    serveProcRoot(root);
    const [row] = await listNativeProcesses();
    expect(row?.argv).toEqual(["codex", "-p", "managed work", "resume", token, ""]);
    expect((await verifyCodexPaneProcess({ target: "%1", tmux: { getPanePid: async () => 41 },
      expectedToken: token, requireResume: true }))?.process.pid).toBe(41);
    fs.writeFileSync(path.join(root, "41", "cmdline"), `codex resume ${token}\0`);
    expect((await listNativeProcesses())[0]?.argv).toEqual([`codex resume ${token}`]);
    expect(await verifyCodexPaneProcess({ target: "%1", tmux: { getPanePid: async () => 41 },
      expectedToken: token, requireResume: true })).toBeNull();
  });
  it("gives the rows ps prints: brackets for a process without a command line, ps's escapes, lstart in local time", async () => {
    vi.stubEnv("TZ", "UTC");
    const btime = Date.UTC(2026, 9, 6, 9, 0, 0) / 1000;
    const root = fakeProc(btime, {
      1: { stat: stat(1, "systemd", "S", 0, 1, -1, 1234), cmdline: "/sbin/init\0splash\0" },
      2: { stat: stat(2, "kworker/u32:4-events_unbound", "I", 0, 0, -1, 0), cmdline: "" },
      40: { stat: stat(40, "tmux: server (x)", "S", 1, 40, -1, 30_150), cmdline: "tmux\0new-session\0-d\0" },
      41: { stat: stat(41, "claude", "S", 40, 41, 41, 360_000), cmdline: "claude\0--resume\0tok-1\0a\tb\0x\ny\0é\0\0" },
      42: { stat: stat(42, "sh", "Z", 40, 41, 41, 360_099), cmdline: "" },
    });
    expect((await readProcTable(root)).sort((a, b) => a.pid - b.pid)).toEqual([
      { pid: 1, ppid: 0, pgid: 1, tpgid: -1, executableName: "systemd", startedAt: "Tue Oct  6 09:00:12 2026", command: "/sbin/init splash" },
      { pid: 2, ppid: 0, pgid: 0, tpgid: -1, executableName: "kworker/u32:4-e", startedAt: "Tue Oct  6 09:00:00 2026", command: "[kworker/u32:4-events_unbound]" },
      { pid: 40, ppid: 1, pgid: 40, tpgid: -1, executableName: "tmux: server (x", startedAt: "Tue Oct  6 09:05:01 2026", command: "tmux new-session -d" },
      { pid: 41, ppid: 40, pgid: 41, tpgid: 41, executableName: "claude", startedAt: "Tue Oct  6 10:00:00 2026", command: "claude --resume tok-1 a.b x y ??" },
      { pid: 42, ppid: 40, pgid: 41, tpgid: 41, executableName: "sh", startedAt: "Tue Oct  6 10:00:00 2026", command: "[sh] <defunct>" },
    ]);
  });

  it("cuts a long name at 15 bytes, as the kernel cuts it, whatever bytes it holds", async () => {
    // A worker's name with a byte that is no UTF-8 at the 4th place, then a two-byte é across the 15th byte.
    const comm = Buffer.concat([Buffer.from("abc"), Buffer.from([0xff]), Buffer.from("defghijklméxyz")]);
    const root = fakeProc(0, {
      7: { stat: Buffer.concat([Buffer.from("7 ("), comm, Buffer.from(stat(7, "", "S", 2, 0, -1, 0).slice("7 (".length))]), cmdline: "" },
    });
    const [row] = await readProcTable(root);
    expect(row!.executableName).toBe("abc?defghijklm?");
    expect(row!.command).toBe("[abc?defghijklm??xyz]");
  });

  it("prints each byte of a non-ASCII character as ps does in the C locale, so it never splits an argument", async () => {
    // One argument holding a no-break space (U+00A0, bytes C2 A0) between an option and its value.
    const root = fakeProc(0, { 41: { stat: stat(41, "claude", "S", 1, 41, 41, 0), cmdline: "claude\0--resume tok-1\0" } });
    const [row] = await readProcTable(root);
    expect(row!.command).toBe("claude --resume??tok-1");
    expect(processArgv(row!.command)).toEqual(["claude", "--resume??tok-1"]);
    expect(claudeCommandSessionToken(row!.command)).toBeNull();
  });

  it("reads off the event loop: a command line that waits holds no timer, and its row comes once it is read", async () => {
    const root = fakeProc(0, {
      7: { stat: stat(7, "sleep", "S", 1, 7, -1, 0), cmdline: "sleep\0" },
      8: { stat: stat(8, "node", "S", 1, 8, -1, 0), cmdline: "" },
    });
    // A FIFO's read waits until it is written, as a cmdline's waits on its process's memory lock.
    fs.rmSync(path.join(root, "8", "cmdline"));
    execFileSync("mkfifo", [path.join(root, "8", "cmdline")]);
    let settled = false;
    const pending = readProcTable(root).finally(() => { settled = true; });
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(settled).toBe(false);
    const fifo = await fs.promises.open(path.join(root, "8", "cmdline"), "w");
    await fifo.write("node\0server.js\0");
    await fifo.close();
    expect((await pending).map((row) => row.command).sort()).toEqual(["node server.js", "sleep"]);
  });

  it("keeps a process whose command line cannot be read by its name in brackets, and drops one that is gone", async () => {
    const root = fakeProc(0, {
      7: { stat: stat(7, "node", "S", 1, 7, 7, 0), cmdline: "" },
      8: { stat: stat(8, "node", "S", 1, 8, 8, 0), cmdline: "" },
    });
    // A command line that cannot be read, short of the process being gone (EISDIR here, as root reads past a permission).
    fs.rmSync(path.join(root, "7", "cmdline"));
    fs.mkdirSync(path.join(root, "7", "cmdline"));
    // A process that exited between its stat and its command line.
    fs.rmSync(path.join(root, "8", "cmdline"));
    expect(await readProcTable(root)).toEqual([
      { pid: 7, ppid: 1, pgid: 7, tpgid: 7, executableName: "node", startedAt: expect.any(String), command: "[node]" },
    ]);
  });

  it("keeps a pane's shell from reading idle when its foreground child's command line cannot be read", async () => {
    const started = "Sat Jan  1 12:00:00 2000";
    const root = writeFakeProc([
      { pid: 101, ppid: 1, pgid: 101, tpgid: 101, name: "zsh", startedAt: started, command: "-zsh" },
      { pid: 102, ppid: 101, pgid: 101, tpgid: 101, name: "vim", startedAt: started, command: "vim notes" },
    ]);
    serveProcRoot(root);
    const tmux = { getPanePid: async () => 101 };
    fs.rmSync(path.join(root, "102", "cmdline"));
    fs.mkdirSync(path.join(root, "102", "cmdline"));
    expect(await verifyPaneIdleShell({ target: "s:0.0", tmux })).toBe(false);
    // Gone, the child no longer holds the foreground.
    fs.rmSync(path.join(root, "102"), { recursive: true });
    expect(await verifyPaneIdleShell({ target: "s:0.0", tmux })).toBe(true);
  });

  it("leaves out a process whose stat is gone, and fails the whole read when a stat cannot be read for another reason", async () => {
    const root = fakeProc(0, {
      7: { stat: stat(7, "node", "S", 1, 7, 7, 0), cmdline: "node\0" },
      8: { stat: stat(8, "node", "S", 1, 8, 8, 0), cmdline: "node\0" },
    });
    // Exited between the listing and its stat.
    fs.rmSync(path.join(root, "8", "stat"));
    expect((await readProcTable(root)).map((row) => row.pid)).toEqual([7]);
    // A stat that cannot be read while its process runs (EISDIR here, as EMFILE would be): a shorter table would look whole.
    fs.mkdirSync(path.join(root, "8", "stat"));
    await expect(readProcTable(root)).rejects.toThrow(/8\/stat: EISDIR/);
  });

  it("rejects when there is no process table to read", async () => {
    await expect(readProcTable(path.join(os.tmpdir(), "openrig-no-proc-here"))).rejects.toThrow();
  });

  it.runIf(process.platform === "linux")("reads this process as ps does, start time included", async () => {
    const rows = await readProcTable();
    const self = rows.find((row) => row.pid === process.pid)!;
    const ps = (columns: string) => execFileSync("ps", ["-o", columns, "-p", String(process.pid)], { encoding: "utf8", env: { ...process.env, LC_ALL: "C" } }).trim();
    expect(self).toMatchObject({ ppid: Number(ps("ppid=")), pgid: Number(ps("pgid=")), tpgid: Number(ps("tpgid=")), executableName: ps("ucomm="), command: ps("args=") });
    expect(self.startedAt).toBe(ps("lstart="));
  });
});

describe("readProcessTable", () => {
  it("shares one read among the callers that ask while it runs, and reads again after", async () => {
    const first = readProcessTable();
    expect(readProcessTable()).toBe(first);
    await first;
    const second = readProcessTable();
    expect(second).not.toBe(first);
    await second;
  });

  describe("the short cache (upstream #3146)", () => {
    const shell = (pid: number, child?: number) => fakeProc(0, {
      [pid]: { stat: stat(pid, "bash", "S", 1, pid, child ?? pid, 0), cmdline: "-bash\0" },
      ...(child ? { [child]: { stat: stat(child, "sleep", "S", pid, child, child, 0), cmdline: "sleep\x00600\x00" } } : {}),
    });

    it("serves one read to the callers of the same cycle, and reads again past the TTL", async () => {
      vi.stubEnv("OPENRIG_PROCESS_TABLE_TTL_MS", "60000");
      serveProcRoot(shell(51));
      const before = processTableReadsForTest();
      const first = await readProcessTable();
      for (let i = 0; i < 9; i++) expect(await readProcessTable()).toBe(first);
      // 10 asks, 1 walk of /proc (before the cache: 10)
      expect(processTableReadsForTest() - before).toBe(1);
    });

    it("reads again once the cached table is older than the TTL (the cache on)", async () => {
      vi.stubEnv("OPENRIG_PROCESS_TABLE_TTL_MS", "1000");
      serveProcRoot(shell(57));
      let clock = 5_000_000;
      const now = vi.spyOn(performance, "now").mockImplementation(() => clock);
      try {
        const before = processTableReadsForTest();
        const first = await readProcessTable();
        clock += 999;
        expect(await readProcessTable()).toBe(first);
        clock += 2;
        expect(await readProcessTable()).not.toBe(first);
        expect(processTableReadsForTest() - before).toBe(2);
      } finally {
        now.mockRestore();
      }
    });

    it("counts its reads: 10 back-to-back listers cost 10 walks without the cache and 1 with it", async () => {
      serveProcRoot(shell(52));
      const walks = async (ttl: string) => {
        vi.stubEnv("OPENRIG_PROCESS_TABLE_TTL_MS", ttl);
        invalidateProcessTable();
        const before = processTableReadsForTest();
        for (let i = 0; i < 10; i++) await listNativeProcesses();
        return processTableReadsForTest() - before;
      };
      expect(await walks("0")).toBe(10);
      expect(await walks("60000")).toBe(1);
    });

    it("after invalidateProcessTable() the next caller never gets the cached table", async () => {
      vi.stubEnv("OPENRIG_PROCESS_TABLE_TTL_MS", "60000");
      const root = shell(53);
      serveProcRoot(root);
      const cached = await readProcessTable();
      fs.rmSync(path.join(root, "53"), { recursive: true });
      expect(await readProcessTable()).toBe(cached); // still the cached table, with 53
      invalidateProcessTable();
      expect((await readProcessTable()).some((row) => row.pid === 53)).toBe(false);
    });

    it("a seat stop, clean or launch drops the cached table, however it ends (a refusal included)", async () => {
      vi.stubEnv("OPENRIG_PROCESS_TABLE_TTL_MS", "60000");
      serveProcRoot(shell(56));
      const db = createFullTestDb();
      const service = new SeatLifecycleService({ db, rigRepo: new RigRepository(db), sessionRegistry: new SessionRegistry(db),
        eventBus: new EventBus(db), tmuxAdapter: {} as never });
      try {
        for (const change of [() => service.stopSeat({ seatRef: "x", reason: "" }), () => service.cleanSeat({ seatRef: "x", reason: "" }),
          () => service.launchFresh({ seatRef: "x", fresh: true, reason: "" })]) {
          await readProcessTable();
          const before = processTableReadsForTest();
          await readProcessTable();
          expect(processTableReadsForTest()).toBe(before); // cached
          expect(await change()).toMatchObject({ ok: false });
          await readProcessTable();
          expect(processTableReadsForTest()).toBe(before + 1); // the change dropped it
        }
      } finally {
        db.close();
      }
    });

    it("the idle proof still takes two observations with the cache warm: a child started after the cached table shows", async () => {
      vi.stubEnv("OPENRIG_PROCESS_TABLE_TTL_MS", "60000");
      const root = shell(54);
      serveProcRoot(root);
      const tmux = { getPanePid: async () => 54 };
      expect(await verifyPaneIdleShell({ target: "%1", tmux })).toBe(true);
      await readProcessTable(); // the idle table is cached
      fs.mkdirSync(path.join(root, "55"));
      fs.writeFileSync(path.join(root, "55", "stat"), stat(55, "sleep", "S", 54, 55, 55, 0));
      fs.writeFileSync(path.join(root, "55", "cmdline"), "sleep\x00600\x00");
      fs.writeFileSync(path.join(root, "54", "stat"), stat(54, "bash", "S", 1, 54, 55, 0));
      // the 1st sample is the cached idle table, the 2nd a fresh read with the child in the foreground: not idle
      expect(await verifyPaneIdleShell({ target: "%1", tmux })).toBe(false);
    });

    it("the idle proof reads twice through a caller's own lister too (the Codex adapter's default one)", async () => {
      vi.stubEnv("OPENRIG_PROCESS_TABLE_TTL_MS", "60000");
      const root = shell(58);
      serveProcRoot(root);
      const tmux = { getPanePid: async () => 58 };
      expect(await verifyPaneIdleShell({ target: "%1", tmux, listProcesses: defaultListProcesses })).toBe(true);
      await defaultListProcesses(); // the idle table is cached
      fs.mkdirSync(path.join(root, "59"));
      fs.writeFileSync(path.join(root, "59", "stat"), stat(59, "sleep", "S", 58, 59, 59, 0));
      fs.writeFileSync(path.join(root, "59", "cmdline"), "sleep\x00600\x00");
      fs.writeFileSync(path.join(root, "58", "stat"), stat(58, "bash", "S", 1, 58, 59, 0));
      expect(await verifyPaneIdleShell({ target: "%1", tmux, listProcesses: defaultListProcesses })).toBe(false);
    });
  });

  it.runIf(process.platform === "linux")("spawns no ps for either lister on Linux", async () => {
    scratch = fs.mkdtempSync(path.join(os.tmpdir(), "process-table-ps-"));
    const marker = path.join(scratch, "ps-ran");
    fs.writeFileSync(path.join(scratch, "ps"), `#!/bin/sh\ntouch '${marker}'\nexit 1\n`, { mode: 0o755 });
    vi.stubEnv("PATH", scratch);
    expect((await listNativeProcesses()).some((row) => row.pid === process.pid)).toBe(true);
    expect((await defaultListProcessesStrict()).some((row) => row.pid === process.pid)).toBe(true);
    expect(fs.existsSync(marker)).toBe(false);
  });
});
