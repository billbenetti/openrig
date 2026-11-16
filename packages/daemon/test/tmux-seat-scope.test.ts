// Seat scope (terminal.seat_scope, default off): with the key on, each seat's pane command runs in a transient
// `systemd-run --user --scope` of its own with TasksMax as its only limit, in the openrig-seats.slice and with
// OOMPolicy=continue, so a seat that forks without end hits its own ceiling and not the other seats'
// (mvschwarz/openrig#3253). With the key off, or where no user scope can be made (not Linux, the daemon not a user
// service, no runtime directory, systemd-run missing or refusing a throwaway scope, the user bus not answering now),
// the launch is today's, and the second case says why in a warning.
//
// Synthetic only: the string exec and the argv exec are fakes that record what the adapter runs, so nothing here
// touches the host's systemd or tmux.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TmuxAdapter, SEAT_SCOPE_SLICE } from "../src/adapters/tmux.js";

type Plan = string[] | null;
type Probe = { which?: string | Error; version?: string | Error; scope?: string | Error; bus?: string | Error };
const RT = "/run/user/1000";
const fail = (stderr: string) => Object.assign(new Error(`Command failed\n${stderr}`), { stderr: `${stderr}\n`, code: 1 });

function makeAdapter(opts: { seatScope?: { tasksMax: number }; inherit?: boolean; probe?: Probe }) {
  const shellCalls: string[] = [];
  const argvCalls: string[][] = [];
  const defaults: Required<Probe> = { which: "/usr/bin/systemd-run\n", version: "systemd 255 (255.4-1ubuntu8.11)\n+PAM +AUDIT\n", scope: "", bus: "255.4-1ubuntu8.11\n" };
  // Read at each call, so a test can change an answer between launches.
  const get = (k: keyof Probe) => (opts.probe && k in opts.probe ? opts.probe[k]! : defaults[k]);
  const answer = (value: string | Error) => { if (value instanceof Error) throw value; return value; };
  const exec = async (command: string) => {
    shellCalls.push(command);
    if (command === "command -v systemd-run") return answer(get("which"));
    if (/systemd-run --version$/.test(command)) return answer(get("version"));
    if (command.includes("openrig-seat-probe-")) return answer(get("scope"));
    if (command.includes("systemctl --user show")) return answer(get("bus"));
    return "";
  };
  const argvExec = async (args: string[]) => {
    argvCalls.push(args);
    if (args.some(arg => arg.includes("default-shell"))) return "/bin/bash";
    return args.includes("new-session") || args.includes("source-file") ? "%1" : "";
  };
  let n = 0;
  const writes: string[] = [];
  const files = { writeFile: async (_p: string, content: string) => { writes.push(content); }, unlink: async () => {}, tmpName: () => `/tmp/synthetic-seat-env-${++n}`, bufferName: () => "unused" };
  const adapter = new TmuxAdapter(exec, files, argvExec);
  adapter.launchEnvironment = () => ({ inheritTmuxEnvironment: opts.inherit === true, env: { PATH: "/bin" }, runtimeDir: RT,
    ...(opts.seatScope ? { seatScope: opts.seatScope } : {}) });
  const plan = (env: Record<string, string>, command = "/bin/sh", warnings: string[] = []) =>
    (adapter as unknown as { scopedLaunchCommand(e: Record<string, string>, c: string, w: string[]): Promise<Plan> })
      .scopedLaunchCommand(env, command, warnings);
  return { adapter, plan, shellCalls, argvCalls, writes };
}

const SEAT = { OPENRIG_NODE_ID: "synthetic-node", OPENRIG_SESSION_NAME: "build-dev-1-s12@fabrica" };
const probes = (calls: string[]) => calls.filter(c => c.includes("systemd-run") || c.includes("systemctl"));
const kinds = (calls: string[]) => probes(calls).map(c =>
  c === "command -v systemd-run" ? "which" : / --version$/.test(c) ? "version" : c.includes("openrig-seat-probe-") ? "scope" : "bus");
const sameCommand = (a: Plan, b: Plan) => expect(a?.map(x => x.replace(/synthetic-seat-env-\d+/, "F"))).toEqual(b?.map(x => x.replace(/synthetic-seat-env-\d+/, "F")));

beforeEach(() => {
  vi.stubEnv("MANAGERPID", "4242");
  vi.stubEnv("XDG_RUNTIME_DIR", RT);
  vi.spyOn(console, "warn").mockImplementation(() => {});
});
afterEach(() => { vi.unstubAllEnvs(); vi.restoreAllMocks(); });

describe("seat scope off (the default)", () => {
  it("the pane command is today's: no systemd-run in it and no probe run", async () => {
    const off = makeAdapter({});
    const warnings: string[] = [];
    const plan = await off.plan(SEAT, "/bin/sh", warnings);
    expect(plan?.[0]).toBe("/bin/sh");
    expect(plan?.join(" ")).not.toContain("systemd-run");
    expect(probes(off.shellCalls)).toEqual([]);
    expect(warnings).toEqual([]);
    // A launch without the seat env says nothing either.
    expect(await off.plan({}, "/bin/sh", warnings)).toBeNull();
    expect(warnings).toEqual([]);
  });
});

describe.skipIf(process.platform !== "linux")("seat scope on, on Linux with a user manager", () => {
  it("prefixes the pane command with its own systemd-run --user --scope in the seats' slice: TasksMax its only limit, OOMPolicy=continue", async () => {
    const on = makeAdapter({ seatScope: { tasksMax: 2048 } });
    const off = makeAdapter({});
    const plan = (await on.plan(SEAT))!;
    const sep = plan.indexOf("--");
    expect(plan.slice(0, 4)).toEqual(["/usr/bin/env", "-u", "DBUS_SESSION_BUS_ADDRESS", `XDG_RUNTIME_DIR=${RT}`]);
    expect(plan.slice(4, 9)).toEqual(["/usr/bin/systemd-run", "--user", "--scope", "--quiet", "--collect"]);
    expect(plan[plan.indexOf("--slice") + 1]).toBe(SEAT_SCOPE_SLICE);
    expect(SEAT_SCOPE_SLICE).toBe("openrig-seats.slice");
    const unit = plan[plan.indexOf("--unit") + 1]!;
    expect(unit).toMatch(/^openrig-seat-build-dev-1-s12_fabrica-[0-9a-f]{8}$/);
    expect(plan[plan.indexOf("--description") + 1]).toBe("OpenRig seat build-dev-1-s12@fabrica");
    expect(plan).toContain("--expand-environment=no");
    const props = plan.slice(0, sep).flatMap((a, i, all) => (all[i - 1] === "-p" ? [a] : []));
    expect(props).toEqual(["OOMPolicy=continue", "TasksMax=2048"]);
    // After the separator, exactly today's command (the temp file name differs by one call only).
    sameCommand(plan.slice(sep + 1), await off.plan(SEAT));
  });

  it("uses the configured ceiling and a new unit per launch", async () => {
    const on = makeAdapter({ seatScope: { tasksMax: 300 } });
    const a = (await on.plan(SEAT))!, b = (await on.plan(SEAT))!;
    expect(a).toContain("TasksMax=300");
    expect(a[a.indexOf("--unit") + 1]).not.toBe(b[b.indexOf("--unit") + 1]);
  });

  it("finds systemd-run once (a throwaway scope with the seat's options), and asks the bus at every launch", async () => {
    const on = makeAdapter({ seatScope: { tasksMax: 2048 } });
    await on.plan(SEAT); await on.plan(SEAT); await on.plan(SEAT);
    expect(kinds(on.shellCalls)).toEqual(["which", "version", "scope", "bus", "bus", "bus"]);
    const throwaway = on.shellCalls.find(c => c.includes("openrig-seat-probe-"))!;
    expect(throwaway).toMatch(/^\/usr\/bin\/env -u DBUS_SESSION_BUS_ADDRESS XDG_RUNTIME_DIR=\/run\/user\/1000 timeout 10 \/usr\/bin\/systemd-run --user --scope .*--slice openrig-seats\.slice .*-p TasksMax=2048 -- \/bin\/true$/);
    expect(on.shellCalls.filter(c => c.includes("systemctl")).every(c => c.includes(" timeout 5 systemctl --user show"))).toBe(true);
  });

  for (const [version, expand, oom] of [[252, false, false], [253, false, true], [254, true, true]] as const) {
    it(`systemd ${version}: --expand-environment=no ${expand ? "in" : "out"}, OOMPolicy=continue ${oom ? "in" : "out"}`, async () => {
      const on = makeAdapter({ seatScope: { tasksMax: 2048 }, probe: { version: `systemd ${version} (${version}.1-1)\n` } });
      const plan = (await on.plan(SEAT))!;
      expect(plan).toContain("/usr/bin/systemd-run");
      expect(plan.includes("--expand-environment=no")).toBe(expand);
      expect(plan.includes("OOMPolicy=continue")).toBe(oom);
      expect(plan).toContain("TasksMax=2048");
    });
  }

  it("names the unit from the node id when the seat env has no session name, unit-safe and bounded", async () => {
    const on = makeAdapter({ seatScope: { tasksMax: 2048 } });
    const plan = (await on.plan({ OPENRIG_NODE_ID: "01M7/x y@z" }))!;
    expect(plan[plan.indexOf("--unit") + 1]).toMatch(/^openrig-seat-01M7_x_y_z-[0-9a-f]{8}$/);
    const long = (await on.plan({ OPENRIG_NODE_ID: "n", OPENRIG_SESSION_NAME: "a".repeat(300) }))!;
    expect(long[long.indexOf("--unit") + 1]).toMatch(new RegExp(`^openrig-seat-a{120}-[0-9a-f]{8}$`));
  });

  it("a fresh create and a respawn both start the pane through the seat scope", async () => {
    const on = makeAdapter({ seatScope: { tasksMax: 2048 } });
    expect((await on.adapter.createSession("synthetic-seat", "/private", SEAT)).ok).toBe(true);
    expect((await on.adapter.respawnPane("%1", "/bin/sh", { env: SEAT })).ok).toBe(true);
    // Both travel in a private source-file (the seat env), whose body is the tmux command the server runs.
    const bodies = on.writes.filter(w => /^(new-session|respawn-pane) /.test(w));
    expect(bodies.map(b => b.split(" ")[0])).toEqual(["new-session", "respawn-pane"]);
    // tmux config quoting puts each word in single quotes; read the words.
    for (const body of bodies) expect(body.replace(/'/g, "")).toMatch(/\/usr\/bin\/env -u DBUS_SESSION_BUS_ADDRESS .*\/usr\/bin\/systemd-run --user --scope .*--slice openrig-seats\.slice .*TasksMax=2048 -- \/bin\/sh -c /);
  });

  it("a bus that stops answering after a good launch: today's launch with a warning, not asked again for 60 s, then used again", async () => {
    const probe: Probe = {};
    const on = makeAdapter({ seatScope: { tasksMax: 2048 }, probe });
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    expect((await on.plan(SEAT))?.[4]).toBe("/usr/bin/systemd-run");
    probe.bus = fail("Failed to connect to bus: Connection refused");
    const warnings: string[] = [];
    sameCommand(await on.plan(SEAT, "/bin/sh", warnings), await makeAdapter({}).plan(SEAT));
    expect(warnings[0]).toMatch(/Seat scope unavailable .*bus did not answer/);
    const asked = on.shellCalls.length;
    now += 30_000;
    const quiet: string[] = [];
    expect((await on.plan(SEAT, "/bin/sh", quiet))?.[0]).toBe("/bin/sh");
    expect(quiet[0]).toMatch(/bus did not answer/);
    expect(probes(on.shellCalls.slice(asked))).toEqual([]);
    delete probe.bus;
    now += 31_000;
    expect((await on.plan(SEAT))?.[4]).toBe("/usr/bin/systemd-run");
  });

  // T-009602 (SH-2): a probe that fails (the throwaway scope refused, or its 10 s timeout) keeps the bus's 60 s: with the
  // user manager stuck, each launch would otherwise wait up to 10 s again (98 relaunches ~16 min)
  const probeFailures: Array<[string, Error, RegExp]> = [
    ["refused", fail("Failed to start transient scope unit: Access denied"), /throwaway seat scope could not be made: Failed to start transient scope unit/],
    ["timed out", Object.assign(new Error("Command failed"), { stderr: "", code: 124 }), /throwaway seat scope could not be made/],
  ];
  for (const [name, error, reason] of probeFailures) {
    it(`a throwaway scope ${name}: not kept, not probed again for 60 s, then tried again`, async () => {
      const probe: Probe = { scope: error };
      const on = makeAdapter({ seatScope: { tasksMax: 2048 }, probe });
      let now = 2_000_000;
      vi.spyOn(Date, "now").mockImplementation(() => now);
      const warnings: string[] = [];
      expect((await on.plan(SEAT, "/bin/sh", warnings))?.[0]).toBe("/bin/sh");
      expect(warnings[0]).toMatch(reason);
      const asked = on.shellCalls.length;
      delete probe.scope;
      now += 1_000;
      const quiet: string[] = [];
      expect((await on.plan(SEAT, "/bin/sh", quiet))?.[0]).toBe("/bin/sh");
      expect(quiet[0]).toMatch(reason);
      expect(probes(on.shellCalls.slice(asked))).toEqual([]);
      now += 60_000;
      expect((await on.plan(SEAT))?.[4]).toBe("/usr/bin/systemd-run");
      expect(kinds(on.shellCalls.slice(asked))).toEqual(["which", "version", "scope", "bus"]);
    });
  }
});

describe("seat scope on, but no user scope can be made: today's launch, with a warning", () => {
  const cases: Array<[string, () => void, Probe | undefined, RegExp]> = [
    ["the daemon is not a user service", () => vi.stubEnv("MANAGERPID", ""), undefined, /MANAGERPID/],
    ["there is no runtime directory", () => vi.stubEnv("XDG_RUNTIME_DIR", ""), undefined, /XDG_RUNTIME_DIR/],
    ["systemd-run is missing", () => {}, { which: fail("") }, /systemd-run not found/],
    ["command -v gives no path", () => {}, { which: "systemd-run\n" }, /systemd-run not found \(command -v gave/],
    ["systemd-run --version fails", () => {}, { version: fail("boom") }, /--version failed: boom/],
    ["systemd-run prints no version", () => {}, { version: "garbage\n" }, /printed no version/],
    ["the user manager refuses the throwaway scope", () => {}, { scope: fail("Failed to start transient scope unit") }, /throwaway seat scope/],
    ["the user bus refuses", () => {}, { bus: fail("Failed to connect to bus: Connection refused") }, /bus did not answer: Failed to connect to bus/],
  ];
  for (const [name, setup, probe, reason] of cases) {
    it(name, async () => {
      setup();
      const on = makeAdapter({ seatScope: { tasksMax: 2048 }, probe });
      const off = makeAdapter({});
      const warnings: string[] = [];
      const plan = await on.plan(SEAT, "/bin/sh", warnings);
      sameCommand(plan, await off.plan(SEAT));
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toMatch(/^Seat scope unavailable \(/);
      if (process.platform === "linux") expect(warnings[0]).toMatch(reason);
    });
  }

  it("with terminal.inherit_tmux_environment on there is no daemon launch command to wrap: warns and keeps the inherited launch", async () => {
    const on = makeAdapter({ seatScope: { tasksMax: 2048 }, inherit: true });
    const warnings: string[] = [];
    expect(await on.plan(SEAT, "/bin/sh", warnings)).toBeNull();
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatch(/inherit_tmux_environment/);
    expect(probes(on.shellCalls)).toEqual([]);
  });

  it("a respawn without the seat env is not reinterpreted: no policy read, no probe, no scope", async () => {
    // the contract of tmux-agent-environment ("does not reinterpret a restart that owns its existing command")
    const on = makeAdapter({ seatScope: { tasksMax: 2048 } });
    const policy = on.adapter.launchEnvironment!;
    const read = vi.fn(policy);
    on.adapter.launchEnvironment = read;
    const result = await on.adapter.respawnPane("%1", "codex resume", { cwd: "/private" });
    expect(result.ok).toBe(true);
    expect(result.ok && result.warnings).toBeUndefined();
    expect(read).not.toHaveBeenCalled();
    expect(probes(on.shellCalls)).toEqual([]);
  });

  it("a restart in place with the seat scope on keeps its command, out of the scope, and says so (T-009602, SH-1)", async () => {
    const on = makeAdapter({ seatScope: { tasksMax: 2048 } });
    const result = await on.adapter.respawnPane("%1", "codex resume", { cwd: "/private", restartInPlace: true });
    expect(result.ok).toBe(true);
    expect(result.ok && result.warnings).toHaveLength(1);
    expect(result.ok && result.warnings?.[0]).toMatch(/^Seat scope not applied on a restart in place: .*without this seat's own TasksMax/);
    expect(probes(on.shellCalls)).toEqual([]);
    expect(on.argvCalls.at(-1)).toEqual(["tmux", "respawn-pane", "-t", "%1", "-c", "/private", "codex resume"]);
    // with the key off, the same restart says nothing
    const off = makeAdapter({});
    const quiet = await off.adapter.respawnPane("%1", "codex resume", { cwd: "/private", restartInPlace: true });
    expect(quiet.ok && quiet.warnings).toBeUndefined();
  });
});
