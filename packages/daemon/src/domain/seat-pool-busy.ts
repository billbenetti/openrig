// What keeps a seat parked on its own wait from a 2nd row although its wait looks short: heavy local work running
// under its pane (dispatch_to_parked.busy_processes, T-006090). A seat that left an eval being frozen or a suite queued
// for the host's lock in a background shell sits idle at its prompt, yet it gives a 2nd row back to the model, and the
// row goes round. The planner, the executor's recheck and the demand command's `receives` read this through the
// slot's `busyProcess` (seatPoolCanReceive).

/** One process of the table, as process-table.ts reads it. */
export interface BusyProcessRow {
  pid: number;
  ppid: number;
  command: string;
  /** When the process started, as ps's lstart (local time, process-table.ts); absent or unreadable: its age is unknown,
   *  and a process that left the pane's tree then still holds the seat (the read leans to busy). */
  startedAt?: string;
}

/**
 * Programs that run another one named in their arguments: a shell running a script, an interpreter, the usual
 * prefixes. The program a command really runs is the first argument after them that is not an option, an environment
 * assignment or a number (a priority, a timeout). `flock` is not one: its first argument is a lock file.
 */
export const SEAT_POOL_BUSY_WRAPPERS: ReadonlySet<string> = new Set([
  "bash", "sh", "dash", "zsh", "env", "nice", "ionice", "timeout", "nohup", "setsid", "stdbuf", "time", "exec", "command",
  "python", "python3", "node", "perl",
]);
const ENV_ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=/;
const NUMBER = /^\d+(\.\d+)?[smhd]?$/;
const VERSIONED_PYTHON = /^python3\.\d+$/;

/** Whether a base name is one of the wrappers seatPoolBusyCommand looks past (so it can never be a busy program). */
export function seatPoolBusyWrapper(name: string): boolean {
  return SEAT_POOL_BUSY_WRAPPERS.has(name) || VERSIONED_PYTHON.test(name);
}

/**
 * The program `command` runs (its base name, after the wrappers above) when it is one of `programs`; null otherwise.
 * Only that one word counts, so `grep suite-flock …` or `rg congelar-eval` is not a match, while
 * `bash /…/suite-flock nice -n 10 …`, `nice -n 10 /…/congelar-eval` and `sh -c 'suite-flock …'` are. The command is the
 * process's arguments joined by spaces, as the process table reads it. An option's value given as a separate word
 * (`timeout -s KILL 600 …`) is taken for the program: a miss, not a false match.
 */
export function seatPoolBusyCommand(command: string, programs: readonly string[]): string | null {
  // T-007205 S15: on Linux, a process whose command line could not be read (EIO, EMFILE) is in the table as ps shows
  // it, its task name in brackets (the kernel keeps 15 bytes): it matches by that name when that name is the program's
  // own (a binary, or a script whose shebang names the interpreter's absolute path, as congelar-eval's). A script run
  // through `#!/usr/bin/env bash` (suite-flock) keeps the interpreter's name, `[bash]`: no match here, and
  // seatPoolUnreadInTree holds its seat instead. From 15 bytes on, the name is a prefix and may stand for another
  // program sharing it: the seat is held, the reason names the first program of the list. A zombie
  // (`[name] <defunct>`) holds nothing.
  const bracketed = BRACKETED_NAME.exec(command.trim());
  if (bracketed) {
    const name = bracketed[1]!;
    return programs.find((program) => program === name
      || (Buffer.byteLength(name) >= TASK_NAME_BYTES && program.startsWith(name))) ?? null;
  }
  const program = firstProgram(command);
  return program && programs.includes(program.base) ? program.base : null;
}

/** ps's command (Linux) for a process with no readable command line: its task name in brackets. */
const BRACKETED_NAME = /^\[([^\]\s]+)\]$/;
/** The kernel keeps 15 bytes of a task's name (TASK_COMM_LEN 16, with the NUL). */
const TASK_NAME_BYTES = 15;

/** The program `command` runs, after the wrappers, options, assignments and numbers: its word and its base name. */
function firstProgram(command: string): { token: string; base: string } | null {
  for (const raw of command.split(/\s+/)) {
    if (!raw || raw.startsWith("-") || ENV_ASSIGNMENT.test(raw) || NUMBER.test(raw)) continue;
    const token = raw.replace(/^['"]|['"]$/g, "");
    const base = token.slice(token.lastIndexOf("/") + 1);
    if (seatPoolBusyWrapper(base)) continue;
    return { token, base };
  }
  return null;
}

/**
 * Why the process tree under `panePid` (its descendants, not the pane's own process) makes the seat busy: the first
 * matching process by pid, as `<program> (pid <n>)`; null when none matches.
 */
export function seatPoolBusyProcess(rows: readonly BusyProcessRow[], panePid: number, programs: readonly string[]): string | null {
  const children = new Map<number, BusyProcessRow[]>();
  for (const row of rows) {
    if (row.pid === row.ppid) continue;
    const list = children.get(row.ppid);
    if (list) list.push(row);
    else children.set(row.ppid, [row]);
  }
  const seen = new Set<number>([panePid]);
  const queue = [...(children.get(panePid) ?? [])];
  const matches: Array<{ pid: number; program: string }> = [];
  while (queue.length > 0) {
    const row = queue.shift()!;
    if (seen.has(row.pid)) continue;
    seen.add(row.pid);
    const program = seatPoolBusyCommand(row.command, programs);
    if (program) matches.push({ pid: row.pid, program });
    queue.push(...(children.get(row.pid) ?? []));
  }
  if (matches.length === 0) return null;
  matches.sort((a, b) => a.pid - b.pid);
  return `${matches[0]!.program} (pid ${matches[0]!.pid})`;
}

/**
 * T-007205 S15: a process under `panePid` whose command line could not be read (`[name]`, not a zombie) and whose name
 * matches none of `programs` might still be one of them (a script run through `#!/usr/bin/env` keeps the interpreter's
 * name): a failed read, which holds the seat without counting it as an occupant. The lowest pid, as
 * `command line of pid <n> not read ([name])`; null when every process under the pane was read.
 */
export function seatPoolUnreadInTree(rows: readonly BusyProcessRow[], panePid: number, programs: readonly string[]): string | null {
  const children = new Map<number, BusyProcessRow[]>();
  for (const row of rows) {
    if (row.pid === row.ppid) continue;
    const list = children.get(row.ppid);
    if (list) list.push(row);
    else children.set(row.ppid, [row]);
  }
  const seen = new Set<number>([panePid]);
  const queue = [...(children.get(panePid) ?? [])];
  let first: BusyProcessRow | null = null;
  while (queue.length > 0) {
    const row = queue.shift()!;
    if (seen.has(row.pid)) continue;
    seen.add(row.pid);
    if (BRACKETED_NAME.test(row.command.trim()) && !seatPoolBusyCommand(row.command, programs)
      && (first === null || row.pid < first.pid)) first = row;
    queue.push(...(children.get(row.pid) ?? []));
  }
  return first ? `command line of pid ${first.pid} not read (${first.command.trim()})` : null;
}

/** One pane of the tmux server, as `tmux list-panes -a` prints it. */
export interface SeatPoolPane {
  paneId: string;
  panePid: number;
  /** The tmux session the pane belongs to. */
  session: string;
  /** The pane tmux shows for its session: the active pane of the active window (what a session target resolves to). */
  active: boolean;
}

/** Where a seat's pane is: its bound pane (`%12`), or else its tmux session's active pane. */
export interface SeatPoolPaneRef {
  paneId: string | null;
  tmuxSession: string;
}

export interface SeatPoolBusyReadDeps {
  /** Every pane of the tmux server, read once per read (one `tmux list-panes -a`, T-006251 S3); rejects when tmux
   *  cannot be read. */
  panes(): Promise<readonly SeatPoolPane[]>;
  /** Where the seat's pane is. It may throw (a database read): that seat alone then counts as busy (S3). */
  paneOf(sessionName: string): SeatPoolPaneRef;
  /** The TMUX_PANE a process inherited (its environment), read off the event loop (T-006251 S1): null when it has
   *  none, when the process is gone or when it is not one a seat could own (another user's); rejects when it could not
   *  be read, which holds every seat it could belong to. Absent: a process that left the pane's tree is not looked for. */
  processPane?(pid: number): Promise<string | null>;
  /** The host's process table; rejects when it cannot be read. */
  processes(): Promise<readonly BusyProcessRow[]>;
  /** The clock for a process's age. Default: Date.now. */
  nowMs?(): number;
}

/** How long one read (the table, the panes, an environment) may take before it counts as failed. */
export const SEAT_POOL_BUSY_READ_TIMEOUT_MS = 5000;
/** How many environments are read at once. The reads run on libuv's shared pool (4 threads; the daemon's unit does not
 *  set UV_THREADPOOL_SIZE), which the daemon's other async fs, dns.lookup and zlib calls use too, and a read stuck in
 *  the kernel (D-state on /proc/<pid>/environ) keeps its thread past the deadline: half the pool at most, so stuck
 *  reads never stall the rest of the daemon (T-007205 S12). */
export const SEAT_POOL_BUSY_ENV_CONCURRENCY = 2;
/** libuv's default thread pool, which SEAT_POOL_BUSY_ENV_CONCURRENCY must stay under. */
export const LIBUV_DEFAULT_THREADPOOL_SIZE = 4;

/** What readProcTmuxPane reads with: the daemon's fs.promises, a test's fakes. */
export interface ProcEnvironmentIo {
  readFile(path: string): Promise<string>;
  /** The owner uid of a path; rejects when it cannot be read. */
  ownerUid(path: string): Promise<number>;
  /** The daemon's uid; null where there is none (not POSIX). */
  uid: number | null;
}

/**
 * The TMUX_PANE a process inherited, from `<root>/<pid>/environ` (T-006251 S1): null when it has none, when the process
 * is gone (ENOENT, ESRCH, of the environment or of the owner's stat) or when it is another user's (EACCES or EPERM on a
 * process whose owner is not the daemon's user: no seat runs as one); any other failure rejects, the daemon's own
 * process unreadable (EACCES, EMFILE…) and an owner's stat that fails otherwise (B3) included, which holds the seats it
 * could belong to.
 */
export async function readProcTmuxPane(pid: number, io: ProcEnvironmentIo, root = "/proc"): Promise<string | null> {
  try {
    const found = (await io.readFile(`${root}/${pid}/environ`)).split("\0").find((entry) => entry.startsWith("TMUX_PANE="));
    return found ? found.slice("TMUX_PANE=".length) : null;
  } catch (error) {
    const code = (error as { code?: string }).code;
    if (code === "ENOENT" || code === "ESRCH") return null;
    if (code === "EACCES" || code === "EPERM") {
      let owner: number;
      try {
        owner = await io.ownerUid(`${root}/${pid}`);
      } catch (statError) {
        // T-006251 B3: only "gone between the two reads" frees the seat; any other failure of the stat (EIO, EMFILE…)
        // is a failed read, which holds it, as the environment's own failure does.
        const statCode = (statError as { code?: string }).code;
        if (statCode === "ENOENT" || statCode === "ESRCH") return null;
        throw error;
      }
      if (io.uid !== null && owner !== io.uid) return null;
    }
    throw error;
  }
}

/** The environment reads still running, per reader (the daemon has one): a read past its deadline can't be cancelled
 *  and keeps a libuv thread, so it counts against the next read too (T-006251 S9). */
const pendingReads = new WeakMap<object, { count: number; waiters: Array<() => void> }>();

/**
 * Whether a busy reason names a process the read found (`<program> (pid <n>)` or `<program> (pid <n>, TMUX_PANE <p>)`)
 * rather than a failure to read (the table, the panes, the seat's pane, an environment, pending reads). The planner
 * counts only the first as occupied (T-006251 S6): a failed read holds the seat back from a row but grows nothing.
 */
export function seatPoolBusyFromProcess(reason: string | null | undefined): boolean {
  // The program is one word (a busy_processes entry has no whitespace, parentheses allowed): `run(1) (pid 9)` is a process.
  return !!reason && /^\S+ \(pid \d+(, TMUX_PANE \S+)?\)$/.test(reason);
}

/** Runs `tasks` with at most `limit` at once, keeping their order. */
async function limited<T>(tasks: ReadonlyArray<() => Promise<T>>, limit: number): Promise<T[]> {
  const out: T[] = new Array(tasks.length);
  let next = 0;
  const lane = async () => { while (next < tasks.length) { const i = next++; out[i] = await tasks[i]!(); } };
  await Promise.all(Array.from({ length: Math.min(limit, tasks.length) }, lane));
  return out;
}
/** The default ceiling on the age of a process that left the pane's tree (dispatch_to_parked.busy_orphan_max_age_s):
 *  12 h, the longest a suite-flock waits for its slot (T-006251 S8: at 6 h a slot was let go with its work still queued). */
export const SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S = 43_200;

function within<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const late = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`${what} took over ${ms} ms`)), ms); });
  return Promise.race([promise, late]).finally(() => clearTimeout(timer));
}

const reason = (error: unknown) => error instanceof Error ? error.message : String(error);

/** A process's start (ps's lstart, local time) in ms; null when unreadable. */
function startedMs(row: BusyProcessRow | undefined): number | null {
  if (!row?.startedAt) return null;
  const ms = Date.parse(row.startedAt);
  return Number.isFinite(ms) ? ms : null;
}

/** Login and plain shells: a pane that runs one hosts its runtime as a child; any other pane process is the runtime. */
const PANE_SHELLS: ReadonlySet<string> = new Set(["bash", "sh", "dash", "zsh", "fish", "ksh"]);
/** The agent runtimes a launch starts in a shell pane, by the program the command runs after the wrappers: the
 *  runtimes by name, the native installer's versioned Claude, and Node running the npm package's cli.js. */
const PANE_RUNTIMES: ReadonlySet<string> = new Set(["claude", "codex", "pi", "omp"]);
function paneRuntime(command: string): boolean {
  const program = firstProgram(command);
  if (!program) return false;
  return PANE_RUNTIMES.has(program.base)
    || /\/@anthropic-ai\/claude-code\/cli\.m?js$/.test(program.token)
    || /\/\.local\/share\/claude\/versions\/\d+\.\d+\.\d+(?:[-+][\w.-]+)?$/.test(program.token);
}

/**
 * When the pane's current occupant started, in ms (null when unknown): the runtime a launch starts in the pane. A pane
 * that runs a shell (`/bin/bash -l`, `-bash`) hosts the runtime as a child, and a fresh launch starts a new one in the
 * same shell: its newest runtime child (claude, codex, pi, omp, Node running Claude's cli.js; T-006251 S10: a newer sibling the shell started, a `sleep`, is
 * not the occupant), or its newest child when none is a runtime. A pane that runs anything else runs the runtime
 * itself, whose own children are tool shells (a newer one says nothing of the occupant, T-006251 RC S1): the pane's
 * process.
 */
export function seatPoolPaneOccupantMs(rows: readonly BusyProcessRow[], panePid: number): number | null {
  const pane = rows.find((row) => row.pid === panePid);
  const first = (pane?.command.split(/\s+/).find(Boolean) ?? "").replace(/^-/, "");
  if (!pane || !PANE_SHELLS.has(first.slice(first.lastIndexOf("/") + 1))) return startedMs(pane);
  const children = rows.filter((row) => row.ppid === panePid);
  const runtimes = children.filter((row) => paneRuntime(row.command));
  return (runtimes.length > 0 ? runtimes : children).map(startedMs)
    .reduce<number | null>((max, ms) => ms !== null && (max === null || ms > max) ? ms : max, startedMs(pane));
}

export interface SeatPoolBusyReadOptions {
  timeoutMs?: number;
  /** dispatch_to_parked.busy_orphan_max_age_s. Default SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S. */
  orphanMaxAgeSeconds?: number;
}

/**
 * For each of `sessions`, why it is busy or null: a matching process under its pane (seatPoolBusyProcess, whatever its
 * age), or else a matching process anywhere whose inherited TMUX_PANE is the seat's pane (one started in the
 * background whose parent left, so it was reparented out of the pane's tree; only matching processes have their
 * environment read), started no more than `orphanMaxAgeSeconds` ago and not before the pane's current occupant
 * (seatPoolPaneOccupantMs: a fresh launch starts a new runtime in the same pane, and an orphan of the occupant before it
 * is not this one's work; S2). The process table and the server's panes are read once each
 * (S3). A read that fails or takes over `timeoutMs` holds the seat back: a missing row costs little, a row given to a
 * seat busy with heavy local work is the fault this guards against. So an unreadable table or pane list, a seat whose
 * pane is not found or cannot be told, and an environment that could not be read (S1: it could be any seat's) all say
 * why, as busy.
 */
export async function readSeatPoolBusySessions(
  sessions: readonly string[], programs: readonly string[], deps: SeatPoolBusyReadDeps,
  options: SeatPoolBusyReadOptions | number = {},
): Promise<Map<string, string | null>> {
  const { timeoutMs = SEAT_POOL_BUSY_READ_TIMEOUT_MS, orphanMaxAgeSeconds = SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S } =
    typeof options === "number" ? { timeoutMs: options } : options;
  const busy = new Map<string, string | null>();
  if (sessions.length === 0 || programs.length === 0) return busy;
  const holdAll = (why: string) => { for (const session of sessions) busy.set(session, why); return busy; };
  let rows: readonly BusyProcessRow[];
  try {
    rows = await within(deps.processes(), timeoutMs, "the process table");
  } catch (error) {
    return holdAll(`process table not read: ${reason(error)}`);
  }
  let panes: readonly SeatPoolPane[];
  try {
    panes = await within(deps.panes(), timeoutMs, "the tmux panes");
  } catch (error) {
    return holdAll(`panes not read: ${reason(error)}`);
  }
  const byPid = new Map(rows.map((row) => [row.pid, row]));
  const nowMs = deps.nowMs?.() ?? Date.now();
  // The matching processes outside every pane's tree (only those can have left one: a process under a pane is that
  // pane's, found by seatPoolBusyProcess), with the pane each inherited, read once for every seat, a few at a time,
  // each within the timeout. A process gone or with no TMUX_PANE belongs to no seat; one whose environment could not be
  // read could be any seat's (S1).
  let byPane: Promise<{ byPane: Map<string, number[]>; unread: string | null }> | null = null;
  const matchingByPane = () => byPane ??= (async () => {
    const found = new Map<string, number[]>();
    if (!deps.processPane) return { byPane: found, unread: null };
    const underPanes = new Set<number>();
    const children = new Map<number, number[]>();
    for (const row of rows) if (row.pid !== row.ppid) children.set(row.ppid, [...(children.get(row.ppid) ?? []), row.pid]);
    const queue = panes.map((entry) => entry.panePid);
    while (queue.length > 0) {
      const pid = queue.pop()!;
      if (underPanes.has(pid)) continue;
      underPanes.add(pid);
      queue.push(...(children.get(pid) ?? []));
    }
    const matching = rows.filter((row) => !underPanes.has(row.pid) && seatPoolBusyCommand(row.command, programs));
    // One deadline for all the environments (RC delta, Sol): a read past it can't be cancelled, so once it passes no
    // new read starts and the rest count as not read; at most SEAT_POOL_BUSY_ENV_CONCURRENCY reads stay pending, and
    // the whole read takes at most `timeoutMs`.
    const deadline = Date.now() + timeoutMs;
    let pending = pendingReads.get(deps.processPane);
    if (!pending) pendingReads.set(deps.processPane, pending = { count: 0, waiters: [] });
    const inFlight = pending;
    const read = await limited(matching.map((row) => async () => {
      // S9: the reads still running (this call's, a concurrent call's, or an earlier call's past its deadline) hold
      // libuv threads: past the limit a new read waits its turn until the deadline, and then counts as not read.
      while (inFlight.count >= SEAT_POOL_BUSY_ENV_CONCURRENCY) {
        const left = deadline - Date.now();
        if (left <= 0) return { pid: row.pid, error: `environment reads still pending (${inFlight.count}) at the deadline` };
        let release: () => void = () => {};
        const turn = new Promise<void>((resolve) => { release = resolve; inFlight.waiters.push(resolve); });
        try {
          await within(turn, left, "a turn");
        } catch {
          // N13: the turn may have been handed over in the same tick the wait ran out; then it's no longer queued
          const queued = inFlight.waiters.indexOf(release);
          if (queued >= 0) inFlight.waiters.splice(queued, 1);
        }
      }
      const left = deadline - Date.now();
      if (left <= 0) {
        // T-007205 S13: woken (or freed) past this call's deadline, the read gives up without starting; the turn it
        // was handed goes on to the next waiter, which would otherwise wait out its own deadline with a thread free.
        if (inFlight.count < SEAT_POOL_BUSY_ENV_CONCURRENCY) inFlight.waiters.shift()?.();
        return { pid: row.pid, error: `the environments took over ${timeoutMs} ms` };
      }
      try {
        const started = deps.processPane!(row.pid);
        inFlight.count += 1;
        const done = () => { inFlight.count -= 1; inFlight.waiters.shift()?.(); };
        started.then(done, done);
        return { pid: row.pid, pane: await within(started, left, `the environment of pid ${row.pid}`) };
      } catch (error) {
        // A processPane that throws before its read starts (synchronously) frees the turn it was handed (lens, T-007205)
        if (inFlight.count < SEAT_POOL_BUSY_ENV_CONCURRENCY) inFlight.waiters.shift()?.();
        return { pid: row.pid, error: reason(error) };
      }
    }), SEAT_POOL_BUSY_ENV_CONCURRENCY);
    let unread: string | null = null;
    for (const entry of read.sort((a, b) => a.pid - b.pid)) {
      if ("error" in entry) {
        const why = String(entry.error);
        unread ??= `environment of pid ${entry.pid} not read: ${why}`;
        continue;
      }
      if (!entry.pane) continue;
      const list = found.get(entry.pane);
      if (list) list.push(entry.pid);
      else found.set(entry.pane, [entry.pid]);
    }
    return { byPane: found, unread };
  })();
  for (const session of sessions) {
    let pane: SeatPoolPane | undefined;
    try {
      const ref = deps.paneOf(session);
      pane = ref.paneId
        ? panes.find((entry) => entry.paneId === ref.paneId)
        : panes.find((entry) => entry.session === ref.tmuxSession && entry.active);
    } catch (error) {
      busy.set(session, `pane not read: ${reason(error)}`);
      continue;
    }
    if (!pane) {
      busy.set(session, "pane not found");
      continue;
    }
    const under = seatPoolBusyProcess(rows, pane.panePid, programs);
    if (under) {
      busy.set(session, under);
      continue;
    }
    const { byPane: found, unread } = await matchingByPane();
    const occupantMs = seatPoolPaneOccupantMs(rows, pane.panePid);
    const left = (found.get(pane.paneId) ?? []).find((pid) => {
      const started = startedMs(byPid.get(pid));
      if (started === null) return true;
      return nowMs - started <= orphanMaxAgeSeconds * 1000 && (occupantMs === null || started >= occupantMs);
    });
    if (left !== undefined) {
      busy.set(session, `${seatPoolBusyCommand(byPid.get(left)!.command, programs)} (pid ${left}, TMUX_PANE ${pane.paneId})`);
      continue;
    }
    busy.set(session, unread ?? seatPoolUnreadInTree(rows, pane.panePid, programs));
  }
  return busy;
}
