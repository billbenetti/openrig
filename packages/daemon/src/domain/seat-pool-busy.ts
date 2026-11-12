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
  for (const token of command.split(/\s+/)) {
    if (!token || token.startsWith("-") || ENV_ASSIGNMENT.test(token) || NUMBER.test(token)) continue;
    const base = token.slice(token.lastIndexOf("/") + 1).replace(/^['"]|['"]$/g, "");
    if (seatPoolBusyWrapper(base)) continue;
    return programs.includes(base) ? base : null;
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
/** How many environments are read at once: the reads run on libuv's shared pool (4 threads by default), which the
 *  daemon's other async fs and dns calls use too. */
export const SEAT_POOL_BUSY_ENV_CONCURRENCY = 4;

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
 * is gone (ENOENT, ESRCH) or when it is another user's (EACCES or EPERM on a process whose owner is not the daemon's
 * user: no seat runs as one); any other failure rejects, the daemon's own process unreadable (EACCES, EMFILE…) included,
 * which holds the seats it could belong to.
 */
export async function readProcTmuxPane(pid: number, io: ProcEnvironmentIo, root = "/proc"): Promise<string | null> {
  try {
    const found = (await io.readFile(`${root}/${pid}/environ`)).split("\0").find((entry) => entry.startsWith("TMUX_PANE="));
    return found ? found.slice("TMUX_PANE=".length) : null;
  } catch (error) {
    const code = (error as { code?: string }).code;
    if (code === "ENOENT" || code === "ESRCH") return null;
    if (code === "EACCES" || code === "EPERM") {
      const owner = await io.ownerUid(`${root}/${pid}`).catch(() => null);
      if (owner === null) return null; // gone between the two reads
      if (io.uid !== null && owner !== io.uid) return null;
    }
    throw error;
  }
}

/** Runs `tasks` with at most `limit` at once, keeping their order. */
async function limited<T>(tasks: ReadonlyArray<() => Promise<T>>, limit: number): Promise<T[]> {
  const out: T[] = new Array(tasks.length);
  let next = 0;
  const lane = async () => { while (next < tasks.length) { const i = next++; out[i] = await tasks[i]!(); } };
  await Promise.all(Array.from({ length: Math.min(limit, tasks.length) }, lane));
  return out;
}
/** The default ceiling on the age of a process that left the pane's tree (dispatch_to_parked.busy_orphan_max_age_s). */
export const SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S = 21_600;

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

/**
 * When the pane's current occupant started, in ms (null when unknown): the runtime a launch starts in the pane. A pane
 * that runs a shell (`/bin/bash -l`, `-bash`) hosts the runtime as a child, and a fresh launch starts a new one in the
 * same shell: the newest child. A pane that runs anything else runs the runtime itself, whose own children are tool
 * shells (a newer one says nothing of the occupant, T-006251 RC S1): the pane's process.
 */
export function seatPoolPaneOccupantMs(rows: readonly BusyProcessRow[], panePid: number): number | null {
  const pane = rows.find((row) => row.pid === panePid);
  const first = (pane?.command.split(/\s+/).find(Boolean) ?? "").replace(/^-/, "");
  if (!pane || !PANE_SHELLS.has(first.slice(first.lastIndexOf("/") + 1))) return startedMs(pane);
  return rows.filter((row) => row.ppid === panePid).map(startedMs)
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
    const read = await limited(matching.map((row) => async () => {
      const left = deadline - Date.now();
      if (left <= 0) return { pid: row.pid, error: `the environments took over ${timeoutMs} ms` };
      try {
        return { pid: row.pid, pane: await within(deps.processPane!(row.pid), left, `the environment of pid ${row.pid}`) };
      } catch (error) {
        return { pid: row.pid, error: reason(error) };
      }
    }), SEAT_POOL_BUSY_ENV_CONCURRENCY);
    let unread: string | null = null;
    for (const entry of read.sort((a, b) => a.pid - b.pid)) {
      if ("error" in entry) { unread ??= `environment of pid ${entry.pid} not read: ${entry.error}`; continue; }
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
    busy.set(session, unread);
  }
  return busy;
}
