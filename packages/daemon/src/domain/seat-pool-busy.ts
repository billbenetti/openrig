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

export interface SeatPoolBusyReadDeps {
  /** The pid of the process the seat's pane runs; null when the pane is not found. */
  panePid(sessionName: string): Promise<number | null>;
  /** The seat's pane id (tmux `#{pane_id}`, as `%12`), to know a process that left the pane's tree; null: unknown. */
  paneId?(sessionName: string): string | null;
  /** The TMUX_PANE a process inherited (its environment), null when it has none or it cannot be read. */
  processPane?(pid: number): string | null;
  /** The host's process table; rejects when it cannot be read. */
  processes(): Promise<readonly BusyProcessRow[]>;
}

/** How long one read (the table, a pane) may take before it counts as failed. */
export const SEAT_POOL_BUSY_READ_TIMEOUT_MS = 5000;

function within<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const late = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`${what} took over ${ms} ms`)), ms); });
  return Promise.race([promise, late]).finally(() => clearTimeout(timer));
}

/**
 * For each of `sessions`, why it is busy or null: a matching process under its pane (seatPoolBusyProcess), or else a
 * matching process anywhere whose inherited TMUX_PANE is the seat's pane (one started in the background whose parent
 * left, so it was reparented out of the pane's tree; only matching processes have their environment read). The process
 * table is read once. A read that fails or takes over `timeoutMs` holds the seat back: a missing 2nd row costs little,
 * a 2nd row given to a seat busy with heavy local work is the fault this guards against. So an unreadable table, a pane
 * not found or a pane read that throws all say why, as busy.
 */
export async function readSeatPoolBusySessions(
  sessions: readonly string[], programs: readonly string[], deps: SeatPoolBusyReadDeps, timeoutMs = SEAT_POOL_BUSY_READ_TIMEOUT_MS,
): Promise<Map<string, string | null>> {
  const busy = new Map<string, string | null>();
  if (sessions.length === 0 || programs.length === 0) return busy;
  let rows: readonly BusyProcessRow[];
  try {
    rows = await within(deps.processes(), timeoutMs, "the process table");
  } catch (error) {
    const why = `process table not read: ${error instanceof Error ? error.message : String(error)}`;
    for (const session of sessions) busy.set(session, why);
    return busy;
  }
  // The matching processes and the pane each inherited, read once for every seat.
  let byPane: Map<string, Array<{ pid: number; program: string }>> | null = null;
  const matchingByPane = () => {
    if (byPane) return byPane;
    byPane = new Map();
    if (!deps.processPane) return byPane;
    for (const row of rows) {
      const program = seatPoolBusyCommand(row.command, programs);
      if (!program) continue;
      const pane = deps.processPane(row.pid);
      if (!pane) continue;
      const list = byPane.get(pane);
      if (list) list.push({ pid: row.pid, program });
      else byPane.set(pane, [{ pid: row.pid, program }]);
    }
    return byPane;
  };
  for (const session of sessions) {
    let pid: number | null;
    try {
      pid = await within(deps.panePid(session), timeoutMs, "the pane");
    } catch (error) {
      busy.set(session, `pane not read: ${error instanceof Error ? error.message : String(error)}`);
      continue;
    }
    if (pid === null) {
      busy.set(session, "pane not found");
      continue;
    }
    const under = seatPoolBusyProcess(rows, pid, programs);
    if (under) {
      busy.set(session, under);
      continue;
    }
    const paneId = deps.paneId?.(session) ?? null;
    const left = paneId ? [...(matchingByPane().get(paneId) ?? [])].sort((a, b) => a.pid - b.pid)[0] : undefined;
    busy.set(session, left ? `${left.program} (pid ${left.pid}, TMUX_PANE ${paneId})` : null);
  }
  return busy;
}
