# Seat pools

A seat pool keeps extra seats beside one model seat and adds or removes them as work comes and goes. It is a watchdog
job with the `seat-pool` policy, so it is registered, changed and inspected with `rig watchdog`, and it survives a
daemon restart.

```bash
rig watchdog register --policy seat-pool --target-session dev-impl@my-rig \
  --spec pool.yaml --interval-seconds 60 --registered-by <your session>
```

The target is the model seat. Each new seat is a member of the model's pod named `<member>-s<n>` (`dev.impl-s1`,
`dev.impl-s2`, ...), with the model's runtime, agent, profile, model, effort, permissions, environment and launch
wrapper, its own working directory, and a fresh session. `n` only grows: a removed seat's name is never given to a new
seat. The model seat itself is never added, stopped or removed; the pool only launches it when it stopped with work
waiting (see [Waiting rows](#waiting-rows-dispatch-and-wake)). A seat the pool added whose session is gone is reaped
instead (see [Reaping a seat](#reaping-a-seat)).

## The spec

```yaml
policy: seat-pool
context:
  max: 4                    # required: the most extra seats
  min: 0                    # the fewest extra seats (default 0)
  cooldown_s: 60            # wait after an add or removal before the next one (default 60)
  idle_grace_s: 300         # how long a seat must sit idle before it is drained (default 300)
  dispatch_after_s: 60      # how long a row waits at its seat before the pool moves it or wakes the seat (default 60)
  dispatch_tag: role:impl   # move only the model's rows with this tag (default: every row to the model)
  drain: false              # true empties the pool (see below)
  drain_deadline_s: 900     # how long a drain waits for seats to become free before taking them out (default 900, at most 86400; 0: no deadline)
  adopt: [dev.impl2]        # existing seats the pool counts and gives rows to, but never stops or removes
  cwd_template: /work/{rig}/{slot}   # required: absolute, with {slot}; {rig} is optional
  demand_cmd:  { argv: [/usr/local/bin/pool-demand], timeout_s: 30 }
  prepare_cmd: { argv: [/usr/local/bin/pool-prepare], timeout_s: 120 }
  release_cmd: { argv: [/usr/local/bin/pool-release], timeout_s: 60 }
  reap_cmd:    { argv: [/usr/local/bin/pool-reap], timeout_s: 60 }
  remove_cmd:  { argv: [/usr/local/bin/pool-remove], timeout_s: 60 }
  escalation_owner: lead@my-rig     # where the pool's escalations go (default: the model's escalation owner)
```

Registration and `rig watchdog update --spec` refuse a spec with an unknown field, `max` below `min`, a relative
`cwd_template` or one without `{slot}`, a command that isn't an argv, or an `escalation_owner` that isn't a seat
address (`<session>@<rig>`). Commands run without a shell, with the daemon's
environment plus:

| Variable | Set for | Value |
| --- | --- | --- |
| `OPENRIG_SEAT_POOL_JOB` | every command | the job id |
| `OPENRIG_SEAT_POOL_MODEL` | every command | the model's session name |
| `OPENRIG_SEAT_POOL_SLOT` | `prepare_cmd`, `release_cmd`, `reap_cmd`, `remove_cmd` | the seat's session name |
| `OPENRIG_SEAT_POOL_SLOT_CWD` | `prepare_cmd` | the seat's working directory |
| `OPENRIG_SEAT_POOL_RELEASE_REASON` | `release_cmd` | why it is asked: `idle` (a free seat drained, by `drain` or by `idle_grace_s`), `drain-deadline` (the forced pass at a drain's deadline) or `reap` (a seat whose session is gone) |
| `OPENRIG_SEAT_POOL_SLOTS` | `demand_cmd` | JSON list of `{session, phase}` for the pool's seats |

A command that runs past `timeout_s` is killed and counts as failed. Each command has its own `timeout_s` (default 60),
so a slow `release_cmd` can be given longer than `demand_cmd`; `release_cmd` runs under the seat's lifecycle lease,
which holds deliveries to that seat back for as long as it runs.

## How many seats

The pool wants as many extra seats as its demand, clamped to `min`..`max`, and never fewer than the seats holding work
plus the seats still starting. Without `demand_cmd`, the demand is one seat per waiting row that no free seat takes
(see below), plus the seats holding work. With `demand_cmd`, it is the `desired` whole number the command prints as JSON
(`{"desired": 3}`). The command runs after each evaluation, and its answer is used for the next one. When it fails or
its last good answer is older than two evaluation intervals, the pool holds as it is: it finishes starting the seats
it has begun, and adds or drains nothing.

A seat holds work when it has a pending, in-progress or blocked row, a message the typing guard holds for it, or isn't
idle at a prompt. Only a seat the pool added, with no open row and no held message, that has been idle at a prompt, with
no pending input and no background shell, for `idle_grace_s` is drained, the newest first. An adopted seat counts and
takes rows like the pool's own, but the pool never stops or removes it: it was there before the pool and stays after.

`max` counts every seat on record: each adopted seat, running or stopped, and each seat the pool added, starting, live
or draining. The plan asks for no more seats than that leaves room for, and the executor checks the same count again
before each seat, so every seat an add asks for is made and none past `max`. A pool whose adopted seats already fill
`max` adds none; to let it grow, set `max` to the adopted seats plus the extra ones you want. A seat the same plan forgets (its
node is gone) or reaps leaves its room to the add of that plan, which runs after it. A pool that wants more seats than
`max` leaves room for, with nothing else to change, records `seat_pool_held` with `reason: max_reached` at each
evaluation, so a pool that doesn't grow says why. The executor reads usage before it reads the room and reserves a
launch, with nothing awaited between the room it reads and the seat it records, so an add that finds no room when it
runs (`seat_pool_held`, `reason: max_reached`) takes none of the tick's launches.

## Waiting rows: dispatch and wake

A pending row waits once it has been at its seat for `dispatch_after_s`, counted from its creation or its latest
reroute. Only pending rows are read: a claimed or blocked row is never moved, woken for or counted, and the pool reads
only rows addressed to the model, to its own seats and to the seats it removed, so a row to a person
(`human@kernel`) or to any other seat is never touched.

A waiting row addressed to the model, or to a seat the pool removed, goes to a free seat: one that is live, idle at
its prompt with nothing in the background, and holds no open row and no held message. The longest waiting row goes
first, one row per free seat. The move is `rig queue fallback` with `--expect-state pending`,
`--expect-destination <the row's seat>` and `--wake`, so a row claimed or moved since the plan stays where it is, and
the new seat is woken in the same transaction. A row no free seat takes counts as demand, so the pool adds a seat for
it. A row addressed to one of the pool's own seats stays with that seat.

When the model is itself a seat that works, a row to it may be about the work it already holds: a review coming back,
an answer to its question. Set `dispatch_tag` and tag the rows that are new work for the role when you create them
(`rig queue create --tags <tag>`): only a row to the model carrying that tag is moved or counted as demand, and any other
row to the model stays with it. A row to a seat the pool removed is moved whatever its tags, since nothing else will
take it.

A waiting row the pool doesn't move wakes the seat holding it (the model, or a seat of the pool), by why that seat
isn't taking it:

| The seat | The pool |
| --- | --- |
| has no live session | launches the model or an adopted seat, as `rig launch` does (with a backoff when it keeps failing, below); reaps a seat the pool added (see [Reaping a seat](#reaping-a-seat)) |
| is idle at its prompt | nothing: the queue's own wakes cover it (the nudge when the row was created or moved, the wake ladder and the stuck sweep) |
| needs input (a permission prompt or a question) | opens one row to the pool's escalation owner, tagged `seat-pool-needs-input:<seat>`; no other is opened about that seat while it is open, and a row that waits at the seat later is added to it as a note. With `dispatch_tag`, the model and an adopted seat are escalated only for waiting rows that carry the tag: their other rows are their own work |
| is out of provider usage | nothing: the row waits |
| is working | nothing: the row waits, and counts as demand if it is the model's |

The pool's escalation owner is the spec's `escalation_owner` when it is set, and the model's escalation owner
otherwise. An `escalation_owner` the queue doesn't accept as a destination is no owner at all: the escalation fails
(`seat_pool_escalation_failed`) rather than going to the model's escalation owner it replaced. Use it when the model's
escalation owner is a seat that must not be woken by the pool.

A relaunch that fails (or whose seat is stopped again when the next plan reads it, or whose launch throws) waits before the next try: 60 s,
doubled by each failure in a row, at most 30 minutes. While it waits, the relaunch takes none of the tick's launches,
so an add of the same plan (or of another pool) is not starved by a seat that never comes up; the wait is recorded once,
as `seat_pool_held` with `reason: relaunch_backoff`, the seat, the failures in a row and `retryAt`. A plan that no longer
asks to relaunch the seat forgets its count: the seat runs again or holds no waiting row, and also a plan held for a
passing reason (`usage_limit`, `drain`, a restore in progress, the model not found), which adds and launches nothing
either. The count lives in the daemon's
memory: a restart allows one more try before the backoff starts again. The launch a reap makes when the release command
refuses has no backoff: the reap goes on as soon as that launch fails.

While any seat of the pool is out of usage (its provider account reports the limit, as the wake ladder reads it), the
pool adds no seat and launches none, neither a stopped seat nor one it added and hasn't started, and records
`usage_limit`; a seat added but not launched waits until the limit lifts. A run reads usage again just before each add,
launch and relaunch, so a limit that appears after the plan, or while `prepare_cmd` runs, holds the seat too. Under
`drain`, nothing is moved or woken, even a move or a wake planned before `drain` was set.

## Adding and removing a seat

Adding a seat is three steps: the member is added without a session, `prepare_cmd` runs, and the seat gets its first
launch. If `prepare_cmd` or the launch fails, or the pool was set to `drain` while `prepare_cmd` ran, the seat is
removed without being launched.

Draining a seat takes it out of the pool's count and, holding the seat's lifecycle lease so no delivery, launch or stop
comes between the steps, checks that it still has no open row and no held message and is still idle since the moment
the plan read, runs `release_cmd`, checks again (the lease holds deliveries back, but a row can still be created while
the command runs), stops it with a [checked stop](checked-seat-stop.md) and removes its node with the same `expected`.
A seat woken between the plan and the stop, by a message, a nudge or a delivery that left no row behind, is not idle
since then, even once it is back at its prompt. If work arrived, the seat was woken, or `release_cmd` exits non-zero,
the seat goes back into the pool untouched. The removal never passes a fallback: a seat that still has
rows is not removed.

Once the node of a seat the pool added is gone, whether drained, reaped, removed before its launch or removed by
someone else, `remove_cmd` runs, so whatever `prepare_cmd` set up for the seat can be undone. The pool forgets the seat
only when it exits 0; until then the seat stays on record, counted against `max` but taking no row, and each plan runs
`remove_cmd` again (`seat_pool_remove_cmd_failed` records each refusal). An adopted seat never runs it.

Every add, launch, removal, row moved (`seat_pool_dispatched`, or `seat_pool_dispatch_skipped` when the row was claimed
or moved first), relaunch, escalation, refusal and failure is written to the job's history
(`rig watchdog status <jobId>`). An evaluation that plans changes records the plan (`seat_pool_planned`), and one that
holds because the model seat is gone, the rig is being restored, the demand answer is stale, a seat is out of usage
or `max` leaves no room records why (`seat_pool_held`). One with nothing to change records nothing.

## Host budget

All the pools of one daemon share one budget for starting seats, so pools evaluated at the same moment don't all read
the same load and memory and launch together. Each add (with its `prepare_cmd`), each first launch and each relaunch
reserves room before it starts and keeps it until it settles, even past the run's effect timeout. It also counts
against the watchdog tick in which its pool's plan was evaluated, however long the run applying that plan takes, and
settling does not give that back: load and free memory lag a seat that has just started, so launches one after another
are bounded as well as launches at once. With the default of 1, a pool starts at most one seat per evaluation, and
pools evaluated in the same tick share that one. The limits are the daemon's settings, not a pool's spec, and each
reservation reads them again, so `rig config set` takes effect on the next launch:

| Setting | Default | A launch starts only when |
| --- | --- | --- |
| `policies.seat_pool.max_launches_in_flight` | 1 | fewer launches than this are in flight, across every pool |
| `policies.seat_pool.max_launches_per_tick` | 1 | fewer launches than this were admitted for the plans of its watchdog tick, across every pool (0: none starts) |
| `policies.seat_pool.min_free_memory_mb` | 1024 | free memory, as `rig ps --resources` reports it, is at least this (0: no floor) |
| `policies.seat_pool.max_load_per_cpu_percent` | 100 | the 1-minute load, plus one for each launch in flight and one for this launch, divided by the CPU count, stays at or below this percentage |

A launch in flight does not show in free memory yet: with more than one launch in flight, raise the floor by what that
many new seats use. Where the platform has no load average (Windows), load is not checked. When there is no room,
nothing starts and the run records `seat_pool_held` with `reason: launch_budget`, which limit refused (`why`:
`launches_per_tick`, `launches_in_flight`, `low_memory` or `high_load`) and the figures; an add records it once and
adds nothing more in that run. A seat added but not launched stays as it is, and the next plan tries again. Draining,
reaping and moving rows take no room; the launch a reap makes when the release command refuses does.

## Reaping a seat

A seat the pool added whose session is gone (it exited, crashed or was stopped) is reaped: its rows go back to the
model and its node is removed, so the pool can add a fresh seat in its place. This goes on even while its account is
out of usage. Everything happens under the seat's lifecycle lease, after the pool reads the seat again; a seat that came
back since the plan is left alone. The reap runs in this order, and a failed step stops it there for the next plan to
pick up:

1. `release_cmd`, if set, as for a drained seat: the session is gone, but its work may not be saved. When it exits
   non-zero, nothing is reaped: the lease is let go and the seat is launched again, as `rig launch` does
   (`seat_pool_relaunched` with `why: release_refused`), so its session resumes where it can and finishes its work.
   Only when that launch fails is the seat reaped, from the next step, without asking `release_cmd` again.
2. `rig seat clean`, which checks in tmux that the session is really gone; a live session stops the reap before anything
   is moved.
3. `reap_cmd`, if set, so whatever tracks the seat's work (an executor lease, a task claim) can release it. A non-zero
   exit stops the reap.
4. Each in-progress row addressed to the seat is unclaimed, and every open row is moved with `rig queue fallback`,
   `--expect-destination <the seat>` and the row's state expected, to the model, with a wake when the row is pending.
   A blocked row keeps what wakes it: one with a `--wake-after` timer of its own stays blocked on the same gate, with a
   new timer of the same interval and message aimed at its new owner; one blocked on a live row, or on a person, stays
   blocked (that row's end, or the person's answer, wakes its owner); one with a repeating wait keeps it. A blocked row
   that nothing would wake (a gate that isn't a live row, with no timer, or a watchdog attached for the old seat) goes
   back to pending, with a note naming the blocker it had, and wakes its new owner. `seat_pool_reaped` notes `park:
   rearmed` or `park: unparked` on such a row. A drain's forced pass moves blocked rows the same way.
5. The node is removed with the `expected` read at the start of the reap, as a drain removes it, and `remove_cmd` runs
   as for a drained seat.

Each move gives the reason `reap:<seat>`. A row that a reap already moved once (the pool counts the row's reroutes with
that reason) goes to the pool's escalation owner (the spec's `escalation_owner`, or the model's) instead of the model,
so a row whose seats keep dying stops cycling through the pool. With no escalation owner it goes to the model, and
`seat_pool_escalation_failed` is recorded.

An adopted seat is never reaped: it existed before the pool, so its node and its conversation stay. While its session is
gone it is not counted among the pool's seats and takes no row, and it is launched again when a row waiting at it
needs it.

The history records `seat_pool_reaped` with the rows moved and where each went, and `seat_pool_reap_failed` with the
step that failed. Reaping goes on under `drain` and while a seat is out of usage; the launch after a refused release
waits out a usage limit like any other launch.

## Stopping a pool: drain

`drain: true` is the kill switch. Set it with `rig watchdog update <jobId> --spec <file>`: the pool adds nothing,
removes the seats that haven't started, and drains the rest of its own seats as each becomes free, so no work is
killed. It gives the adopted seats back at once (`seat_pool_unlinked`): their link ends, and their nodes, sessions and
rows stay as they are.

A seat that never becomes free (it holds a row it didn't take, a blocked row, or a claim it never finishes) would hold
the drain forever, so a drain has a deadline, `drain_deadline_s` after it began (default 900, 15 minutes: the window a
working seat has to finish on its own). The first evaluation that reads `drain: true` records
`seat_pool_drain_started` with its `deadlineAt`; setting `drain: false` before then records `seat_pool_drain_ended`,
and the seats go back to admission, and a later `drain: true` starts a new deadline. The deadline is read back from
the job's history, so a daemon restart doesn't reset it. `drain_deadline_s: 0` keeps the drain without a deadline.
The deadline is fixed when the drain starts: changing `drain_deadline_s` during a drain applies to the next one. To
start over, set `drain: false`, wait until `seat_pool_drain_ended` is recorded (or `rig watchdog show <jobId> --json`
shows `seatPool.drain.startedAt` as null), and only then set `drain: true` again: the end is recorded by the first
evaluation that reads `drain: false`, so a `true` set before that keeps the old deadline, or, once its pass ran, no
pass at all. Only a pool holding a seat it added starts the clock; a pool
already set to `drain: true` when the daemon is upgraded starts it at its first evaluation after the restart.
The deadline is at most 86400 (1 day), the shortest history retention the settings allow, so the history keeps the
drain's start until its deadline passes. A drain left open for longer than the retention window (14 days by default,
with 50 or more newer entries for the job) can lose its entries: it then starts a new deadline, the forced pass runs
again, and `rig watchdog stop` refuses until that pass is recorded.

Once the deadline passes, the pool takes out each seat it added that is still there, once, in the order a reap uses,
under the seat's lifecycle lease:

1. `release_cmd`, if set, with `OPENRIG_SEAT_POOL_RELEASE_REASON=drain-deadline`. In this pass the seat's open rows go
   to the model after its stop (step 3), so the release command doesn't need to refuse for them; it still refuses when
   the seat's work may not be saved. When it refuses (any exit code but the ones below), the seat is **not** stopped:
   it keeps running, with its rows, and is left (`why: release_refused`). When it doesn't answer (exit 3, exit 5, or
   past its `timeout_s`), the seat is asked again on the next evaluations, for up to 10 minutes after the deadline, and
   then left (`why: release_unavailable`); the pass isn't through until then (`seat_pool_drain_retrying` records what it
   did so far, so a restart carries on with it). A release command that answers 3 to every seat (a stop switch it
   honours) therefore leaves every seat. After the release, the pass reads the spec again: with `drain: false` set
   while the command ran, the seat is not stopped and goes back to admission. It also reads the seat's rows again: the
   lease holds deliveries back, not the queue, so a turn already under way can claim a row while the command runs. A
   row claimed since the command was asked means the seat is working on something the command never answered for: it
   is not stopped, keeps its rows, and is left (`why: work_arrived`). A pending row that only arrived goes to the model
   with the others.
2. A [checked stop](checked-seat-stop.md), with the `expected` read at the start; a refused stop leaves the seat
   (`why: stop_failed`), its rows unmoved: back in admission while its session runs, draining once it is gone (a stop
   that failed after the kill, or a seat the pass had stopped before), so the reap does not launch it again.
3. Each open row goes to the model: an in-progress row is unclaimed first, and each row moves only while it is still
   addressed to the seat, in one transaction, keeping its id and its state (a pending row wakes the model; a blocked
   one keeps what wakes it, as a reap moves it, and only one nothing would wake goes back to pending). The reroute's reason is `drain:<seat>`, not `reap:<seat>`, so a drained row is not counted as one
   that lost its seat. A row claimed, closed or moved since it was read is left as it went.
4. The node is removed and `remove_cmd` runs; when `remove_cmd` refuses, the seat stays on record
   (`why: remove_cmd_failed`).

Once a seat is stopped, a failure that follows (a row that won't move: `why: move_failed`; a removal refused:
`why: remove_failed`) leaves it draining, with the step it was at, never back in admission, where the reap would launch
it again. Its entry in `left` carries `stopped: true`, and it stays draining after the pass too, whatever its
session's record reads (a stop that failed after the kill can leave it reading running). Any other seat stopped on its
way out once the pass is through (a free seat the plain drain stopped and a restart cut short) is the plain drain's
again. A pass cut short after the stop (a restart) carries on from that step: a seat whose session is gone is not
asked `release_cmd` again; one whose session is running is.

Messages the typing guard holds for the seat are left as a reap leaves them. The pass records
`seat_pool_drain_expired`, once, with `left` (each seat it couldn't take out, why, and `exitCode`, the release
command's in that pass, null when it did not run) and `moved` (each row and where it went, on every evaluation of the
pass). A seat left for good (refused, or a failed stop, move or removal) is not asked or stopped again in that pass.
An adopted seat is never stopped or removed by the deadline, and its rows are never moved. A pool shrinking by
`idle_grace_s` without `drain` has no deadline.

`rig watchdog stop` refuses a pool that still holds seats (`pool_has_slots`), and says when the drain's deadline
passes. Once `seat_pool_drain_expired` is recorded for the current drain, it stops a pool whose remaining seats are all
listed in that entry's `left`: in the same transaction those seats are retired (`seat_pool_left`, one per seat) and the
answer lists them in `leftRunning` (`slot`, `sessionName`, `why`), for you to deal with by hand. It still refuses
without `drain`, before the deadline, with `drain_deadline_s: 0`, or when a seat appeared after the pass. An adopted
seat never holds the stop of a draining pool.

To prove a pool is empty, read the pool, not the rig's nodes: `rig watchdog show <jobId> --json` carries `seatPool`,
with `drain` (`startedAt`, `deadlineAt`, `expired`) and `slots` (`logicalId`, `phase`, `adopted`, `nodeExists`,
`openRows`), every seat the pool still holds, from `seat_pool_slots`. A seat whose node is gone but whose `remove_cmd`
refused is still listed. The pool is empty when `seatPool.slots` is `[]`, or once the stop is accepted with its
`leftRunning`.

A pool can still end with seats in it: a daemon that doesn't know the `seat-pool` policy, such as an earlier version
started on the same `OPENRIG_HOME`, marks the job terminal (`unknown_policy:seat-pool`), and the job can't be updated
or restarted. Its seats keep running and holding their rows, and no pool holds them any more. Register a new pool with
those seats in `adopt` to take them over: a seat the ended pool added stays a seat a pool added, which the new pool
drains and reaps, and only a seat that was there before any pool is treated as adopted. Drain the pool before going
back to such a version to avoid this.

## Restarts and restores

The pool records each seat in `seat_pool_slots`, and records each step on the seat before it runs. After a daemon
restart it resumes from those records: an add is resolved through its Operation-Id, a seat that was added but not
launched is prepared and launched (or removed if it is no longer wanted), and a drain finishes. A seat whose launch
completed but wasn't recorded is recognized by its running session. The pool acts only on seats it created or adopted
(`adopt`); it never takes a node over because of its name. While the rig is being restored, the pool does nothing.

An add, a launch, a drain or a reap the pool waits on for more than 10 minutes is recorded as failed with
`code: effect_timeout`, once, and the pool takes no new plan until that effect settles, so two runs never act on the
same seat. A daemon that stops cancels the pool's runs without waiting for such an effect, or for one in flight; the
step recorded on the seat is resumed at the next start.

A seat the pool added whose session died is reaped, unless its release command refuses (see
[Reaping a seat](#reaping-a-seat)); an adopted one is only launched again.

## Seeing a pool

`rig ps --nodes` lists each seat a pool holds right after its model seat, marked `└` in the SESSION column (MEMBER
with `--full`). With `--json` the daemon's order is kept, and such a seat carries `seatPoolModel`, the model's logical
id (also selectable with `--fields seatPoolModel`). The link is the pool's `seat_pool_slots` row, as it is for the pool
itself: an adopted seat is listed under the model whatever its name, and a node named like a pool seat that no pool
holds is not. A seat whose model isn't listed, because `--session`, `--filter` or `--limit` left the model out, keeps
its place unmarked.

The TUI draws the seats the same way under the model, in the topology tree and in the seat tables, within the model's
pod: an adopted seat in another pod stays in its own pod, unmarked.

## Limits

- A pool works only on the daemon it's registered with: its model, its seats and the rows it reads all belong to that
  daemon. To grow seats on two hosts, register a pool on each. The pools don't coordinate, so whatever hands out the
  work decides which host's seat takes it.
- A pool has one model, and a seat belongs to one pool at a time. The pool's own seats are members of the model's pod;
  an adopted seat stays in the pod it is in.
