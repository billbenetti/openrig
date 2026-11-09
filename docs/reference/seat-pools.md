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
waiting (see [Waiting rows](#waiting-rows-dispatch-and-wake)). A seat of the pool that stops working is reaped instead
(see [Reaping a seat](#reaping-a-seat)).

## The spec

```yaml
policy: seat-pool
context:
  max: 4                    # required: the most extra seats
  min: 0                    # the fewest extra seats (default 0)
  cooldown_s: 60            # wait after an add or removal before the next one (default 60)
  idle_grace_s: 300         # how long a seat must sit idle before it is drained (default 300)
  dispatch_after_s: 60      # how long a row waits at its seat before the pool moves it or wakes the seat (default 60)
  stuck_after_s: 14400      # how long a live seat may go without an activity change before it is reaped (default: never)
  drain: false              # true empties the pool (see below)
  adopt: [dev.impl2]        # existing seats the pool takes over
  cwd_template: /work/{rig}/{slot}   # required: absolute, with {slot}; {rig} is optional
  demand_cmd:  { argv: [/usr/local/bin/pool-demand], timeout_s: 30 }
  prepare_cmd: { argv: [/usr/local/bin/pool-prepare], timeout_s: 120 }
  release_cmd: { argv: [/usr/local/bin/pool-release], timeout_s: 60 }
  reap_cmd:    { argv: [/usr/local/bin/pool-reap], timeout_s: 60 }
```

Registration and `rig watchdog update --spec` refuse a spec with an unknown field, `max` below `min`, a relative
`cwd_template` or one without `{slot}`, a `stuck_after_s` that isn't a positive whole number, or a command that isn't an
argv. Commands run without a shell, with the
daemon's environment plus:

| Variable | Set for | Value |
| --- | --- | --- |
| `OPENRIG_SEAT_POOL_JOB` | every command | the job id |
| `OPENRIG_SEAT_POOL_MODEL` | every command | the model's session name |
| `OPENRIG_SEAT_POOL_SLOT` | `prepare_cmd`, `release_cmd`, `reap_cmd` | the seat's session name |
| `OPENRIG_SEAT_POOL_SLOT_CWD` | `prepare_cmd` | the seat's working directory |
| `OPENRIG_SEAT_POOL_SLOTS` | `demand_cmd` | JSON list of `{session, phase}` for the pool's seats |

A command that runs past `timeout_s` is killed and counts as failed.

## How many seats

The pool wants as many extra seats as its demand, clamped to `min`..`max`, and never fewer than the seats holding work
plus the seats still starting. Without `demand_cmd`, the demand is one seat per waiting row that no free seat takes
(see below), plus the seats holding work. With `demand_cmd`, it is the `desired` whole number the command prints as JSON
(`{"desired": 3}`). The command runs after each evaluation, and its answer is used for the next one. When it fails or
its last good answer is older than two evaluation intervals, the pool holds as it is: it finishes starting the seats
it has begun, and adds or drains nothing.

A seat holds work when it has a pending, in-progress or blocked row, a message the typing guard holds for it, or isn't
idle at a prompt. Only a seat with no open row and no held message that has been idle at a prompt, with no pending input
and no background shell, for `idle_grace_s` is drained; the pool's own seats go before adopted ones, newest first.

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

A waiting row the pool doesn't move wakes the seat holding it (the model, or a seat of the pool), by why that seat
isn't taking it:

| The seat | The pool |
| --- | --- |
| has no live session | launches the model, as `rig launch` does; reaps a seat of the pool (see [Reaping a seat](#reaping-a-seat)) |
| is idle at its prompt | nudges it about the row, once per row and seat; the row records the nudge (`seat-pool nudge: <seat>`) before it is sent, so a restart never sends a second |
| needs input (a permission prompt or a question) | opens one row to the model's escalation owner, tagged `seat-pool-needs-input:<seat>`; no other is opened about that seat while it is open |
| is out of provider usage | nothing: the row waits |
| is working | nothing: the row waits, and counts as demand if it is the model's |

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
comes between the steps, checks that it still has no open row and no held message, runs `release_cmd`, checks again
(the lease holds deliveries back, but a row can still be created while the command runs), stops it with a
[checked stop](checked-seat-stop.md) and removes its node with the same `expected`. If work arrived, or `release_cmd`
exits non-zero, the seat goes back into the pool untouched. The removal never passes a fallback: a seat that still has
rows is not removed.

Every add, launch, removal, row moved (`seat_pool_dispatched`, or `seat_pool_dispatch_skipped` when the row was claimed
or moved first), relaunch, nudge, escalation, refusal and failure is written to the job's history
(`rig watchdog status <jobId>`). An evaluation that plans changes records the plan (`seat_pool_planned`), and one that
holds because the model seat is gone, the rig is being restored, the demand answer is stale or a seat is out of usage
records why (`seat_pool_held`). One with nothing to change records nothing.

## Reaping a seat

A seat of the pool that stops working is reaped: its rows go back to the model and its node is removed, so the pool can
add a fresh seat in its place. The pool reaps a live seat (one it launched or adopted)

- whose session is gone (it exited, crashed or was stopped), even while its account is out of usage, or
- with `stuck_after_s` set, whose activity hasn't changed for that long while it is out of provider usage, waiting for
  input or working. Set it above the longest turn a seat may take: a seat that works without an activity change counts.

Everything happens under the seat's lifecycle lease, after the pool reads the seat again; a seat that recovered since
the plan is left alone. A live seat goes first through the release command, as a drained seat does, and is stopped with
a [checked stop](checked-seat-stop.md) only if `release_cmd` exits 0 and the seat is still stuck afterwards. Without a
`release_cmd`, or when it refuses, the seat is **quarantined**: it is neither stopped nor removed, and one row tagged
`seat-pool-quarantine:<seat>` goes to the model's escalation owner with the seat's open rows. While that row is open the
pool leaves the seat as it is, even if its session ends or it becomes free: it neither reaps nor drains it, and gives it
no row. Once the row is closed and the seat is still stuck, or its session is gone, the pool tries again.

A seat whose session is gone, or that was just stopped, is then reaped in this order, and a failed step stops the reap
there for the next plan to pick up:

1. `rig seat clean`, which checks in tmux that the session is really gone; a live session stops the reap before anything
   is moved.
2. `reap_cmd`, if set, so whatever tracks the seat's work (an executor lease, a task claim) can release it. A non-zero
   exit stops the reap.
3. Each in-progress row addressed to the seat is unclaimed, and every open row is moved with `rig queue fallback`,
   `--expect-destination <the seat>` and the row's state expected, to the model, with a wake when the row is pending.
   A blocked row keeps its blocker; a one-shot `--wake-after` timer ends with the move, as with any reroute.
4. The node is removed with the `expected` read at the start of the reap, as a drain removes it.

Each move gives the reason `reap:<seat>`. A row that a reap already moved once (the pool counts the row's reroutes with
that reason) goes to the model's escalation owner instead of the model, so a row whose seats keep dying stops cycling
through the pool. With no escalation owner it goes to the model, and `seat_pool_escalation_failed` is recorded.

The history records `seat_pool_reaped` with the rows moved and where each went, `seat_pool_quarantined`, and
`seat_pool_reap_failed` with the step that failed. Reaping goes on under `drain` and while a seat is out of usage: it
adds and launches nothing.

## Stopping a pool: drain

`drain: true` is the kill switch. Set it with `rig watchdog update <jobId> --spec <file>`: the pool adds nothing,
removes the seats that haven't started, and drains the rest as each becomes free, so no work is killed. `rig watchdog
stop` refuses a pool that still holds seats (`pool_has_slots`); stop it once its seats are gone.

## Restarts and restores

The pool records each seat in `seat_pool_slots`, and records each step on the seat before it runs. After a daemon
restart it resumes from those records: an add is resolved through its Operation-Id, a seat that was added but not
launched is prepared and launched (or removed if it is no longer wanted), and a drain finishes. A seat whose launch
completed but wasn't recorded is recognized by its running session. The pool acts only on seats it created or adopted
(`adopt`); it never takes a node over because of its name. While the rig is being restored, the pool does nothing.

A seat of the pool whose session died is reaped, never relaunched.
