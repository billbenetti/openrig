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
seat. The model seat itself is never added, stopped or removed.

## The spec

```yaml
policy: seat-pool
context:
  max: 4                    # required: the most extra seats
  min: 0                    # the fewest extra seats (default 0)
  cooldown_s: 60            # wait after an add or removal before the next one (default 60)
  idle_grace_s: 300         # how long a seat must sit idle before it is drained (default 300)
  drain: false              # true empties the pool (see below)
  adopt: [dev.impl2]        # existing seats the pool takes over
  cwd_template: /work/{rig}/{slot}   # required: absolute, with {slot}; {rig} is optional
  demand_cmd:  { argv: [/usr/local/bin/pool-demand], timeout_s: 30 }
  prepare_cmd: { argv: [/usr/local/bin/pool-prepare], timeout_s: 120 }
  release_cmd: { argv: [/usr/local/bin/pool-release], timeout_s: 60 }
```

Registration and `rig watchdog update --spec` refuse a spec with an unknown field, `max` below `min`, a relative
`cwd_template` or one without `{slot}`, or a command that isn't an argv. Commands run without a shell, with the
daemon's environment plus:

| Variable | Set for | Value |
| --- | --- | --- |
| `OPENRIG_SEAT_POOL_JOB` | every command | the job id |
| `OPENRIG_SEAT_POOL_MODEL` | every command | the model's session name |
| `OPENRIG_SEAT_POOL_SLOT` | `prepare_cmd`, `release_cmd` | the seat's session name |
| `OPENRIG_SEAT_POOL_SLOT_CWD` | `prepare_cmd` | the seat's working directory |
| `OPENRIG_SEAT_POOL_SLOTS` | `demand_cmd` | JSON list of `{session, phase}` for the pool's seats |

A command that runs past `timeout_s` is killed and counts as failed.

## How many seats

The pool wants as many extra seats as its demand, clamped to `min`..`max`, and never fewer than the seats holding work
plus the seats still starting. Without `demand_cmd`, the demand is one seat per pending row addressed to the model,
plus the seats holding work. With `demand_cmd`, it is the `desired` whole number the command prints as JSON
(`{"desired": 3}`). The command runs after each evaluation, and its answer is used for the next one. When it fails or
its last good answer is older than two evaluation intervals, the pool holds as it is: it finishes starting the seats
it has begun, and adds or drains nothing.

A seat holds work when it has a pending, in-progress or blocked row, a message the typing guard holds for it, or isn't
idle at a prompt. Only a seat with no open row and no held message that has been idle at a prompt, with no pending input
and no background shell, for `idle_grace_s` is drained; the pool's own seats go before adopted ones, newest first.

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

Every add, launch, removal, refusal and failure is written to the job's history (`rig watchdog status <jobId>`). An
evaluation that plans changes records the plan (`seat_pool_planned`), and one that holds because the model seat is
gone, the rig is being restored or the demand answer is stale records why (`seat_pool_held`). One with nothing to
change records nothing.

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

A seat whose session died is left in place; the pool does not relaunch it.
