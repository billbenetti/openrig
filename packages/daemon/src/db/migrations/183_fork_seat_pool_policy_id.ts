import type { Migration } from "../migrate.js";

/**
 * Fork (T-018881): the fork's seat pool moves from the `seat-pool` id to `fork-seat-pool`, because the upstream's own
 * seat pool (#3495, policies/seat-pool.ts) took `seat-pool`. Every fork job registered under the old id moves, in its
 * `policy` column and in its spec's top-level `policy` (an update refuses a spec whose `policy` differs from the job's,
 * watchdog-job-update.ts), whatever its state, so a stopped or ended pool still reads as the fork's.
 *
 * An upstream job never moves: the upstream registers its pool jobs as `daemon@kernel` (seat-pool-registration.ts),
 * with `context.model_node_id` and `context.pool` in the spec, so a job with either mark stays `seat-pool`. The spec
 * is JSON (the VM's pools, registered by hand) or YAML (the CLI's `--spec` file, the upstream's registration):
 * - JSON: `$.policy` is set with json_set only when it says `seat-pool`, and `$.context.pool` marks an upstream job;
 * - YAML: a top-level `policy: seat-pool` line is rewritten in place, and a `  pool:` line under the context (how the
 *   upstream's stringifyYaml writes it) marks an upstream job.
 * Every json_* call that would throw on a non-JSON spec is inside a CASE on json_valid, which SQLite evaluates lazily.
 * Run again, it changes nothing: no fork job is left under `seat-pool`.
 *
 * No other table keeps the policy of a job: watchdog_history and fork_seat_pool_slots key on job_id, and only
 * emitted events and queue tags (`policy:<id>`) written before the move name the old id, as history.
 */
export const forkSeatPoolPolicyIdSchema: Migration = {
  name: "183_fork_seat_pool_policy_id.sql",
  sql: `
    UPDATE watchdog_jobs
       SET policy = 'fork-seat-pool',
           spec_yaml = CASE
             WHEN json_valid(spec_yaml) THEN
               CASE WHEN json_type(spec_yaml) = 'object' AND json_extract(spec_yaml, '$.policy') = 'seat-pool'
                    THEN json_set(spec_yaml, '$.policy', 'fork-seat-pool')
                    ELSE spec_yaml END
             WHEN spec_yaml = 'policy: seat-pool' THEN 'policy: fork-seat-pool'
             WHEN substr(spec_yaml, 1, 18) = 'policy: seat-pool' || char(10)
               THEN 'policy: fork-seat-pool' || substr(spec_yaml, 18)
             ELSE replace(spec_yaml, char(10) || 'policy: seat-pool' || char(10), char(10) || 'policy: fork-seat-pool' || char(10))
           END
     WHERE policy = 'seat-pool'
       AND registered_by_session <> 'daemon@kernel'
       AND CASE
             WHEN json_valid(spec_yaml) THEN
               CASE WHEN json_type(spec_yaml) = 'object'
                    THEN json_type(spec_yaml, '$.context.pool') IS NULL AND json_type(spec_yaml, '$.context.model_node_id') IS NULL
                    ELSE 1 END
             ELSE instr(spec_yaml, char(10) || '  pool:') = 0 AND instr(spec_yaml, char(10) || '  model_node_id:') = 0
           END;
  `,
};
