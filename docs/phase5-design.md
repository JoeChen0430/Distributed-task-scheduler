# Phase 5 Design — Benchmark

> Status: done — see `docs/phase5-results.md` for the measured numbers and analysis.
> Phase 5 measures the engine's performance and names its bottlenecks.

## Goal

Turn "I built a scheduler" into "I measured it: ~X tasks/sec, scales to N workers,
then plateaus on Y." The value is the *shape* of the scaling curve and the bottleneck
analysis — not absolute numbers on one laptop.

## Metrics

1. **Throughput** — tasks completed per second for a large workload
   (`task_count / makespan`, where makespan = max(finished_at) − run start).
2. **Scheduling latency** — time from a task being runnable to a worker starting it.
   Computed from the timestamps already stored (`created_at`, `started_at`,
   `finished_at`); reported as p50 / p95 / p99 (tail latency, not just the mean).
3. **Scalability** — the same workload run with 1 / 2 / 4 / 8 workers; throughput vs
   worker count, to find where it stops scaling.

## Approach

- **No-op handlers** (`bench_noop`, returns immediately) so we measure the
  *scheduler's* overhead, not fake work.
- Two DAG shapes, each stressing a different thing:
  - **Wide / flat**: N independent tasks → throughput, parallelism, claim contention.
  - **Chain / linear**: N tasks in a line → scheduling latency; directly exposes the
    `poll_interval` cost (each step waits ~one poll).
- Drive the existing engine via `scheduler.run_dag(pool, id, poll_interval, n_workers)`
  — `n_workers` already exists, so **no engine changes are needed**. Sweep
  `n_workers ∈ {1,2,4,8}` and a couple of `poll_interval` values.
- An analysis step reads the run's timestamps and computes makespan, throughput, and
  latency percentiles.
- **Output: a markdown results table** written to `docs/phase5-results.md` (no
  plotting dependency).

## Files

- `benchmark/benchmark.py` — load generator (wide + chain DAG builders, `bench_noop`
  handler), a runner that sweeps workers/poll_interval via `run_dag`, and a
  timestamp-based analysis that prints/writes the results table. Kept out of `src/`.
- `docs/phase5-results.md` — the measured tables + the bottleneck write-up.
- No changes to `src/` (Phase 5 only drives the engine and reads timestamps).

## What it should reveal (the analysis)

- **poll_interval is a latency floor**: chain end-to-end ≈ N × (poll_interval +
  overhead); halving poll_interval roughly halves it.
- **Postgres is the throughput ceiling**: throughput rises with workers then flattens —
  every claim/enqueue/status update is a DB round-trip, plus lock contention on the
  `tasks` table.
- **the single dispatcher caps scaling**: computing ready + enqueuing is serial work
  through one dispatcher, so adding workers eventually stops helping.
- **Redis is not the bottleneck**: `BRPOP`/`LPUSH` are cheap — the benchmark should
  confirm the limit is the DB, not the queue.

## Honesty / setup to report

Single laptop, Dockerized Postgres + Redis, in-process `run_dag` workers, no-op
handlers. Not a rigorous distributed benchmark — report the setup alongside numbers,
and lead with the scaling shape + bottleneck, not the absolute tasks/sec.

## Milestones

1. **Generator + analysis**: wide + chain builders, `bench_noop`, run one workload,
   print makespan / throughput / latency percentiles from timestamps.
2. **Sweep**: run {1,2,4,8} workers (and 2 poll_interval values); collect a table.
3. **Write-up**: `docs/phase5-results.md` (tables + bottleneck analysis) + a short
   README "Benchmark" section.

## Out of scope

Charts/plots (text tables only), a true multi-process benchmark (in-process worker
count is the control), rigorous statistical runs / warmup, comparing against real
Airflow/Celery.
