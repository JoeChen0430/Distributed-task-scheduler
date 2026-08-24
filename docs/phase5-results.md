# Phase 5 Results — Benchmark

Measured with `python -m benchmark.benchmark`. Handlers are no-ops, so these numbers
are the **scheduler's own overhead**, not task work.

**Setup (be honest about it):** single laptop (macOS), Postgres + Redis in Docker,
workers run as in-process coroutines via `scheduler.run_dag(n_workers=...)`, one run
per config (no warmup/averaging). Read the *shape* of the curves and the bottleneck,
not the absolute tasks/sec.

## Sweep 1 — wide DAG: throughput vs workers (N=200, poll=0.02)

```
workers   wall(s)   tasks/s
   1        0.48      416
   2        0.33      604
   4        0.39      515
   8        0.28      711
```

**Throughput does *not* scale with worker count** — it wobbles in a ~400–700 tasks/s
band with no clear upward trend. Why:

- The workers here are **coroutines in one process sharing one event loop**. With no-op
  handlers there's no real work to overlap, so extra workers don't add parallelism —
  they just interleave on the same serial resource: **DB round-trips**. Every task costs
  an `enqueue` + a `claim` + a `mark_success` (≈3 Postgres round-trips), and the single
  **dispatcher** enqueues serially.
- So the ceiling is *single-process + Postgres round-trips + one dispatcher*, not the
  number of workers. Adding in-process workers past ~2 mostly adds contention.
- Getting real scaling would mean running workers as **separate processes** (true
  parallelism) and easing the dispatcher/DB bottleneck (batch enqueues, `SELECT ... FOR
  UPDATE SKIP LOCKED` to claim in bulk). That's the interesting next optimization.

## Sweep 2 — chain DAG: poll_interval as a latency floor (N=12, workers=2)

```
poll(s)   wall(s)   tasks/s   p50 latency(ms)
 0.02      0.31      38.8        162
 0.10      1.35       8.9        725
 0.50      6.19       1.9       3171
```

**Crisp, linear result.** A chain forces one task per dispatcher poll, so makespan
tracks `N × poll_interval` almost exactly:

- poll 0.50 → 6.19s ≈ 12 × 0.50 = 6.0s
- poll 0.10 → 1.35s ≈ 12 × 0.10 = 1.2s
- poll 0.02 → 0.31s ≈ 12 × 0.02 = 0.24s (+ overhead)

The polling design puts a hard floor under scheduling latency: a task can't start until
the *next* dispatcher tick after its dependency finishes. Lowering `poll_interval` cuts
latency but raises dispatcher/DB load — the classic latency-vs-load knob. An
event-driven push (LISTEN/NOTIFY, or workers waking on the queue) would remove this
floor; polling was chosen for simplicity.

## Bottlenecks, ranked

1. **`poll_interval`** — the dominant cost for dependency chains / anything latency-
   sensitive (Sweep 2).
2. **Postgres round-trips + the single dispatcher** — the throughput ceiling for wide
   fan-out; more in-process workers don't help (Sweep 1).
3. **Redis is *not* a bottleneck** — `LPUSH`/`BRPOP` are cheap; the limit is the DB.

## Caveats

- The wide-DAG `p*` latency includes each task's `created_at → started_at` gap, which
  also captures `create_dag`'s serial inserts — treat it as directional, not pure
  scheduling latency.
- One run per config; expect run-to-run noise (visible in the non-monotonic wide row).
- In-process workers ≠ multi-core parallelism; a separate-process run would tell the
  scaling story more faithfully.

## Reproduce

```bash
docker compose up -d && python -m scripts.migrate   # if not already set up
python -m benchmark.benchmark
```
