"""
Phase 5 benchmark — measure the scheduler's throughput, scheduling latency, and how
it scales with workers.

Handlers are NO-OPs on purpose: we want to measure the scheduler's own overhead
(dispatch, atomic claim, DB round-trips, poll interval), not fake work. It drives the
existing engine via scheduler.run_dag — no engine changes; run_dag already takes
n_workers, so worker count is just a parameter.

Two DAG shapes stress different things:
  - wide  : N independent tasks       -> throughput, parallelism, claim contention
  - chain : N tasks in a line         -> scheduling latency, exposes poll_interval cost

Run from the project root (Postgres + Redis up, schema migrated):
    python -m benchmark.benchmark
"""

import asyncio
import contextlib
import io
import time

from src import db, scheduler
from src.dag import create_dag
from src.task_registry import register_task


@register_task("bench_noop")
async def bench_noop(ctx: dict) -> dict:
    return {}


def wide_defs(n: int) -> list[dict]:
    """N independent tasks — all ready at once."""
    return [{"name": f"t{i}", "task_type": "bench_noop", "depends_on": []} for i in range(n)]


def chain_defs(n: int) -> list[dict]:
    """N tasks in a line: t0 -> t1 -> ... -> t{n-1}."""
    return [
        {
            "name": f"t{i}",
            "task_type": "bench_noop",
            "depends_on": [f"t{i - 1}"] if i else [],
        }
        for i in range(n)
    ]


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = min(len(s) - 1, int(round((p / 100) * (len(s) - 1))))
    return s[k]


async def _analyze(pool, dag_run_id: int) -> dict:
    rows = await pool.fetch(
        "SELECT created_at, started_at, finished_at FROM tasks WHERE dag_run_id = $1",
        dag_run_id,
    )
    # Per-task creation->start gap (ms). For a WIDE dag this is scheduling latency
    # (every task is ready immediately); for a chain it's dominated by waiting on the
    # parent, so read the chain's story from makespan/throughput instead.
    waits = [
        (r["started_at"] - r["created_at"]).total_seconds() * 1000
        for r in rows
        if r["started_at"] is not None
    ]
    return {"p50_ms": _pct(waits, 50), "p95_ms": _pct(waits, 95), "p99_ms": _pct(waits, 99)}


async def run_one(pool, shape: str, n: int, n_workers: int, poll_interval: float = 0.05) -> dict:
    """Build one DAG of the given shape/size, run it, and return timing stats."""
    defs = wide_defs(n) if shape == "wide" else chain_defs(n)
    dag_run_id = await create_dag(pool, f"bench_{shape}_{n}_{n_workers}w", defs)

    # Silence the engine's per-task prints: they're noise here and, more importantly,
    # terminal I/O would pollute the timing. No engine change — just redirect stdout
    # for the duration of the run (single-threaded asyncio, so it covers the workers).
    start = time.perf_counter()
    with contextlib.redirect_stdout(io.StringIO()):
        await scheduler.run_dag(pool, dag_run_id, poll_interval=poll_interval, n_workers=n_workers)
    wall = time.perf_counter() - start

    stats = await _analyze(pool, dag_run_id)
    stats.update(shape=shape, n=n, workers=n_workers, poll=poll_interval,
                 wall_s=wall, tps=n / wall)
    return stats


def format_table(results: list[dict]) -> str:
    header = (
        f"{'shape':6} {'N':>5} {'workers':>7} {'poll':>5} "
        f"{'wall(s)':>8} {'tasks/s':>8} {'p50ms':>6} {'p95ms':>6} {'p99ms':>6}"
    )
    lines = [header, "-" * len(header)]
    for r in results:
        lines.append(
            f"{r['shape']:6} {r['n']:>5} {r['workers']:>7} {r['poll']:>5} "
            f"{r['wall_s']:>8.2f} {r['tps']:>8.1f} "
            f"{r['p50_ms']:>6.0f} {r['p95_ms']:>6.0f} {r['p99_ms']:>6.0f}"
        )
    return "\n".join(lines)


async def main() -> None:
    pool = await db.get_pool()

    # Sweep 1 — wide DAG: throughput vs worker count (does it scale?).
    print("Wide DAG — throughput vs workers (N=200, poll=0.02):")
    wide = [await run_one(pool, "wide", 200, w, 0.02) for w in (1, 2, 4, 8)]
    print(format_table(wide))

    # Sweep 2 — chain DAG: poll_interval as a scheduling-latency floor.
    print("\nChain DAG — poll_interval as a latency floor (N=12, workers=2):")
    chain = [await run_one(pool, "chain", 12, 2, p) for p in (0.02, 0.1, 0.5)]
    print(format_table(chain))


if __name__ == "__main__":
    asyncio.run(main())
