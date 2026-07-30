"""
A DAG that shows timeout + retry + failure propagation composing together.

    slow (sleeps 3s, but timeout_seconds=1, max_retries=1) -> report

`slow` always runs longer than its 1s timeout, so every attempt is cancelled and
counted as a failure. With max_retries=1 it times out, backs off, times out again,
then fails for real — and `report` (which depends on it) is blocked. This is all
three Phase 2 features on one task:
    timeout -> retry (backoff) -> timeout -> retries exhausted -> failed -> block

Run from the project root (after `docker compose up -d` and `python -m scripts.migrate`):
    python -m examples.timeout_dag
"""

import asyncio

from src import db, scheduler
from src.dag import create_dag
from src.task_registry import register_task


@register_task("slow")
async def slow(ctx: dict) -> dict:
    # Deliberately longer than the task's timeout_seconds. The await is what lets
    # asyncio actually cancel it when the timeout fires (cooperative cancellation).
    print("    (slow task started, will sleep 3s...)")
    await asyncio.sleep(3)
    return {"never": "reached"}


@register_task("report")
async def report(ctx: dict) -> dict:
    # Never runs — its upstream 'slow' ultimately fails.
    return {"reported": True}


async def main() -> None:
    pool = await db.get_pool()

    dag_run_id = await create_dag(
        pool,
        dag_name="timeout_demo",
        task_defs=[
            {"name": "slow",   "task_type": "slow",   "depends_on": [],       "timeout_seconds": 1, "max_retries": 1},
            {"name": "report", "task_type": "report", "depends_on": ["slow"]},
        ],
    )
    print(f"Created dag_run id={dag_run_id}\n")

    await scheduler.run_dag(pool, dag_run_id, poll_interval=0.5)


if __name__ == "__main__":
    asyncio.run(main())
