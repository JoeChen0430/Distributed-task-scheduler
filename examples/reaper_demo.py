"""
Simulate a worker crashing mid-task, and watch the reaper reclaim it.

    long_task (max_retries=2) -> after

We create the DAG, then directly force long_task into the state a crashed worker
leaves behind: status 'running', held by a fake worker 'ghost', with an
ALREADY-EXPIRED lease. Then we run the DAG normally. The dispatcher's reaper notices
the lapsed lease, counts it as one failed attempt, re-queues long_task, and a live
worker picks it up and runs it to completion — after which `after` runs.

This is the Phase 3 answer to "what if a worker dies holding a task?" — the piece the
in-process timeout could never solve (a dead process has no asyncio to cancel).

Run from the project root (after `docker compose up -d` and `python -m scripts.migrate`):
    python -m examples.reaper_demo
"""

import asyncio

from src import db, scheduler
from src.dag import create_dag
from src.task_registry import register_task


@register_task("long_task")
async def long_task(ctx: dict) -> dict:
    print("      long_task actually running now (on a live worker)")
    await asyncio.sleep(1)
    return {"ok": True}


@register_task("after")
async def after(ctx: dict) -> dict:
    return {"after": True}


async def main() -> None:
    pool = await db.get_pool()

    dag_run_id = await create_dag(
        pool,
        dag_name="reaper_demo",
        task_defs=[
            {"name": "long_task", "task_type": "long_task", "depends_on": [],           "max_retries": 2},
            {"name": "after",     "task_type": "after",     "depends_on": ["long_task"]},
        ],
    )
    print(f"Created dag_run id={dag_run_id}")

    # --- Simulate a crashed worker ---------------------------------------------
    # Put long_task in exactly the state a worker leaves it in when it dies mid-run:
    # 'running', owned by a worker that will never come back, lease already expired.
    row = await pool.fetchrow(
        "SELECT id FROM tasks WHERE dag_run_id = $1 AND name = 'long_task'", dag_run_id
    )
    await pool.execute(
        """
        UPDATE tasks
        SET status = 'running', worker_id = 'ghost',
            started_at = now() - interval '1 minute',
            lease_expires_at = now() - interval '1 second'
        WHERE id = $1
        """,
        row["id"],
    )
    print("Simulated: 'long_task' stuck in running, held by dead worker 'ghost'\n")

    await scheduler.run_dag(pool, dag_run_id, poll_interval=0.3, n_workers=2)


if __name__ == "__main__":
    asyncio.run(main())
