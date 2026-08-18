"""
A diamond DAG to show two workers running parallel branches at the same time.

    extract -> {branch_a, branch_b} -> join

extract fans out to two independent branches; with 2 workers, branch_a and branch_b
are claimed by DIFFERENT workers and run concurrently (watch the [w1]/[w2] tags and
note the two ~1s branches overlap instead of adding up). join waits for both.

Run from the project root (after `docker compose up -d` and `python -m scripts.migrate`):
    python -m examples.parallel_dag
"""

import asyncio

from src import db, scheduler
from src.dag import create_dag
from src.task_registry import register_task


@register_task("extract_p")
async def extract_p(ctx: dict) -> dict:
    await asyncio.sleep(0.3)
    return {"rows": 1000}


@register_task("branch_a")
async def branch_a(ctx: dict) -> dict:
    print("      branch_a working (~1s)")
    await asyncio.sleep(1)
    return {"a": True}


@register_task("branch_b")
async def branch_b(ctx: dict) -> dict:
    print("      branch_b working (~1s)")
    await asyncio.sleep(1)
    return {"b": True}


@register_task("join_p")
async def join_p(ctx: dict) -> dict:
    await asyncio.sleep(0.3)
    return {"joined": True}


async def main() -> None:
    pool = await db.get_pool()

    dag_run_id = await create_dag(
        pool,
        dag_name="parallel_demo",
        task_defs=[
            {"name": "extract",  "task_type": "extract_p", "depends_on": []},
            {"name": "branch_a", "task_type": "branch_a",  "depends_on": ["extract"]},
            {"name": "branch_b", "task_type": "branch_b",  "depends_on": ["extract"]},
            {"name": "join",     "task_type": "join_p",    "depends_on": ["branch_a", "branch_b"]},
        ],
    )
    print(f"Created dag_run id={dag_run_id}\n")

    await scheduler.run_dag(pool, dag_run_id, poll_interval=0.3, n_workers=2)


if __name__ == "__main__":
    asyncio.run(main())
