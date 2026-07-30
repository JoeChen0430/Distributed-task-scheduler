"""
A DAG that shows retry recovering from a transient failure.

    flaky (fails twice, succeeds on the 3rd attempt) -> finalize

`flaky` is registered with max_retries=3. It raises on its first two attempts and
succeeds on the third, so the scheduler retries it (with exponential backoff — 1s,
then 2s) instead of failing the DAG. Once it succeeds, `finalize` runs. Contrast
with examples/failing_dag.py, where the failure is permanent and descendants get
blocked instead.

Run from the project root (after `docker compose up -d` and `python -m scripts.migrate`):
    python -m examples.retry_dag
"""

import asyncio

from src import db, scheduler
from src.dag import create_dag
from src.task_registry import register_task

# Module-level counter so the handler "remembers" how many times it's been tried
# across separate scheduler invocations (single process, so a plain global works).
_attempts = {"flaky": 0}


@register_task("flaky")
async def flaky(ctx: dict) -> dict:
    _attempts["flaky"] += 1
    n = _attempts["flaky"]
    print(f"    (flaky attempt #{n})")
    await asyncio.sleep(0.2)
    if n < 3:
        raise RuntimeError(f"transient glitch on attempt {n}")
    return {"succeeded_on_attempt": n}


@register_task("finalize")
async def finalize(ctx: dict) -> dict:
    print("    (finalizing now that flaky recovered...)")
    await asyncio.sleep(0.2)
    return {"done": True}


async def main() -> None:
    pool = await db.get_pool()

    dag_run_id = await create_dag(
        pool,
        dag_name="retry_demo",
        task_defs=[
            {"name": "flaky",    "task_type": "flaky",    "depends_on": [],        "max_retries": 3},
            {"name": "finalize", "task_type": "finalize", "depends_on": ["flaky"]},
        ],
    )
    print(f"Created dag_run id={dag_run_id}\n")

    await scheduler.run_dag(pool, dag_run_id, poll_interval=0.5)


if __name__ == "__main__":
    asyncio.run(main())
