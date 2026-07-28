"""
A DAG where one task fails on purpose, to show Phase 2 failure propagation.

    extract -> transform (RAISES) -> validate -> load

In Phase 1 this would hang forever: transform fails, validate/load stay PENDING,
and the scheduler loop never exits (you'd Ctrl+C). In Phase 2, validate and load
are marked BLOCKED, the DAG reaches a finished state, and run_dag returns on its
own.

Run from the project root (after `docker compose up -d` and `python -m scripts.migrate`):
    python -m examples.failing_dag
"""

import asyncio

from src import db, scheduler
from src.dag import create_dag
from src.task_registry import register_task


@register_task("extract_ok")
async def extract_ok(ctx: dict) -> dict:
    print("    (extracting rows...)")
    await asyncio.sleep(0.5)
    return {"rows_extracted": 1000}


@register_task("transform_boom")
async def transform_boom(ctx: dict) -> dict:
    print("    (transforming... about to hit bad data)")
    await asyncio.sleep(0.5)
    raise ValueError("row 42 has a null where a number was required")


@register_task("validate_ok")
async def validate_ok(ctx: dict) -> dict:
    # Never runs — it depends (transitively) on the failing transform.
    return {"valid": True}


@register_task("load_ok")
async def load_ok(ctx: dict) -> dict:
    # Never runs either.
    return {"rows_loaded": 0}


async def main() -> None:
    pool = await db.get_pool()

    dag_run_id = await create_dag(
        pool,
        dag_name="failing_etl",
        task_defs=[
            {"name": "extract",   "task_type": "extract_ok",     "depends_on": []},
            {"name": "transform", "task_type": "transform_boom", "depends_on": ["extract"]},
            {"name": "validate",  "task_type": "validate_ok",    "depends_on": ["transform"]},
            {"name": "load",      "task_type": "load_ok",        "depends_on": ["validate"]},
        ],
    )
    print(f"Created dag_run id={dag_run_id}\n")

    await scheduler.run_dag(pool, dag_run_id, poll_interval=0.5)


if __name__ == "__main__":
    asyncio.run(main())
