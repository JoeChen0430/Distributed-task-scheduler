"""
A 4-step DAG: extract -> transform -> validate -> load.

Each handler here is a stand-in (just sleeps and returns a dict) so you can
watch the scheduling machinery work without worrying about real ETL logic.
Swap these out for real work whenever you want.

Run from the project root (after `docker compose up -d` and `python -m scripts.migrate`):
    python -m examples.etl_dag
"""

import asyncio

from src import db, scheduler
from src.dag import create_dag
from src.task_registry import register_task


@register_task("extract")
async def extract(ctx: dict) -> dict:
    print("    (extracting rows from source system...)")
    await asyncio.sleep(1)
    return {"rows_extracted": 1000}


@register_task("transform")
async def transform(ctx: dict) -> dict:
    print("    (cleaning + reshaping rows...)")
    await asyncio.sleep(1)
    return {"rows_transformed": 980}


@register_task("validate")
async def validate(ctx: dict) -> dict:
    print("    (checking row counts + schema...)")
    await asyncio.sleep(1)
    return {"valid": True}


@register_task("load")
async def load(ctx: dict) -> dict:
    print("    (writing to destination table...)")
    await asyncio.sleep(1)
    return {"rows_loaded": 980}


async def main() -> None:
    pool = await db.get_pool()

    dag_run_id = await create_dag(
        pool,
        dag_name="sample_etl",
        task_defs=[
            {"name": "extract",   "task_type": "extract",   "depends_on": []},
            {"name": "transform", "task_type": "transform", "depends_on": ["extract"]},
            {"name": "validate",  "task_type": "validate",  "depends_on": ["transform"]},
            {"name": "load",      "task_type": "load",      "depends_on": ["validate"]},
        ],
    )
    print(f"Created dag_run id={dag_run_id}\n")

    await scheduler.run_dag(pool, dag_run_id, poll_interval=0.5)


if __name__ == "__main__":
    asyncio.run(main())
