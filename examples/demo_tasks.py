"""
Shared task handlers for the true multi-process demo (create_demo_dag.py +
run_worker.py). Kept in one module so BOTH the DAG-creating process and every worker
process import the same handler definitions.

This is the crux of a distributed scheduler: the Redis queue only carries task ids,
and Postgres only stores task_type strings — the actual handler CODE is not shipped
over the wire. Every worker process must import these definitions itself (that's what
run_worker.py does), exactly like an Airflow/Celery worker imports the same task code.
"""

import asyncio

from src.task_registry import register_task


@register_task("demo_extract")
async def demo_extract(ctx: dict) -> dict:
    await asyncio.sleep(0.3)
    return {"rows": 100}


@register_task("demo_a")
async def demo_a(ctx: dict) -> dict:
    print("      demo_a working (~1s)")
    await asyncio.sleep(1)
    return {"a": 1}


@register_task("demo_b")
async def demo_b(ctx: dict) -> dict:
    print("      demo_b working (~1s)")
    await asyncio.sleep(1)
    return {"b": 1}


@register_task("demo_join")
async def demo_join(ctx: dict) -> dict:
    await asyncio.sleep(0.3)
    return {"joined": True}


# A diamond: extract fans out to two branches that run in parallel, then join.
DEMO_TASK_DEFS = [
    {"name": "extract", "task_type": "demo_extract", "depends_on": []},
    {"name": "a",       "task_type": "demo_a",       "depends_on": ["extract"]},
    {"name": "b",       "task_type": "demo_b",       "depends_on": ["extract"]},
    {"name": "join",    "task_type": "demo_join",    "depends_on": ["a", "b"]},
]
