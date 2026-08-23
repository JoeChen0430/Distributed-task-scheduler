"""
Integration tests — these DO need the Docker Postgres + Redis running
(`docker compose up -d`), unlike the pure tests in test_graph.py / test_retry.py.
If the services aren't reachable, the whole module skips, so `pytest tests/` still
runs the pure suite with nothing running.

They exercise the multi-worker guarantees that can't be unit-tested: no
double-execution across workers, and reaper reclamation of a dead worker's task.
"""

import asyncio

import pytest
import pytest_asyncio

from src import db, queue, scheduler
from src.dag import create_dag
from src.task_registry import register_task

# --- handlers used only by these tests (unique task_type names) ---------------

_run_counts: dict[str, int] = {}


@register_task("it_noop")
async def _it_noop(ctx: dict) -> dict:
    await asyncio.sleep(0.02)
    return {"ok": True}


@register_task("it_counter")
async def _it_counter(ctx: dict) -> dict:
    # Count how many times the handler actually runs, to detect double-execution.
    _run_counts["counter"] = _run_counts.get("counter", 0) + 1
    await asyncio.sleep(0.05)
    return {}


@pytest_asyncio.fixture
async def pool():
    """Fresh pool + Redis client on THIS test's event loop; skip if unavailable.

    asyncpg pools and the redis client are bound to the loop they're created on, and
    pytest-asyncio gives each test its own loop — so we reset the module globals,
    build them on this loop, and tear them down at the end of the test.
    """
    db._pool = None
    queue._client = None
    try:
        p = await db.get_pool()
        await p.execute("SELECT 1")
        client = await queue.get_client()
        await client.delete(queue.READY_QUEUE)  # don't inherit another test's ids
    except Exception as exc:  # Postgres or Redis not up
        pytest.skip(f"integration services not available: {exc}")
    try:
        yield p
    finally:
        await queue.aclose()
        await p.close()
        db._pool = None


async def test_dag_runs_to_completion(pool):
    dag_run_id = await create_dag(
        pool,
        "it_basic",
        [
            {"name": "a", "task_type": "it_noop", "depends_on": []},
            {"name": "b", "task_type": "it_noop", "depends_on": ["a"]},
        ],
    )
    await scheduler.run_dag(pool, dag_run_id, poll_interval=0.05, n_workers=2)

    tasks, _ = await db.fetch_dag_state(pool, dag_run_id)
    assert {t["name"]: t["status"] for t in tasks} == {"a": "success", "b": "success"}


async def test_no_double_execution_across_workers(pool):
    _run_counts["counter"] = 0
    dag_run_id = await create_dag(
        pool,
        "it_once",
        [{"name": "only", "task_type": "it_counter", "depends_on": []}],
    )
    # 3 workers race for the single task; the atomic claim must let exactly one win.
    await scheduler.run_dag(pool, dag_run_id, poll_interval=0.05, n_workers=3)

    assert _run_counts["counter"] == 1


async def test_reaper_reclaims_orphaned_task(pool):
    dag_run_id = await create_dag(
        pool,
        "it_reap",
        [{"name": "t", "task_type": "it_noop", "depends_on": [], "max_retries": 2}],
    )
    row = await pool.fetchrow(
        "SELECT id FROM tasks WHERE dag_run_id = $1 AND name = 't'", dag_run_id
    )
    # Simulate a worker that died holding the task: running, lease already expired.
    await pool.execute(
        """
        UPDATE tasks
        SET status = 'running', worker_id = 'ghost',
            lease_expires_at = now() - interval '1 second'
        WHERE id = $1
        """,
        row["id"],
    )

    await scheduler.run_dag(pool, dag_run_id, poll_interval=0.1, n_workers=2)

    reclaimed = await pool.fetchrow(
        "SELECT status, retry_count FROM tasks WHERE id = $1", row["id"]
    )
    assert reclaimed["status"] == "success"
    assert reclaimed["retry_count"] == 1  # reclamation counted as one failed attempt
