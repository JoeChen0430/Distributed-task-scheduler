"""
Every SQL statement in the project lives here. Nothing else in src/ imports
asyncpg directly — scheduler.py and dag.py only call functions from this
module. That boundary matters later: in Phase 3, when the queue moves to
Redis, this is the only file that should need to change shape.
"""

import json

import asyncpg

from src import config

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=config.DATABASE_URL, min_size=2, max_size=10)
    return _pool


async def create_dag_run(pool: asyncpg.Pool, name: str) -> int:
    row = await pool.fetchrow(
        "INSERT INTO dag_runs (name) VALUES ($1) RETURNING id",
        name,
    )
    return row["id"]


async def create_task(pool: asyncpg.Pool, dag_run_id: int, name: str, task_type: str) -> int:
    row = await pool.fetchrow(
        """
        INSERT INTO tasks (dag_run_id, name, task_type)
        VALUES ($1, $2, $3)
        RETURNING id
        """,
        dag_run_id, name, task_type,
    )
    return row["id"]


async def create_dependency(pool: asyncpg.Pool, task_id: int, depends_on_task_id: int) -> None:
    await pool.execute(
        "INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES ($1, $2)",
        task_id, depends_on_task_id,
    )


async def fetch_dag_state(pool: asyncpg.Pool, dag_run_id: int) -> tuple[list[dict], list[dict]]:
    """Everything the scheduler needs to decide what to do next, in two lists."""
    task_rows = await pool.fetch(
        "SELECT id, name, task_type, status FROM tasks WHERE dag_run_id = $1",
        dag_run_id,
    )
    dep_rows = await pool.fetch(
        """
        SELECT d.task_id, d.depends_on_task_id
        FROM task_dependencies d
        JOIN tasks t ON t.id = d.task_id
        WHERE t.dag_run_id = $1
        """,
        dag_run_id,
    )
    return [dict(r) for r in task_rows], [dict(r) for r in dep_rows]


async def claim_task(pool: asyncpg.Pool, task_id: int) -> dict | None:
    """
    Atomically flip a task from pending -> running, and only return it if
    THIS call was the one that did the flipping.

    Why not just "SELECT the task, check it's pending, then run it"? Because
    between the SELECT and the UPDATE, another worker could do the exact same
    thing — that's a race condition, and it's the single most common bug in
    homemade schedulers. The WHERE status = 'pending' inside the UPDATE makes
    the check-and-flip one atomic operation instead of two separate steps.

    Right now there's only ever one worker (this process), so the race can't
    actually happen yet — but writing it this way means Phase 3 (multiple
    worker processes) needs zero changes here to stay correct.
    """
    row = await pool.fetchrow(
        """
        UPDATE tasks
        SET status = 'running', started_at = now()
        WHERE id = $1 AND status = 'pending'
        RETURNING id, name, task_type, status
        """,
        task_id,
    )
    return dict(row) if row else None


async def mark_task_success(pool: asyncpg.Pool, task_id: int, result: dict) -> None:
    await pool.execute(
        """
        UPDATE tasks
        SET status = 'success', result = $2::jsonb, finished_at = now()
        WHERE id = $1
        """,
        task_id, json.dumps(result),
    )


async def mark_task_failed(pool: asyncpg.Pool, task_id: int, error: str) -> None:
    await pool.execute(
        """
        UPDATE tasks
        SET status = 'failed', error = $2, finished_at = now()
        WHERE id = $1
        """,
        task_id, error,
    )
