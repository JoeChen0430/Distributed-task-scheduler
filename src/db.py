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


async def create_task(
    pool: asyncpg.Pool,
    dag_run_id: int,
    name: str,
    task_type: str,
    max_retries: int = 0,
    timeout_seconds: int | None = None,
) -> int:
    row = await pool.fetchrow(
        """
        INSERT INTO tasks (dag_run_id, name, task_type, max_retries, timeout_seconds)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        dag_run_id, name, task_type, max_retries, timeout_seconds,
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


async def enqueue_task(pool: asyncpg.Pool, task_id: int) -> bool:
    """Atomically move a ready task pending -> queued. Returns True iff THIS call
    won the flip (so the dispatcher knows to actually LPUSH the id to Redis).

    This is the dispatcher's half of the claim dance. compute_ready_tasks keeps
    returning a task every loop until it leaves 'pending', so this atomic transition
    is what guarantees each ready task is enqueued EXACTLY ONCE even if the dispatcher
    loops several times before a worker picks it up.

    The Phase 2 backoff gate lives here (not in claim_task) because the dispatcher is
    what decides when a retryable task becomes runnable again: a task awaiting a retry
    is 'pending' with a future next_retry_at, and this WHERE clause keeps it out of
    the queue until its backoff expires (measured against the DB clock).
    """
    row = await pool.fetchrow(
        """
        UPDATE tasks
        SET status = 'queued'
        WHERE id = $1 AND status = 'pending'
          AND (next_retry_at IS NULL OR next_retry_at <= now())
        RETURNING id
        """,
        task_id,
    )
    return row is not None


async def claim_task(
    pool: asyncpg.Pool, task_id: int, worker_id: str, lease_ttl_seconds: float
) -> dict | None:
    """
    Atomically flip a task from queued -> running, and only return it if THIS call
    was the one that did the flipping.

    A worker pops a task id off the Redis queue and calls this. Why not "SELECT to
    check it's still queued, then UPDATE"? Because between the two, another worker
    could do the same — a race condition, the single most common bug in homemade
    schedulers. Putting WHERE status = 'queued' inside the UPDATE makes the
    check-and-flip one atomic operation, so only one worker's UPDATE wins; the loser
    gets None and drops the id.

    This is the real guard against double-execution across processes. Redis BRPOP
    already hands each id to just one worker, but this stays correct even when that
    isn't enough — e.g. the reaper re-enqueues a task that a slow-but-alive worker
    still holds. Written back in Phase 1, load-bearing now.

    Claiming also takes a lease: worker_id records who holds the task and
    lease_expires_at is when that hold expires. The worker heartbeats to extend it;
    if the worker dies, the lease lapses and the reaper reclaims the task.
    """
    row = await pool.fetchrow(
        """
        UPDATE tasks
        SET status = 'running', started_at = now(),
            worker_id = $2,
            lease_expires_at = now() + make_interval(secs => $3)
        WHERE id = $1 AND status = 'queued'
        RETURNING id, name, task_type, status, retry_count, max_retries, timeout_seconds
        """,
        task_id, worker_id, lease_ttl_seconds,
    )
    return dict(row) if row else None


async def heartbeat_task(pool: asyncpg.Pool, task_id: int, lease_ttl_seconds: float) -> None:
    """Extend a running task's lease — the worker's "I'm still alive" signal.

    Guarded by status = 'running' so it's a no-op once the task has finished (the
    heartbeat loop and the task completing can race; this makes the loser harmless).
    """
    await pool.execute(
        """
        UPDATE tasks
        SET lease_expires_at = now() + make_interval(secs => $2)
        WHERE id = $1 AND status = 'running'
        """,
        task_id, lease_ttl_seconds,
    )


async def fetch_expired_leases(pool: asyncpg.Pool, dag_run_id: int) -> list[dict]:
    """Running tasks in this dag_run whose lease has lapsed — their worker is
    presumed dead. Returns just the fields the reaper needs to apply the retry
    policy. Scoped to one dag_run so each dispatcher only reaps its own tasks.
    """
    rows = await pool.fetch(
        """
        SELECT id, name, retry_count, max_retries, worker_id
        FROM tasks
        WHERE dag_run_id = $1 AND status = 'running'
          AND lease_expires_at IS NOT NULL AND lease_expires_at < now()
        """,
        dag_run_id,
    )
    return [dict(r) for r in rows]


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


async def mark_task_for_retry(
    pool: asyncpg.Pool, task_id: int, delay_seconds: float, error: str
) -> None:
    """Send a failed-but-retryable task back to pending with a backoff deadline.

    Bumps retry_count, records the error from this attempt, and sets next_retry_at
    to now() + delay so claim_task won't pick it up again until the backoff passes.
    started_at is cleared because the next attempt hasn't started yet — claim_task
    stamps it fresh when the retry actually runs.
    """
    await pool.execute(
        """
        UPDATE tasks
        SET status = 'pending',
            retry_count = retry_count + 1,
            next_retry_at = now() + make_interval(secs => $2),
            error = $3,
            started_at = NULL
        WHERE id = $1
        """,
        task_id, delay_seconds, error,
    )


async def mark_tasks_blocked(pool: asyncpg.Pool, task_ids: list[int]) -> None:
    """Mark a batch of tasks BLOCKED because an upstream dependency failed.

    Unlike claim_task(), there's no race to win here: blocking isn't a claim on
    work to execute, it's bookkeeping the scheduler derives from graph state. So
    a plain bulk UPDATE is enough — no atomic check-and-flip needed. The
    WHERE status = 'pending' guard is just belt-and-suspenders: it stops us ever
    stomping a task that meanwhile reached a real terminal state.
    """
    if not task_ids:
        return
    await pool.execute(
        """
        UPDATE tasks
        SET status = 'blocked', finished_at = now()
        WHERE id = ANY($1::int[]) AND status = 'pending'
        """,
        task_ids,
    )
