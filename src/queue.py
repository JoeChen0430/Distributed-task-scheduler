"""
The only file that talks to Redis — the Phase 3 counterpart to db.py's role for
Postgres. If you're writing a Redis command anywhere else, move it here instead.

Redis is the *work queue*, not the source of truth. The dispatcher LPUSHes ready
task ids; workers BRPOP them. Postgres still holds the authoritative task state,
so a lost/emptied queue is not data loss — the dispatcher rebuilds it from the
task rows (any task left 'pending' simply gets enqueued again).
"""

import redis.asyncio as redis

from src import config

# All ready task ids live in one Redis list. LPUSH on the left + BRPOP on the right
# makes it FIFO: the oldest ready task is the next one a worker picks up.
READY_QUEUE = "scheduler:ready"

_client: redis.Redis | None = None


async def get_client() -> redis.Redis:
    global _client
    if _client is None:
        # decode_responses=True so we get str back from Redis instead of bytes.
        _client = redis.from_url(config.REDIS_URL, decode_responses=True)
    return _client


async def aclose() -> None:
    """Close the shared Redis client. Call on shutdown so its socket is released
    before the event loop closes (otherwise redis-py's __del__ fires too late and
    prints a harmless-but-noisy 'Event loop is closed' traceback)."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def enqueue(task_id: int) -> None:
    """Push a ready task id onto the queue (called by the dispatcher)."""
    client = await get_client()
    await client.lpush(READY_QUEUE, task_id)


async def dequeue(timeout: float = 5.0) -> int | None:
    """Blocking pop of the next task id (called by a worker).

    Blocks up to `timeout` seconds waiting for work; returns the task id, or None
    if it timed out with the queue still empty (so the worker can loop and retry
    rather than block forever). BRPOP removing the id is atomic, so two workers
    never pop the same id — though db.claim_task is still the real guard against
    double-execution.
    """
    client = await get_client()
    result = await client.brpop(READY_QUEUE, timeout=timeout)
    if result is None:
        return None
    _key, task_id = result
    return int(task_id)
