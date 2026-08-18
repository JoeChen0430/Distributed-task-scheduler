"""
A worker: pull a ready task id off the Redis queue, atomically claim it, run its
handler, persist the outcome — then repeat. Run several of these (as separate
processes, or as coroutines via scheduler.run_dag) to execute a DAG's tasks in
parallel.

A worker is a dumb executor: it doesn't reason about the DAG at all. It trusts the
dispatcher to only enqueue ready tasks, and trusts db.claim_task's atomic flip to
stop two workers running the same one.

Run standalone:
    python -m src.worker [worker_id]
"""

import asyncio
import sys

from src import db, queue, retry, task_registry

# Lease policy. A claimed task's lease lasts LEASE_TTL_SECONDS; the worker re-extends
# it every HEARTBEAT_INTERVAL (< TTL) while running. If the worker dies, the lease
# lapses within ~TTL and the dispatcher's reaper reclaims the task. TTL must exceed a
# normal handler's runtime plus a heartbeat margin, or a slow-but-alive worker gets
# its task reclaimed out from under it.
LEASE_TTL_SECONDS = 30.0
HEARTBEAT_INTERVAL = 10.0


async def execute_task(pool, task_row: dict, worker_id: str = "w?") -> None:
    """Run one task's handler and persist the outcome.

    A per-task timeout_seconds bounds how long the handler may run — asyncio cancels
    it if it overruns (cooperative cancellation; only works while the handler awaits).
    A timeout and a raised exception both funnel into the SAME path: retry with
    exponential backoff up to max_retries, and only once retries are exhausted is the
    task marked FAILED for real (which then triggers blocked-propagation downstream).
    """
    handler = task_registry.get_handler(task_row["task_type"])
    timeout = task_row["timeout_seconds"]
    print(f"  -> [{worker_id}] running '{task_row['name']}' (id={task_row['id']})")
    try:
        coro = handler({"task": task_row})
        if timeout is not None:
            result = await asyncio.wait_for(coro, timeout=timeout)
        else:
            result = await coro
        await db.mark_task_success(pool, task_row["id"], result or {})
        print(f"  <- [{worker_id}] '{task_row['name']}' succeeded: {result}")
        return
    except asyncio.TimeoutError:
        reason = f"timed out after {timeout}s"
    except Exception as exc:
        reason = str(exc)

    # Reached only on failure/timeout — one shared decision: retry or give up.
    delay = retry.plan_retry(task_row["retry_count"], task_row["max_retries"])
    if delay is None:
        await db.mark_task_failed(pool, task_row["id"], reason)
        print(f"  <- [{worker_id}] '{task_row['name']}' FAILED (no retries left): {reason}")
    else:
        await db.mark_task_for_retry(pool, task_row["id"], delay, reason)
        attempt = task_row["retry_count"] + 1
        print(
            f"  <- [{worker_id}] '{task_row['name']}' failed, retrying in {delay}s "
            f"(retry {attempt}/{task_row['max_retries']}): {reason}"
        )


async def _heartbeat_loop(pool, task_id: int) -> None:
    """Keep extending a running task's lease until cancelled (when it finishes)."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        await db.heartbeat_task(pool, task_id, LEASE_TTL_SECONDS)


async def run_worker(pool, worker_id: str, dequeue_timeout: float = 2.0) -> None:
    """Loop forever: pop an id, claim it (taking a lease), run it while heartbeating.
    Cancel the task to stop it."""
    while True:
        task_id = await queue.dequeue(timeout=dequeue_timeout)
        if task_id is None:
            continue  # queue empty right now — loop and block-wait again
        claimed = await db.claim_task(pool, task_id, worker_id, LEASE_TTL_SECONDS)
        if claimed is None:
            # Another worker already claimed it, or it's no longer queued. Drop it.
            continue
        heartbeat = asyncio.create_task(_heartbeat_loop(pool, task_id))
        try:
            await execute_task(pool, claimed, worker_id)
        finally:
            heartbeat.cancel()


async def _main() -> None:
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker"
    pool = await db.get_pool()
    print(f"[worker {worker_id}] waiting for tasks...")
    try:
        await run_worker(pool, worker_id)
    finally:
        await queue.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
