"""
The dispatcher: decide which tasks are ready and push them onto the Redis queue for
workers to execute. It also propagates failure (blocked) and detects when the DAG is
finished. This is the "decide" half of what used to be scheduler.run_dag — it never
runs a task handler itself.

One dispatcher drives one dag_run to completion.

Run standalone:
    python -m src.dispatcher <dag_run_id>
"""

import asyncio
import sys

from src import db, graph, queue, retry
from src.models import TaskStatus


async def reap_expired_leases(pool, dag_run_id: int) -> None:
    """Reclaim tasks whose worker died (lease lapsed while still 'running').

    A dead worker is treated as one failed attempt and funnelled through the exact
    same retry policy as any other failure: still have retries left -> back to
    pending (re-queued after backoff); exhausted -> failed (which then propagates
    as blocked to descendants). No special-casing — reuse plan_retry.
    """
    for t in await db.fetch_expired_leases(pool, dag_run_id):
        reason = f"worker '{t['worker_id']}' lease expired (presumed dead)"
        delay = retry.plan_retry(t["retry_count"], t["max_retries"])
        if delay is None:
            await db.mark_task_failed(pool, t["id"], reason)
            print(f"  ! reaped '{t['name']}': {reason} -> FAILED (no retries left)")
        else:
            await db.mark_task_for_retry(pool, t["id"], delay, reason)
            print(f"  ! reaped '{t['name']}': {reason} -> re-queue in {delay}s")


async def run_dispatcher(pool, dag_run_id: int, poll_interval: float = 1.0) -> None:
    print(f"[dispatcher] watching dag_run={dag_run_id}")
    while True:
        # Reclaim any tasks orphaned by a dead worker before reading state, so the
        # rest of this loop sees them back as pending (or failed).
        await reap_expired_leases(pool, dag_run_id)

        tasks, deps = await db.fetch_dag_state(pool, dag_run_id)

        # Phase 2 failure propagation (unchanged): any pending task whose dependency
        # failed (or was itself blocked) can never become ready, so mark it blocked.
        blocked_ids = graph.compute_blocked_tasks(tasks, deps)
        if blocked_ids:
            await db.mark_tasks_blocked(pool, blocked_ids)
            newly_blocked = set(blocked_ids)
            for t in tasks:
                if t["id"] in newly_blocked:
                    t["status"] = TaskStatus.BLOCKED.value  # keep local view in sync
                    print(f"  ~ '{t['name']}' blocked (upstream failed)")

        # Finished only when every task is terminal. queued/running tasks (held in the
        # queue or by a worker) are NOT terminal, so we keep looping until they land.
        if graph.is_dag_finished(tasks):
            print(f"[dispatcher] dag_run={dag_run_id} finished")
            return

        # Enqueue each ready task exactly once: enqueue_task's atomic pending->queued
        # flip dedupes, so re-seeing a still-in-queue task here is harmless.
        for task_id in graph.compute_ready_tasks(tasks, deps):
            if await db.enqueue_task(pool, task_id):
                await queue.enqueue(task_id)

        await asyncio.sleep(poll_interval)


async def _main() -> None:
    if len(sys.argv) < 2:
        print("usage: python -m src.dispatcher <dag_run_id>")
        raise SystemExit(2)
    dag_run_id = int(sys.argv[1])
    pool = await db.get_pool()
    try:
        await run_dispatcher(pool, dag_run_id)
    finally:
        await queue.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
