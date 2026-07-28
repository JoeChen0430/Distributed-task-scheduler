"""
The scheduler loop, in plain words:

  1. Ask the DB for every task's current status + the dependency edges.
  2. Ask graph.py which pending tasks are now unblocked.
  3. Try to claim each one (atomically) and fire it off concurrently.
  4. Sleep briefly, repeat, until every task has finished.

This is intentionally a polling loop rather than something event-driven —
polling is simple to reason about and is exactly what Phase 3 will swap out
for a Redis-backed queue once this version's limits (see below) start to hurt.
"""

import asyncio

from src import db, graph, task_registry
from src.models import TaskStatus


async def execute_task(pool, task_row: dict) -> None:
    """Run one task's handler and persist the outcome. No retry yet (Phase 2)."""
    handler = task_registry.get_handler(task_row["task_type"])
    print(f"  -> running '{task_row['name']}' (id={task_row['id']})")
    try:
        result = await handler({"task": task_row})
        await db.mark_task_success(pool, task_row["id"], result or {})
        print(f"  <- '{task_row['name']}' succeeded: {result}")
    except Exception as exc:
        await db.mark_task_failed(pool, task_row["id"], str(exc))
        print(f"  <- '{task_row['name']}' FAILED: {exc}")


async def run_dag(pool, dag_run_id: int, poll_interval: float = 1.0) -> None:
    """
    Drive a single dag_run to completion.

    NOTE — single-process only: `in_flight` lives in this process's memory,
    so it only prevents double-claiming within THIS scheduler instance.
    Two separate `run_dag` processes pointed at the same dag_run_id are
    already safe from double-EXECUTION though, because db.claim_task()'s
    atomic UPDATE is what actually prevents that — `in_flight` here is just
    an optimization to avoid re-querying tasks this process already claimed.
    """
    print(f"[scheduler] watching dag_run={dag_run_id}")
    in_flight: dict[int, asyncio.Task] = {}

    while True:
        tasks, deps = await db.fetch_dag_state(pool, dag_run_id)

        # Phase 2 — failure propagation. Any pending task whose dependency has
        # failed (or was itself blocked) can never become ready, so mark it
        # blocked now. Without this a failed task leaves its descendants stuck in
        # PENDING and the loop below would spin forever (the Phase 1 gap).
        blocked_ids = graph.compute_blocked_tasks(tasks, deps)
        if blocked_ids:
            await db.mark_tasks_blocked(pool, blocked_ids)
            newly_blocked = set(blocked_ids)
            for t in tasks:
                if t["id"] in newly_blocked:
                    t["status"] = TaskStatus.BLOCKED.value  # keep local view in sync
                    print(f"  ~ '{t['name']}' blocked (upstream failed)")

        if graph.is_dag_finished(tasks) and not in_flight:
            print(f"[scheduler] dag_run={dag_run_id} finished")
            break

        for task_id in graph.compute_ready_tasks(tasks, deps):
            if task_id in in_flight:
                continue
            claimed = await db.claim_task(pool, task_id)
            if claimed is None:
                # Someone else (another worker, in Phase 3) already claimed it.
                continue
            in_flight[task_id] = asyncio.create_task(execute_task(pool, claimed))

        for task_id in list(in_flight):
            if in_flight[task_id].done():
                del in_flight[task_id]

        await asyncio.sleep(poll_interval)
