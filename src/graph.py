"""
The core scheduling question, answered without touching a database:

    "Given everyone's current status, which tasks are allowed to run right now?"

A task is ready when:
  1. it is still PENDING (hasn't been claimed/run yet), and
  2. every task it depends on has already finished with SUCCESS.

This is deliberately plain Python — no asyncpg, no async/await — so you can
unit test the actual scheduling logic (tests/test_graph.py) without needing
a running Postgres. scheduler.py is the only file that wires this up to the
database.
"""

from src.models import TaskStatus, TERMINAL_STATUSES


def compute_ready_tasks(tasks: list[dict], deps: list[dict]) -> list[int]:
    """
    tasks: [{"id": int, "status": str, ...}, ...]
    deps:  [{"task_id": int, "depends_on_task_id": int}, ...]
          (read as: task_id depends on depends_on_task_id)

    Returns the ids of tasks that are pending AND fully unblocked.
    """
    status_by_id = {t["id"]: t["status"] for t in tasks}

    required_by_task: dict[int, list[int]] = {}
    for d in deps:
        required_by_task.setdefault(d["task_id"], []).append(d["depends_on_task_id"])

    ready_ids = []
    for t in tasks:
        if t["status"] != TaskStatus.PENDING.value:
            continue
        required = required_by_task.get(t["id"], [])
        if all(status_by_id.get(dep_id) == TaskStatus.SUCCESS.value for dep_id in required):
            ready_ids.append(t["id"])
    return ready_ids


def is_dag_finished(tasks: list[dict]) -> bool:
    """True once every task has landed in a terminal state (success or failed).

    KNOWN PHASE 1 GAP, worth understanding rather than papering over:
    compute_ready_tasks() requires a dependency to be SUCCESS, not just
    "terminal". So if task A fails, task B (which depends on A) stays
    PENDING forever — it can never become ready, but it's also never
    terminal. That means this function will never return True for that
    DAG, and scheduler.run_dag()'s loop never exits on its own.

    For now that just means: if a task fails, expect to Ctrl+C the run.
    A natural Phase 2 addition is walking the graph to mark descendants
    of a failed task as e.g. "blocked" (a new terminal-ish status) so the
    DAG can actually finish instead of hanging.
    """
    return all(t["status"] in TERMINAL_STATUSES for t in tasks)
