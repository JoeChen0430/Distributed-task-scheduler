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


def compute_blocked_tasks(tasks: list[dict], deps: list[dict]) -> list[int]:
    """
    The Phase 2 counterpart to compute_ready_tasks — and the fix for the Phase 1
    "a failed task hangs the whole DAG forever" gap.

    compute_ready_tasks() only ever releases a task once EVERY dependency is
    SUCCESS. So a PENDING task with a FAILED dependency can never become ready —
    it's doomed. Left alone it sits in PENDING forever, is_dag_finished() never
    returns True, and run_dag()'s loop never exits. The fix is to recognise those
    doomed tasks and mark them BLOCKED (a terminal state), so the DAG can finish.

    Blocking is TRANSITIVE: if A fails, B (depends on A) is doomed, and so is
    C (depends on B), because a BLOCKED dependency will never turn SUCCESS either.
    We resolve that to a fixed point right here rather than leaning on the
    scheduler's poll loop to trickle it down one level per iteration — so the
    answer is deterministic and unit-testable with no DB and no timing games.

    Returns the ids of currently-PENDING tasks that should now be marked BLOCKED.
    Like compute_ready_tasks, this is pure: statuses + edges in, ids out.
    """
    required_by_task: dict[int, list[int]] = {}
    for d in deps:
        required_by_task.setdefault(d["task_id"], []).append(d["depends_on_task_id"])

    # A dependency in one of these states will never become SUCCESS, so anything
    # still waiting on it is doomed.
    doomed_states = {TaskStatus.FAILED.value, TaskStatus.BLOCKED.value}

    # Work on a local copy of the statuses so we can propagate newly-blocked tasks
    # into the same pass (that's what makes the transitive closure converge).
    working_status = {t["id"]: t["status"] for t in tasks}
    newly_blocked: list[int] = []

    changed = True
    while changed:
        changed = False
        for t in tasks:
            task_id = t["id"]
            if working_status[task_id] != TaskStatus.PENDING.value:
                continue
            required = required_by_task.get(task_id, [])
            if any(working_status.get(dep_id) in doomed_states for dep_id in required):
                working_status[task_id] = TaskStatus.BLOCKED.value
                newly_blocked.append(task_id)
                changed = True
    return newly_blocked


def is_dag_finished(tasks: list[dict]) -> bool:
    """True once every task has landed in a terminal state (success, failed, or
    blocked).

    Phase 2 note: this used to be able to hang forever. compute_ready_tasks()
    requires a dependency to be SUCCESS, not merely terminal — so before BLOCKED
    existed, a task whose dependency FAILED stayed PENDING (never ready, never
    terminal), and this function never returned True. compute_blocked_tasks()
    now moves those doomed tasks to BLOCKED (which is terminal), so a DAG with a
    failed task actually finishes instead of requiring a Ctrl+C.
    """
    return all(t["status"] in TERMINAL_STATUSES for t in tasks)
