"""
Shared enums / constants for the scheduler.

Keeping this in one tiny file matters more than it looks: db.py, graph.py,
and scheduler.py all need to agree on exactly what the valid task states
are and how they're spelled. If "success" and "SUCCESS" ever drift apart
between files, tasks silently never get picked up — a classic distributed
systems bug that's boring to debug. One shared enum removes the whole
category of mistake.
"""

from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"    # created, waiting on dependencies
    QUEUED = "queued"      # ready and pushed to the Redis queue, awaiting a worker.
                           # Phase 3 — the dispatcher flips pending -> queued.
    RUNNING = "running"    # claimed by a worker, currently executing
    SUCCESS = "success"    # finished without raising
    FAILED = "failed"      # finished by raising an exception
    BLOCKED = "blocked"    # will never run: an upstream dependency failed (or was
                           # itself blocked). Phase 2 — see graph.compute_blocked_tasks.


# Statuses that mean "this task will not run again". BLOCKED joins the terminal set
# in Phase 2: it's how a DAG containing a failed task can actually finish instead of
# hanging forever (the Phase 1 gap — a doomed task used to sit in PENDING with no way
# to reach a terminal state, so is_dag_finished never became True).
TERMINAL_STATUSES = {
    TaskStatus.SUCCESS.value,
    TaskStatus.FAILED.value,
    TaskStatus.BLOCKED.value,
}
