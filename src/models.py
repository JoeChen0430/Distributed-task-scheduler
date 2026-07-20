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
    RUNNING = "running"    # claimed by a worker, currently executing
    SUCCESS = "success"    # finished without raising
    FAILED = "failed"      # finished by raising an exception


# Statuses that mean "this task will not run again" (Phase 1 has no retry yet —
# that's Phase 2. A FAILED task here is final, not "waiting to be retried").
TERMINAL_STATUSES = {TaskStatus.SUCCESS.value, TaskStatus.FAILED.value}
