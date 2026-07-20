"""
These tests only import src.graph and src.models — no asyncpg, no database.
Run with:
    python -m pytest tests/
"""

from src.graph import compute_ready_tasks, is_dag_finished
from src.models import TaskStatus


def test_task_with_no_deps_is_ready():
    tasks = [{"id": 1, "status": TaskStatus.PENDING.value}]
    assert compute_ready_tasks(tasks, deps=[]) == [1]


def test_task_blocked_while_dependency_still_running():
    tasks = [
        {"id": 1, "status": TaskStatus.RUNNING.value},
        {"id": 2, "status": TaskStatus.PENDING.value},
    ]
    deps = [{"task_id": 2, "depends_on_task_id": 1}]
    assert compute_ready_tasks(tasks, deps) == []


def test_task_ready_once_dependency_succeeds():
    tasks = [
        {"id": 1, "status": TaskStatus.SUCCESS.value},
        {"id": 2, "status": TaskStatus.PENDING.value},
    ]
    deps = [{"task_id": 2, "depends_on_task_id": 1}]
    assert compute_ready_tasks(tasks, deps) == [2]


def test_task_stays_blocked_if_dependency_failed():
    tasks = [
        {"id": 1, "status": TaskStatus.FAILED.value},
        {"id": 2, "status": TaskStatus.PENDING.value},
    ]
    deps = [{"task_id": 2, "depends_on_task_id": 1}]
    assert compute_ready_tasks(tasks, deps) == []


def test_already_running_task_is_not_ready_again():
    tasks = [{"id": 1, "status": TaskStatus.RUNNING.value}]
    assert compute_ready_tasks(tasks, deps=[]) == []


def test_diamond_dag_only_final_task_waits_for_both_branches():
    # extract -> {transform_a, transform_b} -> load
    tasks = [
        {"id": 1, "status": TaskStatus.SUCCESS.value},  # extract
        {"id": 2, "status": TaskStatus.SUCCESS.value},  # transform_a
        {"id": 3, "status": TaskStatus.RUNNING.value},  # transform_b (still going)
        {"id": 4, "status": TaskStatus.PENDING.value},  # load
    ]
    deps = [
        {"task_id": 2, "depends_on_task_id": 1},
        {"task_id": 3, "depends_on_task_id": 1},
        {"task_id": 4, "depends_on_task_id": 2},
        {"task_id": 4, "depends_on_task_id": 3},
    ]
    assert compute_ready_tasks(tasks, deps) == []  # load must wait for BOTH branches


def test_dag_not_finished_while_a_task_is_pending():
    tasks = [
        {"id": 1, "status": TaskStatus.SUCCESS.value},
        {"id": 2, "status": TaskStatus.PENDING.value},
    ]
    assert is_dag_finished(tasks) is False


def test_dag_finished_when_all_terminal():
    tasks = [
        {"id": 1, "status": TaskStatus.SUCCESS.value},
        {"id": 2, "status": TaskStatus.FAILED.value},
    ]
    assert is_dag_finished(tasks) is True
