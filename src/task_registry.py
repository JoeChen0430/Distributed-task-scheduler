"""
A task in the database is just a row with a `task_type` string like "extract".
Something has to map that string to actual code to run. That's all this file
does — a decorator-based registry, similar in spirit to how Airflow/Celery
let you register functions by name.

Usage (see examples/etl_dag.py):

    @register_task("extract")
    async def extract(ctx: dict) -> dict:
        ...
        return {"rows": 100}
"""

from typing import Awaitable, Callable

TaskHandler = Callable[[dict], Awaitable[dict]]

_registry: dict[str, TaskHandler] = {}


def register_task(task_type: str) -> Callable[[TaskHandler], TaskHandler]:
    def decorator(fn: TaskHandler) -> TaskHandler:
        if task_type in _registry:
            raise ValueError(f"task_type={task_type!r} is already registered")
        _registry[task_type] = fn
        return fn
    return decorator


def get_handler(task_type: str) -> TaskHandler:
    if task_type not in _registry:
        raise ValueError(
            f"No handler registered for task_type={task_type!r}. "
            f"Known types: {sorted(_registry.keys())}"
        )
    return _registry[task_type]


def registered_types() -> list[str]:
    return sorted(_registry.keys())
