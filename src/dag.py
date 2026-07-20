"""
A small convenience layer so you can describe a DAG by name instead of by id
(much easier to read and write than juggling integer ids by hand).
"""

from src import db


async def create_dag(pool, dag_name: str, task_defs: list[dict]) -> int:
    """
    task_defs example:
        [
            {"name": "extract",   "task_type": "extract",   "depends_on": []},
            {"name": "transform", "task_type": "transform", "depends_on": ["extract"]},
            {"name": "validate",  "task_type": "validate",  "depends_on": ["transform"]},
            {"name": "load",      "task_type": "load",      "depends_on": ["validate"]},
        ]

    `name` must be unique within this DAG. `depends_on` refers to other
    `name` values, not ids — this function resolves that mapping for you.
    Returns the new dag_run's id.
    """
    dag_run_id = await db.create_dag_run(pool, dag_name)

    name_to_id: dict[str, int] = {}
    for task_def in task_defs:
        task_id = await db.create_task(pool, dag_run_id, task_def["name"], task_def["task_type"])
        name_to_id[task_def["name"]] = task_id

    for task_def in task_defs:
        for dep_name in task_def.get("depends_on", []):
            if dep_name not in name_to_id:
                raise ValueError(
                    f"Task {task_def['name']!r} depends_on unknown task {dep_name!r}"
                )
            await db.create_dependency(pool, name_to_id[task_def["name"]], name_to_id[dep_name])

    return dag_run_id
