# CLAUDE.md

## Project

Distributed Task Scheduler — a DAG-based task scheduler (simplified Airflow), built in phases to learn one distributed-systems concept at a time. This is a portfolio/interview project, not production software.

## Current status

This project is deliberately built phase-by-phase. **Don't skip ahead or add later-phase features early**, even if it would make the code more "complete" — that defeats the point of the exercise. If you're working on something and notice a gap that belongs to a later phase, flag it, don't fix it inline.

- **Phase 1 (single-process): done.** Scheduler runs a DAG end-to-end on one machine.
- **Phase 2 (retry, timeout, failure propagation): in progress.**
  - ✅ Failure propagation: failed task's descendants are marked `BLOCKED` (a terminal status) so the DAG finishes instead of hanging. See `graph.compute_blocked_tasks`.
  - ⬜ Retry (with backoff): not started.
  - ⬜ Timeout on stuck tasks: not started.
- **Phase 3 (multi-worker via Redis): not started.**
- **Phase 4 (React dashboard): not started.**
- **Phase 5 (benchmark): not started.**

Full phase breakdown and rationale: see README.md.

## Commands

Activate the virtualenv first (`.venv`, Python 3.12 — system Python 3.14 has no `asyncpg==0.29.0` wheel):
```bash
source .venv/bin/activate         # or prefix commands with .venv/bin/python
```

```bash
docker compose up -d              # start Postgres
python -m scripts.migrate         # apply schema
python -m examples.etl_dag        # run the sample DAG
python -m pytest tests/           # run tests (no DB needed for tests/test_graph.py)
```

Reset from scratch (migrations aren't idempotent yet):
```bash
docker compose down -v && docker compose up -d && python -m scripts.migrate
```

## Architecture — keep this separation

- `src/graph.py` — pure logic only. No asyncpg, no async/await, no I/O. It answers "given these statuses and edges, which tasks are ready?" as plain Python in/out. This is what makes `tests/test_graph.py` runnable without a database. Do not add DB calls here.
- `src/db.py` — the only file that talks to Postgres. If you're writing SQL anywhere else, stop and move it here instead.
- `src/scheduler.py` — orchestration only. Wires `graph.py` decisions to `db.py` persistence. No business logic of its own.
- `src/task_registry.py` — maps `task_type` strings to handler functions via `@register_task("name")`. New task types register here, not by editing the scheduler.
- Task status strings always come from `src.models.TaskStatus` — never hardcode `"pending"` / `"success"` etc. as bare strings in new code.

## Failure propagation (fixed in Phase 2)

Previously a failed task left its descendants stuck in `PENDING` forever and `run_dag`'s loop never exited. Fixed: `graph.compute_blocked_tasks` (pure logic, mirrors `compute_ready_tasks`) computes the transitive set of doomed pending tasks, and the scheduler persists them as `BLOCKED` via `db.mark_tasks_blocked` each loop. `BLOCKED` is a terminal status, so `is_dag_finished` now returns True and the DAG ends cleanly. See `examples/failing_dag.py` for a demo.

## Adding a new task type

1. Write an `async def handler(ctx: dict) -> dict` function.
2. Register it with `@register_task("your_type_name")` above the function.
3. Reference `"your_type_name"` in a `task_defs` list when building a DAG (see `examples/etl_dag.py`).
4. No changes needed to `scheduler.py` or `db.py`.

## Testing

- `tests/test_graph.py` covers dependency-resolution logic only, no DB required — keep it that way as new test cases get added.
- There are no DB-integration tests yet. When Phase 2+ adds real complexity (retry logic, multi-worker races), add integration tests that run against the Docker Postgres — don't just extend `test_graph.py` with DB calls bolted on.

## Conventions

- Async everywhere in `src/` and `examples/` — no blocking calls inside handlers or the scheduler loop.
- Run scripts as modules from the project root (`python -m examples.etl_dag`, not `python examples/etl_dag.py`) so `from src import ...` resolves correctly.
- Keep `docker-compose.yml` Postgres-only until Phase 3 adds Redis — don't add other services speculatively.

## Out of scope for now

Deferred on purpose, not forgotten — don't add unless the roadmap explicitly moves to that phase:
- Retry / exponential backoff / timeouts (Phase 2)
- Multiple worker processes, Redis, distributed locking (Phase 3)
- Any frontend/UI (Phase 4)
- Formal benchmarking (Phase 5)
