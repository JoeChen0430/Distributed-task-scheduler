# CLAUDE.md

## Project

Distributed Task Scheduler — a DAG-based task scheduler (simplified Airflow), built in phases to learn one distributed-systems concept at a time. This is a portfolio/interview project, not production software.

## Current status

This project is deliberately built phase-by-phase. **Don't skip ahead or add later-phase features early**, even if it would make the code more "complete" — that defeats the point of the exercise. If you're working on something and notice a gap that belongs to a later phase, flag it, don't fix it inline.

- **Phase 1 (single-process): done.** Scheduler runs a DAG end-to-end on one machine.
- **Phase 2 (retry, timeout, failure propagation): done.**
  - ✅ Failure propagation: failed task's descendants are marked `BLOCKED` (a terminal status) so the DAG finishes instead of hanging. See `graph.compute_blocked_tasks`.
  - ✅ Retry (with exponential backoff): a handler failure is retried up to `max_retries` times before becoming a real `failed`. Policy is pure logic in `src/retry.py`; backoff is enforced by a `next_retry_at` gate (now inside `db.enqueue_task`, moved there in Phase 3). See `examples/retry_dag.py`.
  - ✅ Timeout: per-task `timeout_seconds` bounds handler runtime via `asyncio.wait_for` in `worker.execute_task`; a timeout funnels into the same retry→fail→block path as any failure. Covers slow-but-alive tasks; reclaiming tasks orphaned in `running` by a dead worker is Phase 3 (leases). See `examples/timeout_dag.py`.
- **Phase 3 (multi-worker via Redis): in progress.** Design in `docs/phase3-design.md`.
  - ✅ M1 plumbing: `src/queue.py` (the only file that talks to Redis), `queued` status, lease columns, redis service.
  - ✅ M2 split: `src/dispatcher.py` (decide ready → atomic `pending→queued` via `db.enqueue_task` → LPUSH) + `src/worker.py` (BRPOP → `db.claim_task` `queued→running` → execute). `scheduler.run_dag` is now a thin local orchestrator running a dispatcher + N in-process workers; examples are unchanged. See `examples/parallel_dag.py`.
  - ✅ M3 leases: `claim_task` takes a lease (`lease_expires_at`/`worker_id`); workers heartbeat via `db.heartbeat_task`; the dispatcher's reaper (`dispatcher.reap_expired_leases` + `db.fetch_expired_leases`) reclaims tasks whose worker died, funnelling them through `plan_retry`. See `examples/reaper_demo.py`.
  - ✅ M4: integration tests (`tests/test_integration.py`, skip if services down) + a true multi-process demo (`examples/create_demo_dag.py` + `run_worker.py`; workers must import the task handlers — the registry isn't shared over Redis).
- **Phase 4 (React dashboard): done (read-only).** Design in `docs/phase4-design.md`.
  - Read-only FastAPI over the same Postgres (`src/api.py` → `db.list_dag_runs` / `db.fetch_dag_run_detail`); the dashboard is a pure observer — it never touches Redis, the queue, or execution.
  - React + Vite UI in `dashboard/`: runs list + per-run DAG graph (React Flow) and task table, polling the API every ~1s.
  - Write actions (trigger/retry/cancel from the UI) were deliberately left out — they'd couple the UI to the engine and need an always-on dispatcher. See the end of `docs/phase4-design.md`.
- **Phase 5 (benchmark): not started.**

Full phase breakdown and rationale: see README.md.

## Commands

Activate the virtualenv first (`.venv`, Python 3.12 — system Python 3.14 has no `asyncpg==0.29.0` wheel):
```bash
source .venv/bin/activate         # or prefix commands with .venv/bin/python
```

```bash
docker compose up -d              # start Postgres + Redis
python -m scripts.migrate         # apply schema
python -m examples.etl_dag        # run the sample DAG (in-process dispatcher + workers)
python -m pytest tests/           # pure tests always run; integration tests skip if services down
uvicorn src.api:app --reload      # Phase 4 dashboard API on :8000
cd dashboard && npm run dev        # Phase 4 dashboard UI on :5173 (needs Node)
```

Reset from scratch (migrations aren't idempotent yet):
```bash
docker compose down -v && docker compose up -d && python -m scripts.migrate
```

## Architecture — keep this separation

- `src/graph.py` — pure logic only. No asyncpg, no async/await, no I/O. It answers "given these statuses and edges, which tasks are ready / which are blocked?" as plain Python in/out. This is what makes `tests/test_graph.py` runnable without a database. Do not add DB calls here.
- `src/retry.py` — pure retry policy (same rule as graph.py: no I/O, unit-tested in `tests/test_retry.py`). `plan_retry(retry_count, max_retries)` returns the backoff delay or `None` when retries are exhausted. Keep timing/DB out of here.
- `src/db.py` — the only file that talks to Postgres. If you're writing SQL anywhere else, stop and move it here instead.
- `src/queue.py` — the only file that talks to Redis (the Phase 3 counterpart to `db.py`). Enqueue/dequeue the work queue; nothing else should import `redis`.
- `src/dispatcher.py` — decides what's ready and enqueues it; also blocked-propagation, DAG-done detection, and the lease reaper. Never runs a handler.
- `src/worker.py` — pulls ids off the queue, claims them, and runs handlers (`execute_task` lives here). Dumb executor: no DAG reasoning.
- `src/scheduler.py` — thin local orchestrator: `run_dag` runs a dispatcher + N in-process workers so examples/tests run in one command. The same dispatcher/worker also run as separate processes.
- `src/api.py` — Phase 4 read-only FastAPI for the dashboard. Reads via `db.py` only (no SQL here); never touches Redis/queue/execution. The `dashboard/` React app (Vite) polls it. Keep write actions out (see Phase 4 status).
- `src/task_registry.py` — maps `task_type` strings to handler functions via `@register_task("name")`. New task types register here, not by editing the scheduler.
- Task status strings always come from `src.models.TaskStatus` — never hardcode `"pending"` / `"success"` etc. as bare strings in new code.

## Failure propagation (fixed in Phase 2)

Previously a failed task left its descendants stuck in `PENDING` forever and `run_dag`'s loop never exited. Fixed: `graph.compute_blocked_tasks` (pure logic, mirrors `compute_ready_tasks`) computes the transitive set of doomed pending tasks, and the scheduler persists them as `BLOCKED` via `db.mark_tasks_blocked` each loop. `BLOCKED` is a terminal status, so `is_dag_finished` now returns True and the DAG ends cleanly. See `examples/failing_dag.py` for a demo.

## Adding a new task type

1. Write an `async def handler(ctx: dict) -> dict` function.
2. Register it with `@register_task("your_type_name")` above the function.
3. Reference `"your_type_name"` in a `task_defs` list when building a DAG (see `examples/etl_dag.py`). Optionally add `"max_retries": N` (default 0 = no retry) and/or `"timeout_seconds": N` (default None = no limit) to that task def.
4. No changes needed to `scheduler.py` or `db.py`.

## Testing

- `tests/test_graph.py` covers dependency-resolution logic only, no DB required — keep it that way as new test cases get added.
- There are no DB-integration tests yet. When Phase 2+ adds real complexity (retry logic, multi-worker races), add integration tests that run against the Docker Postgres — don't just extend `test_graph.py` with DB calls bolted on.

## Conventions

- Async everywhere in `src/` and `examples/` — no blocking calls inside handlers or the dispatcher/worker loops.
- Run scripts as modules from the project root (`python -m examples.etl_dag`, not `python examples/etl_dag.py`) so `from src import ...` resolves correctly.
- `docker-compose.yml` now runs Postgres + Redis (Phase 3). Don't add further services speculatively.

## Out of scope for now

Deferred on purpose, not forgotten — don't add unless the roadmap explicitly moves to that phase:
- Dashboard write actions (trigger/retry/cancel from the UI) — needs an always-on global dispatcher + worker pool, guarded atomic transitions, and auth. Would be a "Phase 4.5"; see end of `docs/phase4-design.md`.
- Dispatcher HA/leader election, Redis persistence, task priorities (leftover Phase 3 niceties).
- Formal benchmarking (Phase 5).
