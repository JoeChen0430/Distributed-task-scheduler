# Distributed Task Scheduler — Phase 2 (in progress)

A minimal DAG-based task scheduler, built in phases so each distributed
systems concept gets introduced one at a time instead of all at once.

**Phase 1 (single-process, single-machine) is done**: define a DAG, resolve
dependencies, run tasks in the right order, see status per task. **Phase 2 is
now in progress** — hardening how the scheduler handles things going wrong,
starting with failure propagation. The "distributed" part (multiple workers,
Redis, locking) still comes later in Phase 3.

## What works vs. what's coming later

| Concept | Status | Notes |
|---|---|---|
| Define a DAG (tasks + dependencies) | ✅ Phase 1 | Postgres tables |
| Resolve "what's ready to run" | ✅ Phase 1 | `src/graph.py` |
| Run tasks concurrently | ✅ Phase 1 | `asyncio.create_task` |
| Prevent double-execution | ✅ Phase 1 | atomic claim (`db.claim_task`); matters across *processes* in Phase 3 |
| Failure propagation (a failed task doesn't hang the DAG) | ✅ Phase 2 | doomed tasks marked `blocked`; `graph.compute_blocked_tasks` |
| Retry on failure | ⬜ Phase 2 | not started |
| Timeout on stuck tasks | ⬜ Phase 2 | not started |
| Multiple worker processes/machines | ⬜ Phase 3 | Redis-backed queue |
| Web dashboard | ⬜ Phase 4 | — |

## Project structure

```
distributed-task-scheduler/
├── docker-compose.yml       # Postgres for local dev
├── migrations/
│   ├── 001_init_schema.sql       # dag_runs / tasks / task_dependencies tables
│   └── 002_add_blocked_status.sql # Phase 2: adds the 'blocked' task status
├── src/
│   ├── config.py            # reads DATABASE_URL from .env
│   ├── models.py            # TaskStatus enum — the vocabulary everything shares
│   ├── graph.py              # PURE logic: which tasks are ready / which are blocked? (no DB, unit-testable)
│   ├── task_registry.py     # maps task_type strings -> Python functions
│   ├── db.py                 # the only file that talks to Postgres
│   ├── dag.py                 # helper to build a DAG from a list of task defs
│   └── scheduler.py         # the polling loop that ties it all together
├── examples/
│   ├── etl_dag.py            # runnable example: extract -> transform -> validate -> load
│   └── failing_dag.py        # Phase 2 demo: a failing task blocks its descendants, DAG still finishes
├── scripts/
│   └── migrate.py            # applies migrations/*.sql without needing the psql CLI
└── tests/
    └── test_graph.py          # tests for graph.py — no DB needed to run these
```

## Quick start

Requires Docker and Python 3.11+.

```bash
# 1. Install dependencies
pip install -r requirements.txt --break-system-packages   # or use a venv

# 2. Start Postgres
docker compose up -d

# 3. Set up your .env
cp .env.example .env

# 4. Apply the schema
python -m scripts.migrate

# 5. Run the example DAG
python -m examples.etl_dag
```

Expected output looks like:

```
Created dag_run id=1

[scheduler] watching dag_run=1
  -> running 'extract' (id=1)
  <- 'extract' succeeded: {'rows_extracted': 1000}
  -> running 'transform' (id=2)
  <- 'transform' succeeded: {'rows_transformed': 980}
  -> running 'validate' (id=3)
  <- 'validate' succeeded: {'valid': True}
  -> running 'load' (id=4)
  <- 'load' succeeded: {'rows_loaded': 980}
[scheduler] dag_run=1 finished
```

Peek at the data directly any time with:
```bash
docker compose exec postgres psql -U scheduler -d scheduler -c "SELECT name, status, result FROM tasks ORDER BY id;"
```

### Seeing failure propagation (Phase 2)

`examples/failing_dag.py` is the same 4-step pipeline, but `transform` raises on
purpose. In Phase 1 this would hang forever (`validate`/`load` stuck PENDING).
Now the doomed tasks are marked `blocked` and the scheduler exits on its own:

```bash
python -m examples.failing_dag
```
```
  -> running 'transform' (id=...)
  <- 'transform' FAILED: row 42 has a null where a number was required
  ~ 'validate' blocked (upstream failed)
  ~ 'load' blocked (upstream failed)
[scheduler] dag_run=... finished        # exits cleanly — no Ctrl+C needed
```

### Re-running from scratch

The migration script isn't idempotent yet (a deliberate simplification for
Phase 1 — a real migration tool tracks which migrations already ran). To
reset:
```bash
docker compose down -v   # wipes the Postgres volume
docker compose up -d
python -m scripts.migrate
```

### Running the tests

```bash
python -m pytest tests/
```
These only exercise `src/graph.py` — no Postgres needed, since that module
takes plain dicts in and returns plain lists out.

## Two ideas worth understanding

**1. Why `graph.py` has zero database code.**
The "which tasks are ready?" question is pure logic — given statuses and
edges, produce a list of ids. Keeping it separate from `db.py` means you can
test the actual scheduling *decisions* without spinning up Postgres, and it's
the cleanest place to reason about correctness. This separation (pure
decision logic vs. I/O) is a pattern worth reusing anywhere, not just here.

**2. Why `db.claim_task()` is one atomic `UPDATE`, not a `SELECT` then an `UPDATE`.**
It would be simpler to write "check if a task is pending, then update it" as
two separate steps. But with two workers, both could pass the check at the
same instant and both start running the same task — a race condition. Doing
the check *inside* the `UPDATE ... WHERE status = 'pending'` makes it one
atomic operation: only one caller's `UPDATE` can win. There's only one
worker right now, so this can't bite you yet — but it means Phase 3 doesn't
need to touch this function at all to become safe with multiple workers.

## Known gaps in this version (see inline comments for details)

- ~~A failed task hangs the DAG forever.~~ **Fixed in Phase 2** — doomed tasks
  are marked `blocked` (see `graph.compute_blocked_tasks`) so the run finishes.
- No retry, no timeout — a slow or hung task just runs forever. (Next up in Phase 2.)
- Migrations aren't versioned/idempotent, so applying a *new* migration means a
  reset (see "Re-running from scratch"). Individual files use `IF NOT EXISTS`
  where they can, but the runner still replays every file.

None of these are bugs to panic about — they're the reason Phase 2 and 3
exist. Building the naive version first, then noticing exactly where it
breaks, is the point.
