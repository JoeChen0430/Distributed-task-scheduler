# Distributed Task Scheduler

A minimal DAG-based task scheduler, built in phases so each distributed
systems concept gets introduced one at a time instead of all at once.
**All five phases are done.**

- **Phase 1** — single-process core: define a DAG, resolve dependencies, run tasks.
- **Phase 2** — failure handling: failure propagation, retry with backoff, timeouts.
- **Phase 3** — multi-worker: a dispatcher enqueues ready tasks to Redis, multiple
  worker processes execute them, and leases + a reaper reclaim tasks whose worker died.
- **Phase 4** — a read-only React dashboard (FastAPI over the same Postgres) to watch runs live.
- **Phase 5** — a benchmark that measures throughput/latency and names the bottlenecks
  (see [`docs/phase5-results.md`](docs/phase5-results.md)).

## What works vs. what's coming later

| Concept | Status | Notes |
|---|---|---|
| Define a DAG (tasks + dependencies) | ✅ Phase 1 | Postgres tables |
| Resolve "what's ready to run" | ✅ Phase 1 | `src/graph.py` (pure logic) |
| Failure propagation (a failed task doesn't hang the DAG) | ✅ Phase 2 | doomed tasks marked `blocked`; `graph.compute_blocked_tasks` |
| Retry on failure (exponential backoff) | ✅ Phase 2 | per-task `max_retries`; policy in `src/retry.py`, backoff gate in `db.enqueue_task` |
| Timeout on slow tasks | ✅ Phase 2 | per-task `timeout_seconds` via `asyncio.wait_for`; funnels into retry→fail→block |
| Multiple worker processes | ✅ Phase 3 | Redis work queue; dispatcher enqueues, workers `BRPOP` + atomic `db.claim_task` |
| Prevent double-execution across processes | ✅ Phase 3 | atomic claim (`queued→running`) is the guard; the point of writing it in Phase 1 |
| Reclaim tasks orphaned by a dead worker | ✅ Phase 3 | claim takes a lease; workers heartbeat; the dispatcher's reaper reclaims expired ones |
| Web dashboard (live, read-only) | ✅ Phase 4 | FastAPI read API (`src/api.py`) + React/Vite UI (`dashboard/`), polling |
| Measured performance + bottleneck analysis | ✅ Phase 5 | `benchmark/`; results in `docs/phase5-results.md` |

## Project structure

```
distributed-task-scheduler/
├── docker-compose.yml       # Postgres + Redis for local dev
├── docs/
│   ├── phase3-design.md     # the multi-worker design (dispatcher/worker/queue/leases)
│   ├── phase4-design.md     # the dashboard design (read-only API + React)
│   ├── phase5-design.md     # the benchmark design
│   └── phase5-results.md    # measured numbers + bottleneck analysis
├── migrations/
│   ├── 001_init_schema.sql         # dag_runs / tasks / task_dependencies tables
│   ├── 002_add_blocked_status.sql  # Phase 2: 'blocked' task status
│   ├── 003_add_retry_columns.sql   # Phase 2: max_retries / retry_count / next_retry_at
│   ├── 004_add_timeout.sql         # Phase 2: timeout_seconds
│   └── 005_add_queue_and_lease.sql # Phase 3: 'queued' status + lease_expires_at / worker_id
├── src/
│   ├── config.py            # reads DATABASE_URL / REDIS_URL from .env
│   ├── models.py            # TaskStatus enum — the vocabulary everything shares
│   ├── graph.py             # PURE logic: which tasks are ready / which are blocked? (no DB)
│   ├── retry.py             # PURE logic: retry policy / backoff delay (no DB)
│   ├── task_registry.py     # maps task_type strings -> Python functions
│   ├── db.py                # the only file that talks to Postgres
│   ├── queue.py             # the only file that talks to Redis (the work queue)
│   ├── dag.py               # helper to build a DAG from a list of task defs
│   ├── dispatcher.py        # decides what's ready, enqueues it, propagates failure, reaps dead leases
│   ├── worker.py            # pulls tasks off the queue, claims them, runs handlers
│   ├── scheduler.py         # local orchestrator: run one DAG with an in-process dispatcher + N workers
│   └── api.py               # Phase 4: read-only FastAPI over db.py (the dashboard's backend)
├── examples/
│   ├── etl_dag.py           # extract -> transform -> validate -> load
│   ├── failing_dag.py       # Phase 2: a failing task blocks its descendants, DAG still finishes
│   ├── retry_dag.py         # Phase 2: a flaky task fails, backs off, recovers on retry
│   ├── timeout_dag.py       # Phase 2: a slow task times out, retries, then fails and blocks
│   ├── parallel_dag.py      # Phase 3: two workers run parallel branches concurrently
│   ├── reaper_demo.py       # Phase 3: a dead worker's task is reclaimed and rerun
│   ├── demo_tasks.py        # shared handlers for the multi-process demo
│   ├── create_demo_dag.py   # create the demo DAG only (for the multi-process demo)
│   └── run_worker.py        # a standalone worker process that imports the demo handlers
├── scripts/
│   └── migrate.py           # applies migrations/*.sql without needing the psql CLI
├── benchmark/
│   └── benchmark.py         # Phase 5: no-op load generator + timestamp analysis
├── tests/
│   ├── test_graph.py        # pure dependency-resolution tests — no DB
│   ├── test_retry.py        # pure retry-policy tests — no DB
│   └── test_integration.py  # multi-worker tests against Docker Postgres+Redis (skip if down)
└── dashboard/               # Phase 4: React + Vite read-only UI
    └── src/                 # RunsList / RunDetail / DagGraph (React Flow), polling the API
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

[dispatcher] watching dag_run=1
  -> [w2] running 'extract' (id=1)
  <- [w2] 'extract' succeeded: {'rows_extracted': 1000}
  -> [w1] running 'transform' (id=2)
  <- [w1] 'transform' succeeded: {'rows_transformed': 980}
  -> [w2] running 'validate' (id=3)
  <- [w2] 'validate' succeeded: {'valid': True}
  -> [w1] running 'load' (id=4)
  <- [w1] 'load' succeeded: {'rows_loaded': 980}
[dispatcher] dag_run=1 finished
```

The `[w1]`/`[w2]` tags are the two in-process workers `run_dag` starts by default —
tasks are handed out through Redis, so which worker runs which task varies per run.

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

### Seeing retry with backoff (Phase 2)

`examples/retry_dag.py` has a `flaky` task (registered with `max_retries: 3`) that
raises on its first two attempts and succeeds on the third. Instead of failing the
DAG, the scheduler retries it with exponential backoff, then carries on:

```bash
python -m examples.retry_dag
```
```
    (flaky attempt #1)
  <- 'flaky' failed, retrying in 1.0s (retry 1/3): transient glitch on attempt 1
    (flaky attempt #2)
  <- 'flaky' failed, retrying in 2.0s (retry 2/3): transient glitch on attempt 2
    (flaky attempt #3)
  <- 'flaky' succeeded: {'succeeded_on_attempt': 3}
  -> running 'finalize' ...
[scheduler] dag_run=... finished
```

Set `max_retries` per task in its `task_defs` entry; it defaults to `0` (no retry).

### Seeing timeout (Phase 2)

`examples/timeout_dag.py` has a `slow` task that sleeps 3s but is given
`timeout_seconds: 1` and `max_retries: 1`. Every attempt overruns and is cancelled,
so it times out, retries once, times out again, fails, and blocks its dependent —
timeout, retry, and failure propagation all composing on one task:

```bash
python -m examples.timeout_dag
```
```
  <- 'slow' failed, retrying in 1.0s (retry 1/1): timed out after 1s
  <- 'slow' FAILED (no retries left): timed out after 1s
  ~ 'report' blocked (upstream failed)
[scheduler] dag_run=... finished
```

Set `timeout_seconds` per task in its `task_defs` entry; it defaults to no limit.
Note this bounds *slow-but-alive* tasks; reclaiming a task orphaned in `running` by
a crashed worker is handled by leases (Phase 3, below).

### Seeing multiple workers (Phase 3)

`examples/parallel_dag.py` fans out to two branches; the two in-process workers run
them at the same time (`branch_a` on one worker, `branch_b` on the other):

```bash
python -m examples.parallel_dag
```

`examples/reaper_demo.py` forces a task into the state a *crashed* worker leaves
behind (stuck `running`, expired lease) and shows the dispatcher's reaper reclaim it
so a live worker reruns it:

```bash
python -m examples.reaper_demo
```

### Running in distributed (multi-process) mode

`run_dag` above is a convenience that runs a dispatcher + workers in one process. The
same code also runs as *separate* processes — the real distributed setup. Each worker
process must import the task handlers itself (Redis only carries task ids, not code),
which is what `examples/run_worker.py` does:

```bash
python -m examples.create_demo_dag     # prints: Created dag_run id=N
python -m examples.run_worker w1       # in a second terminal (blocks, waiting)
python -m examples.run_worker w2       # in a third terminal
python -m src.dispatcher N             # drives dag_run N; workers pick up the tasks
```

Watch which worker ran what:
```bash
docker compose exec postgres psql -U scheduler -d scheduler -c \
  "SELECT name, status, worker_id FROM tasks WHERE dag_run_id = N ORDER BY id;"
```

### The dashboard (Phase 4)

A read-only web UI to watch runs live: a list of DAG runs, and per-run a DAG graph
(nodes colored by status) + a task table, all polling the API every ~1s. It reads the
same Postgres the engine writes — it never touches Redis, the queue, or execution.

Needs Node.js. In three terminals:
```bash
# 1. the read API (from the project root)
uvicorn src.api:app --reload              # http://localhost:8000  (/docs for Swagger UI)

# 2. the dashboard dev server
cd dashboard && npm install && npm run dev # http://localhost:5173

# 3. produce some live activity to watch
python -m examples.parallel_dag           # or failing_dag / retry_dag / timeout_dag
```
Open http://localhost:5173, click a run, and watch task statuses move
pending → queued → running → success/failed/blocked without reloading. Design:
`docs/phase4-design.md`.

### Benchmark (Phase 5)

Measure the scheduler's own overhead (no-op handlers) and see where it bottlenecks:

```bash
python -m benchmark.benchmark
```

It sweeps a wide DAG across worker counts and a chain DAG across poll intervals. The
headline findings: a dependency chain's latency is floored by `poll_interval`
(makespan ≈ N × poll), while wide-fan-out throughput plateaus on Postgres round-trips
and the single dispatcher rather than scaling with in-process workers. Numbers +
analysis: [`docs/phase5-results.md`](docs/phase5-results.md).

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
`test_graph.py` and `test_retry.py` are pure — plain dicts in, plain values out, no
Postgres or Redis needed. `test_integration.py` exercises the multi-worker path
against the Docker Postgres + Redis and **skips automatically** if they aren't
running, so this command works either way.

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
