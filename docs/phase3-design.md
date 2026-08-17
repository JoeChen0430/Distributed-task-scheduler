# Phase 3 Design — Multi-worker with a Redis queue

> Status: design approved, implementation in progress.
> Phases 1–2 (single-process core, failure propagation, retry, timeout) are done.

## Goal

Move from a single process that both *decides* and *runs* work, to multiple
processes that split the job:

- a **dispatcher** decides which tasks are ready and pushes them onto a Redis queue,
- multiple **workers** pull task ids off the queue and execute them.

Postgres stays the single **source of truth** for task state. Redis is only a fast
work-distribution queue — if it's lost, the queue can be rebuilt from Postgres.

```
                    ┌─────────────┐
                    │ dispatcher  │  compute_ready_tasks -> mark queued -> LPUSH
                    │  (1 proc)   │  also: blocked propagation, DAG-done, reaper
                    └──────┬──────┘
                           │ LPUSH task_id
                    ┌──────▼───────┐
                    │ Redis queue  │
                    └──────┬───────┘
              BRPOP        │        BRPOP
        ┌──────────────────┼──────────────────┐
   ┌────▼────┐        ┌────▼────┐        ┌────▼────┐
   │ worker1 │        │ worker2 │        │ worker3 │  claim (queued->running)
   └────┬────┘        └────┬────┘        └────┬────┘  execute + heartbeat lease
        └───────────  Postgres (state = source of truth)  ──────────┘
```

## Process responsibilities

| Process | Responsibility | Reuses |
|---|---|---|
| **dispatcher** (1) | poll DAG state; ready → atomic `pending→queued` + `LPUSH`; blocked propagation; detect finished; run reaper | `graph.compute_ready_tasks`, `graph.compute_blocked_tasks` (pure, unchanged) |
| **worker** (N) | `BRPOP` id → claim (`queued→running`) → run handler (with timeout) → heartbeat lease → success/retry/fail | `execute_task` logic, `retry.plan_retry`, `db.claim_task` (now genuinely multi-worker) |
| **reaper** (in dispatcher, or standalone) | find running tasks whose lease expired (worker died) → treat as a failed attempt via retry policy | `retry.plan_retry` |

Architecture boundary continues: **`src/queue.py` is the only file that talks to
Redis** (mirrors `db.py` for Postgres). Pure logic stays in `graph.py`/`retry.py`;
dispatcher/worker/reaper are orchestration (imperative shell).

## Key design decisions

### 1. New `QUEUED` status → enqueue exactly once
The dispatcher recomputes ready tasks every loop; a naive `LPUSH` would flood the
queue with duplicates. Add a lifecycle step `pending → queued → running`. The
dispatcher enqueues via an atomic transition:

```sql
UPDATE tasks SET status = 'queued'
WHERE id = $1 AND status = 'pending'
  AND (next_retry_at IS NULL OR next_retry_at <= now())   -- backoff gate moves here
RETURNING id
```

Only the caller that won the `pending→queued` flip does the `LPUSH`. Since
`compute_ready_tasks` only returns `pending` tasks, an already-`queued` task is
never recomputed → each task is enqueued exactly once. The backoff gate moves from
`claim_task` to this enqueue step (the dispatcher now decides when a task becomes
runnable).

### 2. Lease + heartbeat → reclaim orphaned tasks
If a worker dies mid-task, the row is stuck in `running` with nobody executing it.
Add two columns to `tasks`:

- `lease_expires_at TIMESTAMPTZ` — when the current worker's lease expires.
- `worker_id TEXT` — who holds it (observability/debugging).

Flow:
- **claim**: `queued→running`, set `lease_expires_at = now() + LEASE_TTL` (~30s), `worker_id`.
- **heartbeat**: while the handler runs, a background task every ~TTL/3 does
  `UPDATE lease_expires_at = now() + TTL WHERE id = $1 AND status = 'running'`.
- **reaper**: periodically finds `status='running' AND lease_expires_at < now()`
  and treats each as a failed attempt through `retry.plan_retry` — retry if
  attempts remain, else fail (and then blocked-propagation runs as usual).

Counting reclamation as a failed *attempt* (not a free re-run) prevents a "poison
task" that keeps killing workers from looping forever. Trade-off: the lease TTL
must exceed normal handler time plus a heartbeat margin, or a briefly-stalled but
alive worker gets its task reclaimed. Worth calling out in interviews.

### 3. Concurrency correctness
- Redis `BRPOP` pops-and-removes atomically → two workers usually get *different* ids.
- If the same id reaches two workers (e.g. the reaper re-enqueued while a slow — not
  dead — worker still holds it), `claim_task`'s atomic `WHERE status='queued'`
  transition ensures only one wins. The atomic claim written in Phase 1 finally
  carries real weight here — with zero changes.
- Redis loss is survivable: the task is still `pending`/`queued` in Postgres and
  gets re-enqueued. Postgres is truth; Redis is rebuildable.

## Files

**New**
- `src/queue.py` — only file touching Redis: `enqueue(task_id)`, `dequeue()` (BRPOP), connection mgmt
- `src/dispatcher.py` — dispatch loop (the "decide" half of the old `scheduler.run_dag`)
- `src/worker.py` — worker loop (BRPOP → claim → execute + heartbeat)
- reaper — inside `dispatcher.py` or a standalone `src/reaper.py`

**Changed**
- `migrations/005_*.sql` — `ADD VALUE 'queued'`; add `lease_expires_at`, `worker_id`
- `src/models.py` — add `QUEUED`
- `src/db.py` — `enqueue_task` (pending→queued), `claim_task` (queued→running + lease), `heartbeat_task`, `reap_expired_leases`
- `docker-compose.yml` — add a `redis:` service
- `requirements.txt` — add `redis`
- entrypoints — `python -m src.dispatcher`, `python -m src.worker` (run several)

**Unchanged (reused)**: `graph.py`, `retry.py`, `task_registry.py`, and their
existing DB-free tests.

## Implementation milestones

1. **Plumbing**: docker redis + `src/queue.py` + migration 005 + `QUEUED` status (enqueue/dequeue works).
2. **Split**: dispatcher (enqueue) + worker (BRPOP→claim→execute); multiple workers run one DAG.
3. **Orphan reclamation**: lease + heartbeat + reaper.
4. **Integration tests + demo**: two workers don't double-execute; reaper reclaims an orphan; run a DAG across several workers.

## Verification (per milestone)

- Pure logic (`graph`, `retry`) stays DB-free and already tested — unchanged.
- Add integration tests against Docker Postgres + Redis (the point CLAUDE.md flags
  for "multi-worker races"): no double-execution; reaper reclaims an orphan;
  each ready task enqueued once.
- Demo: launch a dispatcher + 2–3 workers, run a DAG, watch tasks spread across
  workers; kill a worker mid-task to show reclamation.

## Deliberately out of scope (later)

Dispatcher high-availability / leader election (run a single dispatcher; its single
point of failure is a known limitation), Redis persistence, task priorities /
fairness across DAG runs, backpressure.

## New concepts this phase teaches

Work queue / broker, dispatcher–worker split, lease/heartbeat, orphan reclamation,
source-of-truth vs. cache (two-datastore split), poison tasks, single point of failure.
