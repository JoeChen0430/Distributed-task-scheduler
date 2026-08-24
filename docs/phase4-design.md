# Phase 4 Design — React dashboard

> Status: design approved. Phases 1–3 (single-process core, retry/timeout/failure
> propagation, multi-worker via Redis with leases) are done.

## Goal

A web UI to observe the scheduler: list DAG runs, drill into one to see its tasks
(status, retry count, which worker ran it, timing, error) and its DAG structure, all
updating live.

## Approved decisions

- **Live updates: polling** — the frontend re-fetches every ~1s. Simplest, and it
  echoes the scheduler's own polling design (dashboard polls API → API reads DB).
- **Scope: read-only** — Phase 4 only *observes*. No triggering runs or retrying
  tasks from the UI (that would couple the UI to the execution engine).
- **DAG graph: React Flow** — nodes colored by status, edges = dependencies.

## Architecture — a read-only observer

```
Browser (React SPA, Vite dev server :5173)
   │  HTTP JSON, poll every ~1s
   ▼
FastAPI read-only API  (src/api.py -> db.py)  :8000
   │  asyncpg (reuse db.get_pool)
   ▼
Postgres  (the SAME tables the dispatcher/workers write)
```

**Core principle:** the API is read-only and reads the same Postgres tables the
engine writes. The dashboard never touches Redis, the queue, or execution — UI and
engine are decoupled, sharing only the DB (the source of truth). The engine doesn't
know the dashboard exists.

## Backend — FastAPI (`src/api.py`)

Read-only endpoints:
- `GET /api/dag-runs` — all runs: id, name, created_at, and per-status task counts /
  an overall state (running / success / failed).
- `GET /api/dag-runs/{id}` — one run: its tasks (id, name, task_type, status,
  retry_count, worker_id, started_at, finished_at, error) plus dependency edges
  (`task_id`, `depends_on_task_id`) for the graph.

Keep all SQL in `db.py` (new read functions `list_dag_runs()`,
`fetch_dag_run_detail(id)`); `api.py` only shapes JSON. Reuse `db.get_pool()`. Enable
CORS for the Vite dev origin. Add `fastapi` + `uvicorn[standard]` to
`requirements.txt`. Run with `uvicorn src.api:app --reload`.

## Frontend — Vite + React (`dashboard/`)

A separate Node project in `dashboard/`, kept out of the Python package.
- **Runs list**: table of DAG runs with an overall status badge; click to drill in.
- **Run detail**: task table (name, status badge, type, retries, worker, duration,
  error) + a React Flow DAG graph (nodes colored by status, edges = deps).
- **Live**: poll the relevant endpoint every ~1s (small `useEffect` + interval, or a
  tiny data-fetching hook).
- Status legend / colors: pending, queued, running, success, failed, blocked.
- Minimal styling — clean, not a heavy component library.

## What stays untouched

The engine (`dispatcher.py`, `worker.py`, `queue.py`, `graph.py`, `retry.py`,
`db.py` writes) is unchanged. Phase 4 only *adds* a read API + frontend.
`docker-compose.yml` stays Postgres+Redis; the API and frontend run as local dev
processes.

## Milestones

1. **Read API**: `db.list_dag_runs` / `db.fetch_dag_run_detail` + FastAPI endpoints +
   CORS; verify with `curl`.
2. **Frontend scaffold**: Vite + React, runs list + run-detail table, ~1s polling.
3. **DAG graph**: React Flow view, nodes colored by status.
4. **Polish + docs**: status badges, durations, empty/loading states, README section.

## Verification

- API: run the engine on a DAG (`examples/*` or the multi-process demo), then
  `curl localhost:8000/api/dag-runs` and `.../dag-runs/{id}` return the live rows.
- End-to-end: start the API + `dashboard/` dev server, run a DAG, watch statuses move
  pending → queued → running → success/failed/blocked in the UI without reloading.
- Pure backend tests stay green; a couple of API read-function tests can join the
  integration suite (needs Postgres).

## Out of scope (later)

Triggering runs / retrying from the UI (write actions), auth, pagination for large
run lists, SSE/WebSocket push, containerizing the API/frontend, historical charts.
