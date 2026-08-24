"""
Read-only HTTP API for the dashboard (Phase 4).

The dashboard is a pure observer: this API only reads, and it reads the SAME
Postgres tables the dispatcher/workers write (via db.py — no new tables, no SQL
here). It never touches Redis, the queue, or execution, so the engine and the UI
stay decoupled, sharing only the database.

Run it (with Postgres up):
    uvicorn src.api:app --reload
Then e.g.:
    curl localhost:8000/api/dag-runs
    curl localhost:8000/api/dag-runs/1
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from src import db

app = FastAPI(title="Distributed Task Scheduler — Dashboard API")

# The Vite dev server (Phase 4 M2) runs on :5173; allow it to call this API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/dag-runs")
async def list_dag_runs():
    """All dag_runs, newest first, each with a per-status task breakdown."""
    pool = await db.get_pool()
    return await db.list_dag_runs(pool)


@app.get("/api/dag-runs/{dag_run_id}")
async def get_dag_run(dag_run_id: int):
    """One run's detail: run meta + tasks + dependency edges (for the graph)."""
    pool = await db.get_pool()
    detail = await db.fetch_dag_run_detail(pool, dag_run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="dag_run not found")
    return detail
