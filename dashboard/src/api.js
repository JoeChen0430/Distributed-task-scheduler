// Every call to the read-only Phase 4 API (src/api.py, default :8000).
const BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

async function getJSON(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export const listDagRuns = () => getJSON("/api/dag-runs");
export const getDagRun = (id) => getJSON(`/api/dag-runs/${id}`);
