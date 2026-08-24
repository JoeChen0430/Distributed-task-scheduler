import { usePolling } from "./usePolling.js";
import { listDagRuns } from "./api.js";
import StatusBadge from "./StatusBadge.jsx";

export default function RunsList({ onSelect }) {
  const { data: runs, error } = usePolling(listDagRuns, 2000, []);

  if (error) {
    return (
      <p className="error">
        Can&apos;t reach the API at :8000 — is <code>uvicorn src.api:app</code>{" "}
        running? ({error.message})
      </p>
    );
  }
  if (!runs) return <p className="muted">Loading…</p>;
  if (runs.length === 0) {
    return (
      <p className="muted">
        No DAG runs yet. Run an example, e.g.{" "}
        <code>python -m examples.parallel_dag</code>.
      </p>
    );
  }

  return (
    <table className="table">
      <thead>
        <tr>
          <th>#</th>
          <th>Name</th>
          <th>Status</th>
          <th>Tasks</th>
          <th>Created</th>
        </tr>
      </thead>
      <tbody>
        {runs.map((r) => (
          <tr key={r.id} className="clickable" onClick={() => onSelect(r.id)}>
            <td>{r.id}</td>
            <td>{r.name}</td>
            <td>
              <StatusBadge status={r.status} />
            </td>
            <td className="muted">
              {r.success}✓ {r.failed}✗ {r.blocked}⊘ {r.active}… / {r.total}
            </td>
            <td className="muted">{new Date(r.created_at).toLocaleString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
