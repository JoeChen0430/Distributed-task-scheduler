import { usePolling } from "./usePolling.js";
import { getDagRun } from "./api.js";
import StatusBadge from "./StatusBadge.jsx";
import DagGraph from "./DagGraph.jsx";

function duration(started, finished) {
  if (!started) return "—";
  const end = finished ? new Date(finished) : new Date();
  const secs = (end - new Date(started)) / 1000;
  return `${secs.toFixed(1)}s`;
}

export default function RunDetail({ runId, onBack }) {
  const { data, error } = usePolling(() => getDagRun(runId), 1000, [runId]);

  return (
    <div>
      <button className="back" onClick={onBack}>
        ← all runs
      </button>

      {error && <p className="error">{error.message}</p>}

      {!data ? (
        <p className="muted">Loading…</p>
      ) : (
        <>
          <h2>
            #{data.run.id} · {data.run.name}
          </h2>
          <DagGraph tasks={data.tasks} edges={data.edges} />
          <table className="table">
            <thead>
              <tr>
                <th>Task</th>
                <th>Type</th>
                <th>Status</th>
                <th>Retries</th>
                <th>Worker</th>
                <th>Duration</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {data.tasks.map((t) => (
                <tr key={t.id}>
                  <td>{t.name}</td>
                  <td className="muted">{t.task_type}</td>
                  <td>
                    <StatusBadge status={t.status} />
                  </td>
                  <td className="muted">
                    {t.retry_count}/{t.max_retries}
                  </td>
                  <td className="muted">{t.worker_id ?? "—"}</td>
                  <td className="muted">
                    {duration(t.started_at, t.finished_at)}
                  </td>
                  <td className="error-cell">{t.error ?? ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
