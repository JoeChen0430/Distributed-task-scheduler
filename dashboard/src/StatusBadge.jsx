// A colored pill for a task/run status. Colors are defined in styles.css as
// .badge-<status>, matching src.models.TaskStatus (+ "empty" for runs with no tasks).
export default function StatusBadge({ status }) {
  return <span className={`badge badge-${status}`}>{status}</span>;
}
