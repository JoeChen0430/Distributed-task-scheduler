import StatusBadge from "./StatusBadge.jsx";

// The six task statuses, so the badge/graph colors are legible at a glance.
const STATUSES = ["pending", "queued", "running", "success", "failed", "blocked"];

export default function Legend() {
  return (
    <footer className="legend">
      {STATUSES.map((s) => (
        <StatusBadge key={s} status={s} />
      ))}
    </footer>
  );
}
