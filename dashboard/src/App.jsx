import { useState } from "react";
import RunsList from "./RunsList.jsx";
import RunDetail from "./RunDetail.jsx";
import Legend from "./Legend.jsx";

// Tiny two-view app: the runs list, or one run's detail. No router needed —
// a single piece of state (which run is selected) is enough.
export default function App() {
  const [selectedRun, setSelectedRun] = useState(null);

  return (
    <div className="app">
      <header className="topbar">
        <h1>Task Scheduler Dashboard</h1>
        <span className="hint">live · polling</span>
      </header>

      {selectedRun == null ? (
        <RunsList onSelect={setSelectedRun} />
      ) : (
        <RunDetail runId={selectedRun} onBack={() => setSelectedRun(null)} />
      )}

      <Legend />
    </div>
  );
}
