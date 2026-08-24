import { useMemo } from "react";
import { ReactFlow, Background, Controls, MarkerType } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

// [background, foreground] per status — mirrors the badge colors in styles.css.
const STATUS_COLORS = {
  pending: ["#33384a", "#c7ccd9"],
  queued: ["#1e3a5f", "#8ec5ff"],
  running: ["#4a3a10", "#ffd479"],
  success: ["#12492c", "#6ee7a0"],
  failed: ["#4a1420", "#ff8a9c"],
  blocked: ["#3a2a4a", "#d0a9ff"],
};

// React Flow doesn't lay out graphs for you. Since we already have the DAG, place
// each node by its longest-path depth (→ column) and its order within that depth
// (→ row). Good enough for the small DAGs here, and no layout library needed.
function layout(tasks, edges) {
  const parents = new Map(tasks.map((t) => [t.id, []]));
  for (const e of edges) {
    if (parents.has(e.task_id)) parents.get(e.task_id).push(e.depends_on_task_id);
  }
  const depthCache = new Map();
  const depth = (id) => {
    if (depthCache.has(id)) return depthCache.get(id);
    const ps = parents.get(id) ?? [];
    const d = ps.length === 0 ? 0 : 1 + Math.max(...ps.map(depth));
    depthCache.set(id, d);
    return d;
  };
  const rowsPerCol = new Map();
  const pos = new Map();
  for (const t of tasks) {
    const col = depth(t.id);
    const row = rowsPerCol.get(col) ?? 0;
    rowsPerCol.set(col, row + 1);
    pos.set(t.id, { x: col * 200, y: row * 90 });
  }
  return pos;
}

export default function DagGraph({ tasks, edges }) {
  // Recompute on every poll so node colors track live status changes.
  const { nodes, rfEdges } = useMemo(() => {
    const pos = layout(tasks, edges);
    const nodes = tasks.map((t) => {
      const [bg, fg] = STATUS_COLORS[t.status] ?? STATUS_COLORS.pending;
      return {
        id: String(t.id),
        position: pos.get(t.id) ?? { x: 0, y: 0 },
        data: { label: t.name },
        style: {
          background: bg,
          color: fg,
          border: `1px solid ${fg}`,
          borderRadius: 8,
          padding: 8,
          fontSize: 12,
          width: 140,
        },
      };
    });
    const rfEdges = edges.map((e, i) => ({
      id: `e${i}`,
      source: String(e.depends_on_task_id), // upstream
      target: String(e.task_id), // downstream
      markerEnd: { type: MarkerType.ArrowClosed, color: "#4a5266" },
      style: { stroke: "#4a5266" },
    }));
    return { nodes, rfEdges };
  }, [tasks, edges]);

  return (
    <div
      style={{
        height: 360,
        border: "1px solid var(--border)",
        borderRadius: 8,
        marginBottom: 16,
      }}
    >
      <ReactFlow
        nodes={nodes}
        edges={rfEdges}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
      >
        <Background color="#262a35" />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
