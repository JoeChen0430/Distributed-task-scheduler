-- Phase 1 schema: everything the scheduler needs to store and resolve a DAG.

CREATE TYPE task_status AS ENUM ('pending', 'running', 'success', 'failed');

CREATE TABLE dag_runs (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tasks (
    id           SERIAL PRIMARY KEY,
    dag_run_id   INTEGER NOT NULL REFERENCES dag_runs(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    task_type    TEXT NOT NULL,
    status       task_status NOT NULL DEFAULT 'pending',
    result       JSONB,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    UNIQUE (dag_run_id, name)
);

-- task_id depends_on depends_on_task_id (i.e. depends_on_task_id must run first)
CREATE TABLE task_dependencies (
    task_id             INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id  INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_task_id)
);

CREATE INDEX idx_tasks_dag_run_id ON tasks(dag_run_id);
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_task_deps_task_id ON task_dependencies(task_id);
