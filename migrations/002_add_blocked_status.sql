-- Phase 2: add the 'blocked' task status.
--
-- A task becomes 'blocked' when an upstream dependency failed (or was itself
-- blocked), so it can never run. This is what lets a DAG containing a failed
-- task actually finish instead of hanging in PENDING forever (the Phase 1 gap).
--
-- IF NOT EXISTS makes re-running this one file harmless. (The migrate script
-- still isn't versioned overall — that's a separate deferred simplification.)
ALTER TYPE task_status ADD VALUE IF NOT EXISTS 'blocked';
