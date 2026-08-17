-- Phase 3: multi-worker support — a queued status and task leases.
--
-- 'queued' is a new lifecycle step between pending and running: the dispatcher
-- atomically flips a ready task pending -> queued and LPUSHes its id to Redis.
-- Because compute_ready_tasks only returns 'pending' tasks, an already-queued
-- task is never recomputed, so each task is enqueued exactly once.
--
-- Leases handle workers dying mid-task. claim sets lease_expires_at; the worker
-- heartbeats to extend it; a reaper reclaims tasks whose lease has expired.
--   lease_expires_at  when the current worker's lease on a running task expires
--   worker_id         which worker holds it (observability / debugging)
--
-- IF NOT EXISTS keeps re-applying this one file harmless (runner isn't versioned).
ALTER TYPE task_status ADD VALUE IF NOT EXISTS 'queued';

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS worker_id        TEXT;
