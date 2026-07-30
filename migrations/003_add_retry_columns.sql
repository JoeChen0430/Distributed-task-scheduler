-- Phase 2: retry support.
--
-- A failed task is no longer terminal straight away — it can be retried up to
-- max_retries times (with exponential backoff) before it truly fails. Three
-- columns on tasks carry that state:
--   max_retries   how many RETRIES are allowed (total attempts = max_retries + 1).
--                 Default 0 keeps the old behaviour: fail immediately, no retry.
--   retry_count   how many retries have happened so far.
--   next_retry_at when the task is allowed to run again. claim_task won't hand a
--                 task out until now() >= next_retry_at, which is how backoff is
--                 enforced without any timing logic in the scheduler or graph.
--
-- IF NOT EXISTS makes re-running this one file harmless. (The migrate runner still
-- replays every file, so applying a NEW migration means a reset — known gap.)
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS max_retries   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS retry_count   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;
