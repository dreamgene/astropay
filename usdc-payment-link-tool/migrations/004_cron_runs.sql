-- Append-only audit of reconcile / settle cron invocations (HTTP handlers).
-- metadata holds the response-shaped summary (counts, per-invoice or per-payout outcomes).
-- error_detail is set when success is false (handler error, not implemented, or future partial-failure modes).

CREATE TABLE cron_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_type TEXT NOT NULL CHECK (job_type = ANY (ARRAY['reconcile'::text, 'settle'::text])),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  success BOOLEAN NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_detail TEXT
);

CREATE INDEX cron_runs_job_type_started_at_idx ON cron_runs (job_type, started_at DESC);
