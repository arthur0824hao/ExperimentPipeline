BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
                 WHERE n.nspname = 'skill_system' AND t.typname = 'refresh_job_status') THEN
    CREATE TYPE skill_system.refresh_job_status AS ENUM (
      'queued',
      'running',
      'succeeded',
      'failed',
      'cancelled'
    );
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS skill_system.refresh_jobs (
  id BIGSERIAL PRIMARY KEY,
  job_type TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  status skill_system.refresh_job_status NOT NULL DEFAULT 'queued',
  requested_by TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT,
  queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS skill_system.refresh_job_events (
  id BIGSERIAL PRIMARY KEY,
  refresh_job_id BIGINT NOT NULL REFERENCES skill_system.refresh_jobs(id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  level TEXT NOT NULL DEFAULT 'info',
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS skill_system.artifact_versions (
  id BIGSERIAL PRIMARY KEY,
  artifact_type TEXT NOT NULL,
  artifact_key TEXT NOT NULL,
  version_tag TEXT NOT NULL,
  source_hash TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(artifact_type, artifact_key, version_tag)
);

CREATE INDEX IF NOT EXISTS idx_refresh_jobs_status ON skill_system.refresh_jobs(status);
CREATE INDEX IF NOT EXISTS idx_refresh_jobs_scope ON skill_system.refresh_jobs(scope_type, scope_key);
CREATE INDEX IF NOT EXISTS idx_refresh_job_events_job_id ON skill_system.refresh_job_events(refresh_job_id);
CREATE INDEX IF NOT EXISTS idx_artifact_versions_lookup ON skill_system.artifact_versions(artifact_type, artifact_key, created_at DESC);

COMMIT;
