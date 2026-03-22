BEGIN;

CREATE SCHEMA IF NOT EXISTS skill_system;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
                 WHERE n.nspname = 'skill_system' AND t.typname = 'run_status') THEN
    CREATE TYPE skill_system.run_status AS ENUM (
      'queued',
      'running',
      'succeeded',
      'failed',
      'cancelled'
    );
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS skill_system.policy_profiles (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  allowed_effects TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  allowed_exec TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  allowed_write_roots TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS skill_system.runs (
  id BIGSERIAL PRIMARY KEY,
  goal TEXT,
  agent_id TEXT,
  policy_profile_id BIGINT REFERENCES skill_system.policy_profiles(id) ON DELETE SET NULL,
  task_spec_id BIGINT,
  status skill_system.run_status NOT NULL DEFAULT 'queued',
  started_at TIMESTAMPTZ NULL,
  ended_at TIMESTAMPTZ NULL,
  effective_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
  metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS skill_system.run_events (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES skill_system.runs(id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  level TEXT NOT NULL DEFAULT 'info',
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

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

CREATE TABLE IF NOT EXISTS skill_system.rule_sets (
  rule_set_id BIGSERIAL PRIMARY KEY,
  rule_scope TEXT NOT NULL CHECK (rule_scope IN ('skill_system', 'project', 'compat')),
  source_path TEXT NOT NULL,
  priority INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(rule_scope, source_path)
);

CREATE TABLE IF NOT EXISTS skill_system.rule_entries (
  rule_id BIGSERIAL PRIMARY KEY,
  rule_set_id BIGINT NOT NULL REFERENCES skill_system.rule_sets(rule_set_id) ON DELETE CASCADE,
  rule_text TEXT NOT NULL,
  rule_hash TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(rule_set_id, rule_hash)
);

CREATE TABLE IF NOT EXISTS skill_system.project_nodes (
  node_id BIGSERIAL PRIMARY KEY,
  node_type TEXT NOT NULL CHECK (node_type IN ('skill', 'doc', 'runtime', 'workflow')),
  name TEXT NOT NULL,
  source_path TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS skill_system.project_edges (
  edge_id BIGSERIAL PRIMARY KEY,
  source_node BIGINT NOT NULL REFERENCES skill_system.project_nodes(node_id) ON DELETE CASCADE,
  target_node BIGINT NOT NULL REFERENCES skill_system.project_nodes(node_id) ON DELETE CASCADE,
  relation_type TEXT NOT NULL CHECK (relation_type IN ('depends_on', 'renders', 'reads', 'writes')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(source_node, target_node, relation_type)
);

CREATE INDEX IF NOT EXISTS idx_runs_agent_id ON skill_system.runs(agent_id) WHERE agent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_runs_status ON skill_system.runs(status);
CREATE INDEX IF NOT EXISTS idx_run_events_run_id ON skill_system.run_events(run_id);
CREATE INDEX IF NOT EXISTS idx_refresh_jobs_status ON skill_system.refresh_jobs(status);
CREATE INDEX IF NOT EXISTS idx_refresh_jobs_scope ON skill_system.refresh_jobs(scope_type, scope_key);
CREATE INDEX IF NOT EXISTS idx_refresh_job_events_job_id ON skill_system.refresh_job_events(refresh_job_id);
CREATE INDEX IF NOT EXISTS idx_artifact_versions_lookup ON skill_system.artifact_versions(artifact_type, artifact_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rule_sets_scope_priority ON skill_system.rule_sets(rule_scope, priority, source_path);
CREATE INDEX IF NOT EXISTS idx_rule_entries_rule_set_id ON skill_system.rule_entries(rule_set_id);
CREATE INDEX IF NOT EXISTS idx_rule_entries_enabled ON skill_system.rule_entries(enabled);
CREATE INDEX IF NOT EXISTS idx_project_nodes_type ON skill_system.project_nodes(node_type);
CREATE INDEX IF NOT EXISTS idx_project_edges_source ON skill_system.project_edges(source_node);
CREATE INDEX IF NOT EXISTS idx_project_edges_target ON skill_system.project_edges(target_node);
CREATE INDEX IF NOT EXISTS idx_project_edges_relation ON skill_system.project_edges(relation_type);

COMMIT;
