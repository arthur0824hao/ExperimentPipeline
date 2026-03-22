BEGIN;

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

CREATE INDEX IF NOT EXISTS idx_rule_sets_scope_priority
  ON skill_system.rule_sets(rule_scope, priority, source_path);
CREATE INDEX IF NOT EXISTS idx_rule_entries_rule_set_id
  ON skill_system.rule_entries(rule_set_id);
CREATE INDEX IF NOT EXISTS idx_rule_entries_enabled
  ON skill_system.rule_entries(enabled);

COMMIT;
