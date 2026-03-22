BEGIN;

CREATE TABLE IF NOT EXISTS evolution_nodes (
  node_id         TEXT PRIMARY KEY,
  proposal_id     TEXT NOT NULL UNIQUE,
  kind            VARCHAR(64) NOT NULL,
  summary         TEXT NOT NULL,
  status          VARCHAR(32) NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  accepted_at     TIMESTAMPTZ NOT NULL,
  parent_node_id  TEXT REFERENCES evolution_nodes(node_id) DEFERRABLE INITIALLY DEFERRED,
  memory_id       BIGINT REFERENCES agent_memories(id) ON DELETE SET NULL,
  semantic_identity JSONB,
  semantic_fingerprint TEXT,
  CHECK (status = 'ACCEPTED')
);

CREATE TABLE IF NOT EXISTS evolution_rejections (
  proposal_id     TEXT PRIMARY KEY,
  kind            VARCHAR(64) NOT NULL,
  summary         TEXT NOT NULL,
  status          VARCHAR(32) NOT NULL,
  rejected_via    VARCHAR(32) NOT NULL,
  rejected_at     TIMESTAMPTZ NOT NULL,
  memory_id       BIGINT REFERENCES agent_memories(id) ON DELETE SET NULL,
  semantic_identity JSONB,
  semantic_fingerprint TEXT,
  CHECK (status = 'REJECTED')
);

ALTER TABLE evolution_nodes ADD COLUMN IF NOT EXISTS semantic_identity JSONB;
ALTER TABLE evolution_nodes ADD COLUMN IF NOT EXISTS semantic_fingerprint TEXT;
ALTER TABLE evolution_rejections ADD COLUMN IF NOT EXISTS semantic_identity JSONB;
ALTER TABLE evolution_rejections ADD COLUMN IF NOT EXISTS semantic_fingerprint TEXT;

CREATE TABLE IF NOT EXISTS evolution_tasks (
  task_id         BIGINT PRIMARY KEY REFERENCES agent_tasks(id) ON DELETE CASCADE,
  source_node_id  TEXT NOT NULL UNIQUE REFERENCES evolution_nodes(node_id) ON DELETE CASCADE,
  summary         TEXT NOT NULL,
  status          task_status NOT NULL DEFAULT 'open',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_evolution_nodes_parent ON evolution_nodes(parent_node_id);
CREATE INDEX IF NOT EXISTS idx_evolution_nodes_accepted ON evolution_nodes(accepted_at DESC);
CREATE INDEX IF NOT EXISTS idx_evolution_rejections_rejected_at ON evolution_rejections(rejected_at DESC);

CREATE OR REPLACE FUNCTION sync_evolution_task_from_agent_task()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE evolution_tasks
  SET summary = NEW.title,
      status = NEW.status,
      created_at = NEW.created_at
  WHERE task_id = NEW.id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_evolution_task_from_agent_task ON agent_tasks;
CREATE TRIGGER trg_sync_evolution_task_from_agent_task
AFTER UPDATE OF title, status ON agent_tasks
FOR EACH ROW EXECUTE FUNCTION sync_evolution_task_from_agent_task();

INSERT INTO evolution_nodes (
  node_id,
  proposal_id,
  kind,
  summary,
  status,
  created_at,
  accepted_at,
  parent_node_id,
  memory_id
)
SELECT
  COALESCE(NULLIF(m.metadata->>'node_id', ''), 'legacy-node-' || m.id::text),
  COALESCE(NULLIF(m.metadata->>'proposal_id', ''), 'legacy-proposal-' || m.id::text),
  COALESCE(NULLIF(m.metadata->>'kind', ''), 'unknown'),
  COALESCE(NULLIF(regexp_replace(m.title, '^Evolution Node Accepted: ', ''), ''), m.title),
  'ACCEPTED',
  m.created_at,
  m.created_at,
  NULLIF(m.metadata->>'parent_node_id', ''),
  m.id
FROM agent_memories m
WHERE m.category = 'evolution-node'
  AND m.deleted_at IS NULL
ON CONFLICT (proposal_id) DO NOTHING;

INSERT INTO evolution_rejections (
  proposal_id,
  kind,
  summary,
  status,
  rejected_via,
  rejected_at,
  memory_id
)
SELECT
  COALESCE(NULLIF(m.metadata->>'proposal_id', ''), 'legacy-proposal-' || m.id::text),
  COALESCE(NULLIF(m.metadata->>'kind', ''), 'unknown'),
  COALESCE(NULLIF(regexp_replace(m.title, '^Evolution Proposal Rejected: ', ''), ''), m.title),
  'REJECTED',
  COALESCE(NULLIF(m.metadata->>'rejected_via', ''), 'legacy'),
  m.created_at,
  m.id
FROM agent_memories m
WHERE m.category = 'evolution-rejected'
  AND m.deleted_at IS NULL
ON CONFLICT (proposal_id) DO NOTHING;

COMMIT;
