BEGIN;

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

CREATE INDEX IF NOT EXISTS idx_project_nodes_type ON skill_system.project_nodes(node_type);
CREATE INDEX IF NOT EXISTS idx_project_edges_source ON skill_system.project_edges(source_node);
CREATE INDEX IF NOT EXISTS idx_project_edges_target ON skill_system.project_edges(target_node);
CREATE INDEX IF NOT EXISTS idx_project_edges_relation ON skill_system.project_edges(relation_type);

COMMIT;
