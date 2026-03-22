BEGIN;

CREATE TABLE IF NOT EXISTS skill_system.skill_graph_nodes (
  id BIGSERIAL PRIMARY KEY,
  skill_name TEXT NOT NULL UNIQUE,
  description TEXT,
  version TEXT,
  capabilities TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  effects TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS skill_system.skill_graph_edges (
  id BIGSERIAL PRIMARY KEY,
  from_skill TEXT NOT NULL REFERENCES skill_system.skill_graph_nodes(skill_name) ON DELETE CASCADE,
  to_skill TEXT NOT NULL REFERENCES skill_system.skill_graph_nodes(skill_name) ON DELETE CASCADE,
  edge_type TEXT NOT NULL CHECK (edge_type IN ('depends_on', 'delegates_to')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(from_skill, to_skill, edge_type)
);

CREATE INDEX IF NOT EXISTS idx_sgn_name ON skill_system.skill_graph_nodes(skill_name);
CREATE INDEX IF NOT EXISTS idx_sge_from ON skill_system.skill_graph_edges(from_skill);
CREATE INDEX IF NOT EXISTS idx_sge_to ON skill_system.skill_graph_edges(to_skill);
CREATE INDEX IF NOT EXISTS idx_sge_type ON skill_system.skill_graph_edges(edge_type);

CREATE OR REPLACE FUNCTION skill_system.upsert_graph_node(
  p_skill_name TEXT, p_description TEXT, p_version TEXT,
  p_capabilities TEXT[], p_effects TEXT[], p_metadata JSONB DEFAULT '{}'
) RETURNS BIGINT AS $$
DECLARE v_id BIGINT;
BEGIN
  INSERT INTO skill_system.skill_graph_nodes (skill_name, description, version, capabilities, effects, metadata)
  VALUES (p_skill_name, p_description, p_version, p_capabilities, p_effects, p_metadata)
  ON CONFLICT (skill_name) DO UPDATE SET
    description = EXCLUDED.description,
    version = EXCLUDED.version,
    capabilities = EXCLUDED.capabilities,
    effects = EXCLUDED.effects,
    metadata = EXCLUDED.metadata,
    updated_at = NOW()
  RETURNING id INTO v_id;
  RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION skill_system.upsert_graph_edge(
  p_from TEXT, p_to TEXT, p_type TEXT, p_metadata JSONB DEFAULT '{}'
) RETURNS BIGINT AS $$
DECLARE v_id BIGINT;
BEGIN
  INSERT INTO skill_system.skill_graph_edges (from_skill, to_skill, edge_type, metadata)
  VALUES (p_from, p_to, p_type, p_metadata)
  ON CONFLICT (from_skill, to_skill, edge_type) DO UPDATE SET
    metadata = EXCLUDED.metadata
  RETURNING id INTO v_id;
  RETURN v_id;
END;
$$ LANGUAGE plpgsql;

COMMIT;
