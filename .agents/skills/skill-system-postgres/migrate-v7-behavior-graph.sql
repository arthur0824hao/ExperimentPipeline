BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'skill_system'
      AND c.relname = 'skill_graph_nodes'
      AND c.relkind = 'r'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'skill_system'
      AND c.relname = 'project_graph_nodes'
      AND c.relkind = 'r'
  ) THEN
    ALTER TABLE skill_system.skill_graph_nodes RENAME TO project_graph_nodes;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'skill_system'
      AND c.relname = 'skill_graph_edges'
      AND c.relkind = 'r'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'skill_system'
      AND c.relname = 'project_graph_edges'
      AND c.relkind = 'r'
  ) THEN
    ALTER TABLE skill_system.skill_graph_edges RENAME TO project_graph_edges;
  END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS skill_system.project_graph_nodes (
  node_id BIGSERIAL PRIMARY KEY,
  node_key TEXT NOT NULL UNIQUE,
  node_type TEXT NOT NULL DEFAULT 'behavior-node' CHECK (btrim(node_type) <> ''),
  description TEXT,
  version TEXT,
  capabilities TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  effects TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  content_hash TEXT,
  spec_path TEXT,
  operations_count INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_nodes'
      AND column_name = 'id'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_nodes'
      AND column_name = 'node_id'
  ) THEN
    ALTER TABLE skill_system.project_graph_nodes RENAME COLUMN id TO node_id;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_nodes'
      AND column_name = 'skill_name'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_nodes'
      AND column_name = 'node_key'
  ) THEN
    ALTER TABLE skill_system.project_graph_nodes RENAME COLUMN skill_name TO node_key;
  END IF;
END;
$$;

ALTER TABLE IF EXISTS skill_system.project_graph_nodes
  ADD COLUMN IF NOT EXISTS node_type TEXT,
  ADD COLUMN IF NOT EXISTS content_hash TEXT,
  ADD COLUMN IF NOT EXISTS spec_path TEXT,
  ADD COLUMN IF NOT EXISTS operations_count INTEGER NOT NULL DEFAULT 0;

UPDATE skill_system.project_graph_nodes
SET node_type = CASE
  WHEN COALESCE(spec_path, '') LIKE '%.behavior.yaml' OR position('::' IN node_key) > 0 THEN 'behavior-node'
  ELSE 'skill'
END
WHERE node_type IS NULL OR btrim(node_type) = '';

ALTER TABLE skill_system.project_graph_nodes
  ALTER COLUMN node_type SET DEFAULT 'behavior-node',
  ALTER COLUMN node_type SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'skill_system.project_graph_nodes'::regclass
      AND conname = 'project_graph_nodes_node_type_nonempty'
  ) THEN
    ALTER TABLE skill_system.project_graph_nodes
      ADD CONSTRAINT project_graph_nodes_node_type_nonempty
      CHECK (btrim(node_type) <> '');
  END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS skill_system.project_graph_edges (
  edge_id BIGSERIAL PRIMARY KEY,
  source_node TEXT NOT NULL REFERENCES skill_system.project_graph_nodes(node_key) ON DELETE CASCADE,
  target_node TEXT NOT NULL REFERENCES skill_system.project_graph_nodes(node_key) ON DELETE CASCADE,
  relation_type TEXT NOT NULL CHECK (btrim(relation_type) <> ''),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(source_node, target_node, relation_type)
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_edges'
      AND column_name = 'id'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_edges'
      AND column_name = 'edge_id'
  ) THEN
    ALTER TABLE skill_system.project_graph_edges RENAME COLUMN id TO edge_id;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_edges'
      AND column_name = 'from_skill'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_edges'
      AND column_name = 'source_node'
  ) THEN
    ALTER TABLE skill_system.project_graph_edges RENAME COLUMN from_skill TO source_node;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_edges'
      AND column_name = 'to_skill'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_edges'
      AND column_name = 'target_node'
  ) THEN
    ALTER TABLE skill_system.project_graph_edges RENAME COLUMN to_skill TO target_node;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_edges'
      AND column_name = 'edge_type'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'skill_system'
      AND table_name = 'project_graph_edges'
      AND column_name = 'relation_type'
  ) THEN
    ALTER TABLE skill_system.project_graph_edges RENAME COLUMN edge_type TO relation_type;
  END IF;
END;
$$;

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  FOR constraint_name IN
    SELECT conname
    FROM pg_constraint
    WHERE conrelid = 'skill_system.project_graph_edges'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) ILIKE '%depends_on%delegates_to%'
  LOOP
    EXECUTE format(
      'ALTER TABLE skill_system.project_graph_edges DROP CONSTRAINT %I',
      constraint_name
    );
  END LOOP;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'skill_system.project_graph_edges'::regclass
      AND conname = 'project_graph_edges_relation_type_nonempty'
  ) THEN
    ALTER TABLE skill_system.project_graph_edges
      ADD CONSTRAINT project_graph_edges_relation_type_nonempty
      CHECK (btrim(relation_type) <> '');
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_project_graph_nodes_key ON skill_system.project_graph_nodes(node_key);
CREATE INDEX IF NOT EXISTS idx_project_graph_nodes_type ON skill_system.project_graph_nodes(node_type);
CREATE INDEX IF NOT EXISTS idx_project_graph_nodes_content_hash ON skill_system.project_graph_nodes(content_hash);
CREATE INDEX IF NOT EXISTS idx_project_graph_nodes_spec_path ON skill_system.project_graph_nodes(spec_path);
CREATE INDEX IF NOT EXISTS idx_project_graph_edges_source ON skill_system.project_graph_edges(source_node);
CREATE INDEX IF NOT EXISTS idx_project_graph_edges_target ON skill_system.project_graph_edges(target_node);
CREATE INDEX IF NOT EXISTS idx_project_graph_edges_relation ON skill_system.project_graph_edges(relation_type);

DROP VIEW IF EXISTS skill_system.skill_graph_edges;
DROP VIEW IF EXISTS skill_system.skill_graph_nodes;

CREATE VIEW skill_system.skill_graph_nodes AS
SELECT
  node_id AS id,
  node_key AS skill_name,
  description,
  version,
  capabilities,
  effects,
  metadata,
  content_hash,
  spec_path,
  operations_count,
  updated_at
FROM skill_system.project_graph_nodes;

CREATE VIEW skill_system.skill_graph_edges AS
SELECT
  edge_id AS id,
  source_node AS from_skill,
  target_node AS to_skill,
  relation_type AS edge_type,
  metadata,
  created_at
FROM skill_system.project_graph_edges;

CREATE OR REPLACE FUNCTION skill_system.upsert_graph_node(
  p_skill_name TEXT,
  p_description TEXT,
  p_version TEXT,
  p_capabilities TEXT[],
  p_effects TEXT[],
  p_metadata JSONB DEFAULT '{}'
) RETURNS BIGINT AS $$
DECLARE
  v_id BIGINT;
  v_node_type TEXT := COALESCE(
    NULLIF(COALESCE(p_metadata, '{}'::jsonb)->>'node_type', ''),
    CASE WHEN position('::' IN p_skill_name) > 0 THEN 'behavior-node' ELSE 'skill' END
  );
BEGIN
  INSERT INTO skill_system.project_graph_nodes (
    node_key,
    node_type,
    description,
    version,
    capabilities,
    effects,
    metadata
  )
  VALUES (
    p_skill_name,
    v_node_type,
    p_description,
    p_version,
    COALESCE(p_capabilities, ARRAY[]::TEXT[]),
    COALESCE(p_effects, ARRAY[]::TEXT[]),
    COALESCE(p_metadata, '{}'::jsonb)
  )
  ON CONFLICT (node_key) DO UPDATE SET
    node_type = EXCLUDED.node_type,
    description = EXCLUDED.description,
    version = EXCLUDED.version,
    capabilities = EXCLUDED.capabilities,
    effects = EXCLUDED.effects,
    metadata = EXCLUDED.metadata,
    updated_at = NOW()
  RETURNING node_id INTO v_id;

  RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION skill_system.upsert_graph_edge(
  p_from TEXT,
  p_to TEXT,
  p_type TEXT,
  p_metadata JSONB DEFAULT '{}'
) RETURNS BIGINT AS $$
DECLARE
  v_id BIGINT;
BEGIN
  INSERT INTO skill_system.project_graph_edges (
    source_node,
    target_node,
    relation_type,
    metadata
  )
  VALUES (
    p_from,
    p_to,
    p_type,
    COALESCE(p_metadata, '{}'::jsonb)
  )
  ON CONFLICT (source_node, target_node, relation_type) DO UPDATE SET
    metadata = EXCLUDED.metadata
  RETURNING edge_id INTO v_id;

  RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION skill_system.refresh_project_graph_node(
  p_node_key TEXT,
  p_description TEXT,
  p_version TEXT,
  p_capabilities TEXT[],
  p_effects TEXT[],
  p_metadata JSONB,
  p_content_hash TEXT,
  p_spec_path TEXT,
  p_operations_count INTEGER,
  p_node_type TEXT DEFAULT 'behavior-node'
) RETURNS TABLE(node_id BIGINT, skipped BOOLEAN) AS $$
DECLARE
  v_existing_id BIGINT;
  v_existing_hash TEXT;
  v_existing_spec_path TEXT;
  v_node_id BIGINT;
  v_node_type TEXT := COALESCE(
    NULLIF(p_node_type, ''),
    NULLIF(COALESCE(p_metadata, '{}'::jsonb)->>'node_type', ''),
    CASE WHEN position('::' IN p_node_key) > 0 THEN 'behavior-node' ELSE 'skill' END
  );
BEGIN
  SELECT existing.node_id, existing.content_hash, existing.spec_path
    INTO v_existing_id, v_existing_hash, v_existing_spec_path
    FROM skill_system.project_graph_nodes AS existing
    WHERE existing.node_key = p_node_key;

  IF v_existing_id IS NOT NULL
     AND v_existing_hash IS NOT NULL
     AND p_content_hash IS NOT NULL
     AND v_existing_hash = p_content_hash
     AND v_existing_spec_path IS NOT DISTINCT FROM p_spec_path THEN
    RETURN QUERY SELECT v_existing_id, TRUE;
    RETURN;
  END IF;

  INSERT INTO skill_system.project_graph_nodes (
    node_key,
    node_type,
    description,
    version,
    capabilities,
    effects,
    metadata,
    content_hash,
    spec_path,
    operations_count
  )
  VALUES (
    p_node_key,
    v_node_type,
    p_description,
    p_version,
    COALESCE(p_capabilities, ARRAY[]::TEXT[]),
    COALESCE(p_effects, ARRAY[]::TEXT[]),
    COALESCE(p_metadata, '{}'::jsonb),
    p_content_hash,
    p_spec_path,
    COALESCE(p_operations_count, 0)
  )
  ON CONFLICT (node_key) DO UPDATE SET
    node_type = EXCLUDED.node_type,
    description = EXCLUDED.description,
    version = EXCLUDED.version,
    capabilities = EXCLUDED.capabilities,
    effects = EXCLUDED.effects,
    metadata = EXCLUDED.metadata,
    content_hash = EXCLUDED.content_hash,
    spec_path = EXCLUDED.spec_path,
    operations_count = EXCLUDED.operations_count,
    updated_at = NOW()
  RETURNING project_graph_nodes.node_id INTO v_node_id;

  RETURN QUERY SELECT v_node_id, FALSE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION skill_system.refresh_graph_node(
  p_skill_name TEXT,
  p_description TEXT,
  p_version TEXT,
  p_capabilities TEXT[],
  p_effects TEXT[],
  p_metadata JSONB,
  p_content_hash TEXT,
  p_spec_path TEXT,
  p_operations_count INTEGER
) RETURNS TABLE(node_id BIGINT, skipped BOOLEAN) AS $$
BEGIN
  RETURN QUERY
  SELECT refreshed.node_id, refreshed.skipped
  FROM skill_system.refresh_project_graph_node(
    p_skill_name,
    p_description,
    p_version,
    p_capabilities,
    p_effects,
    p_metadata,
    p_content_hash,
    p_spec_path,
    p_operations_count,
    COALESCE(
      NULLIF(COALESCE(p_metadata, '{}'::jsonb)->>'node_type', ''),
      CASE WHEN position('::' IN p_skill_name) > 0 THEN 'behavior-node' ELSE 'skill' END
    )
  ) AS refreshed;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION skill_system.find_neighbors(
  p_skill_name TEXT,
  p_edge_types TEXT[] DEFAULT ARRAY['depends_on', 'delegates_to'],
  p_max_depth INTEGER DEFAULT 10
) RETURNS TABLE(neighbor_skill TEXT, depth INTEGER, path TEXT[], edge_type TEXT) AS $$
DECLARE
  v_edge_types TEXT[] := COALESCE(NULLIF(p_edge_types, ARRAY[]::TEXT[]), ARRAY['depends_on', 'delegates_to']);
BEGIN
  RETURN QUERY
  SELECT DISTINCT ON (neighbor_skill, edge_type, path)
    neighbor_skill,
    depth,
    path,
    edge_type
  FROM (
    SELECT
      e.target_node AS neighbor_skill,
      1 AS depth,
      ARRAY[e.source_node, e.target_node]::TEXT[] AS path,
      e.relation_type AS edge_type
    FROM skill_system.project_graph_edges e
    WHERE e.source_node = p_skill_name
      AND e.relation_type = ANY(v_edge_types)

    UNION ALL

    SELECT
      e.source_node AS neighbor_skill,
      1 AS depth,
      ARRAY[e.source_node, e.target_node]::TEXT[] AS path,
      e.relation_type AS edge_type
    FROM skill_system.project_graph_edges e
    WHERE e.target_node = p_skill_name
      AND e.relation_type = ANY(v_edge_types)
  ) AS direct_edges
  ORDER BY neighbor_skill, edge_type, path;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION skill_system.find_path(
  p_from_skill TEXT,
  p_to_skill TEXT,
  p_edge_types TEXT[] DEFAULT ARRAY['depends_on', 'delegates_to'],
  p_max_depth INTEGER DEFAULT 10
) RETURNS TABLE(path TEXT[], depth INTEGER) AS $$
DECLARE
  v_max_depth INTEGER := COALESCE(NULLIF(p_max_depth, NULL), 10);
  v_edge_types TEXT[] := COALESCE(NULLIF(p_edge_types, ARRAY[]::TEXT[]), ARRAY['depends_on', 'delegates_to']);
BEGIN
  IF v_max_depth < 1 THEN
    v_max_depth := 1;
  END IF;

  IF p_from_skill = p_to_skill THEN
    RETURN QUERY SELECT ARRAY[p_from_skill]::TEXT[], 0;
    RETURN;
  END IF;

  RETURN QUERY
  WITH RECURSIVE graph_walk AS (
    SELECT
      ARRAY[p_from_skill]::TEXT[] AS path,
      p_from_skill AS current_skill,
      0 AS depth
    UNION ALL
    SELECT
      gw.path || e.target_node,
      e.target_node,
      gw.depth + 1
    FROM graph_walk gw
    JOIN skill_system.project_graph_edges e
      ON e.source_node = gw.current_skill
    WHERE gw.depth < v_max_depth
      AND e.relation_type = ANY(v_edge_types)
      AND NOT (e.target_node = ANY(gw.path))
  )
  SELECT path, depth
  FROM graph_walk
  WHERE current_skill = p_to_skill
  ORDER BY depth
  LIMIT 1;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION skill_system.find_impact(
  p_skill_name TEXT,
  p_edge_types TEXT[] DEFAULT ARRAY['depends_on', 'delegates_to'],
  p_max_depth INTEGER DEFAULT 10
) RETURNS TABLE(impact_skill TEXT, depth INTEGER, path TEXT[]) AS $$
DECLARE
  v_max_depth INTEGER := COALESCE(NULLIF(p_max_depth, NULL), 10);
  v_edge_types TEXT[] := COALESCE(NULLIF(p_edge_types, ARRAY[]::TEXT[]), ARRAY['depends_on', 'delegates_to']);
BEGIN
  IF v_max_depth < 1 THEN
    v_max_depth := 1;
  END IF;

  RETURN QUERY
  WITH RECURSIVE impact_walk AS (
    SELECT
      e.source_node AS impact_skill,
      1 AS depth,
      ARRAY[e.target_node, e.source_node]::TEXT[] AS path
    FROM skill_system.project_graph_edges e
    WHERE e.target_node = p_skill_name
      AND e.relation_type = ANY(v_edge_types)
    UNION ALL
    SELECT
      e.source_node,
      iw.depth + 1,
      iw.path || e.source_node
    FROM impact_walk iw
    JOIN skill_system.project_graph_edges e
      ON e.target_node = iw.impact_skill
    WHERE iw.depth < v_max_depth
      AND e.relation_type = ANY(v_edge_types)
      AND NOT (e.source_node = ANY(iw.path))
  )
  SELECT DISTINCT ON (impact_skill)
    impact_skill,
    depth,
    path
  FROM impact_walk
  ORDER BY impact_skill, depth;
END;
$$ LANGUAGE plpgsql STABLE;

COMMIT;
