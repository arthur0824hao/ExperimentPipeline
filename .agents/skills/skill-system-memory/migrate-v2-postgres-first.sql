BEGIN;

CREATE TABLE IF NOT EXISTS session_summaries (
    id               BIGSERIAL PRIMARY KEY,
    memory_id        BIGINT REFERENCES agent_memories(id) ON DELETE SET NULL,
    session_id       VARCHAR(100) NOT NULL,
    project_key      VARCHAR(100),
    summary_text     TEXT NOT NULL,
    summary_version  INTEGER NOT NULL DEFAULT 1,
    source_hash      TEXT,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(session_id)
);

CREATE INDEX IF NOT EXISTS idx_ssum_project   ON session_summaries(project_key) WHERE project_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ssum_updated   ON session_summaries(updated_at DESC);

CREATE TABLE IF NOT EXISTS project_summaries (
    id               BIGSERIAL PRIMARY KEY,
    memory_id        BIGINT REFERENCES agent_memories(id) ON DELETE SET NULL,
    project_key      VARCHAR(100) NOT NULL,
    summary_text     TEXT NOT NULL,
    summary_version  INTEGER NOT NULL DEFAULT 1,
    source_hash      TEXT,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_key)
);

CREATE INDEX IF NOT EXISTS idx_psum_updated   ON project_summaries(updated_at DESC);

CREATE TABLE IF NOT EXISTS context_rollups (
    id               BIGSERIAL PRIMARY KEY,
    memory_id        BIGINT REFERENCES agent_memories(id) ON DELETE SET NULL,
    scope_type       VARCHAR(50) NOT NULL,
    scope_key        VARCHAR(100) NOT NULL,
    rollup_key       VARCHAR(100) NOT NULL,
    rollup_value     TEXT NOT NULL,
    source           VARCHAR(50) NOT NULL DEFAULT 'derived',
    confidence       NUMERIC(3,2) NOT NULL DEFAULT 1.00 CHECK (confidence BETWEEN 0 AND 1),
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(scope_type, scope_key, rollup_key)
);

CREATE INDEX IF NOT EXISTS idx_cr_scope       ON context_rollups(scope_type, scope_key);

CREATE TABLE IF NOT EXISTS behavior_sources (
    id               BIGSERIAL PRIMARY KEY,
    skill_id         VARCHAR(100) NOT NULL,
    source_path      TEXT NOT NULL,
    source_kind      VARCHAR(50) NOT NULL,
    content_hash     TEXT,
    parser_version   VARCHAR(50),
    status           VARCHAR(30) NOT NULL DEFAULT 'active',
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_parsed_at   TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(skill_id, source_path)
);

CREATE INDEX IF NOT EXISTS idx_bs_skill       ON behavior_sources(skill_id);
CREATE INDEX IF NOT EXISTS idx_bs_kind        ON behavior_sources(source_kind);
CREATE INDEX IF NOT EXISTS idx_bs_status      ON behavior_sources(status);

CREATE TABLE IF NOT EXISTS behavior_nodes (
    id               BIGSERIAL PRIMARY KEY,
    skill_id         VARCHAR(100) NOT NULL,
    source_id        BIGINT REFERENCES behavior_sources(id) ON DELETE SET NULL,
    node_type        VARCHAR(50) NOT NULL,
    node_key         TEXT NOT NULL,
    title            TEXT NOT NULL,
    description      TEXT,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(skill_id, node_type, node_key)
);

CREATE INDEX IF NOT EXISTS idx_bn_skill       ON behavior_nodes(skill_id);
CREATE INDEX IF NOT EXISTS idx_bn_type        ON behavior_nodes(node_type);
CREATE INDEX IF NOT EXISTS idx_bn_source      ON behavior_nodes(source_id) WHERE source_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS behavior_edges (
    id               BIGSERIAL PRIMARY KEY,
    source_id        BIGINT REFERENCES behavior_sources(id) ON DELETE SET NULL,
    from_node_id     BIGINT NOT NULL REFERENCES behavior_nodes(id) ON DELETE CASCADE,
    to_node_id       BIGINT NOT NULL REFERENCES behavior_nodes(id) ON DELETE CASCADE,
    edge_type        VARCHAR(50) NOT NULL,
    label            TEXT,
    confidence       NUMERIC(3,2) NOT NULL DEFAULT 1.00 CHECK (confidence BETWEEN 0 AND 1),
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_be_from        ON behavior_edges(from_node_id);
CREATE INDEX IF NOT EXISTS idx_be_to          ON behavior_edges(to_node_id);
CREATE INDEX IF NOT EXISTS idx_be_type        ON behavior_edges(edge_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_be_unique ON behavior_edges(from_node_id, to_node_id, edge_type, COALESCE(label, ''));

CREATE TABLE IF NOT EXISTS behavior_snapshots (
    id               BIGSERIAL PRIMARY KEY,
    memory_id        BIGINT REFERENCES agent_memories(id) ON DELETE SET NULL,
    skill_id         VARCHAR(100) NOT NULL,
    snapshot_tag     VARCHAR(100) NOT NULL,
    graph_json       JSONB NOT NULL DEFAULT '{}'::jsonb,
    mermaid          TEXT,
    source_hash_set  JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(skill_id, snapshot_tag)
);

CREATE INDEX IF NOT EXISTS idx_bsnap_skill    ON behavior_snapshots(skill_id, created_at DESC);

COMMIT;
