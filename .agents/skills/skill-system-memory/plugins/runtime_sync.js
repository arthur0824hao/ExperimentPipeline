export const SUPPORTED_NOW = 'SUPPORTED_NOW'
export const GATED_OPTIONAL = 'GATED_OPTIONAL'
export const DEFERRED_UNSUPPORTED = 'DEFERRED_UNSUPPORTED'

export const AGENT_MEMORY_CAPABILITY_TABLES = [
  'agent_memories',
  'evolution_snapshots',
  'soul_states',
  'insight_facets',
  'user_preferences',
  'session_summaries',
  'project_summaries',
  'context_rollups',
  'behavior_sources',
  'behavior_nodes',
  'behavior_edges',
  'behavior_snapshots',
]

export const AGENT_MEMORY_CAPABILITY_FUNCTIONS = [
  'store_memory',
  'search_memories',
  'memory_health_check',
  'insert_evolution_snapshot',
  'get_evolution_history',
  'get_agent_context',
  'get_soul_state',
  'get_recent_facets',
  'get_user_preferences',
]

export const SKILL_SYSTEM_CAPABILITY_TABLES = [
  'policy_profiles',
  'runs',
  'run_events',
  'refresh_jobs',
  'refresh_job_events',
  'artifact_versions',
]

const CORE_MEMORY_TABLES = ['agent_memories']
const CORE_MEMORY_FUNCTIONS = ['store_memory', 'search_memories', 'memory_health_check']
const EVOLUTION_LEDGER_TABLES = ['evolution_snapshots']
const EVOLUTION_LEDGER_FUNCTIONS = ['insert_evolution_snapshot', 'get_evolution_history']
const TYPED_CONTEXT_TABLES = ['soul_states', 'insight_facets', 'user_preferences']
const TYPED_CONTEXT_FUNCTIONS = ['get_agent_context', 'get_soul_state', 'get_recent_facets', 'get_user_preferences']
const RUNTIME_SYNC_PROJECTION_TABLES = ['session_summaries', 'project_summaries', 'context_rollups']
const BEHAVIOR_GRAPH_TABLES = ['behavior_sources', 'behavior_nodes', 'behavior_edges', 'behavior_snapshots']
const CONTROL_PLANE_TABLES = ['refresh_jobs', 'refresh_job_events', 'artifact_versions']

function sqlLiteral(value) {
  if (value === null || value === undefined) return 'NULL'
  return `'${String(value).replace(/'/g, "''")}'`
}

function presentNames(values, names) {
  return names.filter(name => values[name])
}

function missingNames(values, names) {
  return names.filter(name => !values[name])
}

function capabilityEntry(status, currentSupportSurface, gatingBehavior, evidence) {
  return {
    status,
    currentSupportSurface,
    gatingBehavior,
    evidence,
  }
}

function jsonSql(value) {
  return `${sqlLiteral(JSON.stringify(value))}::jsonb`
}

function sqlTextArray(values) {
  return `ARRAY[${values.map(sqlLiteral).join(', ')}]::text[]`
}

export function buildContextRollupRows({ project, sessionID, typedContext }) {
  const rows = []
  for (const row of typedContext || []) {
    if (!row?.type || !row?.key) continue
    rows.push({
      scopeType: 'session',
      scopeKey: sessionID,
      rollupKey: `${row.type}.${row.key}`,
      rollupValue: String(row.value ?? ''),
      source: 'plugin-sync',
      confidence: 1,
    })
  }

  return rows
}

export function buildCapabilityMap({
  agentMemoryTables = {},
  agentMemoryFunctions = {},
  skillSystemTables = {},
}) {
  const coreMissingTables = missingNames(agentMemoryTables, CORE_MEMORY_TABLES)
  const coreMissingFunctions = missingNames(agentMemoryFunctions, CORE_MEMORY_FUNCTIONS)
  const evolutionMissingTables = missingNames(agentMemoryTables, EVOLUTION_LEDGER_TABLES)
  const evolutionMissingFunctions = missingNames(agentMemoryFunctions, EVOLUTION_LEDGER_FUNCTIONS)
  const typedContextMissingTables = missingNames(agentMemoryTables, TYPED_CONTEXT_TABLES)
  const typedContextMissingFunctions = missingNames(agentMemoryFunctions, TYPED_CONTEXT_FUNCTIONS)
  const projectionMissingTables = missingNames(agentMemoryTables, RUNTIME_SYNC_PROJECTION_TABLES)
  const behaviorMissingTables = missingNames(agentMemoryTables, BEHAVIOR_GRAPH_TABLES)
  const controlPlaneMissingTables = missingNames(skillSystemTables, CONTROL_PLANE_TABLES)

  return {
    core_memory: capabilityEntry(
      coreMissingTables.length === 0 && coreMissingFunctions.length === 0 ? SUPPORTED_NOW : DEFERRED_UNSUPPORTED,
      'Canonical agent_memories storage plus read/search/health routines',
      'Fail closed if canonical memory tables or routines are missing',
      [
        `tables present: ${presentNames(agentMemoryTables, CORE_MEMORY_TABLES).join(', ') || '<none>'}`,
        `routines present: ${presentNames(agentMemoryFunctions, CORE_MEMORY_FUNCTIONS).join(', ') || '<none>'}`,
      ],
    ),
    evolution_ledger: capabilityEntry(
      evolutionMissingTables.length === 0 && evolutionMissingFunctions.length === 0 ? SUPPORTED_NOW : DEFERRED_UNSUPPORTED,
      'Typed evolution_snapshots ledger plus evolution history routines',
      'Fail closed if typed evolution ledger tables or routines are missing',
      [
        `tables present: ${presentNames(agentMemoryTables, EVOLUTION_LEDGER_TABLES).join(', ') || '<none>'}`,
        `routines present: ${presentNames(agentMemoryFunctions, EVOLUTION_LEDGER_FUNCTIONS).join(', ') || '<none>'}`,
      ],
    ),
    compaction_logging: capabilityEntry(
      coreMissingTables.length === 0 && coreMissingFunctions.length === 0 ? SUPPORTED_NOW : GATED_OPTIONAL,
      'JSONL compaction log plus store_memory writeback into agent_memories',
      'Keep JSONL logging active and skip DB writes when target resolution or canonical surfaces are unsafe',
      [
        'depends on store_memory for database writeback',
      ],
    ),
    typed_context_reads: capabilityEntry(
      typedContextMissingTables.length === 0 && typedContextMissingFunctions.length === 0 ? SUPPORTED_NOW : DEFERRED_UNSUPPORTED,
      'Typed soul/facet/preference reads via get_agent_context and related getters',
      'Fail closed if typed context tables or getter routines are missing',
      [
        `tables present: ${presentNames(agentMemoryTables, TYPED_CONTEXT_TABLES).join(', ') || '<none>'}`,
        `routines present: ${presentNames(agentMemoryFunctions, TYPED_CONTEXT_FUNCTIONS).join(', ') || '<none>'}`,
      ],
    ),
    runtime_sync_projections: capabilityEntry(
      projectionMissingTables.length === 0 ? SUPPORTED_NOW : GATED_OPTIONAL,
      'Optional session/project/context projection upserts from compaction summaries',
      projectionMissingTables.length === 0
        ? 'Projection upserts may run because all projection tables are present'
        : `Skip runtime projection writes when missing required tables: ${projectionMissingTables.join(', ')}`,
      [
        `tables present: ${presentNames(agentMemoryTables, RUNTIME_SYNC_PROJECTION_TABLES).join(', ') || '<none>'}`,
        `tables missing: ${projectionMissingTables.join(', ') || '<none>'}`,
      ],
    ),
    behavior_refresh_graph: capabilityEntry(
      behaviorMissingTables.length === 0 ? SUPPORTED_NOW : GATED_OPTIONAL,
      'Optional behavior graph refresh into behavior_* tables',
      behaviorMissingTables.length === 0
        ? 'Behavior refresh may run because all behavior graph tables are present'
        : `Skip behavior refresh when missing required tables: ${behaviorMissingTables.join(', ')}`,
      [
        `tables present: ${presentNames(agentMemoryTables, BEHAVIOR_GRAPH_TABLES).join(', ') || '<none>'}`,
        `tables missing: ${behaviorMissingTables.join(', ') || '<none>'}`,
      ],
    ),
    control_plane_refresh: capabilityEntry(
      DEFERRED_UNSUPPORTED,
      'Deferred control-plane refresh orchestration outside the current plugin runtime',
      'Do not attempt control-plane refresh writes from the current runtime',
      [
        `tables present: ${presentNames(skillSystemTables, CONTROL_PLANE_TABLES).join(', ') || '<none>'}`,
        `tables missing: ${controlPlaneMissingTables.join(', ') || '<none>'}`,
      ],
    ),
  }
}

function buildRollupSql(row) {
  return `INSERT INTO context_rollups (scope_type, scope_key, rollup_key, rollup_value, source, confidence, metadata)
VALUES (${sqlLiteral(row.scopeType)}, ${sqlLiteral(row.scopeKey)}, ${sqlLiteral(row.rollupKey)}, ${sqlLiteral(row.rollupValue)}, ${sqlLiteral(row.source)}, ${row.confidence}, ${jsonSql({ source: 'opencode-plugin' })})
ON CONFLICT (scope_type, scope_key, rollup_key) DO UPDATE SET
  rollup_value = EXCLUDED.rollup_value,
  source = EXCLUDED.source,
  confidence = EXCLUDED.confidence,
  updated_at = NOW();`
}

export function buildSessionProjectionSql({ sessionID, project, directory, summary, typedContext }) {
  const safeSummary = String(summary || '').slice(0, 8000)
  const projectKey = project || 'unknown'
  const metadata = {
    source: 'opencode-plugin',
    cwd: directory,
    session_id: sessionID,
    project_key: projectKey,
  }

  const rollups = buildContextRollupRows({ project: projectKey, sessionID, typedContext })

  const statements = [
    `WITH session_mem AS (
  SELECT store_memory(
    'episodic',
    'session-summary',
    ${sqlTextArray(['session-summary', `project:${projectKey}`, `session:${sessionID}`])},
    ${sqlLiteral(`Session summary: ${sessionID}`)},
    ${sqlLiteral(safeSummary)},
    ${jsonSql({ ...metadata, typed_table: 'session_summaries' })},
    'opencode-plugin',
    ${sqlLiteral(sessionID)},
    8.0
  ) AS memory_id
)
INSERT INTO session_summaries (memory_id, session_id, project_key, summary_text, source_hash, metadata)
SELECT memory_id, ${sqlLiteral(sessionID)}, ${sqlLiteral(projectKey)}, ${sqlLiteral(safeSummary)}, md5(${sqlLiteral(safeSummary)}), ${jsonSql(metadata)}
FROM session_mem
ON CONFLICT (session_id) DO UPDATE SET
  memory_id = EXCLUDED.memory_id,
  project_key = EXCLUDED.project_key,
  summary_text = EXCLUDED.summary_text,
  source_hash = EXCLUDED.source_hash,
  metadata = EXCLUDED.metadata,
  summary_version = session_summaries.summary_version + 1,
  updated_at = NOW();`,
    `WITH project_mem AS (
  SELECT store_memory(
    'semantic',
    'project-summary',
    ${sqlTextArray(['project-summary', `project:${projectKey}`])},
    ${sqlLiteral(`Project summary: ${projectKey}`)},
    ${sqlLiteral(safeSummary)},
    ${jsonSql({ ...metadata, typed_table: 'project_summaries' })},
    'opencode-plugin',
    NULL,
    7.5
  ) AS memory_id
)
INSERT INTO project_summaries (memory_id, project_key, summary_text, source_hash, metadata)
SELECT memory_id, ${sqlLiteral(projectKey)}, ${sqlLiteral(safeSummary)}, md5(${sqlLiteral(safeSummary)}), ${jsonSql(metadata)}
FROM project_mem
ON CONFLICT (project_key) DO UPDATE SET
  memory_id = EXCLUDED.memory_id,
  summary_text = EXCLUDED.summary_text,
  source_hash = EXCLUDED.source_hash,
  metadata = EXCLUDED.metadata,
  summary_version = project_summaries.summary_version + 1,
  updated_at = NOW();`,
    ...rollups.map(buildRollupSql),
  ]

  return statements.join('\n\n')
}

export function buildRuntimeSyncPlan({ capabilityMap, sessionID, project, directory, summary, typedContext }) {
  const runtimeSyncCapability = capabilityMap?.runtime_sync_projections
  if (!runtimeSyncCapability || runtimeSyncCapability.status !== SUPPORTED_NOW) {
    return {
      sql: '',
      executedCapabilities: [],
      skippedCapabilities: [
        {
          capability: 'runtime_sync_projections',
          status: runtimeSyncCapability?.status || GATED_OPTIONAL,
          reason: runtimeSyncCapability?.gatingBehavior || 'Capability probe did not prove projection support',
        },
      ],
    }
  }

  return {
    sql: buildSessionProjectionSql({ sessionID, project, directory, summary, typedContext }),
    executedCapabilities: ['runtime_sync_projections'],
    skippedCapabilities: [],
  }
}

export function buildBehaviorRefreshDecision({ capabilityMap, scriptAvailable }) {
  const behaviorCapability = capabilityMap?.behavior_refresh_graph
  if (!scriptAvailable) {
    return {
      allowed: false,
      capability: 'behavior_refresh_graph',
      status: behaviorCapability?.status || GATED_OPTIONAL,
      reason: 'Behavior refresh script is not available in the current workspace',
    }
  }
  if (!behaviorCapability || behaviorCapability.status !== SUPPORTED_NOW) {
    return {
      allowed: false,
      capability: 'behavior_refresh_graph',
      status: behaviorCapability?.status || GATED_OPTIONAL,
      reason: behaviorCapability?.gatingBehavior || 'Capability probe did not prove behavior graph support',
    }
  }
  return {
    allowed: true,
    capability: 'behavior_refresh_graph',
    status: behaviorCapability.status,
    reason: 'Behavior graph tables are present',
  }
}
