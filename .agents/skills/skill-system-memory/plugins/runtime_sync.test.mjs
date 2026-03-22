import test from 'node:test'
import assert from 'node:assert/strict'

import {
  buildCapabilityMap,
  buildBehaviorRefreshDecision,
  buildRuntimeSyncPlan,
  buildSessionProjectionSql,
  buildContextRollupRows,
} from './runtime_sync.js'

test('buildCapabilityMap distinguishes supported, gated, and deferred surfaces', () => {
  const capabilityMap = buildCapabilityMap({
    agentMemoryTables: {
      agent_memories: true,
      evolution_snapshots: true,
      soul_states: true,
      insight_facets: true,
      user_preferences: true,
      session_summaries: false,
      project_summaries: false,
      context_rollups: false,
      behavior_sources: false,
      behavior_nodes: false,
      behavior_edges: false,
      behavior_snapshots: false,
    },
    agentMemoryFunctions: {
      store_memory: true,
      search_memories: true,
      memory_health_check: true,
      insert_evolution_snapshot: true,
      get_evolution_history: true,
      get_agent_context: true,
      get_soul_state: true,
      get_recent_facets: true,
      get_user_preferences: true,
    },
    skillSystemTables: {
      policy_profiles: true,
      runs: true,
      run_events: true,
      refresh_jobs: false,
      refresh_job_events: false,
      artifact_versions: false,
    },
  })

  assert.equal(capabilityMap.core_memory.status, 'SUPPORTED_NOW')
  assert.equal(capabilityMap.evolution_ledger.status, 'SUPPORTED_NOW')
  assert.equal(capabilityMap.typed_context_reads.status, 'SUPPORTED_NOW')
  assert.equal(capabilityMap.runtime_sync_projections.status, 'GATED_OPTIONAL')
  assert.equal(capabilityMap.behavior_refresh_graph.status, 'GATED_OPTIONAL')
  assert.equal(capabilityMap.control_plane_refresh.status, 'DEFERRED_UNSUPPORTED')
})

test('buildRuntimeSyncPlan skips projection writes when optional tables are unavailable', () => {
  const capabilityMap = buildCapabilityMap({
    agentMemoryTables: {
      agent_memories: true,
      evolution_snapshots: true,
      soul_states: true,
      insight_facets: true,
      user_preferences: true,
      session_summaries: false,
      project_summaries: false,
      context_rollups: false,
      behavior_sources: false,
      behavior_nodes: false,
      behavior_edges: false,
      behavior_snapshots: false,
    },
    agentMemoryFunctions: {
      store_memory: true,
      search_memories: true,
      memory_health_check: true,
      insert_evolution_snapshot: true,
      get_evolution_history: true,
      get_agent_context: true,
      get_soul_state: true,
      get_recent_facets: true,
      get_user_preferences: true,
    },
    skillSystemTables: {
      policy_profiles: true,
      runs: true,
      run_events: true,
      refresh_jobs: false,
      refresh_job_events: false,
      artifact_versions: false,
    },
  })

  const plan = buildRuntimeSyncPlan({
    capabilityMap,
    sessionID: 'ses_123',
    project: 'skills',
    directory: '/repo/skills',
    summary: 'Implemented capability gating',
    typedContext: [{ type: 'pref', key: 'language', value: 'zh-TW' }],
  })

  assert.equal(plan.sql, '')
  assert.equal(plan.executedCapabilities.length, 0)
  assert.deepEqual(plan.skippedCapabilities.map(item => item.capability), [
    'runtime_sync_projections',
  ])
  assert.match(plan.skippedCapabilities[0].reason, /missing required tables/i)
})

test('buildBehaviorRefreshDecision skips graph refresh when behavior tables are unavailable', () => {
  const capabilityMap = buildCapabilityMap({
    agentMemoryTables: {
      agent_memories: true,
      evolution_snapshots: true,
      soul_states: true,
      insight_facets: true,
      user_preferences: true,
      session_summaries: false,
      project_summaries: false,
      context_rollups: false,
      behavior_sources: false,
      behavior_nodes: false,
      behavior_edges: false,
      behavior_snapshots: false,
    },
    agentMemoryFunctions: {
      store_memory: true,
      search_memories: true,
      memory_health_check: true,
      insert_evolution_snapshot: true,
      get_evolution_history: true,
      get_agent_context: true,
      get_soul_state: true,
      get_recent_facets: true,
      get_user_preferences: true,
    },
    skillSystemTables: {
      policy_profiles: true,
      runs: true,
      run_events: true,
      refresh_jobs: false,
      refresh_job_events: false,
      artifact_versions: false,
    },
  })

  const decision = buildBehaviorRefreshDecision({
    capabilityMap,
    scriptAvailable: true,
  })

  assert.equal(decision.allowed, false)
  assert.equal(decision.capability, 'behavior_refresh_graph')
  assert.equal(decision.status, 'GATED_OPTIONAL')
  assert.match(decision.reason, /missing required tables/i)
})

test('buildContextRollupRows converts typed context into scoped rollups', () => {
  const rows = buildContextRollupRows({
    project: 'skills',
    sessionID: 'ses_123',
    typedContext: [
      { type: 'soul', key: 'personality', value: '{"mode":"strict"}' },
      { type: 'pref', key: 'language', value: 'zh-TW' },
      { type: 'facet', key: 'coding:success', value: 'likes spec-first' },
    ],
  })

  assert.equal(rows.length, 3)
  assert.deepEqual(rows[0], {
    scopeType: 'session',
    scopeKey: 'ses_123',
    rollupKey: 'soul.personality',
    rollupValue: '{"mode":"strict"}',
    source: 'plugin-sync',
    confidence: 1,
  })
})

test('buildSessionProjectionSql emits session, project, and rollup upserts', () => {
  const sql = buildSessionProjectionSql({
    sessionID: 'ses_123',
    project: 'skills',
    directory: '/repo/skills',
    summary: 'Implemented postgres-first phase 1',
    typedContext: [
      { type: 'pref', key: 'language', value: 'zh-TW' },
    ],
  })

  assert.match(sql, /INSERT INTO session_summaries/)
  assert.match(sql, /INSERT INTO project_summaries/)
  assert.match(sql, /INSERT INTO context_rollups/)
  assert.match(sql, /ON CONFLICT \(session_id\)/)
  assert.match(sql, /ON CONFLICT \(project_key\)/)
  assert.match(sql, /ON CONFLICT \(scope_type, scope_key, rollup_key\)/)
})
