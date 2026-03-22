// OpenCode plugin: skill-system-memory compaction logger + auto-context aggregation
// v3: Auto-context — queries relevant memories during compaction and injects them into agent context

import fs from 'node:fs'
import path from 'node:path'
import os from 'node:os'

import {
  AGENT_MEMORY_CAPABILITY_FUNCTIONS,
  AGENT_MEMORY_CAPABILITY_TABLES,
  SKILL_SYSTEM_CAPABILITY_TABLES,
  buildBehaviorRefreshDecision,
  buildCapabilityMap,
  buildRuntimeSyncPlan,
} from './runtime_sync.js'

/** @type {import('@opencode-ai/plugin').Plugin} */
export const SkillSystemMemory = async ({ $, directory, client }) => {
  const home = os.homedir()
  const stateDir = path.join(home, '.config', 'opencode', 'skill-system-memory')
  const eventsPath = path.join(stateDir, 'compaction-events.jsonl')
  const setupPath = path.join(stateDir, 'setup.json')

  const defaultPgUser = (() => {
    try { return os.userInfo().username } catch { return 'postgres' }
  })()

  const hasSetup = () => {
    try { return fs.existsSync(setupPath) } catch { return false }
  }

  const ensureDir = () => {
    try { fs.mkdirSync(stateDir, { recursive: true }) } catch {}
  }

  const readSetup = () => {
    try {
      const raw = fs.readFileSync(setupPath, { encoding: 'utf8' })
      return JSON.parse(raw)
    } catch {
      return null
    }
  }

  const isPluginEnabled = () => {
    const setup = readSetup()
    return setup?.selected?.opencode_plugin === true
  }

  const appendJsonl = (obj) => {
    try {
      ensureDir()
      fs.appendFileSync(eventsPath, `${JSON.stringify(obj)}\n`, { encoding: 'utf8' })
    } catch {}
  }

  const nowUtc = () => new Date().toISOString().replace(/\.\d{3}Z$/, 'Z')

  const escapeSqlLiteral = (s) => String(s ?? '').replace(/'/g, "''")

  let lastTargetResolutionError = null

  const resolveMemoryDbTarget = () => {
    const explicit = String(process.env.SKILL_PGDATABASE ?? '').trim()
    const ambient = String(process.env.PGDATABASE ?? '').trim()

    if (explicit) {
      if (ambient && explicit !== ambient) {
        return { ok: true, db: explicit, source: `SKILL_PGDATABASE(overrides:${ambient})` }
      }
      return { ok: true, db: explicit, source: 'SKILL_PGDATABASE' }
    }

    if (ambient) {
      return {
        ok: false,
        error: `ambient PGDATABASE=${ambient} is not accepted for memory; set SKILL_PGDATABASE explicitly`,
      }
    }

    return { ok: true, db: 'agent_memory', source: 'default:agent_memory' }
  }

  const noteTargetResolutionError = async (message) => {
    if (message === lastTargetResolutionError) return
    lastTargetResolutionError = message
    appendJsonl({ event: 'memory.db_target.error', time_utc: nowUtc(), cwd: directory, error: message })
    try {
      await client.tui.showToast({
        directory,
        title: 'skill-system-memory',
        message,
        variant: 'warning',
        duration: 8000,
      })
    } catch {}
  }

  const getPgConfig = async () => {
    const target = resolveMemoryDbTarget()
    if (!target.ok) {
      await noteTargetResolutionError(target.error)
      return target
    }

    return {
      ok: true,
      host: process.env.PGHOST ?? 'localhost',
      port: process.env.PGPORT ?? '5432',
      db: target.db,
      dbSource: target.source,
      user: process.env.PGUSER ?? defaultPgUser,
    }
  }

  /** Extract project name from directory path (last non-empty segment) */
  const getProjectName = () => {
    const parts = directory.replace(/[\\/]+$/, '').split(/[\\/]/)
    return parts[parts.length - 1] || 'unknown'
  }

  /** Count session events in JSONL for a given sessionID */
  const countSessionEvents = (sessionID) => {
    try {
      const lines = fs.readFileSync(eventsPath, { encoding: 'utf8' }).split('\n').filter(Boolean)
      return lines.reduce((count, line) => {
        try {
          const obj = JSON.parse(line)
          return (obj.session_id === sessionID) ? count + 1 : count
        } catch { return count }
      }, 0)
    } catch { return 0 }
  }

  /** Read recent JSONL events for a session (last N) to build context */
  const getRecentSessionEvents = (sessionID, maxEvents = 10) => {
    try {
      const lines = fs.readFileSync(eventsPath, { encoding: 'utf8' }).split('\n').filter(Boolean)
      const sessionLines = []
      for (const line of lines) {
        try {
          const obj = JSON.parse(line)
          if (obj.session_id === sessionID) sessionLines.push(obj)
        } catch { /* skip malformed */ }
      }
      return sessionLines.slice(-maxEvents)
    } catch { return [] }
  }

  const sessionCompactionCount = new Map()
  const sessionTypedContext = new Map()
  let lastBehaviorRefreshAt = 0
  let capabilityCache = null
  let capabilityCacheAt = 0

  const findMemPyDir = () => {
    const candidates = [
      path.join(directory, 'skills', 'skill-system-memory', 'scripts'),
      path.join(directory, '.agents', 'skills', 'skill-system-memory', 'scripts'),
      path.join(directory, '.agent', 'skills', 'skill-system-memory', 'scripts'),
    ]
    for (const dir of candidates) {
      try {
        if (fs.existsSync(path.join(dir, 'mem.py'))) return dir
      } catch { /* skip */ }
    }
    return null
  }

  const memPyDir = findMemPyDir()

  const findBehaviorRefreshScript = () => {
    const candidates = [
      path.join(directory, 'skills', 'skill-system-behavior', 'scripts', 'refresh_behavior_projections.py'),
      path.join(directory, '.agents', 'skills', 'skill-system-behavior', 'scripts', 'refresh_behavior_projections.py'),
      path.join(directory, '.agent', 'skills', 'skill-system-behavior', 'scripts', 'refresh_behavior_projections.py'),
    ]
    for (const file of candidates) {
      try {
        if (fs.existsSync(file)) return file
      } catch { /* skip */ }
    }
    return null
  }

  const behaviorRefreshScript = findBehaviorRefreshScript()

  const parsePresenceOutput = (output, names) => {
    const present = new Set(String(output || '').split('\n').map(line => line.trim()).filter(Boolean))
    return Object.fromEntries(names.map(name => [name, present.has(name)]))
  }

  const queryTablePresence = async ({ pg, db = pg.db, schema, names }) => {
    const quoted = names.map(name => `'${escapeSqlLiteral(name)}'`).join(', ')
    const sql = `SELECT table_name FROM information_schema.tables WHERE table_schema = '${escapeSqlLiteral(schema)}' AND table_name IN (${quoted}) ORDER BY table_name;`
    const result = await $`psql -w -h ${pg.host} -p ${pg.port} -U ${pg.user} -d ${db} -v ON_ERROR_STOP=1 -t -A -F '|' -c ${sql}`.quiet()
    return parsePresenceOutput(result.stdout, names)
  }

  const queryRoutinePresence = async ({ pg, db = pg.db, schema, names }) => {
    const quoted = names.map(name => `'${escapeSqlLiteral(name)}'`).join(', ')
    const sql = `SELECT routine_name FROM information_schema.routines WHERE routine_schema = '${escapeSqlLiteral(schema)}' AND routine_name IN (${quoted}) ORDER BY routine_name;`
    const result = await $`psql -w -h ${pg.host} -p ${pg.port} -U ${pg.user} -d ${db} -v ON_ERROR_STOP=1 -t -A -F '|' -c ${sql}`.quiet()
    return parsePresenceOutput(result.stdout, names)
  }

  const getRuntimeCapabilityMap = async (pg) => {
    const now = Date.now()
    if (capabilityCache && (now - capabilityCacheAt) < 60_000) {
      return capabilityCache
    }

    try {
      const [agentMemoryTables, agentMemoryFunctions, skillSystemTables] = await Promise.all([
        queryTablePresence({ pg, schema: 'public', names: AGENT_MEMORY_CAPABILITY_TABLES }),
        queryRoutinePresence({ pg, schema: 'public', names: AGENT_MEMORY_CAPABILITY_FUNCTIONS }),
        queryTablePresence({ pg, db: 'skill_system', schema: 'skill_system', names: SKILL_SYSTEM_CAPABILITY_TABLES }),
      ])
      capabilityCache = buildCapabilityMap({
        agentMemoryTables,
        agentMemoryFunctions,
        skillSystemTables,
      })
      capabilityCacheAt = now
      return capabilityCache
    } catch (error) {
      appendJsonl({
        event: 'runtime.capability_probe.error',
        time_utc: nowUtc(),
        cwd: directory,
        error: String(error),
      })
      capabilityCache = null
      capabilityCacheAt = 0
      return null
    }
  }

  const queryTypedContext = async (user = 'arthu') => {
    if (!isPluginEnabled()) return null
    const setup = readSetup()
    if (!setup?.selected?.pgpass) return null

    try {
      const pg = await getPgConfig()
      if (!pg.ok) return null
      const u = escapeSqlLiteral(user)
      const sql = `SELECT context_type, context_key, LEFT(context_value, 300) AS context_value FROM get_agent_context('${u}', 5);`
      const result = await $`psql -w -h ${pg.host} -p ${pg.port} -U ${pg.user} -d ${pg.db} -v ON_ERROR_STOP=1 -t -A -F '|' -c ${sql}`.quiet()
      const output = result.stdout?.trim()
      if (!output) return null

      const rows = output.split('\n').filter(Boolean).map(line => {
        const [ctype, ckey, cval] = line.split('|')
        return { type: ctype, key: ckey, value: (cval || '').replace(/\n/g, ' ') }
      })
      return rows.length > 0 ? rows : null
    } catch {
      return null
    }
  }

  const queryRelevantMemories = async (keywords, limit = 5) => {
    if (!isPluginEnabled()) return null
    const setup = readSetup()
    if (!setup?.selected?.pgpass) return null

    try {
      const pg = await getPgConfig()
      if (!pg.ok) return null
      const kw = escapeSqlLiteral(keywords)
      const sql = `SELECT id, memory_type, category, title, LEFT(content, 200) AS snippet, importance_score, relevance_score FROM search_memories('${kw}', NULL, NULL, NULL, NULL, 5.0, ${limit}) ORDER BY relevance_score DESC;`
      const result = await $`psql -w -h ${pg.host} -p ${pg.port} -U ${pg.user} -d ${pg.db} -v ON_ERROR_STOP=1 -t -A -F '|' -c ${sql}`.quiet()
      const output = result.stdout?.trim()
      if (!output) return null

      const rows = output.split('\n').filter(Boolean).map(line => {
        const [id, mtype, cat, title, snippet, imp, rel] = line.split('|')
        return { id, mtype, cat, title, snippet: (snippet || '').replace(/\n/g, ' '), imp, rel }
      })
      return rows.length > 0 ? rows : null
    } catch {
      return null
    }
  }

  const extractKeywords = (project, recentEvents) => {
    const parts = [project]
    for (const evt of recentEvents) {
      if (evt.topic) parts.push(evt.topic)
      if (evt.summary) parts.push(evt.summary)
    }
    const unique = [...new Set(parts.filter(Boolean))]
    return unique.join(' ').slice(0, 200)
  }

  appendJsonl({ event: 'plugin.loaded', time_utc: nowUtc(), cwd: directory, project: getProjectName(), mem_py: !!memPyDir })

  if (!hasSetup()) {
    try {
      await client.tui.showToast({
        directory,
        title: 'skill-system-memory',
        message: 'Optional setup not completed. Run bootstrap to enable pgpass/pgvector/Ollama and record setup.json.',
        variant: 'warning',
        duration: 8000,
      })
    } catch {}
  }

  const tryVerifySetup = async () => {
    if (!isPluginEnabled()) return
    const setup = readSetup()
    const selected = setup?.selected
    if (!selected) return

    const pg = await getPgConfig()
    if (!pg.ok) return
    const time = nowUtc()
    const results = { pgvector: null, ollama: null }

    if (selected.pgvector) {
      try {
        await $`psql -w -h ${pg.host} -p ${pg.port} -U ${pg.user} -d ${pg.db} -v ON_ERROR_STOP=1 -c "SELECT 1 FROM pg_extension WHERE extname='vector';"`.quiet()
        results.pgvector = true
      } catch {
        results.pgvector = false
        try {
          await client.tui.showToast({
            directory,
            title: 'skill-system-memory',
            message: 'Setup selected pgvector=true but extension "vector" is not available (or psql auth failed).',
            variant: 'warning',
            duration: 8000,
          })
        } catch {}
      }
    }

    if (selected.ollama) {
      try {
        await $`curl -fsS --max-time 2 http://localhost:11434/api/tags`.quiet()
        results.ollama = true
      } catch {
        results.ollama = false
        try {
          await client.tui.showToast({
            directory,
            title: 'skill-system-memory',
            message: 'Setup selected ollama=true but http://localhost:11434 is not reachable.',
            variant: 'warning',
            duration: 8000,
          })
        } catch {}
      }
    }

    appendJsonl({ event: 'setup.verified', time_utc: time, selected, results })
  }

  try { await tryVerifySetup() } catch {}

  const tryPsqlStore = async ({ sessionID, summary }) => {
    if (!isPluginEnabled()) return
    const setup = readSetup()
    if (!setup?.selected?.pgpass) return

    try {
      const pg = await getPgConfig()
      if (!pg.ok) return
      const time = nowUtc()
      const sid = escapeSqlLiteral(sessionID)
      const cwd = escapeSqlLiteral(directory)
      const t = escapeSqlLiteral(time)
      const project = escapeSqlLiteral(getProjectName())
      const compactionNum = sessionCompactionCount.get(sessionID) ?? 1

      const body = summary
        ? escapeSqlLiteral(summary.slice(0, 4000))
        : escapeSqlLiteral(`[no summary captured] Compaction #${compactionNum} — ${getProjectName()} — ${directory}`)

      const title = escapeSqlLiteral(
        `Compaction #${compactionNum} — ${getProjectName()} — ${sessionID.slice(0, 12)}`
      )

      const sql = `SELECT store_memory('episodic','compaction',ARRAY['compaction','opencode','${project}'],'${title}','${body}',jsonb_build_object('session_id','${sid}','cwd','${cwd}','time_utc','${t}','project','${project}','compaction_num',${compactionNum},'has_summary',${!!summary},'source','opencode-plugin'),'opencode-plugin','${sid}',7.0);`
      await $`psql -w -h ${pg.host} -p ${pg.port} -U ${pg.user} -d ${pg.db} -v ON_ERROR_STOP=1 -c ${sql}`.quiet()
    } catch {}
  }

  const trySyncRuntimeState = async ({ sessionID, project, summary, typedCtx }) => {
    if (!isPluginEnabled()) return
    const setup = readSetup()
    if (!setup?.selected?.pgpass) return

    try {
      const pg = await getPgConfig()
      if (!pg.ok) return
      const capabilityMap = await getRuntimeCapabilityMap(pg)
      if (!capabilityMap) {
        appendJsonl({
          event: 'runtime.sync.skipped',
          time_utc: nowUtc(),
          session_id: sessionID,
          project,
          skipped: [
            {
              capability: 'runtime_sync_projections',
              status: 'GATED_OPTIONAL',
              reason: 'Capability probe failed; optional runtime projections were skipped safely.',
            },
          ],
        })
        return
      }

      const plan = buildRuntimeSyncPlan({
        capabilityMap,
        sessionID,
        project,
        directory,
        summary,
        typedContext: typedCtx || [],
      })
      if (plan.skippedCapabilities.length > 0) {
        appendJsonl({
          event: 'runtime.sync.skipped',
          time_utc: nowUtc(),
          session_id: sessionID,
          project,
          skipped: plan.skippedCapabilities,
        })
      }
      if (!plan.sql) return

      await $`psql -w -h ${pg.host} -p ${pg.port} -U ${pg.user} -d ${pg.db} -v ON_ERROR_STOP=1 -c ${plan.sql}`.quiet()
      appendJsonl({
        event: 'runtime.synced',
        time_utc: nowUtc(),
        session_id: sessionID,
        project,
        rollup_count: (typedCtx || []).length,
        executed_capabilities: plan.executedCapabilities,
      })
    } catch (error) {
      appendJsonl({ event: 'runtime.sync.error', time_utc: nowUtc(), session_id: sessionID, project, error: String(error) })
    }
  }

  const tryRefreshBehaviorProjections = async () => {
    if (!isPluginEnabled()) return
    const setup = readSetup()
    if (!setup?.selected?.pgpass) return
    if (!behaviorRefreshScript) return

    const now = Date.now()
    if (now - lastBehaviorRefreshAt < 60_000) return
    lastBehaviorRefreshAt = now

    try {
      const pg = await getPgConfig()
      if (!pg.ok) return
      const capabilityMap = await getRuntimeCapabilityMap(pg)
      const decision = buildBehaviorRefreshDecision({
        capabilityMap,
        scriptAvailable: Boolean(behaviorRefreshScript),
      })
      if (!decision.allowed) {
        appendJsonl({
          event: 'behavior.refresh.skipped',
          time_utc: nowUtc(),
          cwd: directory,
          capability: decision.capability,
          status: decision.status,
          reason: decision.reason,
        })
        return
      }
      const skillsDir = path.join(directory, 'skills')
      const result = await $`python3 ${behaviorRefreshScript} --skills-dir ${skillsDir} --format sql`.quiet()
      const sql = result.stdout?.trim()
      if (!sql) return
      await $`psql -w -h ${pg.host} -p ${pg.port} -U ${pg.user} -d ${pg.db} -v ON_ERROR_STOP=1 -c ${sql}`.quiet()
      appendJsonl({ event: 'behavior.refresh.ok', time_utc: nowUtc(), cwd: directory, script: behaviorRefreshScript })
    } catch (error) {
      appendJsonl({ event: 'behavior.refresh.error', time_utc: nowUtc(), cwd: directory, script: behaviorRefreshScript, error: String(error) })
    }
  }

  return {
    'experimental.session.compacting': async (input, output) => {
      const sessionID = input.sessionID
      const time = nowUtc()
      const project = getProjectName()
      const summary = input.summary || input.context || ''

      const prevCount = sessionCompactionCount.get(sessionID) ?? 0
      sessionCompactionCount.set(sessionID, prevCount + 1)

      appendJsonl({
        event: 'session.compacting',
        time_utc: time,
        session_id: sessionID,
        cwd: directory,
        project,
        compaction_num: prevCount + 1,
        summary_length: summary.length,
        summary_preview: summary.slice(0, 500),
      })

      if (!hasSetup()) {
        output.context.push(`## Setup Missing (skill-system-memory)
- Ask the user if they want to enable optional components: pgpass, pgvector, local embeddings (Ollama)
- Record the choice by running the bootstrap script in the skill directory (writes setup.json)
- Recommended: install all optional components, then fix any failures reported
`)
      }

      const target = resolveMemoryDbTarget()
      if (!target.ok) {
        output.context.push(`## Memory DB Target Warning
- ${target.error}
- Memory plugin DB reads/writes are fail-closed until SKILL_PGDATABASE is set explicitly.
`)
      } else {
        output.context.push(`## Memory DB Target
- db: ${target.db}
- source: ${target.source}
`)
      }

      const recentEvents = getRecentSessionEvents(sessionID, 5)
      const keywords = extractKeywords(project, recentEvents)

      const [typedCtx, memories] = await Promise.all([
        queryTypedContext(),
        queryRelevantMemories(keywords),
      ])

      let typedBlock = ''
      if (typedCtx && typedCtx.length > 0) {
        sessionTypedContext.set(sessionID, typedCtx)
        const soulLines = typedCtx.filter(r => r.type === 'soul').map(r => `  ${r.key}: ${r.value}`)
        const prefLines = typedCtx.filter(r => r.type === 'pref').map(r => `  ${r.key} = ${r.value}`)
        const facetLines = typedCtx.filter(r => r.type === 'facet').map(r => `  - ${r.key}: ${r.value}`)
        const parts = []
        if (soulLines.length) parts.push(`**Soul State:**\n${soulLines.join('\n')}`)
        if (prefLines.length) parts.push(`**User Preferences:**\n${prefLines.join('\n')}`)
        if (facetLines.length) parts.push(`**Recent Facets:**\n${facetLines.join('\n')}`)
        typedBlock = `\n\n### User Context (typed tables)\n${parts.join('\n')}\n`
      }

      let memoryBlock = ''
      if (memories && memories.length > 0) {
        const lines = memories.map(m =>
          `- [#${m.id}] (${m.mtype}/${m.cat}) **${m.title}** [imp=${m.imp}]\n  ${m.snippet}`
        )
        memoryBlock = `\n\n### Recalled Memories (auto-aggregated)\n${lines.join('\n')}\n`
      }

      output.context.push(`## Memory System (skill-system-memory)
- Project: ${project} | Session: ${sessionID.slice(0, 12)} | Compaction #${prevCount + 1}
- Use store_memory(...) after solving non-obvious problems
- When writing the compaction summary, PRESERVE: task goals, key decisions, file paths being edited, and any experiment results
- Compaction is logged (local JSONL + Postgres)
- **IMPORTANT**: Include the ACTUAL content/summary of what was discussed, not just session ID and timestamp
${typedBlock}${memoryBlock}`)
    },

    event: async ({ event }) => {
      if (event.type === 'session.compacted') {
        const sessionID = event.properties.sessionID
        const summary = event.properties.summary || event.properties.context || ''
        const time = nowUtc()

        appendJsonl({
          event: 'session.compacted',
          time_utc: time,
          session_id: sessionID,
          cwd: directory,
          project: getProjectName(),
          compaction_num: sessionCompactionCount.get(sessionID) ?? 1,
          summary_length: summary.length,
          summary_preview: summary.slice(0, 500),
        })

        await tryPsqlStore({ sessionID, summary })
        const typedCtx = sessionTypedContext.get(sessionID) ?? await queryTypedContext()
        await trySyncRuntimeState({ sessionID, project: getProjectName(), summary, typedCtx })
        await tryRefreshBehaviorProjections()

        try {
          await client.app.log({
            body: {
              service: 'skill-system-memory',
              level: 'info',
              message: `Compaction #${sessionCompactionCount.get(sessionID) ?? 1} logged for ${getProjectName()} (${sessionID.slice(0, 12)}) [${summary.length} chars captured]`,
            },
          })
        } catch {}
      }
    },
  }
}
