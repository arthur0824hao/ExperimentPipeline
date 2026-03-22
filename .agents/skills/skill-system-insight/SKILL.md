---
name: skill-system-insight
description: "Unified observe-and-evolve engine. Extracts per-session facets, maintains a dual-matrix soul state, and version-evolves soul profiles and workflow recipes with auditability, safety constraints, and rollback."
license: MIT
metadata:
  os: windows, linux, macos
  storage: postgres.agent_memories
  version: 2
---

# Skill System Insight

Unified **OBSERVE → EVOLVE** engine for user adaptation.

This skill turns sessions into structured behavioral insights ("facets"), uses those facets to maintain a dual-matrix soul state, and periodically evolves soul profiles and workflow recipes — all versioned and rollbackable.

## Two Phases, One Skill

```
Phase 1 — OBSERVE
  Extract facets → Update matrix buffers → (optional) Trigger synthesis

Phase 2 — EVOLVE
  Read accumulated data → Plan evidence-backed changes → Apply with versioning → Snapshot for audit/rollback
```

Phase 1 produces raw signal. Phase 2 consumes it to make durable changes.

## Architecture: Hybrid 3-Layer System

```
Layer 1: Base Profile (balanced.md)
  - Static skeleton: section format + safety/quality defaults.

Layer 2: Dual Matrix (soul-state)
  - Personality Matrix (slow/stable): openness, directness, autonomy, rigor, warmth
  - Emotion Matrix (faster baseline): patience, enthusiasm, caution, empathy

Layer 3: Synthesized Profile (user.md)
  - Periodically regenerated from Layer 1 + Layer 2 + accumulated facets.
  - Every regeneration creates a versioned snapshot.
```

Layer 2 is the ground truth. Layer 3 is a readable, versioned projection.

## Data Model

- Facets (per-session extraction): `schema/facet.yaml`
- Soul state (dual matrix + counters/buffers): `schema/soul-state.yaml`
- Evolution plan (proposed changes): `schema/evolution-plan.yaml`
- Evolution snapshot (versioned artifact): `schema/evolution-snapshot.yaml`

Storage uses the Postgres `agent_memories` table and typed sub-tables:

- Facets: `memory_type='episodic'`, `category='insight-facet'` → typed table: `insight_facets`
- Soul state: `memory_type='semantic'`, `category='soul-state'` → typed table: `soul_states`
- Evolution snapshots: `memory_type='episodic'`, `category='evolution-snapshot'` → typed table: `evolution_snapshots`

Typed functions (dual-write to both typed table and agent_memories):
- `insert_insight_facet(user, session_id, yaml, ...)` → stores facet
- `upsert_soul_state(user, yaml, personality, emotion, ...)` → stores/updates soul state
- `get_soul_state(user)` → reads from `soul_states`
- `get_recent_facets(user, limit)` → reads from `insight_facets`
- `insert_evolution_snapshot(user, version_tag, target, ...)` → stores versioned snapshot
- `get_evolution_history(user, limit)` → reads from `evolution_snapshots`

## Phase 1: OBSERVE Pipeline

```
Trigger (manual or suggested) → Extract Facet → Update Matrix → (optional) Synthesize Profile
```

References:
- Facet extraction prompt: `prompts/facet-extraction.md`
- Soul synthesis prompt: `prompts/soul-synthesis.md`
- Extraction procedure: `scripts/extract-facets.md`
- Matrix update algorithm: `scripts/update-matrix.md`
- Profile regeneration procedure: `scripts/synthesize-profile.md`

### How To Trigger (OBSERVE)

This is a manual workflow.

- User can ask explicitly: "insight", "extract facets", "learn my preferences", "update my profile".
- Router suggestion pattern (lightweight, non-pushy):
  - "Want me to run an insight pass to learn from this session? (stores a facet + may update your matrix)"

When the user asks (or agrees), run:

1) `scripts/extract-facets.md`
2) `scripts/update-matrix.md`
3) If the synthesis trigger fires: `scripts/synthesize-profile.md`

## Phase 2: EVOLVE Pipeline

```
Trigger (manual or scheduled) → Read facets + matrix → Plan evolution → Safety check → Apply → Snapshot
```

References:
- Evolution planning prompt: `prompts/evolution-planning.md`
- Recipe evolution prompt: `prompts/recipe-evolution.md`
- Soul evolution procedure: `scripts/evolve-soul.md`
- Recipe evolution procedure: `scripts/evolve-recipes.md`
- Version listing: `scripts/list-versions.md`
- Rollback procedure: `scripts/rollback.md`

### How To Trigger (EVOLVE)

- User can ask: "evolve my profile", "evolve recipes", "list versions", "rollback to v2".
- Automatic trigger when synthesis trigger fires during OBSERVE phase.

### Version Tags

Format: `v{N}_{target}_{timestamp}`
Example: `v3_soul_20260211`

Algorithm: `N = 1 + max(existing N)` for the same user+target. Start at N=1 if none exist.

### Writes

- Evolution snapshots → Postgres (`evolution_snapshots` + `agent_memories`)
- Soul profiles (filesystem) → `../skill-system-soul/profiles/<user>.md`
- Workflow recipes (filesystem) → `../skill-system-workflow/recipes/*.yaml`

## Configuration

All runtime settings (rate limits, thresholds, step sizes, approval gates) are in `config/insight.yaml`.

**Config is the single source of truth.** Values below are documentation defaults — if they differ from config/, config/ wins.

See: `../../config/insight.yaml`

## Constraints (Non-Negotiable)

### Transparency

Always explain what was learned and why.

- Facets must contain evidence strings tied to concrete moments.
- Matrix updates must add short context lines explaining each applied adjustment.
- Every evolution change must include evidence strings from facets or state context.

### Rate Limiting (defaults — see config/insight.yaml)

- OBSERVE: Max `observe.max_facets_per_day` facets per user per rolling 24 hours.
  - If over limit: summarize what you would have captured, ask the user to pick 1 session to record.
- EVOLVE: Max `evolve.max_passes_per_day` evolution pass per day per user (across all targets).
  - Use the most recent `evolution-snapshot` date as the limiter.

### Confidence Threshold (defaults — see config/insight.yaml)

Do not change matrix values on a single observation.

- Threshold: `observe.confidence_threshold`+ similar observations in the same direction.
- Personality step size: +/- `observe.personality_step` per qualifying adjustment.
- Emotion baseline step size: +/- `observe.emotion_step` per qualifying adjustment.

Accumulation tracked in buffers for testability and explainability.

### Safety (Evolution)

- Cannot remove core safety constraints from soul profiles.
- Evolved profiles must retain: honest uncertainty, no hallucinated authority, refusal of harmful/illegal instructions.
- Major changes (any dimension change > `evolve.approval_drift_threshold`) require explicit user approval before applying.

## Where Layer 1 and Layer 3 Live

- Base profile (Layer 1): `../skill-system-soul/profiles/balanced.md`
- Synthesized user profile (Layer 3): `../skill-system-soul/profiles/<user>.md`

The synthesis step preserves the 6-section format:
1. Identity
2. Decision Heuristics
3. Communication Style
4. Quality Bar
5. Tool Preferences
6. Anti-Patterns

## Storage Pattern (agent_memories)

Example SQL templates:

```sql
-- Store a facet
SELECT store_memory(
  'episodic', 'insight-facet',
  ARRAY['session:ses_xxx', 'user:arthu'],
  'Session Facet: <brief_summary>',
  '<full facet YAML as text>',
  '{"session_type": "...", "outcome": "..."}',
  'insight-agent', 'ses_xxx', 5.0
);

-- Store/update matrix state
SELECT store_memory(
  'semantic', 'soul-state',
  ARRAY['user:arthu', 'matrix'],
  'Soul State: arthu',
  '<full soul-state YAML as text>',
  '{"total_insights": 0, "last_updated": "..."}',
  'insight-agent', NULL, 9.0
);

-- Store evolution snapshot (typed — dual-writes to agent_memories)
SELECT insert_evolution_snapshot(
  '<user>', '<version_tag>', '<target>', '<trigger>',
  '<changes JSONB>', '<snapshot_data>', '<full YAML>',
  NULL, 'insight-agent'
);

-- Query recent facets
SELECT * FROM search_memories('insight-facet user:arthu', NULL, NULL, NULL, NULL, 0.0, 50);

-- List evolution history
SELECT * FROM get_evolution_history('<user>', 50);
```

## Operational Notes

- Facet extraction should be completable in one pass. If you cannot justify an adjustment with concrete evidence, propose no adjustment.
- Users may communicate in Chinese; treat that as a signal about comfort, not as a personality dimension.
- Keep values clamped to [0.0, 1.0].

```skill-manifest
{
  "schema_version": "2.0",
  "id": "skill-system-insight",
  "version": "2.0.0",
  "capabilities": [
    "insight-extract", "insight-matrix-update", "insight-synthesize",
    "evolution-soul", "evolution-recipes", "evolution-list", "evolution-rollback"
  ],
  "effects": ["db.read", "db.write", "fs.read", "fs.write"],
  "operations": {
    "extract-facets": {
      "description": "Extract a per-session facet from transcript. Rate limited to 3/24h per user.",
      "input": {
        "session_id": { "type": "string", "required": true, "description": "Session to analyze" },
        "user": { "type": "string", "required": true, "description": "User handle" }
      },
      "output": {
        "description": "Facet YAML stored to agent_memories",
        "fields": { "status": "ok | error", "memory_id": "integer" }
      },
      "entrypoints": {
        "agent": "Follow scripts/extract-facets.md procedure"
      }
    },
    "update-matrix": {
      "description": "Update dual matrix from stored facet with confidence gating.",
      "input": {
        "user": { "type": "string", "required": true, "description": "User handle" }
      },
      "output": {
        "description": "Updated soul-state YAML stored to agent_memories",
        "fields": { "status": "ok | error", "values_changed": "boolean" }
      },
      "entrypoints": {
        "agent": "Follow scripts/update-matrix.md procedure"
      }
    },
    "synthesize-profile": {
      "description": "Regenerate Layer 3 Soul profile from matrix + recent facets. Creates versioned snapshot.",
      "input": {
        "user": { "type": "string", "required": true, "description": "User handle" }
      },
      "output": {
        "description": "User profile written to skill-system-soul/profiles/<user>.md with version snapshot",
        "fields": { "status": "ok | error", "profile_path": "string", "version_tag": "string" }
      },
      "entrypoints": {
        "agent": "Follow scripts/synthesize-profile.md then scripts/evolve-soul.md snapshot step"
      }
    },
    "evolve-soul": {
      "description": "Evolve soul profile from accumulated insight data. Creates versioned snapshot.",
      "input": {
        "user": { "type": "string", "required": true, "description": "User handle" }
      },
      "output": {
        "description": "Evolution result with version tag",
        "fields": { "version_tag": "string", "changes": "array", "profile_path": "string" }
      },
      "entrypoints": {
        "agent": "Follow scripts/evolve-soul.md procedure"
      }
    },
    "evolve-recipes": {
      "description": "Evolve workflow recipes based on effectiveness data from insight facets.",
      "input": {
        "user": { "type": "string", "required": true, "description": "User handle" }
      },
      "output": {
        "description": "Recipe evolution result with version tag",
        "fields": { "version_tag": "string", "recipes_changed": "array" }
      },
      "entrypoints": {
        "agent": "Follow scripts/evolve-recipes.md procedure"
      }
    },
    "list-versions": {
      "description": "List all evolution snapshots for a user.",
      "input": {
        "user": { "type": "string", "required": true },
        "target": { "type": "string", "required": false, "description": "Filter: soul | recipe | all" }
      },
      "output": {
        "description": "Array of version snapshots",
        "fields": { "versions": "array of {tag, target, timestamp, summary}" }
      },
      "entrypoints": {
        "agent": "Follow scripts/list-versions.md procedure"
      }
    },
    "rollback": {
      "description": "Restore a previous evolution version.",
      "input": {
        "user": { "type": "string", "required": true },
        "version_tag": { "type": "string", "required": true, "description": "Version tag to restore" }
      },
      "output": {
        "description": "Rollback result",
        "fields": { "status": "ok | error", "restored_from": "string" }
      },
      "entrypoints": {
        "agent": "Follow scripts/rollback.md procedure"
      }
    }
  },
  "stdout_contract": {
    "last_line_json": false,
    "note": "Agent-executed procedures; output is structured YAML stored to DB, not stdout."
  }
}
```
