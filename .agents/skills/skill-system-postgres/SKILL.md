---
name: skill-system-postgres
description: "Postgres-backed observability and policy store for the skill system. Provides tables for policy profiles (effect allowlists), skill execution runs, and step-level events. Use when setting up the skill system database or querying execution history."
license: MIT
metadata:
  storage: postgresql
  os: windows, linux, macos
---

# Skill System (Postgres State)

Database schema for skill system observability, policy, graph, refresh/control-plane state, canonical rule hierarchy projections, and project architecture graph projections.

## Install

```bash
# Replace 'postgres' with your PostgreSQL superuser if needed (e.g. your OS username)
psql -U postgres -d agent_memory -v ON_ERROR_STOP=1 -f init.sql
```

```powershell
# Replace 'postgres' with your PostgreSQL superuser if needed
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d agent_memory -v "ON_ERROR_STOP=1" -f init.sql
```

For existing v1 installations, also run `migrate-v2.sql`.

For graph and control-plane extensions, also run:

```bash
psql -U postgres -d agent_memory -v ON_ERROR_STOP=1 -f migrate-v3-graph.sql
psql -U postgres -d agent_memory -v ON_ERROR_STOP=1 -f migrate-v4-control-plane.sql
psql -U postgres -d agent_memory -v ON_ERROR_STOP=1 -f migrate-v5-rule-model.sql
psql -U postgres -d agent_memory -v ON_ERROR_STOP=1 -f migrate-v6-project-graph.sql
```

## Tables

- `skill_system.policy_profiles` — effect allowlists (what skills are allowed to do)
- `skill_system.runs` — execution records (goal, agent, status, duration, metrics)
- `skill_system.run_events` — step-level event log (which skill, which op, result)
- `skill_system.skill_graph_nodes` / `skill_system.skill_graph_edges` — global skill dependency graph
- `skill_system.refresh_jobs` / `skill_system.refresh_job_events` — queued refresh jobs and job logs
- `skill_system.artifact_versions` — version registry for generated artifacts
- `skill_system.rule_sets` / `skill_system.rule_entries` — canonical rule hierarchy and projected rule entries
- `skill_system.project_nodes` / `skill_system.project_edges` — canonical repo architecture graph and projected relationships

## Usage

The Agent writes to these tables as instructed by the Router skill. This skill does not execute anything — it's a state store.

```skill-manifest
{
  "schema_version": "2.0",
  "id": "skill-system-postgres",
  "version": "1.4.0",
  "capabilities": ["skill-policy-check", "skill-run-log", "skill-graph", "skill-refresh-control", "skill-rule-model", "skill-project-graph", "skill-projection-engine"],
  "effects": ["db.read", "db.write"],
  "operations": {
    "check-policy": {
      "description": "Check if a skill's effects are allowed by the active policy profile.",
      "input": {
        "policy_name": { "type": "string", "required": true, "description": "Policy profile name (e.g. dev)" },
        "effects": { "type": "json", "required": true, "description": "Array of effect strings to check" }
      },
      "output": {
        "description": "Whether all effects are allowed",
        "fields": { "allowed": "boolean", "blocked_effects": "array" }
      },
      "entrypoints": {
        "agent": "Query skill_system.policy_profiles WHERE name = policy_name"
      }
    },
    "log-run": {
      "description": "Log a skill execution run for observability.",
      "input": {
        "skill_id": { "type": "string", "required": true },
        "operation": { "type": "string", "required": true },
        "status": { "type": "string", "required": true },
        "duration_ms": { "type": "integer", "required": false }
      },
      "output": {
        "description": "Run ID",
        "fields": { "run_id": "integer" }
      },
      "entrypoints": {
        "agent": "INSERT INTO skill_system.runs and skill_system.run_events"
      }
    },
    "migrate-graph": {
      "description": "Apply migrate-v3-graph.sql to create graph nodes/edges schema.",
      "input": {
        "action": { "type": "string", "required": false, "description": "apply or check" }
      },
      "output": {
        "description": "Migration status and table list",
        "fields": { "status": "string", "tables": "array" }
      },
      "entrypoints": {
        "agent": "Apply migrate-v3-graph.sql via psql"
      }
    },
    "migrate-control-plane": {
      "description": "Apply migrate-v4-control-plane.sql to create refresh jobs, refresh job events, and artifact versions tables.",
      "input": {
        "action": { "type": "string", "required": false, "description": "apply or check" }
      },
      "output": {
        "description": "Migration status and table list",
        "fields": { "status": "string", "tables": "array" }
      },
      "entrypoints": {
        "agent": "Apply migrate-v4-control-plane.sql via psql"
      }
    },
    "migrate-rule-model": {
      "description": "Apply migrate-v5-rule-model.sql to create canonical rule hierarchy tables.",
      "input": {
        "action": { "type": "string", "required": false, "description": "apply or check" }
      },
      "output": {
        "description": "Migration status and table list",
        "fields": { "status": "string", "tables": "array" }
      },
      "entrypoints": {
        "agent": "Apply migrate-v5-rule-model.sql via psql"
      }
    },
    "project-rules": {
      "description": "Sync canonical rule hierarchy into PostgreSQL and deterministic markdown rule projections.",
      "input": {
        "write_files": { "type": "boolean", "required": false, "description": "Write note rule projections after syncing" }
      },
      "output": {
        "description": "Rule model summary and written projection paths",
        "fields": { "rule_model": "json", "written_files": "array" }
      },
      "entrypoints": {
        "unix": ["python3", "scripts/rule_projection.py", "--write-files"],
        "agent": "Run scripts/rule_projection.py to upsert canonical rule rows and write deterministic note projections"
      }
    },
    "migrate-project-graph": {
      "description": "Apply migrate-v6-project-graph.sql to create canonical project graph tables.",
      "input": {
        "action": { "type": "string", "required": false, "description": "apply or check" }
      },
      "output": {
        "description": "Migration status and table list",
        "fields": { "status": "string", "tables": "array" }
      },
      "entrypoints": {
        "agent": "Apply migrate-v6-project-graph.sql via psql"
      }
    },
    "project-architecture": {
      "description": "Sync canonical repo architecture graph into PostgreSQL and write note/architecture_map.md.",
      "input": {
        "write_file": { "type": "boolean", "required": false, "description": "Write architecture_map after syncing" }
      },
      "output": {
        "description": "Architecture graph summary and written projection path",
        "fields": { "architecture_graph": "json", "written_file": "string" }
      },
      "entrypoints": {
        "unix": ["python3", "scripts/architecture_graph.py", "--write-file"],
        "agent": "Run scripts/architecture_graph.py to upsert project graph rows and write deterministic note/architecture_map.md"
      }
    },
    "run-projection-engine": {
      "description": "Refresh deterministic markdown projections with GENERATED_START / GENERATED_END managed blocks.",
      "input": {},
      "output": {
        "description": "Projection targets refreshed by the engine",
        "fields": { "written_files": "array" }
      },
      "entrypoints": {
        "unix": ["python3", "scripts/projection_engine.py"],
        "agent": "Run scripts/projection_engine.py after canonical PostgreSQL models are up to date"
      }
    }
  },
  "stdout_contract": {
    "last_line_json": true,
    "note": "Migration operations remain agent-executed SQL; scripts/rule_projection.py returns a JSON payload."
  }
}
```
