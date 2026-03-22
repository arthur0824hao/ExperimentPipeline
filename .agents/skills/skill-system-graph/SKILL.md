---
name: skill-system-graph
description: "Canonical skill graph navigation skill for the Skill System."
---

# Skill System Graph

`skill-system-graph` is an AI-first navigation layer for skill dependencies. It reads
`SKILL.spec.yaml` from each skill directory and normalizes `depends_on` +
`delegates_to` into a queryable behavior graph in PostgreSQL.

This skill is **SKILL.spec.yaml-first**. It does **not** read
`SKILL.behavior.yaml` for canonical graph construction.

## Purpose

- Answer dependency questions quickly: what depends on what, and what changes may
  cascade.
- Provide CLI primitives where `show` reads the current spec-scan model and
  `neighbors`/`path`/`impact` query persisted edges.
- Keep behavior discovery fast via incremental refresh using source hashes.

## Output Contract

Agents should run with `--format json` by default for stable automation.

- `json` output: machine-readable object, one record per command.
- `text` output: human-readable fallback only.

## Core Operations

`skill-system-graph` exposes `graph` subcommands in `scripts/`.

### `graph show`

Display all known nodes and edges from the current `SKILL.spec.yaml` scan model.

```bash
python3 "$(pwd)/skills/skill-system-graph/scripts/graph_cli.py" show [--skills-dir skills] [--format json|text]
```

JSON shape (agent-facing):

```json
{
  "status": "ok",
  "nodes": [{"skill_name": "...", "description": "...", "spec_path": "...", "operations_count": 0, "content_hash": "...", "stub": false}],
  "edges": [{"source": "...", "target": "...", "edge_type": "depends_on|delegates_to"}],
  "node_count": 0,
  "edge_count": 0
}
```

### `graph neighbors <skill_name>`

List incoming and outgoing direct neighbors for a specific skill.

```bash
python3 "$(pwd)/skills/skill-system-graph/scripts/graph_cli.py" neighbors skill-system-router [--format json|text]
```

JSON shape:

```json
{
  "skill": "skill-system-router",
  "outgoing": [{"skill_name": "skill-system-postgres", "edge_type": "depends_on"}],
  "incoming": [{"skill_name": "skill-system-tkt", "edge_type": "delegates_to"}],
  "status": "ok"
}
```

### `graph path <from_skill> <to_skill>`

Find the shortest dependency path between two skills.

```bash
python3 "$(pwd)/skills/skill-system-graph/scripts/graph_cli.py" path skill-system-memory skill-system-postgres [--max-depth 10] [--format json|text]
```

JSON shape:

```json
{
  "from_skill": "skill-system-memory",
  "to_skill": "skill-system-postgres",
  "path": ["skill-system-memory", "skill-system-router", "skill-system-postgres"],
  "found": true,
  "max_depth": 10
}
```

### `graph impact <skill_name>`

Return transitive dependents (all skills that (transitively) depend on the given one).

```bash
python3 "$(pwd)/skills/skill-system-graph/scripts/graph_cli.py" impact skill-system-postgres [--max-depth 10] [--format json|text]
```

JSON shape:

```json
{
  "skill": "skill-system-postgres",
  "impact": [
    {"impact_skill": "skill-system-memory", "depth": 1, "path": ["skill-system-memory", "skill-system-postgres"]},
    {"impact_skill": "skill-system-gui", "depth": 2, "path": ["skill-system-gui", "skill-system-memory", "skill-system-postgres"]}
  ],
  "impact_count": 2,
  "max_depth": 10,
  "status": "ok"
}
```

### `graph refresh`

Rebuild and sync the graph from `SKILL.spec.yaml` in one command.

```bash
python3 "$(pwd)/skills/skill-system-graph/scripts/graph_cli.py" refresh [--skills-dir skills] [--force] [--format json|text]
```

JSON shape:

```json
{
  "status": "ok",
  "parsed": 12,
  "inserted": 12,
  "updated": 0,
  "skipped": 0,
  "removed": 0
}
```

### Internal helper operations

- `parse-specs`: scans `SKILL.spec.yaml`, builds canonical graph model, returns parse summary.
- `sync-graph`: persists graph model into PostgreSQL with hash-aware upserts.

The orchestrator (`skill-system-router`) is expected to use these primitives to keep
data fresh before calls that require persistence guarantees.

## Dependency Direction and Effect

This skill delegates to no further skill for execution (`delegates_to: []`) and
depends on:

- `skill-system-postgres` for persisted graph storage
- `skill-system-behavior` for spec schema and behavior tooling context

## Notes for AI Use

- Prefer `--format json` in automation scripts.
- Use text mode for quick operator inspection during debugging.
- Treat `graph refresh` as the authoritative source update step before long-running
  dependency calculations.

```skill-manifest
{
  "schema_version": "2.0",
  "id": "skill-system-graph",
  "version": "0.1.0",
  "capabilities": ["graph-navigation", "behavior-query", "dependency-analysis"],
  "effects": ["fs.read", "db.read", "db.write"],
  "operations": {
    "parse-specs": {
      "description": "Parse all skill SKILL.spec.yaml files into a normalized graph model.",
      "input": {
        "skills_dir": {"type": "string", "required": false, "description": "Root directory that contains skills/"},
        "include_invalid": {"type": "boolean", "required": false, "description": "Include malformed spec metadata in the parse report"}
      },
      "output": {
        "description": "Normalized graph model plus parse report.",
        "fields": {"graph": "object", "parse_report": "object"}
      },
      "entrypoints": {
        "agent": "Use scripts/graph_core.py parse helpers to scan SKILL.spec.yaml files only"
      }
    },
    "sync-graph": {
      "description": "Sync the normalized graph model into PostgreSQL graph tables.",
      "input": {
        "skills_dir": {"type": "string", "required": false, "description": "Root directory that contains skills/"},
        "force": {"type": "boolean", "required": false, "description": "Force rebuild ignoring cached hashes"}
      },
      "output": {
        "description": "Inserted, updated, skipped, and removed counters.",
        "fields": {"sync_result": "object"}
      },
      "entrypoints": {
        "agent": "Use scripts/graph_core.py sync helpers to upsert graph state into PostgreSQL"
      }
    },
    "show": {
      "description": "Display the full skill graph from the current spec-scan graph model.",
      "input": {
        "skills_dir": {"type": "string", "required": false, "description": "Root directory that contains skills/"},
        "format": {"type": "string", "required": false, "description": "Output format: json or text"}
      },
      "output": {
        "description": "Full graph view with nodes and edges.",
        "fields": {"graph_view": "object"}
      },
      "entrypoints": {
        "unix": ["python3", "{skill_dir}/scripts/graph_cli.py", "show"],
        "windows": ["python", "{skill_dir}/scripts/graph_cli.py", "show"]
      }
    },
    "neighbors": {
      "description": "List direct neighbor skills for a target skill.",
      "input": {
        "skill_name": {"type": "string", "required": true, "description": "Skill id to inspect"},
        "format": {"type": "string", "required": false, "description": "Output format: json or text"}
      },
      "output": {
        "description": "Outgoing and incoming neighbors with edge types.",
        "fields": {"neighbors": "object"}
      },
      "entrypoints": {
        "unix": ["python3", "{skill_dir}/scripts/graph_cli.py", "neighbors", "{skill_name}"],
        "windows": ["python", "{skill_dir}/scripts/graph_cli.py", "neighbors", "{skill_name}"]
      }
    },
    "path": {
      "description": "Find the shortest dependency path between two skills.",
      "input": {
        "from_skill": {"type": "string", "required": true, "description": "Source skill id"},
        "to_skill": {"type": "string", "required": true, "description": "Destination skill id"},
        "max_depth": {"type": "number", "required": false, "description": "Traversal depth guard"},
        "format": {"type": "string", "required": false, "description": "Output format: json or text"}
      },
      "output": {
        "description": "Shortest path list and found flag.",
        "fields": {"path_result": "object"}
      },
      "entrypoints": {
        "unix": ["python3", "{skill_dir}/scripts/graph_cli.py", "path", "{from_skill}", "{to_skill}"],
        "windows": ["python", "{skill_dir}/scripts/graph_cli.py", "path", "{from_skill}", "{to_skill}"]
      }
    },
    "impact": {
      "description": "Find all transitive dependents for one skill.",
      "input": {
        "skill_name": {"type": "string", "required": true, "description": "Skill id whose dependents are requested"},
        "max_depth": {"type": "number", "required": false, "description": "Traversal depth guard"},
        "format": {"type": "string", "required": false, "description": "Output format: json or text"}
      },
      "output": {
        "description": "Transitive dependents with count plus depth/path metadata.",
        "fields": {"impact_result": "object"}
      },
      "entrypoints": {
        "unix": ["python3", "{skill_dir}/scripts/graph_cli.py", "impact", "{skill_name}"],
        "windows": ["python", "{skill_dir}/scripts/graph_cli.py", "impact", "{skill_name}"]
      }
    },
    "refresh": {
      "description": "Rebuild graph from specs and refresh persistence in one command.",
      "input": {
        "skills_dir": {"type": "string", "required": false, "description": "Root directory that contains skills/"},
        "force": {"type": "boolean", "required": false, "description": "Force rebuild ignoring cached hashes"},
        "format": {"type": "string", "required": false, "description": "Output format: json or text"}
      },
      "output": {
        "description": "Parsed spec count plus sync counters.",
        "fields": {"refresh_result": "object"}
      },
      "entrypoints": {
        "unix": ["python3", "{skill_dir}/scripts/graph_cli.py", "refresh"],
        "windows": ["python", "{skill_dir}/scripts/graph_cli.py", "refresh"]
      }
    }
  },
  "stdout_contract": {
    "last_line_json": true,
    "note": "CLI commands support stable JSON output for automation. Internal helper operations are agent-executed."
  }
}
```
