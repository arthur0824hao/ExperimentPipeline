---
name: skill-system-cli
description: "Unified CLI entry point for the entire Skill System. One command (sk) to operate all skills — tickets, memory, config, install, and project init."
license: MIT
metadata:
  os: windows, linux, macos
  version: 1
---

# Skill System CLI

Single command `sk` that dispatches to all skill system operations. Agent-native: JSON output by default, progressive disclosure via `--help`.

Inspired by [CLI-Anything](https://github.com/HKUDS/CLI-Anything) — making everything agent-native through structured CLI interfaces.

## Quick Start

```bash
# Show all domains
python3 "<this-skill-dir>/scripts/sk.py" --help

# Bootstrap a project
python3 "<this-skill-dir>/scripts/sk.py" init

# Check project status
python3 "<this-skill-dir>/scripts/sk.py" status

# Ticket operations
python3 "<this-skill-dir>/scripts/sk.py" tkt claim --ticket-id TKT-001

# Memory operations
python3 "<this-skill-dir>/scripts/sk.py" mem search "query"

# Config operations
python3 "<this-skill-dir>/scripts/sk.py" config get tkt.bundle.max_tickets
```

## Configuration

Runtime settings are in `config/cli.yaml`. Config is the single source of truth.

See: `../../config/cli.yaml`

## Architecture

```
sk (thin dispatcher)
├── init           → bootstrap.md Phase 1+2 logic
├── status         → aggregate project health
├── config         → read/write config/*.yaml
│   ├── list
│   ├── show <file>
│   ├── get <key>
│   └── set <key> <value>
├── tkt            → tkt.sh + tickets.py
│   ├── init-roadmap, create-bundle, bundle-status, close-bundle, list-bundles
│   ├── intake, list-tickets, claim, block, close
│   ├── check-open, summary, loop, startup
│   ├── refresh-new, refresh-inbox
│   ├── closure-report, scope
├── mem            → mem.py
│   ├── search, store, status, tags, categories
└── install        → skills.sh
    ├── list, add, update, sync
```

## Design Principles

1. **Thin dispatcher** — `sk.py` never reimplements logic. It routes to existing scripts via subprocess or import.
2. **JSON-first** — All output follows the last-line JSON contract. Agents parse the last line.
3. **Progressive disclosure** — No args shows domains. `sk tkt` shows tkt actions. `sk tkt claim --help` shows claim options.
4. **Config-aware** — Reads from `config/` for defaults.
5. **Idempotent init** — `sk init` only creates what's missing, never overwrites.

## Output Contract

Every command emits JSON on the last line:

```json
{"status": "ok", ...}
{"status": "error", "message": "...", ...}
```

## Domains

### `sk init`
Bootstrap project structure. Scaffolds config/, note/, .tkt/, checks PostgreSQL.

### `sk status`
Aggregate project health: config, note, tkt, postgres, skills counts.

### `sk config`
Read/write config values using dot-path notation: `sk config get tkt.bundle.max_tickets`.

### `sk tkt`
Full ticket lifecycle — both filesystem bundles (tkt.sh) and DB durable tickets (tickets.py).

### `sk mem`
Memory operations — search, store, list, compact, export, status, tags, categories.

Examples:

```bash
python3 "<this-skill-dir>/scripts/sk.py" mem search "fraud" --scope project --limit 5
python3 "<this-skill-dir>/scripts/sk.py" mem store --type semantic --category fraud --title note --content hello --scope session
python3 "<this-skill-dir>/scripts/sk.py" mem list --scope project --limit 20
python3 "<this-skill-dir>/scripts/sk.py" mem compact --scope project
python3 "<this-skill-dir>/scripts/sk.py" mem export --format json --scope global
```

### `sk install`
Skill management — list, add, update, sync.

```skill-manifest
{
  "schema_version": "2.0",
  "id": "skill-system-cli",
  "version": "1.0.0",
  "capabilities": [
    "cli-dispatch", "project-init", "project-status",
    "config-read", "config-write",
    "tkt-dispatch", "mem-dispatch", "install-dispatch"
  ],
  "effects": ["fs.read", "fs.write", "db.read", "db.write", "proc.exec"],
  "operations": {
    "init": {
      "description": "Bootstrap project structure and report what was created or verified.",
      "input": {
        "check": {"type": "boolean", "required": false, "description": "Detect only, do not create missing structure"}
      },
      "output": {
        "description": "Bootstrap report",
        "fields": {"status": "string", "init_report": "object"}
      },
      "entrypoints": {
        "unix": ["python3", "{skill_dir}/scripts/sk.py", "init"],
        "windows": ["python", "{skill_dir}/scripts/sk.py", "init"]
      }
    },
    "status": {
      "description": "Show global project health across config, note, TKT, postgres, and skills inventory.",
      "input": {},
      "output": {
        "description": "Aggregated health report",
        "fields": {"status": "string", "config": "object", "note": "object", "tkt": "object", "postgres": "object", "skills": "object"}
      },
      "entrypoints": {
        "unix": ["python3", "{skill_dir}/scripts/sk.py", "status"],
        "windows": ["python", "{skill_dir}/scripts/sk.py", "status"]
      }
    },
    "config-get": {
      "description": "Read a config value by dot-path.",
      "input": {
        "key": {"type": "string", "required": true, "description": "Dot-path key to read"}
      },
      "output": {
        "description": "Resolved config value",
        "fields": {"status": "string", "value": "json"}
      },
      "entrypoints": {
        "unix": ["python3", "{skill_dir}/scripts/sk.py", "config", "get", "{key}"],
        "windows": ["python", "{skill_dir}/scripts/sk.py", "config", "get", "{key}"]
      }
    },
    "config-set": {
      "description": "Set a config value by dot-path.",
      "input": {
        "key": {"type": "string", "required": true, "description": "Dot-path key to update"},
        "value": {"type": "string", "required": true, "description": "Value to write"}
      },
      "output": {
        "description": "Updated config confirmation",
        "fields": {"status": "string", "key": "string", "value": "string", "file": "string"}
      },
      "entrypoints": {
        "unix": ["python3", "{skill_dir}/scripts/sk.py", "config", "set", "{key}", "{value}"],
        "windows": ["python", "{skill_dir}/scripts/sk.py", "config", "set", "{key}", "{value}"]
      }
    },
    "tkt-dispatch": {
      "description": "Route any TKT subcommand through the unified CLI.",
      "input": {
        "args": {"type": "string", "required": false, "description": "Pass-through TKT arguments"}
      },
      "output": {
        "description": "Passthrough JSON payload from skill-system-tkt",
        "fields": {"status": "string"}
      },
      "entrypoints": {
        "agent": "Invoke scripts/sk.py with the tkt subcommand family and forward args unchanged"
      }
    },
    "mem-dispatch": {
      "description": "Route any memory subcommand through the unified CLI.",
      "input": {
        "args": {"type": "string", "required": false, "description": "Pass-through memory arguments"}
      },
      "output": {
        "description": "Passthrough JSON payload from skill-system-memory",
        "fields": {"status": "string"}
      },
      "entrypoints": {
        "agent": "Invoke scripts/sk.py with the mem subcommand family and forward args unchanged"
      }
    },
    "install-dispatch": {
      "description": "Route any installer subcommand through the unified CLI.",
      "input": {
        "args": {"type": "string", "required": false, "description": "Pass-through installer arguments"}
      },
      "output": {
        "description": "Passthrough JSON payload from skill-system-installer",
        "fields": {"status": "string"}
      },
      "entrypoints": {
        "agent": "Invoke scripts/sk.py with the install subcommand family and forward args unchanged"
      }
    }
  },
  "stdout_contract": {
    "last_line_json": true,
    "note": "All commands emit JSON on last line. Status is always 'ok' or 'error'."
  }
}
```
