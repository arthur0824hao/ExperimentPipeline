---
name: skill-system-cockpit
description: "Renderer-independent cockpit state for note_feedback, review, and a read-only TUI."
license: MIT
metadata:
  os: windows, linux, macos
  storage: filesystem + postgres.agent_memory(read-only)
  version: 1
---

# Skill System Cockpit

`skill-system-cockpit` is a pure read-model skill for the repo's operator-facing workflow.

It derives one shared cockpit state object from the current note workflow, task authority, durable evolution surfaces, and runtime doctor signals. Both `note/note_feedback.md` and the TUI render that same state so the user-facing current view never forks into separate truths.

When the ticket workflow round is active, cockpit renders ticket visibility and batch queue state from `skill-system-workflow`, but it does not own ticket semantics.

## Core Model

The cockpit state is renderer-independent and JSON-serializable. The kernel includes:

- `generated_at_minute`
- `active_round`
- `current_progress`
- `recent_actions`
- `inferred_next_step`
- `frictions`
- `proposals`
- `active_tasks`
- `active_ticket`
- `batch_id`
- `open_ticket_count`
- `claimed_by_this_session_count`
- `claimable_ticket_count`
- `claimed_by_other_count`
- `blocked_ticket_count`
- `closed_ticket_count`
- `next_claimable_ticket`
- `unresolved_ticket_summary`
- `review_handoff_available`
- `health_signals`
- `profile_watchers`
- `watcher_gaps`

Schema reference: `schema/cockpit-state.yaml`

## Source Adapters

The initial generic workflow profile reads from the currently available repo surfaces only:

- `note/note_tasks.md` for the active round objective and request context
- `note/note_feedback.md` for recent execution log items and open frictions
- `skill-system-workflow` ticket lifecycle reads over `agent_tasks`
- `agent_tasks` as lifecycle authority for current work visibility
- `evolution_nodes` / `evolution_rejections` for accepted recent decisions and hidden rejected counts
- `runtime_doctor.build_report()` for health and watcher-gap inputs

Pending proposals are an overlay produced by `skill-system-insight`. They remain non-durable and are not written to `agent_memories`, `evolution_nodes`, or `evolution_rejections` until some later explicit approval flow exists.

When ticket workflow is active, cockpit also renders:

- active ticket
- batch id
- open ticket count
- claimed-by-other count
- blocked count
- next claimable ticket
- unresolved ticket summary
- review handoff availability
- startup/session snapshot from workflow

## Feedback Surface

`render-feedback` rewrites a bounded managed block inside `note/note_feedback.md`.

- top current-state sections always appear in this order:
  1. `當前 progress`
  2. `作了什麼（簡述）`
  3. `推斷下一步`
  4. `frictions`
  5. `proposals`
- a short rolling history block appears below those sections
- the pre-existing questions section and append-only execution log remain outside the managed block

## TUI

The TUI is deliberately minimal:

- read-only
- single-screen
- one main panel visible at a time
- `Tab` cycles panels
- `q` exits
- dependency-light implementation via stdlib `curses` + `curses.panel`

## Watcher Model

The cockpit kernel does not hardcode app-specific watchers.

- `profile_watchers` lists attached generic read adapters such as `note_workflow`, `evolution_ledger`, `task_authority`, and `runtime_doctor`
- ticket workflow is rendered through generic workflow adapters instead of embedding ticket semantics in the cockpit kernel
- `watcher_gaps` lists abstract next adapters to attach, such as `domain_profile_adapter`

This keeps cockpit generic while still showing what richer watcher surfaces could be added next.

```skill-manifest
{
  "schema_version": "2.0",
  "id": "skill-system-cockpit",
  "version": "1.0.0",
  "capabilities": ["cockpit-state", "cockpit-feedback", "cockpit-tui"],
  "effects": ["fs.read", "fs.write", "db.read", "proc.exec"],
  "operations": {
    "derive-state": {
      "description": "Derive the shared cockpit state for note_feedback, review, and TUI surfaces.",
      "input": {
        "include_rejected": {"type": "boolean", "required": false, "description": "Include rejected proposal details instead of the default hidden-count view"}
      },
      "output": {
        "description": "Renderer-independent cockpit state",
        "fields": {"status": "ok", "cockpit_state": "json"}
      },
      "entrypoints": {
        "unix": ["python3", "scripts/cockpit.py", "state", "--format", "json"],
        "windows": ["python", "scripts/cockpit.py", "state", "--format", "json"]
      }
    },
    "render-feedback": {
      "description": "Refresh the managed current-state block in note_feedback from the shared cockpit state.",
      "input": {
        "file_path": {"type": "string", "required": false, "description": "Target note_feedback path"},
        "history_limit": {"type": "number", "required": false, "description": "Rolling history size"}
      },
      "output": {
        "description": "Render result for note_feedback",
        "fields": {"status": "ok | error", "path": "string", "generated_at_minute": "string"}
      },
      "entrypoints": {
        "unix": ["python3", "scripts/cockpit.py", "render-feedback"],
        "windows": ["python", "scripts/cockpit.py", "render-feedback"]
      }
    },
    "tui": {
      "description": "Launch the read-only single-screen cockpit TUI.",
      "input": {
        "include_rejected": {"type": "boolean", "required": false, "description": "Include rejected proposal details in the underlying state query"}
      },
      "output": {
        "description": "Interactive TUI session",
        "fields": {"status": "interactive"}
      },
      "entrypoints": {
        "agent": "Use python3 scripts/cockpit.py tui for the interactive renderer; it is read-only and uses the same state as render-feedback."
      }
    }
  },
  "stdout_contract": {
    "last_line_json": false,
    "note": "derive-state and render-feedback print JSON; tui is interactive and read-only."
  }
}
```
