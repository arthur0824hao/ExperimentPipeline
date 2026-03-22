# Memory Auto-Write Template

Use this template immediately after you fix a bug or solve a non-obvious problem. The goal is to turn the solution into searchable, reusable memory.

## What To Store

Fill in these fields:

- `memory_type`
  - `semantic` for reusable patterns, playbooks, and generalizable lessons
  - `episodic` for a specific incident, one-off debugging session, or environment-specific failure
- `category`
  - Derive from the domain: `debugging`, `config`, `deployment`, `networking`, `ci`, `database`, `windows`, `linux`, etc.
- `title`
  - One line: "<symptom> caused by <root cause>" or "Fix <problem> by <key action>"
- `tags`
  - Tech stack and context: languages, frameworks, services, tools
  - Error codes / exception names
  - OS / environment (`windows`, `linux`, `macos`, `docker`, `k8s`, `github-actions`, etc.)
- `importance`
  - 7-9: non-obvious fixes, recurring failure modes, costly-to-rediscover knowledge
  - 5-6: routine but still mildly helpful
- `content` (structured)
  - `Problem` -> `Root Cause` -> `Fix` -> `Prevention`

## Content Template

```text
Problem:
- What broke? Include symptoms and the user-visible impact.

Root Cause:
- The specific underlying cause. Avoid speculation.

Fix:
- Exact steps taken (commands/config changes) and why they work.

Prevention:
- How to avoid recurrence (guardrail, test, lint, hook, doc update).
```

## Example (Filled In)

```text
memory_type: semantic
category: networking
title: SSH tunnel fails due to local port conflict
tags: ssh,tunnel,windows,port-conflict
importance: 8

content:
Problem:
- SSH tunnel setup failed intermittently; local port already in use.

Root Cause:
- A previous ssh process was still bound to the port.

Fix:
- Identify the owning process and terminate it, then re-run the tunnel.
- Prefer choosing a new local port when termination is risky.

Prevention:
- Add a pre-flight check for local port availability before opening tunnels.
```

## How To Store (Recommended)

Use the skill's wrapper to avoid quoting issues:

```bash
python3 scripts/mem.py store semantic networking \
  "SSH tunnel fails due to local port conflict" \
  "ssh,tunnel,windows,port-conflict" 8 --content "<paste structured content here>"
```

## When NOT To Store

- Obvious fixes with no nuance (one-liners anyone would do immediately)
- Well-documented behavior already captured in stable docs
- Purely mechanical changes with no decision-making or new insight

## Subagent Delegation Integration

When delegating work to a subagent, include this instruction:

```text
MUST DO AFTER:
If you fixed a bug or solved a non-obvious issue, store a memory using skills/skill-system-memory/scripts/auto-write-template.md.
Provide the proposed memory fields (type/category/title/tags/importance/content) in your final response.
```
