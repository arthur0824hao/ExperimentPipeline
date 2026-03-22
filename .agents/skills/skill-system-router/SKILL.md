---
name: skill-system-router
description: "Meta-skill that teaches the Agent how to discover, select, execute, chain, and observe skills in the skill system. Load this skill when you need to: (1) find which skill can handle a capability, (2) execute a skill operation via its entrypoint, (3) chain multiple skill operations together, (4) check policy before executing, or (5) log skill execution for observability. This skill makes YOU the router — you decide what to run, in what order, based on context."
license: MIT
metadata:
  os: windows, linux, macos
---

# Skill System Router

You are the Router. This skill teaches you how to orchestrate other skills.

## How It Works

```
User goal → Read skills-index.json → Match capabilities → Read skill manifest
→ (Complex goal: plan with workflow) → Check policy → Execute entrypoints → Parse JSON output → Decide next step → Log
```

## Step 0: Installer Bootstrap Check (First Run Only)

Check if the project's AGENTS.md contains `## Skill System`.

- If found: skip to Step 1 (already bootstrapped).
- If not found: use `skill-system-installer` bootstrap to scaffold and embed the skill system into the project.

Bootstrap ownership lives in `skill-system-installer`. After bootstrap, every future session reads AGENTS.md and knows about the skill system automatically.

## Step 1: Discover Skills

Read `skills-index.json` (in the skills root directory, sibling to skill folders) to see available capabilities.

The index maps capabilities → skills and lists each skill's operations with descriptions and input params. Match the user's goal to capabilities. If no match, check if the goal can be decomposed into sub-capabilities.

If the goal is multi-step, ambiguous, or likely to involve 3+ operations, route through workflow planning first via `plan-with-workflow`.

If `skills-index.json` is missing or stale, regenerate it:

```bash
bash "<this-skill-dir>/scripts/build-index.sh"
```

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "<this-skill-dir>\scripts\build-index.ps1"
```

## Session Guard

At session start, run a lightweight guard check before loading any other skill:

- Procedure: `scripts/session-guard-check.md`

## Step 2: Read Skill Manifest

Open the target skill's `SKILL.md` and find the `` `skill-manifest` `` fenced block. This gives you:

- **operations**: what the skill can do, with input/output schemas
- **effects**: side effects (used for policy check)
- **entrypoints**: OS-specific commands to run

Full manifest spec: `references/manifest-spec.md`

## Configuration

Runtime settings are in `config/router.yaml`. Config is the single source of truth.

See: `../../config/router.yaml`

## Step 3: Check Policy

Before executing, verify the skill's declared `effects` against the active policy profile:

```sql
SELECT allowed_effects FROM skill_system.policy_profiles WHERE name = 'dev';
```

- ALL effects in `allowed_effects` → proceed
- ANY effect NOT in `allowed_effects` → **ask the user** before proceeding
- No policy profile exists → ask the user

`report-improvement-issue` policy notes:
- Always requires: `proc.exec`, `net.fetch`
- Also requires `git.read` when `repo` is omitted and inferred from git remote

Standard effects: `db.read`, `db.write`, `proc.exec`, `fs.read`, `fs.write`, `net.fetch`, `git.read`, `git.write`

## Step 4: Execute

1. Determine OS: `OS` env = `Windows_NT` → use `windows` entrypoint, otherwise `unix`
2. Read the operation's `entrypoints` for your OS
3. Substitute `{param}` placeholders with actual values
4. Set cwd to the skill's directory
5. Run the command
6. Parse the **last line of stdout** as JSON — this is the result

Exit code ≠ 0 or last line not JSON → operation failed.

## Step 5: Chain Operations

When a goal requires multiple skills/operations:

1. Execute first operation → parse JSON output
2. Use output values as input to the next operation (you decide the mapping)
3. Repeat until goal is met

You are intelligent — adapt based on intermediate results. This is better than a fixed pipeline because you can handle errors, skip unnecessary steps, and make contextual decisions.

## Step 5.5: Orchestration Call Chain (Router → TKT → Workflow)

For complex goals that need multi-agent coordination, the router orchestrates three layers:

```
1. Router identifies TKT-worthy goal
2. TKT.create-bundle → decomposes into tickets (000 + workers + audit)
3. Workflow.plan → orders tickets into a DAG with dependency waves
4. Router dispatches each ticket's skill execution by wave order
5. TKT.update / TKT.close → tracks results and triggers review
```

**Decision matrix:**

| Goal Type | Route |
|-----------|-------|
| Single-skill, simple | Direct execute (Step 4) |
| Multi-step, single session | Workflow.plan → execute by wave |
| Multi-agent, cross-session | TKT.create-bundle → Workflow.plan → execute → TKT.close |

For workflow-only planning (no TKT), use `scripts/plan-with-workflow.md`.
For TKT-integrated planning, use the call chain above.

## Step 6: Log (Observability)

After non-trivial workflows, log for observability:

```sql
INSERT INTO skill_system.runs(task_spec_id, status, started_at, effective_policy)
VALUES (NULL, 'running', NOW(), '{}'::jsonb) RETURNING id;

INSERT INTO skill_system.run_events(run_id, level, event_type, payload)
VALUES (<run_id>, 'info', 'step_completed',
  jsonb_build_object('skill', '<id>', 'operation', '<op>', 'status', '<ok|error>', 'duration_ms', <ms>));

UPDATE skill_system.runs SET status='succeeded', ended_at=NOW(),
  metrics=jsonb_build_object('steps', <n>, 'duration_ms', <ms>) WHERE id=<run_id>;
```

Skip logging for simple single-operation calls.

## Step 7: Report Improvement Issue

When an agent notices reusable gaps, friction points, or unclear behavior in a skill/system flow, create a GitHub issue so the improvement is trackable.

Use `scripts/report-improvement-issue.md`.

When to report:
- Missing guardrails or policy checks discovered during execution
- Repeated user friction caused by unclear docs/entrypoints
- Reliability gaps (drift-prone steps, weak validation, poor observability)

Before creating a new issue, check for duplicates in open issues.

If a similar issue already exists, link that issue instead of creating a new one.

## Experiment Gate Delegation

For experiment validation and registry safety operations, delegate to `skill-system-gate`:

| Operation | Delegate to |
|---|---|
| Validate experiment code/logs against gate rules | `skill-system-gate` → `validate-experiment` |
| Check registry state safety before reset/rerun | `skill-system-gate` → `verify-registry` |
| Show experiment queue status and health | `skill-system-gate` → `status` |

## GitHub Operations Delegation

For general GitHub PM operations beyond improvement reporting, delegate to `skill-system-github`:

| Operation | Delegate to |
|---|---|
| Report improvement issue (during routing) | **This skill** — `report-improvement-issue` |
| Create/list/comment/close/reopen issues | `skill-system-github` → `manage-issues` |
| Create/update/delete/apply labels | `skill-system-github` → `manage-labels` |
| Bootstrap/update issue templates | `skill-system-github` → `manage-templates` |
| List/check/create/update workflows | `skill-system-github` → `manage-workflows` |
| Check PAT scopes before workflow writes | `skill-system-github` → `safety-check` |

The router retains `report-improvement-issue` because it is triggered inline during skill routing. All other GitHub operations route through the dedicated skill.

## Insight Suggestion

After completing a non-trivial session (multi-step work, significant discussion, or debugging), consider suggesting an insight pass:

> "Want me to run an insight pass on this session? It helps me learn your preferences for better collaboration next time."

This is a lightweight, optional prompt. Only suggest when:
- The session had meaningful interaction (not a single-line fix)
- The user hasn't already triggered insight extraction recently
- The session contained signals worth capturing (frustration, satisfaction, style preferences)

If the user agrees, load `skill-system-insight` and follow `scripts/extract-facets.md`.

Note: `skill-system-insight` is the unified observe-and-evolve engine. It handles both facet extraction and profile/recipe evolution (formerly split into separate insight + evolution skills).

## Common Patterns

**Single operation:**
Goal → index lookup → execute one entrypoint → return result

**Multi-step chain:**
Goal → op1.search → parse results → op2.store(using results) → done

**Cross-skill chain:**
Goal → skill-A.op1 → use output as context → skill-B.op2 → rebuild index → done

**Workflow-assisted chain:**
Goal → router.plan-with-workflow → execute wave 1 tasks → execute dependent waves → log results

## Database

Uses tables from `skill-system-postgres`:
- `skill_system.policy_profiles` — effect allowlists
- `skill_system.runs` — execution records
- `skill_system.run_events` — step-level logs

```skill-manifest
{
  "schema_version": "2.0",
  "id": "skill-system-router",
  "version": "1.0.0",
  "capabilities": ["skill-discover", "skill-execute", "skill-chain", "index-rebuild", "skill-improvement-report", "workflow-route", "session-guard"],
  "effects": ["fs.read", "fs.write", "proc.exec", "db.read", "db.write", "net.fetch", "git.read"],
  "operations": {
    "rebuild-index": {
      "description": "Regenerate skills-index.json by scanning all skill manifests.",
      "input": {},
      "output": {
        "description": "Index file path and skill count",
        "fields": { "file": "string", "skill_count": "integer" }
      },
      "entrypoints": {
        "unix": ["bash", "scripts/build-index.sh"],
        "windows": ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts\\build-index.ps1"]
      }
    },
    "report-improvement-issue": {
      "description": "Create or link a GitHub issue for skill/system improvements discovered during routing.",
      "input": {
        "title": {"type": "string", "required": true, "description": "Issue title"},
        "summary": {"type": "string", "required": true, "description": "Concise problem statement and why it matters"},
        "repo": {"type": "string", "required": false, "description": "owner/repo override; default inferred from git remote"},
        "impact": {"type": "string", "required": false, "description": "User or system impact"},
        "repro_steps": {"type": "string", "required": false, "description": "Steps to reproduce observed friction"},
        "suggested_fix": {"type": "string", "required": false, "description": "Candidate fix or direction"}
      },
      "output": {
        "description": "Created or matched issue information",
        "fields": {"status": "created | linked", "issue_url": "string", "issue_number": "integer"}
      },
      "entrypoints": {
        "agent": "Follow scripts/report-improvement-issue.md procedure"
      }
    },
    "plan-with-workflow": {
      "description": "Use skill-system-workflow to plan complex goals as DAG waves, then route execution by dependency order.",
      "input": {
        "goal": {"type": "string", "required": true, "description": "Complex user goal"},
        "context": {"type": "string", "required": false, "description": "Optional files/constraints context"}
      },
      "output": {
        "description": "Workflow plan prepared for router execution",
        "fields": {"dag": "YAML", "mermaid": "string", "wave_count": "integer", "execution_notes": "string"}
      },
      "entrypoints": {
        "agent": "Follow scripts/plan-with-workflow.md procedure"
      }
    },
    "guard-check": {
      "description": "Session-start guard that ensures router discovery has been performed before other skills are loaded.",
      "input": {},
      "output": {
        "description": "Guard result",
        "fields": {"router_checked": "boolean", "message": "string"}
      },
      "entrypoints": {
        "agent": "Follow scripts/session-guard-check.md procedure"
      }
    }
  },
  "stdout_contract": {
    "last_line_json": false,
    "note": "rebuild-index prints summary to stdout; report-improvement-issue/plan-with-workflow are agent-executed."
  }
}
```
