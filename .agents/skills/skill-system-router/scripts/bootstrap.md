# Bootstrap (First Run Only)

This procedure scaffolds the full Skill System framework into a project so future sessions have everything ready: AGENTS.md, config/, note/, PostgreSQL, and .tkt/.

Precondition: You only need to do this if the project's `AGENTS.md` does **not** contain a `## Skill System` section.

---

## Phase 1: AGENTS.md + Hook

### 1) Find the project's AGENTS.md

Search from the current working directory upward (max 3 parent levels). Check these paths in order and pick the first one that exists:

- `./AGENTS.md`
- `../AGENTS.md`
- `../../AGENTS.md`
- `../../../AGENTS.md`

If none exists, create `./AGENTS.md` in the current working directory.

The directory containing AGENTS.md is the **project root**. All subsequent steps use this as the base.

### 2) Confirm bootstrap is needed

Open the chosen `AGENTS.md` and search for the literal header line:

`## Skill System`

- If it exists anywhere in the file: skip to Phase 2 (project structure may still need scaffolding).
- If it does not exist: continue with step 3.

### 3) Read the snippet template

Open this file:

`templates/agents-md-snippet.md`

### 4) Replace `{SKILLS_DIR}` with the relative skills directory path

In the snippet content, replace the literal placeholder `{SKILLS_DIR}` with the relative path to the skills directory for this installation.

Definition: the skills directory is the folder that contains `skills-index.json` and contains sibling skill folders (including this one).

In a standard layout, it is the parent directory of this skill folder (`skill-system-router`).

Example when AGENTS.md is in the project root and skills are in a sibling `skills/` directory:

`./skills`

### 5) Append the snippet to AGENTS.md

Append the fully-substituted snippet to the end of `AGENTS.md`.

Append rules:

- Ensure there is at least one blank line before the appended `## Skill System` header.
- Do not modify or delete any existing content.

### 6) Install UserPromptSubmit hook (optional but recommended)

Ask the user if they want automatic skill system reminders on every prompt.

If yes:

1. Determine the agent runtime:
   - Claude Code → config path is `.claude/settings.json`
   - OpenCode → equivalent hooks config

2. Add (or merge into existing) the `UserPromptSubmit` hook entry:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "bash {SKILLS_DIR}/skill-system-router/hooks/skill-system-reminder.sh"
          }
        ]
      }
    ]
  }
}
```

3. Replace `{SKILLS_DIR}` with the resolved skills directory path.

This hook injects a one-line skill system reminder before every prompt, ensuring the agent always knows skills are available. Context cost: ~20 tokens per prompt.

### 7) Report Phase 1 result

Report:

- The `AGENTS.md` path that was updated/created.
- That you added a `## Skill System` section.
- The resolved skills directory path you embedded.
- Whether the UserPromptSubmit hook was installed (and where).

---

## Phase 2: Project Structure Scaffold

Phase 2 runs **every time** the bootstrap procedure is invoked (including when Phase 1 was skipped because AGENTS.md already exists). Every step is idempotent — it only creates what's missing, never overwrites existing files.

### 8) Scaffold config/

Check if `{project_root}/config/` exists.

**If the directory does not exist**: create it.

For each of the following template files, check if `{project_root}/config/{filename}` already exists. **Only copy files that do not exist** — never overwrite.

Source templates (in the skill system's own `config/` directory, located at `{SKILLS_DIR}/../config/`):

| File | Purpose |
|------|---------|
| `README.md` | Convention documentation + Authority Rule |
| `insight.yaml` | Insight/Evolution runtime settings |
| `router.yaml` | Routing policies, effect allowlists |
| `workflow.yaml` | DAG planning defaults |
| `tkt.yaml` | TKT system bundle limits, ticket policies |

Also create `{project_root}/config/local/` directory if it does not exist, and add a `.gitignore` containing `*` so per-machine overrides are never committed.

**Report**: List which files were copied and which were skipped (already existed).

### 9) Scaffold note/

Check if `{project_root}/note/` exists.

**If the directory does not exist**: create it.

For each of the following files, check if it already exists. **Only create files that do not exist.**

| File | Template source |
|------|----------------|
| `note_rules.md` | `templates/note_rules.md` |
| `note_tasks.md` | `templates/note_tasks.md` |
| `note_feedback.md` | `templates/note_feedback.md` |

Templates are in this skill's `templates/` directory.

Also preserve any existing files in note/ (e.g. `note.txt`).

**Report**: List which files were created and which were skipped.

### 10) Check PostgreSQL

Detect PostgreSQL readiness in 3 stages. Report status at each stage. **Do not execute anything without asking the user first.**

**Stage A: psql availability**

Check if `psql` is on PATH (`command -v psql` or `where psql`).

- If not available: report "PostgreSQL client (psql) not found on PATH. Install PostgreSQL or add psql to PATH to enable persistent memory." **Stop here** — skip stages B and C.
- If available: continue.

**Stage B: Database existence**

Run: `psql -lqt 2>/dev/null | grep -qw agent_memory`

- If `agent_memory` database exists: continue to Stage C.
- If not: ask the user "The `agent_memory` database does not exist. Create it now?" If user agrees, run:
  ```bash
  psql -w -c "CREATE DATABASE agent_memory;"
  ```
  If this fails (permissions, etc.), report the error and suggest manual creation.

**Stage C: Schema readiness**

Run: `psql -w -d agent_memory -c "SELECT 1 FROM agent_memories LIMIT 0;" 2>/dev/null`

- If the table exists: report "PostgreSQL ready — agent_memory database and schema are initialized." **Done.**
- If not: ask the user "The agent_memories table does not exist. Run init.sql to create the schema?" If user agrees, run:
  ```bash
  psql -w -d agent_memory -v ON_ERROR_STOP=1 -f {SKILLS_DIR}/skill-system-memory/init.sql
  ```
  Report success or failure.

**Report**: Summarize PostgreSQL status (psql found / DB exists / schema ready).

### 11) Scaffold .tkt/

Check if `{project_root}/.tkt/` exists.

- If it exists: report "TKT directory already initialized." **Skip.**
- If not: infer the project name from the directory name of `{project_root}`. Run:
  ```bash
  bash {SKILLS_DIR}/skill-system-tkt/scripts/tkt.sh init-roadmap --project "{project_name}"
  ```
  Report the result.

---

## Phase 3: Summary Report

After all steps, produce a summary table:

```
Component       Status
─────────────   ──────────────────────
AGENTS.md       ✓ created / ✓ existed
Hook            ✓ installed / ○ skipped
config/         ✓ scaffolded (N files) / ✓ existed
note/           ✓ scaffolded (N files) / ✓ existed
PostgreSQL      ✓ ready / △ partial / ✗ unavailable
.tkt/           ✓ initialized / ✓ existed
```

Tell the user: "Project bootstrap complete. The skill system is ready to use."
