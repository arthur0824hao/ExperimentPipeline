---
name: skill-system-memory
description: Persistent shared memory for AI agents backed by PostgreSQL (fts + pg_trgm, optional pgvector). Includes compaction logging and maintenance scripts.
license: MIT
metadata:
  storage: postgresql
  os: windows, linux, macos
---

# Skill System Memory (PostgreSQL)

Persistent shared memory for all AI agents. PostgreSQL 14+ on Linux or Windows.
Memory failures look like intelligence failures — this skill ensures the right memory is retrieved at the right time.

## Quick Start

Database `agent_memory` and all functions are created by `init.sql` in this skill directory.
The current supported-now runtime surface covers core memory, typed context reads, the typed evolution ledger, and doctor/compaction support.
Session/project/context projection tables and behavior graph refresh remain optional capability-gated surfaces, not unconditional runtime guarantees.

```bash
# Linux — replace 'postgres' with your PostgreSQL superuser if different (e.g. your system username)
psql -U postgres -c "CREATE DATABASE agent_memory;"
psql -U postgres -d agent_memory -f init.sql

# Windows (adjust path to your psql.exe; replace 'postgres' with your PG superuser if needed)
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -c "CREATE DATABASE agent_memory;"
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d agent_memory -f init.sql
```

> **Note**: If your PostgreSQL installation does not have a `postgres` role, use your actual
> PostgreSQL superuser name. On many Linux distros this matches your OS username.
> You can override at any time by setting `PGUSER` before running scripts:
> `export PGUSER=your_pg_username` (Linux/macOS) or `$env:PGUSER = "your_pg_username"` (PowerShell).

Verify: `SELECT * FROM memory_health_check();`

For existing installations, also run:

```bash
psql -U postgres -d agent_memory -v ON_ERROR_STOP=1 -f migrate-v2-postgres-first.sql
```

## Pure Skill Mode (default)

This skill works without installing any plugin. In pure skill mode:

- you manually run scripts when you want (progressive disclosure)
- no global OpenCode config is modified automatically

### Optional bootstrap (asks + records choices + tries to install)

Notes:

- Interactive mode defaults to NOT installing heavy optional components.
- Use `-InstallAll` / `--install-all` only when you're ready to install everything.

Run the bootstrap script to choose optional components (pgpass, local embeddings, pgvector) and record decisions.

Bootstrap can also optionally install the OpenCode compaction logging plugin (it will copy the plugin into your OpenCode plugins directory).

Windows:

```powershell
# run from the skill directory
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\bootstrap.ps1"
```

Linux/macOS:

```bash
# run from the skill directory
bash "scripts/bootstrap.sh"
```

The selection record is stored at:

- `~/.config/opencode/skill-system-memory/setup.json`

Agent rule:

- If this file does not exist, ask the user if they want to enable optional components.
- Recommended: run bootstrap with all options enabled (then fix any failures it reports).

## Runtime Capability Model

Current live capability classifications are intentionally narrow:

| capability | current status | current support surface |
|---|---|---|
| `core_memory` | `SUPPORTED_NOW` | `agent_memories` plus `store_memory`, `search_memories`, `memory_health_check` |
| `evolution_ledger` | `SUPPORTED_NOW` | `evolution_snapshots` plus `insert_evolution_snapshot`, `get_evolution_history` |
| `compaction_logging` | `SUPPORTED_NOW` when the repo plugin is synced | local JSONL compaction log plus guarded `store_memory(...)` writeback |
| `typed_context_reads` | `SUPPORTED_NOW` | `get_agent_context`, `get_soul_state`, `get_recent_facets`, `get_user_preferences` |
| `runtime_sync_projections` | `GATED_OPTIONAL` | `session_summaries`, `project_summaries`, `context_rollups` only when those tables exist |
| `behavior_refresh_graph` | `GATED_OPTIONAL` | `behavior_*` refresh only when those tables exist |
| `control_plane_refresh` | `DEFERRED_UNSUPPORTED` | not part of the current user-facing memory runtime |

When the durable evolution proposal model is present, doctor also reports:

- canonical accepted store: `evolution_nodes`
- canonical rejected store: `evolution_rejections`
- canonical task materialization: `evolution_tasks -> agent_tasks`
- role of `agent_memories`: searchable summary/backref surface with explicit legacy compatibility rows
- approve/reject path status: transaction-safe at the CLI boundary
- migration/backfill idempotence: rerun-safe
- decision replay status: replay-idempotent by `proposal_id`
- coordination mechanism: advisory lock by proposal + semantic fingerprint on canonical accepted/rejected rows
- payload mismatch policy: blocked with no mutation
- conflicting terminal policy: blocked with no mutation
- task authority model: `agent_tasks` lifecycle authority, `evolution_tasks` mapping only

In that state, `evolution_snapshots` is treated as `LEGACY_COMPAT_SURFACE` for proposal durability, while remaining active for snapshot/version history.

On Windows, pgvector installation follows the official pgvector instructions (Visual Studio C++ + `nmake /F Makefile.win`). The bootstrap will attempt to install prerequisites via `winget`.

### Optional automation: compaction logging (OpenCode plugin)

If you want automatic compaction logging, install the OpenCode plugin template shipped with this skill.

Option A (recommended): run bootstrap and choose the plugin option.

1) Copy `plugins/skill-system-memory.js` and `plugins/runtime_sync.js` to `~/.config/opencode/plugins/`
2) Restart OpenCode

Safety / rollback (if OpenCode gets stuck on startup):

- Remove or rename `~/.config/opencode/plugins/skill-system-memory.js`
- Remove or rename `~/.config/opencode/plugins/runtime_sync.js`
- Restart OpenCode
- Check logs:
  - macOS/Linux: `~/.local/share/opencode/log/`
  - Windows: `%USERPROFILE%\.local\share\opencode\log`

Plugin behavior notes:

- The plugin is designed to be a no-op unless you explicitly enabled it via bootstrap (`setup.json` sets `selected.opencode_plugin=true`).
- It only attempts a Postgres write if `selected.pgpass=true` (avoids hanging on auth prompts).
- On compaction, it always keeps local JSONL logging and the core `store_memory(...)` path available when the DB target is valid.
- Runtime projection upserts (`session_summaries`, `project_summaries`, `context_rollups`) are capability-gated: if those tables are absent, the plugin skips them with an explicit surfaced reason instead of attempting writes.
- Behavior graph refresh into `behavior_*` tables is also capability-gated and is skipped explicitly when the required tables are absent.

Uninstall:

- Remove `~/.config/opencode/plugins/skill-system-memory.js`
- Remove `~/.config/opencode/plugins/runtime_sync.js`
- Restart OpenCode

## Credentials (psql)

Do NOT hardcode passwords in scripts, skill docs, or config files.

Recommended options for non-interactive `psql`:

- `.pgpass` / `pgpass.conf` (recommended)
  - Linux/macOS: `~/.pgpass` (must be `chmod 0600 ~/.pgpass` or libpq will ignore it)
  - Windows: `%APPDATA%\postgresql\pgpass.conf` (example: `C:\Users\<you>\AppData\Roaming\postgresql\pgpass.conf`)
  - Format: `hostname:port:database:username:password`
  - Docs: https://www.postgresql.org/docs/current/libpq-pgpass.html
- `PGPASSFILE` (optional override): point to a custom location for the password file
- `PGPASSWORD` (not recommended): only for quick local testing; environment variables can leak on some systems
  - Docs: https://www.postgresql.org/docs/current/libpq-envars.html

Tip: set connection defaults once (per shell) to shorten commands:

```bash
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=agent_memory
export PGUSER=postgres   # change to your PG superuser if postgres role does not exist
```

Shell copy/paste safety:

- Avoid copying inline markdown backticks (e.g. `semantic`) into your shell. In zsh, backticks are command substitution.
- Prefer using the wrapper scripts (`scripts/mem.sh`, `scripts/mem.ps1`) or copy from fenced code blocks.

### One-time setup helper scripts

This skill ships helper scripts (relative paths):

- `scripts/setup-pgpass.ps1`
- `scripts/setup-pgpass.sh`

OpenCode usage: run them from the skill directory.

Windows run:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\setup-pgpass.ps1"
```

Linux/macOS run:

```bash
bash "scripts/setup-pgpass.sh"
```

## Memory Types

| Type | Lifespan | Use When |
|------|----------|----------|
| `working` | 24h auto-expire | Current conversation context (requires `session_id`) |
| `episodic` | Permanent + decay | Problem-solving experiences, debugging sessions |
| `semantic` | Permanent | Extracted facts, knowledge, patterns |
| `procedural` | Permanent | Step-by-step procedures, checklists (importance >= 7) |

## Core Functions

### `store_memory(type, category, tags[], title, content, metadata, agent_id, session_id, importance)`

Auto-deduplicates by content hash. Duplicate inserts bump `access_count` and `importance_score`.

```sql
SELECT store_memory(
    'semantic',
    'windows-networking',
    ARRAY['ssh', 'tunnel', 'port-conflict'],
    'SSH Tunnel Port Conflict Resolution',
    'Fix: 1) taskkill /F /IM ssh.exe  2) Use processId not pid  3) Wait 3s',
    '{"os": "Windows 11"}',
    'sisyphus',
    NULL,
    9.0
);
```

### Wrapper: `scripts/mem.py` (推薦 — parameterized query，無 quoting 問題)

> **Requirements**: `pip install psycopg2-binary`

```bash
# DB 連線狀態 + 記憶總數
python3 scripts/mem.py status

# 搜尋記憶（含特殊字元也安全）
python3 scripts/mem.py search "pgvector windows install" --limit 5
python3 scripts/mem.py search "fraud features" --scope project --limit 10

# 儲存記憶（--content flag）
python3 scripts/mem.py store --type semantic --category project --title "pgvector install" --tags "postgres,pgvector,windows" --importance 8 --scope project --content "Steps: ..."

# 儲存記憶（stdin）
printf '%s' "Steps: ..." | python3 scripts/mem.py store semantic project "pgvector install" "postgres,pgvector,windows" 8 --scope session

# 列出 / 匯出 / 壓縮
python3 scripts/mem.py list --scope project --limit 20
python3 scripts/mem.py export --format json --scope global
python3 scripts/mem.py compact --scope project

# session 開頭自動撈出相關記憶
python3 scripts/mem.py context "pgvector ssh tunnel"

# 列出所有已使用的 tags / categories
python3 scripts/mem.py tags
python3 scripts/mem.py categories

# DB 不可用時，將 pending fallback 寫回 DB
python3 scripts/mem.py flush
```

Fallback behavior:

- DB 連線失敗時，`store` 會降級寫入 `.memory/pending/*.json`，`search` / `context` 會讀本地 pending 檔案並印出警告
- `list` / `export` 也會從 pending fallback 讀取；`compact` 在 DB 不可用時只印 warning 不崩潰
- DB 可連線但 memory schema 缺失時，`store` 會 warning + no-op，`search` / `context` / `tags` / `categories` 會回空結果 + warning
- `flush` 會在 DB 恢復後將 `.memory/pending/` 內容寫回 PostgreSQL

Scope model:

- `session` (default): 記錄在 `metadata.scope=session`，並盡量帶上 `session_id`
- `project`: 記錄在 `metadata.scope=project` + `metadata.project_id=<repo>`
- `global`: 記錄在 `metadata.scope=global`，可跨專案搜尋
- `search` / `list` / `export` 不帶 `--scope` 時，會回傳所有 scope 並在結果中標記來源 scope

### Wrapper: `scripts/mem.sh` / `scripts/mem.ps1` (shell fallback)

```bash
# 連線狀態
bash "scripts/mem.sh" status

# 搜尋
bash "scripts/mem.sh" search "pgvector windows install" 5

# 儲存 (content via stdin)
printf '%s' "Steps: ..." | bash "scripts/mem.sh" store semantic project "pgvector install" "postgres,pgvector,windows" 8

# 列出 tags / categories
bash "scripts/mem.sh" tags
bash "scripts/mem.sh" categories
```

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\mem.ps1" types
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\mem.ps1" search "pgvector windows install" 5
"Steps: ..." | powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\mem.ps1" store semantic project "pgvector install" "postgres,pgvector,windows" 8
```

## Router Integration (optional)

If you use a Router skill that executes pinned pipelines, it can read a manifest embedded in this `SKILL.md`.

For portability, the manifest block is fenced as YAML but the content is JSON (valid YAML). The Router parses it.

```skill-manifest
{
  "schema_version": "2.0",
  "id": "skill-system-memory",
  "version": "0.3.0",
  "capabilities": ["memory-search", "memory-store", "memory-list", "memory-compact", "memory-export", "memory-flush", "memory-health", "memory-types", "memory-auto-write", "memory-doctor"],
  "effects": ["proc.exec", "db.read", "db.write"],
  "operations": {
    "search": {
      "description": "Search memories by natural language query. Returns ranked results with relevance scores.",
      "input": {
        "query": { "type": "string", "required": true, "description": "Natural language search query" },
        "limit": { "type": "integer", "required": false, "default": 5, "description": "Max results" },
        "scope": { "type": "string", "required": false, "description": "global | project | session filter" }
      },
      "output": {
        "description": "Array of memory matches with id, title, content, relevance_score",
        "fields": { "status": "ok | error", "data": "array of {id, title, content, relevance_score}" }
      },
      "entrypoints": {
        "unix": ["bash", "scripts/router_mem.sh", "search", "{query}", "{limit}"],
        "windows": ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts\\router_mem.ps1", "search", "{query}", "{limit}"]
      }
    },
    "store": {
      "description": "Store a new memory. Auto-deduplicates by content hash.",
      "input": {
        "memory_type": { "type": "string", "required": true, "description": "One of: semantic, episodic, procedural, working" },
        "category": { "type": "string", "required": true, "description": "Category name" },
        "title": { "type": "string", "required": true, "description": "One-line summary" },
        "tags_csv": { "type": "string", "required": true, "description": "Comma-separated tags" },
        "importance": { "type": "integer", "required": true, "description": "1-10 importance score" },
        "scope": { "type": "string", "required": false, "description": "global | project | session" }
      },
      "output": {
        "description": "Confirmation with stored memory id",
        "fields": { "status": "ok | error", "id": "integer" }
      },
      "entrypoints": {
        "unix": ["bash", "scripts/router_mem.sh", "store", "{memory_type}", "{category}", "{title}", "{tags_csv}", "{importance}"],
        "windows": ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts\\router_mem.ps1", "store", "{memory_type}", "{category}", "{title}", "{tags_csv}", "{importance}"]
      }
    },
    "list": {
      "description": "List memories with optional scope/category filters.",
      "input": {
        "scope": { "type": "string", "required": false },
        "category": { "type": "string", "required": false },
        "limit": { "type": "integer", "required": false, "default": 20 }
      },
      "output": {
        "description": "Memory listing with scope labels",
        "fields": { "status": "ok | error", "results": "array" }
      },
      "entrypoints": {
        "unix": ["python3", "scripts/mem.py", "list"],
        "windows": ["python", "scripts/mem.py", "list"]
      }
    },
    "compact": {
      "description": "Compact duplicate memories within an optional scope.",
      "input": {
        "scope": { "type": "string", "required": false }
      },
      "output": {
        "description": "Compaction result",
        "fields": { "status": "ok | warn | error", "compacted_count": "integer" }
      },
      "entrypoints": {
        "unix": ["python3", "scripts/mem.py", "compact"],
        "windows": ["python", "scripts/mem.py", "compact"]
      }
    },
    "export": {
      "description": "Export memories in json or csv format.",
      "input": {
        "format": { "type": "string", "required": false },
        "scope": { "type": "string", "required": false },
        "limit": { "type": "integer", "required": false, "default": 1000 }
      },
      "output": {
        "description": "Export payload",
        "fields": { "status": "ok | error", "results": "array|string" }
      },
      "entrypoints": {
        "unix": ["python3", "scripts/mem.py", "export"],
        "windows": ["python", "scripts/mem.py", "export"]
      }
    },
    "flush": {
      "description": "Flush local pending fallback files into PostgreSQL when the DB is back.",
      "input": {},
      "output": {
        "description": "Confirmation that pending fallback files were replayed",
        "fields": { "status": "ok | warn | error", "flushed_count": "integer" }
      },
      "entrypoints": {
        "unix": ["python3", "scripts/mem.py", "flush"],
        "windows": ["python", "scripts/mem.py", "flush"]
      }
    },
    "health": {
      "description": "Check memory system health: total count, average importance, stale count.",
      "input": {},
      "output": {
        "description": "Health metrics",
        "fields": { "status": "ok | error", "data": "array of {metric, value, status}" }
      },
      "entrypoints": {
        "unix": ["bash", "scripts/router_mem.sh", "health"],
        "windows": ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts\\router_mem.ps1", "health"]
      }
    },
    "types": {
      "description": "List available memory types and their descriptions.",
      "input": {},
      "output": {
        "description": "Memory type definitions",
        "fields": { "status": "ok | error", "data": "array of {type, lifespan, description}" }
      },
      "entrypoints": {
        "unix": ["bash", "scripts/router_mem.sh", "types"],
        "windows": ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts\\router_mem.ps1", "types"]
      }
    },
    "auto-write": {
      "description": "Procedure template for automatically storing a memory after solving a non-obvious problem.",
      "input": {},
      "output": {
        "description": "Proposed memory fields to store",
        "fields": {"memory_type": "string", "category": "string", "title": "string", "tags_csv": "string", "importance": "integer", "content": "string"}
      },
      "entrypoints": {
        "agent": "Follow scripts/auto-write-template.md"
      }
    },
    "doctor": {
      "description": "Report the current runtime capability map, memory plugin drift, memory DB target, OMO resolution, canonical evolution stores, table/routine readiness, and evolution ledger status.",
      "input": {},
      "output": {
        "description": "Doctor report for the current runtime state",
        "fields": {"status": "ok | warn | error", "doctor_report": "object"}
      },
      "entrypoints": {
        "unix": ["python3", "scripts/runtime_doctor.py", "--format", "json"],
        "windows": ["python", "scripts/runtime_doctor.py", "--format", "json"]
      }
    }
  },
  "stdout_contract": {
    "last_line_json": true
  }
}
```

Notes:

- The Router expects each step to print **last-line JSON**.
- These Router adapter scripts are separate from `mem.sh` / `mem.ps1` to avoid breaking existing workflows.

## Visualize Memories (Markdown export)

If querying PostgreSQL is too inconvenient for daily use, you can export memories into markdown files under `./Memory/` (current directory by default):

```bash
bash "<skill-dir>/scripts/sync_memory_to_md.sh" --out-dir "./Memory"
```

Outputs:

- `Memory/Long.md` (semantic + procedural)
- `Memory/Procedural.md` (procedural only)
- `Memory/Short.md` (friction + compaction-daily + procedural highlights)
- `Memory/Episodic.md` (episodic)

Backups:

- Backups are stored under `Memory/.backups/` to avoid noisy `git status`.
- Use `--no-backup` to disable.

The sync script will also create `Memory/.gitignore` if it doesn't exist (ignores `.backups/` and `SYNC_STATUS.txt`).

Long index:

- `Memory/Long.md` includes an `Index` section (top categories + tags) to make the export browsable.

### `search_memories(query, types[], categories[], tags[], agent_id, min_importance, limit)`

Hybrid search: full-text (tsvector) + trigram similarity (pg_trgm) + tag filtering.
Accepts **plain English** queries — no tsquery syntax needed.
Relevance scoring: `text_score * decay * recency * importance`.

```sql
-- Natural language
SELECT * FROM search_memories('ssh tunnel port conflict', NULL, NULL, NULL, NULL, 7.0, 5);

-- Filter by type + tags
SELECT * FROM search_memories(
    'troubleshooting steps',
    ARRAY['procedural']::memory_type[],
    NULL,
    ARRAY['ssh'],
    NULL, 0.0, 5
);
```

Returns: `id, memory_type, category, title, content, importance_score, relevance_score, match_type`
Where `match_type` is one of: `fulltext`, `trigram_title`, `trigram_content`, `metadata`.

### `memory_health_check()`

Returns: `metric | value | status` for `total_memories`, `avg_importance`, `stale_count`.

### `apply_memory_decay()`

Decays episodic memories by `0.9999^days_since_access`. Run daily.

### `prune_stale_memories(age_days, max_importance, max_access_count)`

Soft-deletes old episodic memories below thresholds. Default: 180 days, importance <= 3, never accessed.

## Agent Workflow

## Auto-Write Template

After fixing a bug or solving a non-obvious problem, store a memory using the standard template:

- Procedure: `scripts/auto-write-template.md`

> **最佳實踐**：優先使用 `mem.py` wrapper，避免 shell quoting 與認證問題。
> Raw SQL 範例請見 [Appendix: Raw SQL](#appendix-raw-sql)。

### Before a task

```bash
# 自動撈出相關記憶摘要（推薦）
python3 scripts/mem.py context "keywords from user request"

# 或搜尋並手動閱讀
python3 scripts/mem.py search "keywords from user request" 5
```

If relevant memories found, reference them: *"Based on past experience (memory #1)..."*

### After solving a problem

```bash
python3 scripts/mem.py store semantic category-name "One-line problem summary" \
  "tag1,tag2,tag3" 8 --content "Detailed problem + solution"
```

### When delegating to subagents

Include in prompt:
```
MUST DO FIRST:
  python3 scripts/mem.py context 'relevant keywords'

MUST DO AFTER:
  If you solved something new:
  python3 scripts/mem.py store semantic <category> '<title>' '<tags>' <importance> --content '<solution>'
```

### Check memory system health

```bash
python3 scripts/mem.py status
```

## Task Memory Layer (optional)

This skill also ships a minimal task/issue layer inspired by Beads: graph semantics + deterministic "ready work" queries.

Objects:

- `agent_tasks`: tasks (status, priority, assignee)
- `task_links`: typed links (`blocks`, `parent_child`, `related`, etc.)
- `blocked_tasks_cache`: materialized cache to make ready queries fast
- `task_memory_links`: link tasks to memories (`agent_memories`) for outcomes/notes

Create tasks:

```sql
INSERT INTO agent_tasks(title, description, created_by, priority)
VALUES ('Install pgvector', 'Windows build + enable extension', 'user', 1);
```

Add dependencies:

```sql
-- Task 1 blocks task 2
INSERT INTO task_links(from_task_id, to_task_id, link_type)
VALUES (1, 2, 'blocks');

-- Task 2 is parent of task 3 (used for transitive blocking)
INSERT INTO task_links(from_task_id, to_task_id, link_type)
VALUES (2, 3, 'parent_child');
```

Rebuild blocked cache (usually auto via triggers):

```sql
SELECT rebuild_blocked_tasks_cache();
```

Ready work query:

```sql
SELECT id, title, priority
FROM agent_tasks t
WHERE t.deleted_at IS NULL
  AND t.status IN ('open','in_progress')
  AND NOT EXISTS (SELECT 1 FROM blocked_tasks_cache b WHERE b.task_id = t.id)
ORDER BY priority ASC, updated_at ASC
LIMIT 50;
```

Claim a task (atomic):

```sql
SELECT claim_task(2, 'agent-1');
```

Link a task to a memory:

```sql
INSERT INTO task_memory_links(task_id, memory_id, link_type)
VALUES (2, 123, 'outcome');
```

Optional add-on: `conditional_blocks` (not implemented yet)

- This is intentionally deferred until the core workflow feels solid.
- If you need it now, store a condition in `task_links.metadata` (e.g., `{ "os": "windows" }`) and treat it as documentation.

### Wrapper scripts (recommended)

To avoid re-typing SQL, use the wrapper scripts shipped with this skill:

Windows:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\tasks.ps1" ready 50
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\tasks.ps1" create "Install pgvector" 1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\tasks.ps1" claim 2 agent-1
```

Linux/macOS:

```bash
bash "scripts/tasks.sh" ready 50
bash "scripts/tasks.sh" create "Install pgvector" 1
bash "scripts/tasks.sh" claim 2 agent-1
```

## Compaction Log (high value)

Compaction can delete context. Treat every compaction as an important event and record it.

If you're using OpenCode, prefer the OpenCode plugin route for automatic compaction logging.

### OpenCode plugin (experimental.session.compacting)

1) Copy `plugins/skill-system-memory.js` and `plugins/runtime_sync.js` to `~/.config/opencode/plugins/`
2) Restart OpenCode

It writes local compaction events to:

- `~/.config/opencode/skill-system-memory/compaction-events.jsonl`

And will also attempt a best-effort Postgres `store_memory(...)` write (requires pgpass).

### Verify

```sql
SELECT id, title, relevance_score
FROM search_memories('compaction', NULL, NULL, NULL, NULL, 0, 10);
```

If nothing is inserted, set up `.pgpass` / `pgpass.conf` so `psql` can authenticate without prompting.

## Daily Compaction Consolidation

Raw compaction events are noisy. Run a daily consolidation job that turns many compaction events into 1 daily memory.

The consolidation scripts default to the OpenCode plugin event log path.

- OpenCode events: `~/.config/opencode/skill-system-memory/compaction-events.jsonl`
- Output directory: `~/.config/opencode/skill-system-memory/compaction-daily/`

Windows run (manual):

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\consolidate-compactions.ps1"
```

Linux/macOS run (manual):

```bash
bash "scripts/consolidate-compactions.sh"
```

Scheduling:

- Windows Task Scheduler: create a daily task that runs the PowerShell command above
- Linux cron example:

```bash
# every day at 02:10 UTC
10 2 * * * bash "<skill-dir>/scripts/consolidate-compactions.sh" >/dev/null 2>&1
```

## Friction Log (turn pain into tooling)

Whenever something is annoying, brittle, or fails:

1. Store an `episodic` memory with category `friction` and tags for the tool/OS/error.
2. If it repeats (2+ times), promote it to `procedural` memory (importance >= 7) with a checklist.
3. Update this skill doc when the fix becomes a stable rule/workflow (so every agent learns it).

## Schema Overview

**`agent_memories`** — General event log. Full-text search, trigram indexes, JSONB metadata, soft-delete.
**`soul_states`** — One row per user. Structured personality/emotion/buffers JSONB. FK → `agent_memories`.
**`insight_facets`** — Per-session facets with structured fields. FK → `agent_memories`.
**`evolution_snapshots`** — Versioned evolution records with changes JSONB. FK → `agent_memories`. Current status: `LEGACY_COMPAT_SURFACE` for proposal durability, while remaining active for snapshot/version history.
**`user_preferences`** — Key-value user preferences with confidence scores.
**`memory_links`** — Graph relationships (references, supersedes, contradicts).
**`working_memory`** — Ephemeral session context with auto-expire.

Typed table functions (dual-write to both typed table and agent_memories):
- `upsert_soul_state(user, yaml, personality, emotion, ...)` → soul_states
- `insert_insight_facet(user, session_id, yaml, ...)` → insight_facets
- `insert_evolution_snapshot(user, version_tag, target, ...)` → evolution_snapshots
- `upsert_user_preference(user, key, value, source, confidence)` → user_preferences
- `get_soul_state(user)`, `get_recent_facets(user, limit)`, `get_evolution_history(user, limit)`, `get_user_preferences(user)` → typed reads
- `get_agent_context(user, facet_limit)` → aggregated context for plugin injection

There is currently no repo-backed `migrate-typed-tables.sql` file. Treat legacy typed-table backfill as deferred; do not claim it as a supported migration path.

Optional Postgres-first runtime projection tables (`session_summaries`, `project_summaries`, `context_rollups`, `behavior_*`) are created by `migrate-v2-postgres-first.sql` when you explicitly choose to enable that surface.

Key columns: `memory_type`, `category`, `tags[]`, `title`, `content`, `content_hash` (auto), `metadata` (JSONB), `importance_score`, `access_count`, `relevance_decay`, `search_vector` (auto).

## Anti-Patterns

| Don't | Do Instead |
|-------|------------|
| Store everything | Only store non-obvious solutions |
| Skip tags | Tag comprehensively: tech, error codes, platform |
| Use `to_tsquery` directly | `search_memories()` handles this via `plainto_tsquery` |
| One type for all data | Use correct memory_type per content |
| Forget importance rating | Rate honestly: 9-10 battle-tested, 5-6 partial |

## Sharp Edges

| Issue | Severity | Mitigation |
|-------|----------|------------|
| Chunks lose context | Critical | Store full problem+solution as one unit |
| Old tech memories | High | `apply_memory_decay()` daily; prune stale |
| Duplicate memories | Medium | `store_memory()` auto-deduplicates by content_hash |
| No vector search | Info | pg_trgm provides fuzzy matching; pgvector can be added later |

## Cross-Platform Notes

- **PostgreSQL 14-18** supported (no partitioning, no GENERATED ALWAYS)
- **pg_trgm** is the only required extension (ships with all PG distributions)
- **Linux**: `psql -U postgres -d agent_memory -f init.sql`
- **Windows**: Use full path to psql.exe or add PG bin to PATH
- **MCP postgres_query**: Works for read operations; DDL requires psql

## Maintenance

```sql
SELECT apply_memory_decay();                         -- daily
SELECT prune_stale_memories(180, 3.0, 0);            -- monthly
DELETE FROM working_memory WHERE expires_at < NOW();  -- daily
SELECT * FROM memory_health_check();                  -- anytime
```

## Optional: pgvector Semantic Search

If pgvector is installed on your PostgreSQL server, `init.sql` will:

- create extension `vector` (non-fatal if missing)
- add `agent_memories.embedding vector` (variable dimension)
- create `search_memories_vector(p_embedding, p_embedding_dim, ...)`

Notes:

- This does NOT generate embeddings. You must populate `agent_memories.embedding` yourself.
- Once embeddings exist, you can do nearest-neighbor search:

```sql
-- p_embedding is a pgvector literal; pass it from your app.
-- Optionally filter by dimension (recommended when using multiple models).
SELECT id, title, similarity
FROM search_memories_vector('[0.01, 0.02, ...]'::vector, 768, NULL, NULL, NULL, NULL, 0.0, 10);
```

Note: variable-dimension vectors cannot be indexed with pgvector indexes. This is a tradeoff to support local models with different embedding sizes.

If pgvector is not installed, everything else still works (fts + pg_trgm).

## Embedding Ingestion Pipeline

pgvector search only works after you populate `agent_memories.embedding`.

This skill ships ingestion scripts (relative paths). Run from the skill directory:

- `scripts/ingest-embeddings.ps1`
- `scripts/ingest-embeddings.sh`

They:

- find memories with `embedding IS NULL`
- call an OpenAI-compatible embeddings endpoint (including Ollama)
- write vectors into `agent_memories.embedding vector`

Requirements:

- pgvector installed + `init.sql` applied (so `agent_memories.embedding` exists)
- `.pgpass` / `pgpass.conf` configured (so `psql -w` can write without prompting)
- env vars for embedding API:
  - `EMBEDDING_PROVIDER` (`ollama` or `openai`; default `openai`)
  - `EMBEDDING_API_KEY` (required for `openai`; optional for `ollama`)
  - `EMBEDDING_API_URL` (default depends on provider)
  - `EMBEDDING_MODEL` (default depends on provider)
  - `EMBEDDING_DIMENSIONS` (optional; forwarded to the embeddings endpoint when supported)

Windows example:

```powershell
$env:EMBEDDING_PROVIDER = "ollama"
$env:EMBEDDING_MODEL = "nomic-embed-text"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "scripts\ingest-embeddings.ps1" -Limit 25
```

Linux/macOS example:

```bash
export EMBEDDING_API_KEY=...
export EMBEDDING_MODEL=text-embedding-3-small
bash "scripts/ingest-embeddings.sh"
```

Scheduling:

- run daily (or hourly) after you add new memories
- keep `Limit` small until you trust it

Robustness note:

- On Windows, very long SQL strings can be fragile when passed via `psql -c`. The ingestion script writes per-row updates to a temporary `.sql` file and runs `psql -f` to avoid command-line length/quoting edge cases.

## Related Skills

`systematic-debugging`, `postgres-pro`, `postgresql-table-design`
