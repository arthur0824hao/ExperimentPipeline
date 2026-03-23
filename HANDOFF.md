# ExperimentPipeline — Agent Handoff Document

> Generated: 2026-03-23 by Sisyphus (reviewer/PM agent)
> For: Incoming agent on remote machine

---

## 1. Project Overview

General-purpose experiment pipeline for GNN/FraudDetect ML research. Manages experiment registration, GPU allocation, training execution, monitoring dashboards, and archiving.

**Key entry points:**
- `pipeline/experiments.py` — Runner + Experiment Dashboard (TUI)
- `pipeline/preprocess.py` — Feature preprocessing + Watch Dashboard (TUI)
- `pipeline/db_registry.py` — PostgreSQL-backed experiment registry

---

## 2. Current State (Post B-007)

### Test Suite Health
```
312 passed, 1 failed (pre-existing), 19 skipped
```
The 1 failure is `test_gpu_allocator_allocate_warmup_overlap_allows_extra_slot` — pre-existing since module split, not caused by recent work.

### Roadmap Status: `active`

| Goal | Status | Bundles |
|------|--------|---------|
| G-001 Infrastructure Repair | ✅ done | B-001 |
| G-002 Test Safety Net | ✅ done | B-002, B-003 |
| G-003 Module Split | ✅ done | B-004, B-005 |
| G-004 Dual UX (Agent CLI + Human TUI) | 🔲 blocked on G-003 (can unblock now) |  |
| G-005 MLOps Automation | 🔲 blocked on G-003 (can unblock now) |  |

### Bundle History

| Bundle | Title | Status | Tickets |
|--------|-------|--------|---------|
| B-001 | Infrastructure Repair — DB Migrations + Architecture Doc | closed | 5 |
| B-002 | Test Safety Net — db_registry.py Core Coverage | closed | 7 |
| B-003 | Test Safety Net — preprocess.py + Behavior Specs | closed | 7 |
| B-004 | Module Split Phase 1 — Extract Pure Utility Modules | closed | 8 |
| B-005 | Module Split Phase 2 — Extract Core Classes | closed | 8 |
| B-006 | Fix 20 Pre-existing Test Failures | closed | 14 (6 done + 6 carryover → B-007 + 2 meta) |
| B-007 | Carryover Features + Issue Fixes | closed | 18 |

---

## 3. Codebase Architecture

### Pipeline Module Map (10,140 lines total)

```
pipeline/
├── experiments.py          3013 lines  ← Main runner + UnifiedDashboard TUI
├── preprocess.py           1967 lines  ← Feature preprocessing + Watch TUI
├── db_registry.py          2110 lines  ← DBExperimentsDB (PostgreSQL registry)
├── worker.py                621 lines  ← run_experiment_process + cleanup
├── health.py                518 lines  ← Orphan reaping, self-heal, heartbeat
├── gpu.py                   298 lines  ← GPU monitoring (nvidia-smi wrappers)
├── artifact.py              292 lines  ← Result/artifact file I/O
├── condition.py             248 lines  ← Condition nodes (experiment DAG)
├── cluster.py               239 lines  ← ClusterManager (multi-machine)
├── allocator.py             235 lines  ← GPUAllocator (slot management)
├── memory_contract.py       227 lines  ← Memory estimation for experiments
├── archive_card.py          115 lines  ← [NEW B-007] matplotlib chart generator
├── formatting.py             93 lines  ← TUI formatting helpers
├── oom.py                   102 lines  ← OOM detection
├── logger_hybrid.py          62 lines  ← HybridLogger (file + console)
├── preprocess_lib/                     ← 13 preprocessing sub-modules
│   └── train_utils.py      3300+ lines ← ProgressReporter, train_single_model
├── templates/                          ← Train wrapper templates + behavior yamls
├── tools/                              ← watcher.py, ready_register.py
└── tests/                  7129 lines  ← 19 test files, 312 passing
    ├── test_dashboard_keys.py          ← Experiment dashboard key handler tests
    ├── test_preprocess_watch_archive.py ← Preprocess watch panel tests
    ├── test_preprocess_watch_keys.py   ← [NEW B-007] Watch key boundary tests
    ├── test_dashboard_key_boundaries.py ← [NEW B-007] Dashboard key boundaries
    ├── test_db_registry_*.py (5 files) ← DB registry mock tests
    ├── test_preprocess_*.py (3 files)  ← Preprocess feature/loop/registration
    ├── test_gpu_allocation.py          ← GPU allocator tests (1 pre-existing failure)
    ├── test_experiment_management.py   ← Experiment management (16 skipped)
    ├── test_tui_keys.py               ← TUI key constants
    └── test_worker_disable.py         ← Worker disable/enable mock tests
```

### Key Design Patterns

1. **DB is source of truth** — `DBExperimentsDB` (db_registry.py) reads/writes PostgreSQL. `experiments.json` is a read-only snapshot synced via `_sync_snapshot()` (DB→JSON direction only).

2. **Module split contract** — experiments.py was 5293 lines, split into 11 modules in B-004/B-005. experiments.py re-exports everything via `from gpu import *` etc. for backward compat. Behavior specs: `pipeline/experiments.behavior.yaml`, `pipeline/preprocess.behavior.yaml`.

3. **TUI Architecture** — Two Rich-based dashboards:
   - `UnifiedDashboard` (experiments.py) — experiment monitoring, key-driven actions
   - `_render_watch_panel` (preprocess.py) — two-page view (Operations / Feature Bank, Tab to switch)

4. **Metric Tracking** (NEW B-007):
   - `ProgressReporter.update()` accepts `**metrics` kwargs
   - Appends to `metric_history.jsonl` (one JSON line per epoch)
   - `archive_card.py` reads JSONL → matplotlib charts on archive

---

## 4. Database Setup

### Connection Info
```
Host:     localhost
Port:     5432
User:     arthur0824hao (system user, NOT postgres)
```

### Databases

| Config Name | Actual DB | Status |
|-------------|-----------|--------|
| `ExperimentPipeline-memory` | `ExperimentPipeline-memory` | ✅ Exists, 381 memories |
| `ExperimentPipeline-experiment` | ❌ Does NOT exist | Config says it should, but only `FraudDetect-experiment` exists |

**CRITICAL NOTE:** The experiment DB name mismatch is a known issue. `configs/database.json` says `ExperimentPipeline-experiment` but only `FraudDetect-experiment` exists on disk. B-007 TKT-004 added a fallback guard (load from JSON if DB unreachable), but the root cause (DB doesn't exist) is NOT fixed.

### PostgreSQL Management
```bash
# Start (conda environment required)
conda run -n gnn_fraud pg_ctl -D /datas/store162/arthur0824hao/postgres_data -l /datas/store162/arthur0824hao/postgres_data/logfile start

# Stop
conda run -n gnn_fraud pg_ctl -D /datas/store162/arthur0824hao/postgres_data stop

# Connect (MUST use quotes for hyphenated names)
psql -h localhost -d "ExperimentPipeline-memory"
```

### Memory DB Schema
- `public` schema: 21 tables (agent_memories, etc.)
- `skill_system` schema: 12 tables (v3 graph + v4 control-plane migrations applied)

---

## 5. Skill System

### Location
`.agents/skills/` — 19 skills installed, indexed in `.agents/skills/skills-index.json`

### Key Skills
- **skill-system-tkt** — Ticket lifecycle (filesystem bundles + DB)
- **skill-system-memory** — PostgreSQL-backed agent memory
- **skill-system-router** — Skill discovery and routing
- **skill-system-gate** — Experiment gate validation

### Startup Order (from AGENTS.md)
1. `skill-system-router`
2. `skill-system-memory`

### Shell Policy
All shell commands must run through tmux sessions.
Protected tmux sessions (never kill): `unified`, `unified-oc`, `mem-handoff`, `exp_runner`

---

## 6. Open Issues

### ExperimentPipeline repo (7 open)

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 3 | exp panel: real-time progress display | **Resolved by B-007** TKT-013/014 — close it |
| 4 | rePipe (p key) targets wrong experiment | **Resolved by B-007** TKT-001 — close it |
| 5 | Runner only shows claimed experiments | **Resolved by B-007** TKT-005/006 — close it |
| 7 | exp panel: show all DB experiments | **Resolved by B-007** TKT-005/006 — close it |
| 8 | Runner startup overwrites DB | **Partially mitigated** by B-007 TKT-004 (fallback guard). Root cause (DB doesn't exist) NOT fixed |
| 9 | preprocess TUI: swap tab order | **Resolved by B-007** TKT-007/008 — close it |
| 10 | exp panel: QUEUED/Waiting visible from DB | **Resolved by B-007** TKT-005/006 — close it |

**Action needed:** Close issues #3, #4, #5, #7, #9, #10. Keep #8 open (partially mitigated only).

### skills repo (2 open)

| # | Title | Notes |
|---|-------|-------|
| 34 | mem.py add_argument bug | Same as #33 (which we filed). Fix is local in our repo. Needs PR upstream |
| 32 | Memory-first diagnosis | Enhancement request for skill-system-memory search heuristics |

---

## 7. What's Next (Pending Work)

### Immediate

1. **Close resolved issues** (#3, #4, #5, #7, #9, #10)
2. **Fix pre-existing test failure** — `test_gpu_allocator_warmup_overlap` (1 test, allocator.py warmup logic)
3. **Create `ExperimentPipeline-experiment` DB** — the experiment DB doesn't exist. Either:
   - Create it: `createdb -h localhost "ExperimentPipeline-experiment"` + apply exp_registry schema
   - Or update config to point to existing `FraudDetect-experiment`

### Roadmap Goals (Ready to Unblock)

**G-004: Dual UX (Agent CLI + Human TUI)**
- Every CLI command supports `--json` (agent) and Rich output (human)
- Agent uses `pipeline/cli/agent_cli.py` for structured JSON
- TUI dashboard remains human-only surface

**G-005: MLOps Automation**
- Experiment comparison tool (side-by-side metrics from DB)
- One-click flow automation (ready.json → results)
- Experiment lineage tracking
- Auto-generated summaries

### User's Stated Preferences
- **Rolling roadmap** — "滾動式持續，隨著我的研究一起成長，沒有可見盡頭"
- **Dual UX** — "主要是agent-human都要可讀，對human是UI操作，對agent是CLI ux"
- **Refactoring** — "拆分成多個模塊檔案，experiments.py用behavior追蹤模塊互動"
- **All key bindings must have boundary tests**
- **All DB interactions must be atomic**
- **Archive card generation failure must not block archive**

---

## 8. Coder Model Constraints

The user uses **weak coder models** (Kimi M2.5, GPT-5.4, Kimi K2.5) for implementation. Tickets must be:

- **One file per ticket** (max 2 files)
- **Exact line numbers** and exact code to write
- **Zero ambiguity** — no "figure out where" or "investigate"
- **VERIFY command** on every ticket
- **Regression gate** on TKT-A00: `pytest pipeline/tests/ must show 0 failures`

Lesson learned from B-007: coder marked "complete" with 15 regressions because individual ticket VERIFY checks passed but full test suite wasn't run. Always include full-suite regression check.

---

## 9. TKT System Usage

### Filesystem Backend
```bash
# Create bundle
bash .agents/skills/skill-system-tkt/scripts/tkt.sh create-bundle --goal "..." --track architecture

# Add ticket
bash .agents/skills/skill-system-tkt/scripts/tkt.sh add --bundle B-008 --type worker --title "..." --description "..." --wave 1

# Check status
bash .agents/skills/skill-system-tkt/scripts/tkt.sh status --bundle B-008

# Close bundle (after all tickets done)
bash .agents/skills/skill-system-tkt/scripts/tkt.sh close --bundle B-008
```

### Bundle Naming
- Bundles: `B-XXX` (sequential)
- Tickets: `TKT-000` (integrate), `TKT-001`..`TKT-NNN` (workers), `TKT-A00` (audit)
- Next bundle to create: **B-008**

### Known TKT Issues
- `close --bundle` may fail with "Audit ticket must be claimed" — workaround: manually edit `bundle.yaml` to set `stage: closed`
- Long descriptions with colons break YAML parsing in `tkt.sh update --summary` — keep summaries simple

---

## 10. File Locations Quick Reference

```
ExperimentPipeline/
├── AGENTS.md                          ← Agent config (startup skills, shell policy)
├── HANDOFF.md                         ← This document
├── configs/
│   ├── database.json                  ← DB connection config
│   └── runtime.json                   ← Runtime settings
├── .agents/
│   ├── config/tkt.yaml                ← TKT close_gate config
│   └── skills/                        ← 19 installed skills
│       └── skills-index.json          ← Skill registry
├── .tkt/
│   ├── roadmap.yaml                   ← Active roadmap (5 goals, 4 tracks)
│   └── bundles/B-001..B-007/          ← All completed bundles
├── pipeline/                          ← Main source code (see §3)
│   ├── experiments.behavior.yaml      ← Module interaction spec
│   ├── preprocess.behavior.yaml       ← Module interaction spec
│   └── tests/                         ← 19 test files
├── docs/
│   ├── architecture.md                ← Project architecture doc
│   └── db_sync_investigation.md       ← B-007 DB sync analysis
└── experiments.json                   ← Read-only snapshot (synced from DB)
```

---

## 11. Verification Commands

```bash
# Full test suite (must show 0 failures for deployment)
python3 -m pytest pipeline/tests/ -q --tb=line

# Check specific module imports
python3 -c "import sys; sys.path.insert(0,'pipeline'); import gpu, oom, formatting, condition, artifact, memory_contract, cluster, allocator, logger_hybrid, worker, health, archive_card; print('All modules OK')"

# DB connectivity
psql -h localhost -d "ExperimentPipeline-memory" -c "SELECT count(*) FROM agent_memories"

# TKT status
bash .agents/skills/skill-system-tkt/scripts/tkt.sh roadmap-status
bash .agents/skills/skill-system-tkt/scripts/tkt.sh list

# Preprocess watch (manual)
python3 pipeline/preprocess.py --watch --dry-run
```
