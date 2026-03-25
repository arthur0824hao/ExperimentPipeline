# ExperimentPipeline Architecture

## 1. Project Overview

ExperimentPipeline is an ML experiment orchestration system designed for GNN-based fraud detection workloads. It handles the full lifecycle from dataset preprocessing through experiment execution, monitoring, and result registration.

**Purpose**: Automate GNN fraud detection experiment lifecycle — from raw data to trained models with GPU cluster management.

**Components**:
- **preprocessor**: Feature generation, gate validation, experiment registration
- **runner**: GPU cluster orchestration, experiment process spawning, health monitoring
- **dashboard**: Rich TUI for human operators to monitor experiment state
- **registry**: PostgreSQL-backed experiment state management with fencing
- **tooling**: File watchers, ready queue, registration helpers

**Tech Stack**:
- Python 3.13
- PostgreSQL 17 (two databases: experiment registry + agent memory)
- Rich TUI library
- psycopg2 (connection pooling, `.pgpass` auth)
- SSH + paramiko (multi-node cluster management)

---

## 2. Directory Structure

```
ExperimentPipeline/
├── pipeline/                      # Core pipeline code
│   ├── experiments.py             # Runner + TUI Dashboard (5293 lines — needs refactoring)
│   ├── db_registry.py             # PostgreSQL experiment registry (2054 lines)
│   ├── preprocess.py              # Preprocessing orchestrator (~2000 lines)
│   ├── experiment_registration.py # Config builder for training runs
│   ├── runtime_config.py          # Runtime config helpers
│   ├── registry_io.py             # File-based registry I/O (legacy)
│   ├── tui_keys.py               # TUI key handling
│   ├── cli_shared.py             # Shared CLI utilities
│   ├── preprocess_lib/            # Preprocessing library (13 modules)
│   │   ├── data_loader.py        # Data loading from disk
│   │   ├── data_loader_base.py   # Base class for data loaders
│   │   ├── feature_bank.py       # Feature bank management
│   │   ├── feature_computer.py    # Feature computation engine
│   │   ├── gate_engine.py        # Gate validation logic
│   │   ├── graph_builder.py       # Graph construction for GNN
│   │   ├── memory_estimator.py    # GPU memory estimation
│   │   ├── node_filter.py        # Node filtering logic
│   │   ├── train_template.py     # Training template management
│   │   ├── train_utils.py        # Training utilities
│   │   ├── trainer.py            # Training orchestration
│   │   ├── cutoff_utils.py       # Cutoff utilities
│   │   └── __init__.py
│   ├── templates/                 # Jinja2 training templates
│   ├── tools/                    # Watcher, ready_register
│   │   ├── watcher.py            # File system watcher for ready.json
│   │   ├── ready_register.py     # Ready queue registration helper
│   │   └── start_watcher.sh      # Shell launcher for watcher
│   ├── tests/                    # Test suite (9 files, ~4473 lines)
│   │   ├── conftest.py           # Pytest fixtures
│   │   ├── test_coverage_gaps.py # Coverage gap analysis
│   │   ├── test_dashboard.py     # Dashboard component tests
│   │   ├── test_dashboard_keys.py # TUI key binding tests
│   │   ├── test_experiment_management.py # Experiment management tests
│   │   ├── test_preprocess_loop.py # Preprocessing loop tests
│   │   ├── test_preprocess_watch_archive.py # Archive watching tests
│   │   ├── test_tui_keys.py      # Key handling tests
│   │   └── test_worker_disable.py # Worker disable logic tests
│   └── lib/                       # (currently empty — future module split target)
├── configs/                       # Runtime configs, database configs, machine configs
├── experiments/                   # Experiment output directory (run artifacts)
├── templates/                     # Top-level templates (legacy)
├── tests/                         # Integration tests
├── tools/                         # Top-level tooling
├── .agents/                       # AI agent skill system
│   └── skills/
│       ├── skill-system-tkt/      # Ticket lifecycle management
│       ├── skill-system-memory/   # Memory backend (Postgres)
│       ├── skill-system-postgres/ # Postgres schema + migrations
│       ├── skill-system-cli/      # CLI entry point (sk)
│       ├── skill-system-router/   # Skill routing
│       └── ...                    # Other skill system modules
├── .tkt/                          # Ticket/roadmap system
│   ├── roadmap.yaml               # Project-level roadmap
│   ├── bundles/                   # Bundle + ticket files
│   └── history.log               # Event log
├── configs/                       # DB configs, machine configs, runtime configs
├── pyrightconfig.json            # Type checking config
├── pytest.ini                    # Test runner config
├── experiments.json               # Experiment registry (file-based)
└── ready.json                    # Ready queue (preprocessing output)
```

---

## 3. Data Flow

```
ready.json
    │
    ▼
preprocess.py ──────────────────────────────────────────────► experiments.json
    │                                                              │
    │  ┌──────────────────────────────────────────────────────────┘
    │  │
    │  ▼
    ├─► feature generation (preprocess_lib/)
    │     ├─ graph_builder.py    → GNN graph construction
    │     ├─ feature_computer.py → feature computation
    │     └─ feature_bank.py     → feature persistence
    │
    ├─► gate validation (gate_engine.py)
    │     └─ checks data quality thresholds
    │
    └─► registration (experiment_registration.py)
          └─ writes experiment config to experiments.json

experiments.json ──► experiments.py (runner)
    │
    │  ┌─ experiments.py
    │  │    ├─ ClusterManager      → SSH to nodes, start workers
    │  │    ├─ GPUAllocator       → allocate GPUs based on memory
    │  │    ├─ run_experiment_process → spawn training processes
    │  │    └─ orphan reaping     → cleanup stale workers
    │  │
    │  ▼
    └─► db_registry.py (PostgreSQL)
           ├─ ExperimentRegistry  → experiment state machine
           ├─ claim+fencing       → concurrency control
           ├─ heartbeat           → liveness detection
           └─ SnapshotRegistry   → best-run tracking

         UnifiedDashboard (Rich TUI)
           ├─ Live status table  → all experiments at a glance
           ├─ GPU utilization     → per-node GPU usage
           └─ Action keys        → q=quit, r=refresh, etc.
```

---

## 4. Database Architecture

### Database 1: `ExperimentPipeline-database`
- **Schema**: `exp_registry`
- **Purpose**: Persistent experiment state tracking
- **Tables**: experiment registry, best run snapshots
- **Connection**: psycopg2 ThreadedConnectionPool
- **Auth**: `.pgpass` file (NOT `postgres` superuser)

### Database 2: `ExperimentPipeline-memory`
- **Schema**: `skill_system`
- **Purpose**: Agent memory + skill system state
- **Tables**:
  - `policy_profiles` — effect allowlists per agent profile
  - `skill_runs` — skill execution history
  - `run_events` — step-level telemetry
  - `refresh_jobs` — async job queue
  - `refresh_job_events` — job event log
  - `artifact_versions` — versioned artifacts
  - `skill_graph_nodes` — skill dependency graph nodes
  - `skill_graph_edges` — skill dependency graph edges
- **Connection**: psycopg2 ThreadedConnectionPool
- **User**: `arthur0824hao` (NOT `postgres`)
- **NOTE**: Database name contains hyphens — always quote: `psql -d "ExperimentPipeline-memory"`

### Shared Patterns
- Both databases use `.pgpass` for authentication
- psycopg2 `ThreadedConnectionPool` for connection reuse
- Advisory locking for conflict-safe claiming

---

## 5. Key Classes

### db_registry.py — `DBExperimentsDB`
- Manages PostgreSQL-backed experiment registry
- State machine: `pending → queued → running → done/failed`
- Claim + fencing logic prevents duplicate experiment starts
- Heartbeat mechanism detects stale runners
- Snapshot tracking for best runs

### experiments.py — `ClusterManager` (line ~1773)
- Multi-node SSH cluster orchestration
- Connects to worker nodes via paramiko
- Dispatches `run_experiment_process` to remote nodes
- Tracks per-node GPU availability

### experiments.py — `GPUAllocator` (line ~1984)
- Memory-aware GPU allocation
- Reserves GPU memory for experiment workloads
- Handles OOM detection and batch size reduction

### experiments.py — `UnifiedDashboard` (line ~2180)
- Rich TUI dashboard
- Live-updating experiment status table
- GPU utilization heatmap
- Keyboard-driven interaction (q, r, s, c keys)

### experiments.py — `HybridLogger` (line ~3873)
- Dual-output logging: file + console
- Rotating log files per experiment
- Structured log format with experiment metadata

---

## 6. Planned Refactoring Target

experiments.py is currently **5293 lines**. The refactoring plan splits it into focused modules:

```
pipeline/
├── experiments.py          → entrypoint only (main, CLI dispatch, ~2300 lines target)
├── gpu.py                  → GPU monitoring + allocation utilities
├── memory_contract.py      → Memory estimation + OOM policy
├── artifact.py             → Result/artifact file helpers
├── oom.py                  → OOM detection + batch size reduction
├── formatting.py           → TUI formatting helpers
├── cluster.py              → ClusterManager class
├── allocator.py            → GPUAllocator class
├── worker.py               → run_experiment_process + remote helpers
├── health.py               → Orphan reaping, stale detection, self-heal
└── condition.py            → Condition nodes + staged matrix
```

**Extraction Phases**:
- **Phase 1 (B-004)**: Pure utility modules — no class dependencies, no state coupling
- **Phase 2 (B-005)**: Core classes — ClusterManager, GPUAllocator, HybridLogger, worker/health functions
- **After Phase 2**: experiments.py contains only UnifiedDashboard + main() + scheduler_loop

**Behavior Contracts**:
- Each extracted module has a `SKILL.spec.yaml` defining exported interfaces
- Tests verify module boundaries before and after extraction
- B-003 creates the behavior specs as refactoring contracts

---

## 7. Configuration Files

| File | Purpose |
|------|---------|
| `pyrightconfig.json` | Pyright type-checker config |
| `pytest.ini` | Pytest runner config |
| `configs/` | Runtime configs, DB configs, machine configs |
| `experiments.json` | File-based experiment registry (legacy) |
| `ready.json` | Preprocessing output queue |

---

## 8. Agent / Skill System

The `.agents/` directory provides AI agent capabilities:

- **skill-system-tkt**: Ticket lifecycle management (roadmap → bundle → ticket → review)
- **skill-system-memory**: Agent memory backend (Postgres FTS + pg_trgm + pgvector)
- **skill-system-postgres**: Postgres schema management + migrations
- **skill-system-cli**: Unified CLI entry point (`sk` command)
- **skill-system-router**: Skill discovery and routing
- **skill-system-workflow**: Goal → DAG planning engine

Agents operate via a ticketclaiming model:
- Integrator (TKT-000) coordinates a bundle
- Workers (TKT-001+) execute individual tasks
- Audit (TKT-A00) spot-checks quality
