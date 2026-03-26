# ExperimentPipeline Architecture (Current Master)

## Overview

ExperimentPipeline runs fraud-detection experiments with a DB-first control plane.
`exp_registry` in PostgreSQL is the truth source for experiment state; `experiments.json` is a compatibility snapshot written from DB state.

Primary runtime surfaces:
- **Control plane / runner**: `pipeline/experiments.py`
- **Registry**: `pipeline/db_registry.py` (PostgreSQL-backed)
- **Preprocess + registration**: `pipeline/preprocess.py`, `pipeline/experiment_registration.py`
- **Cluster + worker execution**: `pipeline/cluster.py`, `pipeline/worker.py`

## Source of Truth

- Truth source: PostgreSQL schema `exp_registry` (default DB name in code: `FraudDetect-experiment`)
- Compatibility artifact: `experiments.json` and `pipeline/experiments.json`
- Sync direction: DB -> JSON via `sync_snapshot_to_json()` in `pipeline/db_registry.py`

There is no JSON->DB promotion path in current master.

## Pipeline Module Layout

Current `pipeline/` boundaries:
- `experiments.py`: keeps `UnifiedDashboard`, `main()`, scheduler loop, and TTY/input flow
- `cluster.py`: cluster lifecycle and node actions
- `allocator.py`: GPU allocation and slot serialization helpers
- `worker.py`: experiment process lifecycle, mark-running/done/error, lock cleanup helpers
- `health.py`: orphan/stale checks and heartbeat conflict healing
- `artifact.py`: result/resource artifact reads and summary extraction
- `memory_contract.py`: memory contract and OOM policy persistence
- `condition.py`: runtime condition nodes and staged matrix synthesis
- `preprocess.py` + `preprocess_lib/`: feature preprocessing and registration pipeline
- `db_registry.py`: DB access, claim/update/query, and snapshot sync

One bounded extraction is now explicit:
- `terminal_state.py`: terminal-reason normalization + artifact reconciliation helpers used by the runner panel.

## Config Surfaces

Config root is `configs/` (or `EP_CONFIG_DIR` override).

Important files in current master:
- `configs/database.json`: DB connectivity defaults
- `configs/machines.json`: worker machine inventory
- `configs/phase3_runtime.json`: runtime tuning file loaded by `pipeline/runtime_config.py` (optional; missing file falls back to defaults)
- `configs/local/`: local, untracked overrides

Notes:
- `runtime_config.py` resolves config path from `EP_CONFIG_DIR` first, then `<repo>/configs`.
- `runtime.json` is not a required runtime input in this repository state.

## Test and CI Surface

Test entrypoint:
- `python3 -m pytest pipeline/tests/ -q --tb=line`

CI gate:
- Workflow: `.github/workflows/ci.yml`
- Required job/check candidate: `ep-gate`
- Trigger: `pull_request` + `push` to `master`
- Gate script: `scripts/ci_gate.sh` (docs-only vs full-path checks)

Deploy scaffold:
- Workflow: `.github/workflows/deploy.yml`
- Mode: protected scaffold (mode B) unless operators satisfy mode A prerequisites
- Scope: control-plane only (no worker fleet rollout, no DB migrations)

Branch/deploy policy details are documented in `docs/branch-protection.md`.
