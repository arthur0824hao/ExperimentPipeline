#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CANONICAL_MEMORY_TARGET = "agent_memory"
ROOT_DIR = Path(__file__).resolve().parents[3]
REPO_PLUGIN_DIR = ROOT_DIR / "skills" / "skill-system-memory" / "plugins"
REPO_SKILL_MD = ROOT_DIR / "skills" / "skill-system-memory" / "SKILL.md"
EVOLUTION_PROPOSAL_PATH = (
    ROOT_DIR / "skills" / "skill-system-evolution" / "scripts" / "evolution_proposal.py"
)
EVOLUTION_MIGRATION_PATH = (
    ROOT_DIR / "skills" / "skill-system-evolution" / "migrate-v2-durable-evolution.sql"
)
MISSING_MIGRATION_REFERENCE = "migrate-typed-tables.sql"
SUPPORTED_NOW = "SUPPORTED_NOW"
GATED_OPTIONAL = "GATED_OPTIONAL"
DEFERRED_UNSUPPORTED = "DEFERRED_UNSUPPORTED"
ACTIVE_CURRENT_SURFACE = "ACTIVE_CURRENT_SURFACE"
LEGACY_COMPAT_SURFACE = "LEGACY_COMPAT_SURFACE"
UNKNOWN = "UNKNOWN"
CORE_MEMORY_TABLES = [
    "agent_memories",
]
CORE_MEMORY_ROUTINES = [
    "store_memory",
    "search_memories",
    "memory_health_check",
]
EVOLUTION_LEDGER_TABLES = [
    "evolution_snapshots",
]
EVOLUTION_LEDGER_ROUTINES = [
    "insert_evolution_snapshot",
    "get_evolution_history",
]
TYPED_CONTEXT_TABLES = [
    "soul_states",
    "insight_facets",
    "user_preferences",
]
TYPED_CONTEXT_ROUTINES = [
    "get_agent_context",
    "get_soul_state",
    "get_recent_facets",
    "get_user_preferences",
]
RUNTIME_SYNC_PROJECTION_TABLES = [
    "session_summaries",
    "project_summaries",
    "context_rollups",
]
BEHAVIOR_GRAPH_TABLES = [
    "behavior_sources",
    "behavior_nodes",
    "behavior_edges",
    "behavior_snapshots",
]
EVOLUTION_CANONICAL_TABLES = [
    "evolution_nodes",
    "evolution_rejections",
    "evolution_tasks",
]
CONTROL_PLANE_TABLES = [
    "policy_profiles",
    "runs",
    "run_events",
    "refresh_jobs",
    "refresh_job_events",
    "artifact_versions",
]
AGENT_MEMORY_TABLES = (
    CORE_MEMORY_TABLES
    + EVOLUTION_LEDGER_TABLES
    + TYPED_CONTEXT_TABLES
    + RUNTIME_SYNC_PROJECTION_TABLES
    + BEHAVIOR_GRAPH_TABLES
    + EVOLUTION_CANONICAL_TABLES
)
AGENT_MEMORY_ROUTINES = (
    CORE_MEMORY_ROUTINES + EVOLUTION_LEDGER_ROUTINES + TYPED_CONTEXT_ROUTINES
)


def utc_minute_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")


def sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_text(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


def resolve_memory_target(env: dict[str, str] | None = None) -> dict[str, Any]:
    values = env or dict(os.environ)
    explicit = values.get("SKILL_PGDATABASE", "").strip()
    ambient = values.get("PGDATABASE", "").strip()

    if explicit:
        if ambient and ambient != explicit:
            source = f"SKILL_PGDATABASE(overrides:{ambient})"
        else:
            source = "SKILL_PGDATABASE"
        return {
            "canonical_target": CANONICAL_MEMORY_TARGET,
            "explicit_value": explicit,
            "ambient_value": ambient or None,
            "target": explicit,
            "source": source,
            "silent_redirection_status": "blocked-by-explicit-target",
            "aligned": explicit == CANONICAL_MEMORY_TARGET,
        }

    if ambient:
        return {
            "canonical_target": CANONICAL_MEMORY_TARGET,
            "explicit_value": None,
            "ambient_value": ambient,
            "target": None,
            "source": "ambient-only-blocked",
            "silent_redirection_status": "ambient-only-risk",
            "aligned": False,
        }

    return {
        "canonical_target": CANONICAL_MEMORY_TARGET,
        "explicit_value": None,
        "ambient_value": None,
        "target": CANONICAL_MEMORY_TARGET,
        "source": f"default:{CANONICAL_MEMORY_TARGET}",
        "silent_redirection_status": "no-ambient-default",
        "aligned": True,
    }


def detect_omo_runtime(home_dir: Path) -> dict[str, Any]:
    config_dir = home_dir / ".config" / "opencode"
    cache_dir = home_dir / ".cache" / "opencode"
    opencode_config_path = config_dir / "opencode.json"
    opencode_config = load_json(opencode_config_path) or {}
    plugin_entries = opencode_config.get("plugin", [])
    enabled_in_config = (
        isinstance(plugin_entries, list) and "oh-my-opencode" in plugin_entries
    )

    cache_package_path = cache_dir / "node_modules" / "oh-my-opencode" / "package.json"
    cache_entry_path = (
        cache_dir / "node_modules" / "oh-my-opencode" / "dist" / "index.js"
    )
    cache_package = load_json(cache_package_path) or {}
    cache_root_package = load_json(cache_dir / "package.json") or {}
    cache_dependencies = cache_root_package.get("dependencies", {})
    cache_dependency_recorded = (
        isinstance(cache_dependencies, dict)
        and cache_dependencies.get("oh-my-opencode") is not None
    )
    global_packages = sorted(
        str(path)
        for path in (home_dir / ".bun" / "install" / "global" / "node_modules").glob(
            "oh-my-opencode*"
        )
    )
    global_binary_path = home_dir / ".bun" / "bin" / "opencode"

    actual_resolution_path = None
    resolution_basis = None
    if (
        enabled_in_config
        and cache_package_path.exists()
        and cache_entry_path.exists()
        and cache_dependency_recorded
    ):
        actual_resolution_path = str(cache_entry_path)
        resolution_basis = "project launcher cache"

    still_unknown: list[str] = []
    if enabled_in_config and actual_resolution_path is None:
        still_unknown.append(
            "oh-my-opencode is enabled in config but cache-backed runtime resolution could not be proven"
        )

    return {
        "enabled_in_config": enabled_in_config,
        "opencode_config_path": str(opencode_config_path),
        "cache_dependency_recorded": cache_dependency_recorded,
        "cache_package_path": str(cache_package_path)
        if cache_package_path.exists()
        else None,
        "cache_entry_path": str(cache_entry_path)
        if cache_entry_path.exists()
        else None,
        "cache_version": cache_package.get("version"),
        "global_binary_path": str(global_binary_path)
        if global_binary_path.exists()
        else None,
        "global_package_paths": global_packages,
        "actual_resolution_path": actual_resolution_path,
        "resolution_basis": resolution_basis,
        "still_unknown": still_unknown,
    }


def live_plugin_paths(home_dir: Path) -> dict[str, Path]:
    plugin_dir = home_dir / ".config" / "opencode" / "plugins"
    return {
        "plugin_dir": plugin_dir,
        "plugin": plugin_dir / "skill-system-memory.js",
        "runtime_sync": plugin_dir / "runtime_sync.js",
        "sync_state": home_dir
        / ".config"
        / "opencode"
        / "skill-system-memory"
        / "plugin-sync.json",
    }


def repo_plugin_paths(repo_root: Path) -> dict[str, Path]:
    repo_dir = repo_root / "skills" / "skill-system-memory" / "plugins"
    return {
        "plugin": repo_dir / "skill-system-memory.js",
        "runtime_sync": repo_dir / "runtime_sync.js",
    }


def sync_live_plugin(home_dir: Path, repo_root: Path) -> dict[str, Any]:
    repo_paths = repo_plugin_paths(repo_root)
    live_paths = live_plugin_paths(home_dir)
    live_paths["plugin_dir"].mkdir(parents=True, exist_ok=True)
    live_paths["sync_state"].parent.mkdir(parents=True, exist_ok=True)

    changed_files: list[str] = []
    for key in ("plugin", "runtime_sync"):
        source = repo_paths[key]
        target = live_paths[key]
        if not source.exists():
            raise FileNotFoundError(f"repo plugin source missing: {source}")
        if sha256_file(source) != sha256_file(target):
            shutil.copy2(source, target)
            changed_files.append(str(target))

    state = {
        "synced_at_utc_minute": utc_minute_now(),
        "source_of_truth": str(repo_paths["plugin"].parent),
        "repo_root": str(repo_root),
        "files": {
            "plugin": {
                "repo_path": str(repo_paths["plugin"]),
                "live_path": str(live_paths["plugin"]),
                "sha256": sha256_file(live_paths["plugin"]),
            },
            "runtime_sync": {
                "repo_path": str(repo_paths["runtime_sync"]),
                "live_path": str(live_paths["runtime_sync"]),
                "sha256": sha256_file(live_paths["runtime_sync"]),
            },
        },
    }
    live_paths["sync_state"].write_text(
        json.dumps(state, indent=2) + "\n", encoding="utf-8"
    )

    return {
        "changed_files": changed_files,
        "sync_state_path": str(live_paths["sync_state"]),
        "sync_state": state,
    }


def build_plugin_report(
    home_dir: Path, repo_root: Path, sync_requested: bool
) -> dict[str, Any]:
    repo_paths = repo_plugin_paths(repo_root)
    live_paths = live_plugin_paths(home_dir)
    before = {
        "plugin": sha256_file(live_paths["plugin"]),
        "runtime_sync": sha256_file(live_paths["runtime_sync"]),
    }
    sync_result = None
    if sync_requested:
        sync_result = sync_live_plugin(home_dir, repo_root)

    after = {
        "plugin": sha256_file(live_paths["plugin"]),
        "runtime_sync": sha256_file(live_paths["runtime_sync"]),
    }
    repo_hashes = {
        "plugin": sha256_file(repo_paths["plugin"]),
        "runtime_sync": sha256_file(repo_paths["runtime_sync"]),
    }

    def drift_status(plugin_hash: str | None, runtime_hash: str | None) -> str:
        if plugin_hash is None:
            return "missing-live-plugin"
        if runtime_hash is None:
            return "missing-live-runtime-sync"
        if (
            plugin_hash != repo_hashes["plugin"]
            or runtime_hash != repo_hashes["runtime_sync"]
        ):
            return "drifted"
        return "in_sync"

    return {
        "source_of_truth": str(repo_paths["plugin"].parent),
        "repo_plugin_path": str(repo_paths["plugin"]),
        "repo_runtime_sync_path": str(repo_paths["runtime_sync"]),
        "live_plugin_path": str(live_paths["plugin"]),
        "live_runtime_sync_path": str(live_paths["runtime_sync"]),
        "repo_hashes": repo_hashes,
        "live_hashes_before": before,
        "live_hashes_after": after,
        "previous_drift_status": drift_status(before["plugin"], before["runtime_sync"]),
        "current_drift_status": drift_status(after["plugin"], after["runtime_sync"]),
        "sync_requested": sync_requested,
        "sync_result": sync_result,
        "sync_state_path": str(live_paths["sync_state"])
        if live_paths["sync_state"].exists()
        else None,
    }


def run_psql_query(
    db_name: str, sql: str, env: dict[str, str] | None = None
) -> tuple[bool, str]:
    if shutil.which("psql") is None:
        return False, "psql not found"

    values = env or dict(os.environ)
    command = [
        "psql",
        "-d",
        db_name,
        "-v",
        "ON_ERROR_STOP=1",
        "-t",
        "-A",
        "-F",
        "|",
        "-c",
        sql,
    ]
    host = values.get("PGHOST", "").strip()
    port = values.get("PGPORT", "").strip()
    user = values.get("PGUSER", "").strip()
    if host:
        command[1:1] = ["-h", host]
    if port:
        command[1:1] = ["-p", port]
    if user:
        command[1:1] = ["-U", user]

    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        message = (
            result.stderr or result.stdout
        ).strip() or f"psql exited {result.returncode}"
        return False, message
    return True, result.stdout.strip()


def query_existing_tables(
    db_name: str,
    schema_name: str,
    table_names: list[str],
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    quoted = ", ".join(f"'{name}'" for name in table_names)
    sql = (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema_name}' AND table_name IN ({quoted}) ORDER BY table_name;"
    )
    ok, output = run_psql_query(db_name, sql, env=env)
    if not ok:
        return {
            "ok": False,
            "db": db_name,
            "schema": schema_name,
            "error": output,
            "tables": {name: False for name in table_names},
        }

    present = {line.strip() for line in output.splitlines() if line.strip()}
    return {
        "ok": True,
        "db": db_name,
        "schema": schema_name,
        "error": None,
        "tables": {name: name in present for name in table_names},
    }


def query_existing_routines(
    db_name: str,
    schema_name: str,
    routine_names: list[str],
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    quoted = ", ".join(f"'{name}'" for name in routine_names)
    sql = (
        "SELECT routine_name FROM information_schema.routines "
        f"WHERE routine_schema = '{schema_name}' AND routine_name IN ({quoted}) ORDER BY routine_name;"
    )
    ok, output = run_psql_query(db_name, sql, env=env)
    if not ok:
        return {
            "ok": False,
            "db": db_name,
            "schema": schema_name,
            "error": output,
            "routines": {name: False for name in routine_names},
        }

    present = {line.strip() for line in output.splitlines() if line.strip()}
    return {
        "ok": True,
        "db": db_name,
        "schema": schema_name,
        "error": None,
        "routines": {name: name in present for name in routine_names},
    }


def count_agent_memory_categories(
    db_name: str,
    categories: list[str],
    env: dict[str, str] | None = None,
) -> dict[str, int]:
    quoted = ", ".join(f"'{name}'" for name in categories)
    sql = (
        "SELECT category, COUNT(*) FROM agent_memories "
        f"WHERE category IN ({quoted}) AND deleted_at IS NULL GROUP BY category ORDER BY category;"
    )
    ok, output = run_psql_query(db_name, sql, env=env)
    if not ok:
        return {name: -1 for name in categories}

    counts = {name: 0 for name in categories}
    for line in output.splitlines():
        if not line.strip():
            continue
        category, count = line.split("|", 1)
        counts[category] = int(count)
    return counts


def present_names(values: dict[str, bool], names: list[str]) -> list[str]:
    return [name for name in names if values.get(name, False)]


def missing_names(values: dict[str, bool], names: list[str]) -> list[str]:
    return [name for name in names if not values.get(name, False)]


def build_capability_model(
    agent_memory_tables: dict[str, Any],
    agent_memory_functions: dict[str, Any],
    skill_system_tables: dict[str, Any],
    plugin_report: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    tables = agent_memory_tables["tables"]
    routines = agent_memory_functions["routines"]
    skill_tables = skill_system_tables["tables"]
    plugin_in_sync = (plugin_report or {}).get("current_drift_status") == "in_sync"

    def capability_entry(
        *,
        status: str,
        current_support_surface: str,
        gating_behavior: str,
        evidence: list[str],
    ) -> dict[str, Any]:
        return {
            "status": status,
            "current_support_surface": current_support_surface,
            "gating_behavior": gating_behavior,
            "evidence": evidence,
        }

    core_missing_tables = missing_names(tables, CORE_MEMORY_TABLES)
    core_missing_routines = missing_names(routines, CORE_MEMORY_ROUTINES)
    core_status = (
        SUPPORTED_NOW
        if not core_missing_tables and not core_missing_routines
        else DEFERRED_UNSUPPORTED
    )

    evolution_missing_tables = missing_names(tables, EVOLUTION_LEDGER_TABLES)
    evolution_missing_routines = missing_names(routines, EVOLUTION_LEDGER_ROUTINES)
    evolution_status = (
        SUPPORTED_NOW
        if not evolution_missing_tables and not evolution_missing_routines
        else DEFERRED_UNSUPPORTED
    )

    typed_context_missing_tables = missing_names(tables, TYPED_CONTEXT_TABLES)
    typed_context_missing_routines = missing_names(routines, TYPED_CONTEXT_ROUTINES)
    typed_context_status = (
        SUPPORTED_NOW
        if not typed_context_missing_tables and not typed_context_missing_routines
        else DEFERRED_UNSUPPORTED
    )

    projection_missing_tables = missing_names(tables, RUNTIME_SYNC_PROJECTION_TABLES)
    projection_status = (
        SUPPORTED_NOW if not projection_missing_tables else GATED_OPTIONAL
    )

    behavior_missing_tables = missing_names(tables, BEHAVIOR_GRAPH_TABLES)
    behavior_status = SUPPORTED_NOW if not behavior_missing_tables else GATED_OPTIONAL

    control_plane_missing_tables = missing_names(skill_tables, CONTROL_PLANE_TABLES)

    capability_model = {
        "core_memory": capability_entry(
            status=core_status,
            current_support_surface="Canonical `agent_memories` storage plus read/search/health functions",
            gating_behavior="Fail closed if canonical tables or routines are missing",
            evidence=[
                "tables present: "
                + ", ".join(present_names(tables, CORE_MEMORY_TABLES)),
                "routines present: "
                + ", ".join(present_names(routines, CORE_MEMORY_ROUTINES)),
            ],
        ),
        "evolution_ledger": capability_entry(
            status=evolution_status,
            current_support_surface="Typed `evolution_snapshots` ledger plus evolution read/write routines",
            gating_behavior="Fail closed if typed ledger table or routines are missing",
            evidence=[
                "tables present: "
                + ", ".join(present_names(tables, EVOLUTION_LEDGER_TABLES)),
                "routines present: "
                + ", ".join(present_names(routines, EVOLUTION_LEDGER_ROUTINES)),
            ],
        ),
        "compaction_logging": capability_entry(
            status=SUPPORTED_NOW
            if core_status == SUPPORTED_NOW and plugin_in_sync
            else GATED_OPTIONAL,
            current_support_surface="Plugin JSONL compaction events plus guarded `store_memory(...)` writeback into `agent_memories` when the repo plugin is synced",
            gating_behavior="Keep JSONL logging active and skip DB writes when target resolution or plugin sync is unsafe",
            evidence=[
                f"plugin drift status: {(plugin_report or {}).get('current_drift_status', '<unknown>')}",
                "uses core memory write path via `store_memory`",
            ],
        ),
        "typed_context_reads": capability_entry(
            status=typed_context_status,
            current_support_surface="Typed reads from soul, facet, and preference tables via `get_agent_context` and related getters",
            gating_behavior="Fail closed if typed context tables or read routines are missing",
            evidence=[
                "tables present: "
                + ", ".join(present_names(tables, TYPED_CONTEXT_TABLES)),
                "routines present: "
                + ", ".join(present_names(routines, TYPED_CONTEXT_ROUTINES)),
            ],
        ),
        "runtime_sync_projections": capability_entry(
            status=projection_status,
            current_support_surface="Optional session/project/context projection upserts from compaction summaries",
            gating_behavior=(
                "Skip runtime projection writes when missing required tables: "
                + ", ".join(projection_missing_tables)
                if projection_missing_tables
                else "Projection upserts may run because all projection tables are present"
            ),
            evidence=[
                "tables present: "
                + (
                    ", ".join(present_names(tables, RUNTIME_SYNC_PROJECTION_TABLES))
                    or "<none>"
                ),
                "tables missing: " + (", ".join(projection_missing_tables) or "<none>"),
            ],
        ),
        "behavior_refresh_graph": capability_entry(
            status=behavior_status,
            current_support_surface="Optional behavior projection refresh from the plugin into `behavior_*` tables",
            gating_behavior=(
                "Skip behavior refresh when missing required tables: "
                + ", ".join(behavior_missing_tables)
                if behavior_missing_tables
                else "Behavior refresh may run because all behavior graph tables are present"
            ),
            evidence=[
                "tables present: "
                + (", ".join(present_names(tables, BEHAVIOR_GRAPH_TABLES)) or "<none>"),
                "tables missing: " + (", ".join(behavior_missing_tables) or "<none>"),
            ],
        ),
        "control_plane_refresh": capability_entry(
            status=DEFERRED_UNSUPPORTED,
            current_support_surface="Deferred control-plane refresh orchestration; not part of the current user-facing memory runtime",
            gating_behavior="Do not attempt control-plane refresh writes from the current plugin/scripts runtime",
            evidence=[
                "tables present: "
                + (
                    ", ".join(present_names(skill_tables, CONTROL_PLANE_TABLES))
                    or "<none>"
                ),
                "tables missing: "
                + (", ".join(control_plane_missing_tables) or "<none>"),
            ],
        ),
    }
    return capability_model


def build_evolution_snapshots_status(
    agent_memory_tables: dict[str, Any],
    agent_memory_functions: dict[str, Any],
    evolution_store_report: dict[str, Any],
) -> dict[str, Any]:
    table_present = agent_memory_tables["tables"].get("evolution_snapshots", False)
    insert_present = agent_memory_functions["routines"].get(
        "insert_evolution_snapshot", False
    )
    read_present = agent_memory_functions["routines"].get(
        "get_evolution_history", False
    )
    canonical_tables_present = evolution_store_report["canonical_tables_present"]
    if table_present and insert_present and read_present and canonical_tables_present:
        return {
            "classification": LEGACY_COMPAT_SURFACE,
            "why": "Typed evolution_snapshots remains available for snapshot/version history, but accepted/rejected/task proposal truth now lives in dedicated canonical stores.",
            "evidence": [
                "table present: evolution_snapshots",
                "routines present: insert_evolution_snapshot, get_evolution_history",
                "canonical proposal stores present: evolution_nodes, evolution_rejections, evolution_tasks",
            ],
        }
    if table_present and insert_present and read_present:
        return {
            "classification": ACTIVE_CURRENT_SURFACE,
            "why": "Typed evolution ledger table and read/write routines are present in the canonical agent_memory runtime.",
            "evidence": [
                "table present: evolution_snapshots",
                "routines present: insert_evolution_snapshot, get_evolution_history",
            ],
        }
    if table_present:
        return {
            "classification": LEGACY_COMPAT_SURFACE,
            "why": "The typed evolution table still exists, but the current read/write routine surface is incomplete.",
            "evidence": [
                "table present: evolution_snapshots",
                "missing routines: "
                + ", ".join(
                    missing_names(
                        agent_memory_functions["routines"],
                        EVOLUTION_LEDGER_ROUTINES,
                    )
                ),
            ],
        }
    return {
        "classification": UNKNOWN,
        "why": "The typed evolution ledger could not be proven from the current runtime tables/functions.",
        "evidence": [
            "missing table: evolution_snapshots",
        ],
    }


def build_evolution_store_report(
    agent_memory_tables: dict[str, Any],
    category_counts: dict[str, int],
) -> dict[str, Any]:
    tables = agent_memory_tables["tables"]
    canonical_tables_present = all(
        tables.get(name, False) for name in EVOLUTION_CANONICAL_TABLES
    )
    accepted_store = (
        "evolution_nodes"
        if tables.get("evolution_nodes", False)
        else "agent_memories.category=evolution-node (legacy-only)"
    )
    rejected_store = (
        "evolution_rejections"
        if tables.get("evolution_rejections", False)
        else "agent_memories.category=evolution-rejected (legacy-only)"
    )
    task_store = (
        "evolution_tasks -> agent_tasks"
        if tables.get("evolution_tasks", False)
        else "<none>"
    )
    if (
        category_counts.get("evolution-node", 0) > 0
        or category_counts.get("evolution-rejected", 0) > 0
    ):
        agent_memories_role = "searchable summary/backref dual-write surface with explicit legacy compatibility rows"
    else:
        agent_memories_role = "searchable summary/backref dual-write surface"
    return {
        "canonical_accepted_store": accepted_store,
        "canonical_rejected_store": rejected_store,
        "canonical_task_store": task_store,
        "role_of_agent_memories": agent_memories_role,
        "legacy_category_counts": category_counts,
        "canonical_tables_present": canonical_tables_present,
    }


def build_write_integrity_report(repo_root: Path) -> dict[str, Any]:
    proposal_source = read_text(
        repo_root
        / "skills"
        / "skill-system-evolution"
        / "scripts"
        / "evolution_proposal.py"
    )
    migration_source = read_text(
        repo_root
        / "skills"
        / "skill-system-evolution"
        / "migrate-v2-durable-evolution.sql"
    )

    approve_transaction_safe = (
        "def run_transactional(" in proposal_source
        and "conn.commit()" in proposal_source
        and "conn.rollback()" in proposal_source
        and 'if args.cmd == "approve":' in proposal_source
        and "lambda: decide_accept_proposal(" in proposal_source
    )
    reject_transaction_safe = (
        "def run_transactional(" in proposal_source
        and "conn.commit()" in proposal_source
        and "conn.rollback()" in proposal_source
        and "lambda: decide_reject_proposal(" in proposal_source
    )
    migration_rerun_safe = all(
        token in migration_source
        for token in (
            "CREATE TABLE IF NOT EXISTS evolution_nodes",
            "CREATE TABLE IF NOT EXISTS evolution_rejections",
            "CREATE TABLE IF NOT EXISTS evolution_tasks",
            "ON CONFLICT (proposal_id) DO NOTHING",
            "DROP TRIGGER IF EXISTS trg_sync_evolution_task_from_agent_task",
        )
    )
    replay_tokens_present = all(
        token in proposal_source
        for token in (
            "def lock_proposal_decision(",
            "pg_advisory_xact_lock",
            "def fingerprint_semantic_identity(",
            '"REPLAYED_EXISTING"',
            '"PAYLOAD_MISMATCH"',
            '"TERMINAL_CONFLICT"',
        )
    )
    replay_schema_present = all(
        token in migration_source
        for token in (
            "ALTER TABLE evolution_nodes ADD COLUMN IF NOT EXISTS semantic_identity JSONB",
            "ALTER TABLE evolution_nodes ADD COLUMN IF NOT EXISTS semantic_fingerprint TEXT",
            "ALTER TABLE evolution_rejections ADD COLUMN IF NOT EXISTS semantic_identity JSONB",
            "ALTER TABLE evolution_rejections ADD COLUMN IF NOT EXISTS semantic_fingerprint TEXT",
        )
    )
    task_authority_clean = (
        'TASK_AUTHORITY_MODEL = "agent_tasks_is_lifecycle_authority"' in proposal_source
        and "FROM evolution_tasks et" in proposal_source
        and "JOIN agent_tasks t ON t.id = et.task_id" in proposal_source
        and "CREATE TRIGGER trg_sync_evolution_task_from_agent_task" in migration_source
        and "ON CONFLICT (source_node_id) DO UPDATE SET" in proposal_source
    )
    semantic_identity_fields = [
        "action",
        "kind",
        "summary",
        "rationale",
        "suggested_change",
        "evidence_refs",
        "requested_parent_node_id",
    ]

    return {
        "approve_path_status": (
            "TRANSACTION_SAFE" if approve_transaction_safe else "NOT_TRANSACTIONAL"
        ),
        "reject_path_status": (
            "TRANSACTION_SAFE" if reject_transaction_safe else "NOT_TRANSACTIONAL"
        ),
        "migration_backfill_idempotence": (
            "RERUN_SAFE" if migration_rerun_safe else "RERUN_RISK"
        ),
        "decision_replay_status": (
            "IDEMPOTENT_BY_PROPOSAL_ID"
            if replay_tokens_present and replay_schema_present
            else "REPLAY_GAPS_PRESENT"
        ),
        "decision_idempotency_key": (
            "proposal_id" if "proposal_id" in proposal_source else "<unknown>"
        ),
        "semantic_identity_fields": semantic_identity_fields,
        "coordination_mechanism": (
            "pg_advisory_xact_lock(proposal_id_hash) + semantic_fingerprint on canonical decision rows"
            if replay_tokens_present and replay_schema_present
            else "<unknown>"
        ),
        "conflicting_terminal_policy": (
            "BLOCKED_NO_MUTATION"
            if '"TERMINAL_CONFLICT"' in proposal_source
            else "UNPROVEN"
        ),
        "payload_mismatch_policy": (
            "BLOCKED_NO_MUTATION"
            if '"PAYLOAD_MISMATCH"' in proposal_source
            else "UNPROVEN"
        ),
        "task_authority_model": (
            "agent_tasks_is_lifecycle_authority"
            if task_authority_clean
            else "authority_boundary_unclear"
        ),
        "evolution_tasks_role": (
            "mapping_only" if task_authority_clean else "potential_dual_authority"
        ),
        "sync_direction": (
            "agent_tasks -> evolution_tasks"
            if "CREATE TRIGGER trg_sync_evolution_task_from_agent_task"
            in migration_source
            else "<unknown>"
        ),
        "evidence": [
            f"transaction_wrapper_present={'def run_transactional(' in proposal_source}",
            f"approve_uses_transaction_wrapper={'lambda: decide_accept_proposal(' in proposal_source}",
            f"reject_uses_transaction_wrapper={'lambda: decide_reject_proposal(' in proposal_source}",
            f"replay_lock_present={'pg_advisory_xact_lock' in proposal_source}",
            f"semantic_fingerprint_present={'def fingerprint_semantic_identity(' in proposal_source}",
            f"task_reads_join_agent_tasks={'JOIN agent_tasks t ON t.id = et.task_id' in proposal_source}",
            f"migration_rerun_guards={migration_rerun_safe}",
        ],
    }


def collect_missing_references(
    agent_memory_tables: dict[str, Any],
    skill_system_tables: dict[str, Any],
    omo: dict[str, Any],
) -> list[str]:
    mismatches: list[str] = []
    if (
        REPO_SKILL_MD.exists()
        and MISSING_MIGRATION_REFERENCE in REPO_SKILL_MD.read_text(encoding="utf-8")
    ):
        if not (
            ROOT_DIR / "skills" / "skill-system-memory" / MISSING_MIGRATION_REFERENCE
        ).exists():
            mismatches.append(
                "skills/skill-system-memory/SKILL.md references missing migrate-typed-tables.sql"
            )

    projection_tables = agent_memory_tables["tables"]
    missing_projection_tables = [
        name
        for name in RUNTIME_SYNC_PROJECTION_TABLES + BEHAVIOR_GRAPH_TABLES
        if not projection_tables.get(name, False)
    ]
    if missing_projection_tables:
        mismatches.append(
            "repo runtime_sync expects tables missing from agent_memory: "
            + ", ".join(missing_projection_tables)
        )

    skill_system_missing = [
        name
        for name in ("refresh_jobs", "refresh_job_events", "artifact_versions")
        if not skill_system_tables["tables"].get(name, False)
    ]
    if skill_system_missing:
        mismatches.append(
            "skill_system control-plane tables missing: "
            + ", ".join(skill_system_missing)
        )

    if omo["enabled_in_config"] and not omo["actual_resolution_path"]:
        mismatches.append(
            "oh-my-opencode enabled in config but actual cache-backed resolution path is still unproven"
        )

    return mismatches


def build_report(
    home_dir: Path | None = None,
    repo_root: Path = ROOT_DIR,
    env: dict[str, str] | None = None,
    sync_live_plugin_requested: bool = False,
) -> dict[str, Any]:
    actual_home = home_dir or Path.home()
    values = env or dict(os.environ)
    opencode_config_paths = {
        "global": str(actual_home / ".config" / "opencode" / "opencode.json"),
        "project": str(repo_root / "opencode.json"),
        "global_omo": str(actual_home / ".config" / "opencode" / "oh-my-opencode.json"),
        "project_omo": str(repo_root / ".opencode" / "oh-my-opencode.json"),
    }
    plugin_report = build_plugin_report(
        actual_home, repo_root, sync_live_plugin_requested
    )
    omo = detect_omo_runtime(actual_home)
    memory_target = resolve_memory_target(values)
    agent_memory_tables = query_existing_tables(
        CANONICAL_MEMORY_TARGET,
        "public",
        AGENT_MEMORY_TABLES,
        env=values,
    )
    agent_memory_routines = query_existing_routines(
        CANONICAL_MEMORY_TARGET,
        "public",
        AGENT_MEMORY_ROUTINES,
        env=values,
    )
    skill_system_tables = query_existing_tables(
        "skill_system",
        "skill_system",
        CONTROL_PLANE_TABLES,
        env=values,
    )
    category_counts = count_agent_memory_categories(
        CANONICAL_MEMORY_TARGET,
        ["evolution-node", "evolution-rejected", "evolution-snapshot"],
        env=values,
    )
    capability_model = build_capability_model(
        agent_memory_tables,
        agent_memory_routines,
        skill_system_tables,
        plugin_report=plugin_report,
    )
    evolution_store_report = build_evolution_store_report(
        agent_memory_tables,
        category_counts,
    )
    write_integrity_report = build_write_integrity_report(repo_root)
    evolution_snapshots_status = build_evolution_snapshots_status(
        agent_memory_tables,
        agent_memory_routines,
        evolution_store_report,
    )
    missing_references = collect_missing_references(
        agent_memory_tables, skill_system_tables, omo
    )

    status = "ok"
    if (
        plugin_report["current_drift_status"] != "in_sync"
        or not memory_target["aligned"]
    ):
        status = "warn"
    if not agent_memory_tables["ok"] or not agent_memory_routines["ok"]:
        status = "error"

    return {
        "status": status,
        "checked_at_utc_minute": utc_minute_now(),
        "repo_root": str(repo_root),
        "active_opencode_config_paths": opencode_config_paths,
        "active_plugin_paths": {
            "live_plugin_path": plugin_report["live_plugin_path"],
            "live_runtime_sync_path": plugin_report["live_runtime_sync_path"],
            "sync_state_path": plugin_report["sync_state_path"],
        },
        "omo": omo,
        "memory_target": memory_target,
        "plugin_source_of_truth": plugin_report,
        "capability_model": capability_model,
        "evolution_store_report": evolution_store_report,
        "write_integrity_report": write_integrity_report,
        "evolution_snapshots_status": evolution_snapshots_status,
        "table_readiness": {
            "agent_memory": agent_memory_tables,
            "skill_system": skill_system_tables,
        },
        "routine_readiness": {
            "agent_memory": agent_memory_routines,
        },
        "missing_reference_mismatches": missing_references,
        "env_snapshot": {
            "PGHOST": values.get("PGHOST") or None,
            "PGPORT": values.get("PGPORT") or None,
            "PGUSER": values.get("PGUSER") or None,
            "PGDATABASE": values.get("PGDATABASE") or None,
            "SKILL_PGDATABASE": values.get("SKILL_PGDATABASE") or None,
        },
    }


def format_text(report: dict[str, Any]) -> str:
    lines = [
        f"status={report['status']}",
        f"checked_at_utc_minute={report['checked_at_utc_minute']}",
        f"canonical_memory_target={report['memory_target']['canonical_target']}",
        f"resolved_memory_target={report['memory_target']['target']}",
        f"memory_target_source={report['memory_target']['source']}",
        f"silent_redirection_status={report['memory_target']['silent_redirection_status']}",
        f"omo_enabled_in_config={report['omo']['enabled_in_config']}",
        f"omo_actual_resolution_path={report['omo']['actual_resolution_path'] or '<unknown>'}",
        f"plugin_previous_drift_status={report['plugin_source_of_truth']['previous_drift_status']}",
        f"plugin_current_drift_status={report['plugin_source_of_truth']['current_drift_status']}",
        f"live_plugin_path={report['plugin_source_of_truth']['live_plugin_path']}",
        f"live_runtime_sync_path={report['plugin_source_of_truth']['live_runtime_sync_path']}",
        f"repo_plugin_path={report['plugin_source_of_truth']['repo_plugin_path']}",
        f"repo_runtime_sync_path={report['plugin_source_of_truth']['repo_runtime_sync_path']}",
        f"evolution_snapshots_classification={report['evolution_snapshots_status']['classification']}",
        f"evolution_accepted_store={report['evolution_store_report']['canonical_accepted_store']}",
        f"evolution_rejected_store={report['evolution_store_report']['canonical_rejected_store']}",
        f"evolution_task_store={report['evolution_store_report']['canonical_task_store']}",
        f"agent_memories_role={report['evolution_store_report']['role_of_agent_memories']}",
        f"approve_path_status={report['write_integrity_report']['approve_path_status']}",
        f"reject_path_status={report['write_integrity_report']['reject_path_status']}",
        f"migration_backfill_idempotence={report['write_integrity_report']['migration_backfill_idempotence']}",
        f"decision_replay_status={report['write_integrity_report']['decision_replay_status']}",
        f"decision_idempotency_key={report['write_integrity_report']['decision_idempotency_key']}",
        "semantic_identity_fields="
        + ",".join(report["write_integrity_report"]["semantic_identity_fields"]),
        f"coordination_mechanism={report['write_integrity_report']['coordination_mechanism']}",
        f"conflicting_terminal_policy={report['write_integrity_report']['conflicting_terminal_policy']}",
        f"payload_mismatch_policy={report['write_integrity_report']['payload_mismatch_policy']}",
        f"task_authority_model={report['write_integrity_report']['task_authority_model']}",
        f"evolution_tasks_role={report['write_integrity_report']['evolution_tasks_role']}",
        f"task_sync_direction={report['write_integrity_report']['sync_direction']}",
        "capability_model:",
    ]
    for capability, details in report["capability_model"].items():
        lines.append(
            "  - "
            + f"{capability}={details['status']}"
            + f" | surface={details['current_support_surface']}"
            + f" | gating={details['gating_behavior']}"
        )
    lines.extend(
        [
            "agent_memory_tables:",
        ]
    )
    for name, present in report["table_readiness"]["agent_memory"]["tables"].items():
        lines.append(f"  - {name}={present}")
    lines.append("agent_memory_routines:")
    for name, present in report["routine_readiness"]["agent_memory"][
        "routines"
    ].items():
        lines.append(f"  - {name}={present}")
    lines.append("skill_system_tables:")
    for name, present in report["table_readiness"]["skill_system"]["tables"].items():
        lines.append(f"  - {name}={present}")
    lines.append("legacy_evolution_category_counts:")
    for name, count in report["evolution_store_report"][
        "legacy_category_counts"
    ].items():
        lines.append(f"  - {name}={count}")
    lines.append("missing_reference_mismatches:")
    if report["missing_reference_mismatches"]:
        lines.extend(f"  - {item}" for item in report["missing_reference_mismatches"])
    else:
        lines.append("  - <none>")
    return "\n".join(lines)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect and optionally converge the live memory runtime surface."
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--sync-live-plugin", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    report = build_report(sync_live_plugin_requested=args.sync_live_plugin)
    if args.format == "json":
        print(json.dumps(report))
    else:
        print(format_text(report))
    return 0 if report["status"] != "error" else 1


if __name__ == "__main__":
    raise SystemExit(main())
