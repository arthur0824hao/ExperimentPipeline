#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL-backed experiment registry.

Replaces:
  - experiments.json read/write via registry_io.py
  - heartbeats/*.json
  - locks/*.lock

All status transitions are atomic DB operations with fencing tokens.
No more NFS race conditions, no more false stale resets.

Usage:
    from db_registry import DBExperimentsDB
    db = DBExperimentsDB()          # uses default DSN from env / .pgpass
    db = DBExperimentsDB(dsn="...")  # explicit DSN
"""

import csv
import json
import os
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

import psycopg2
import psycopg2.extras
import psycopg2.pool

from machine_constraints import (
    filter_worker_heartbeats,
    get_worker_heartbeat as _get_worker_heartbeat,
    load_machine_constraints,
    load_worker_whitelist,
)

# ---------------------------------------------------------------------------
# Connection pool (module-level singleton)
# ---------------------------------------------------------------------------

_pool_lock = threading.Lock()
_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

# Status constants (same as experiments.py)
STATUS_NEEDS_RERUN = "NEEDS_RERUN"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"

PROGRESSION_READY = "READY"
PROGRESSION_RUNNING = "RUNNING"
PROGRESSION_COMPLETED = "COMPLETED"
PROGRESSION_BLOCKED_CONDITION = "BLOCKED_CONDITION"
PROGRESSION_WARM = "WARM"

# Default stale threshold
DEFAULT_STALE_SEC = 120
MAX_RETRY_COUNT = 2
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIGS_DIR = PROJECT_ROOT / "configs"
DATABASE_CONFIG_FILE = CONFIGS_DIR / "database.json"
MACHINES_FILES = [
    CONFIGS_DIR / "machines.json",
    CONFIGS_DIR / "machines.phase3.json",
]


def _load_experiment_db_defaults() -> Dict[str, str]:
    defaults: Dict[str, str] = {
        "host": "localhost",
        "port": "5432",
        "dbname": "ExperimentPipeline-experiment",
        "user": os.environ.get("USER", "arthur0824hao"),
        "connect_timeout": "3",
    }
    try:
        cfg = json.loads(DATABASE_CONFIG_FILE.read_text(encoding="utf-8"))
        exp_cfg = cfg.get("experiment") if isinstance(cfg, dict) else None
        if isinstance(exp_cfg, dict):
            host = str(exp_cfg.get("host", "") or "").strip()
            port = str(exp_cfg.get("port", "") or "").strip()
            dbname = str(exp_cfg.get("dbname", "") or "").strip()
            user = str(exp_cfg.get("user", "") or "").strip()
            connect_timeout = str(exp_cfg.get("connect_timeout", "") or "").strip()
            if host:
                defaults["host"] = host
            if port:
                defaults["port"] = port
            if dbname:
                defaults["dbname"] = dbname
            if user:
                defaults["user"] = user
            if connect_timeout:
                defaults["connect_timeout"] = connect_timeout
    except Exception:
        pass
    return defaults


def _pick_frame_swap_index(
    ids: List[Tuple[int, int, Optional[str]]], idx: int, direction: str
) -> Optional[int]:
    if idx < 0 or idx >= len(ids):
        return None
    current_parent = ids[idx][2]
    frame_indices = [i for i, (_, _, p) in enumerate(ids) if p == current_parent]
    if idx not in frame_indices:
        return None
    frame_pos = frame_indices.index(idx)
    if direction == "up" and frame_pos > 0:
        return frame_indices[frame_pos - 1]
    if direction == "down" and frame_pos < len(frame_indices) - 1:
        return frame_indices[frame_pos + 1]
    return None


def _get_dsn() -> str:
    """Build DSN from environment, falling back to defaults matching ~/.pgpass."""
    defaults = _load_experiment_db_defaults()
    host = (
        os.environ.get("EXP_PGHOST", "").strip()
        or os.environ.get("PGHOST", "").strip()
        or defaults["host"]
    )
    port = (
        os.environ.get("EXP_PGPORT", "").strip()
        or os.environ.get("PGPORT", "").strip()
        or defaults["port"]
    )
    dbname = os.environ.get("EXP_PGDATABASE", "").strip() or defaults["dbname"]
    user = (
        os.environ.get("EXP_PGUSER", "").strip()
        or os.environ.get("PGUSER", "").strip()
        or defaults["user"]
    )
    connect_timeout = (
        os.environ.get("EXP_PGCONNECT_TIMEOUT", "").strip()
        or defaults["connect_timeout"]
    )
    return (
        f"host={host} port={port} dbname={dbname} user={user} "
        f"connect_timeout={connect_timeout}"
    )


def _normalize_registry_status(raw_status: Any) -> str:
    status = str(raw_status or "").upper()
    if status in (STATUS_NEEDS_RERUN, STATUS_RUNNING, STATUS_COMPLETED):
        return status
    if status in ("DONE", "SKIPPED"):
        return STATUS_COMPLETED
    if status in ("READY", "ERROR", "OOM"):
        return STATUS_NEEDS_RERUN
    return STATUS_NEEDS_RERUN


def _clean_optional_text(raw: Any) -> Optional[str]:
    text = str(raw or "").strip()
    return text or None


def derive_progression_status(
    raw_status: Any,
    *,
    condition_parent: Optional[str] = None,
    condition_parent_status: Any = None,
    warmup_hint: bool = False,
) -> Tuple[str, Optional[str]]:
    status = _normalize_registry_status(raw_status)
    parent = _clean_optional_text(condition_parent)

    if status == STATUS_COMPLETED:
        return PROGRESSION_COMPLETED, None
    if status == STATUS_RUNNING:
        if warmup_hint:
            return PROGRESSION_WARM, None
        return PROGRESSION_RUNNING, None

    if parent:
        parent_status = _normalize_registry_status(condition_parent_status)
        if parent_status != STATUS_COMPLETED:
            return (
                PROGRESSION_BLOCKED_CONDITION,
                f"condition_parent_unmet:{parent}",
            )
    return PROGRESSION_READY, None


def enrich_progression_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(snapshot, dict):
        return snapshot

    all_rows: List[Dict[str, Any]] = []
    for bucket_name in ("experiments", "completed", "archived"):
        bucket = snapshot.get(bucket_name)
        if isinstance(bucket, list):
            all_rows.extend(item for item in bucket if isinstance(item, dict))

    status_lookup: Dict[str, str] = {}
    for row in all_rows:
        name = _clean_optional_text(row.get("name"))
        if not name:
            continue
        normalized = _normalize_registry_status(row.get("status"))
        prev = status_lookup.get(name)
        if prev == STATUS_COMPLETED:
            continue
        if normalized == STATUS_COMPLETED or prev is None:
            status_lookup[name] = normalized

    for row in all_rows:
        condition_parent = _clean_optional_text(row.get("condition_parent"))
        parent_status = (
            status_lookup.get(condition_parent) if condition_parent else None
        )
        progression_status, block_reason = derive_progression_status(
            row.get("status"),
            condition_parent=condition_parent,
            condition_parent_status=parent_status,
            warmup_hint=False,
        )
        row["progression_status"] = progression_status
        if block_reason:
            row["block_reason"] = block_reason
            row["progression_block_reason"] = block_reason
        else:
            row.pop("block_reason", None)
            row.pop("progression_block_reason", None)

    return snapshot


def _get_dsn_candidates() -> List[str]:
    defaults = _load_experiment_db_defaults()
    port = os.environ.get("EXP_PGPORT", os.environ.get("PGPORT", defaults["port"]))
    dbname = os.environ.get("EXP_PGDATABASE", defaults["dbname"])
    user = os.environ.get("EXP_PGUSER", os.environ.get("PGUSER", defaults["user"]))
    connect_timeout = os.environ.get(
        "EXP_PGCONNECT_TIMEOUT", defaults["connect_timeout"]
    )
    env_host = os.environ.get("EXP_PGHOST", os.environ.get("PGHOST", "")).strip()
    cfg_host = defaults["host"]  # from database.json (e.g. 192.168.1.4)

    local_aliases = set()
    for value in [
        socket.gethostname(),
        socket.getfqdn(),
        os.environ.get("HOSTNAME", ""),
        os.environ.get("COMPUTERNAME", ""),
    ]:
        token = str(value or "").strip().lower()
        if not token:
            continue
        local_aliases.add(token)
        local_aliases.add(token.split(".")[0])

    tunnel_port: Optional[str] = None
    try:
        machine_constraints = _load_machine_constraints()
        for worker_id, conf in machine_constraints.items():
            if not isinstance(conf, dict):
                continue
            raw_tunnel = conf.get("db_tunnel_port")
            if raw_tunnel in (None, ""):
                continue
            candidates = {
                str(worker_id or "").strip().lower(),
                str(conf.get("host") or "").strip().lower(),
            }
            normalized_candidates = set()
            for item in candidates:
                if not item:
                    continue
                normalized_candidates.add(item)
                normalized_candidates.add(item.split(".")[0])
            if local_aliases & normalized_candidates:
                tunnel_port = str(raw_tunnel).strip()
                break
    except Exception:
        tunnel_port = None

    host_port_pairs: List[Tuple[str, str]] = []
    if env_host:
        host_port_pairs.append((env_host, str(port)))
    if tunnel_port:
        host_port_pairs.append(("localhost", tunnel_port))
    if cfg_host:
        host_port_pairs.append((cfg_host, str(port)))
    host_port_pairs.extend([("localhost", str(port)), ("", str(port))])

    deduped_host_port_pairs: List[Tuple[str, str]] = []
    for pair in host_port_pairs:
        if pair not in deduped_host_port_pairs:
            deduped_host_port_pairs.append(pair)

    candidates: List[str] = []
    for host, candidate_port in deduped_host_port_pairs:
        prefix = f"host={host} " if host else ""
        candidates.append(
            f"{prefix}port={candidate_port} dbname={dbname} user={user} connect_timeout={connect_timeout}"
        )
    return candidates


def _load_machine_constraints() -> Dict[str, Dict[str, Any]]:
    return load_machine_constraints(MACHINES_FILES)


def _load_worker_whitelist() -> List[str]:
    return load_worker_whitelist(MACHINES_FILES)


def _max_allowed_gpu_total_mb(
    worker_id: str,
    heartbeat_gpu_info: List[Dict[str, Any]],
    machine_constraints: Optional[Dict[str, Dict[str, Any]]] = None,
) -> int:
    constraints = machine_constraints or _load_machine_constraints()
    worker_constraints = constraints.get(worker_id, {})
    gpus = [g for g in heartbeat_gpu_info if isinstance(g, dict)]
    preferred_gpu = worker_constraints.get("preferred_gpu")
    max_gpus = worker_constraints.get("max_gpus")
    if preferred_gpu is not None:
        gpus = [g for g in gpus if int(g.get("index", -1)) == int(preferred_gpu)]
    elif max_gpus is not None:
        gpus = [g for g in gpus if int(g.get("index", 9999)) < int(max_gpus)]
    totals = [int(g.get("total", 0) or 0) for g in gpus]
    return max(totals, default=0)


def _preferred_worker_fallback_allowed(
    preferred_worker: Optional[str],
    claiming_worker: Optional[str],
    est_mem_mb: int,
    preferred_worker_max_mb: int,
) -> bool:
    if (
        not preferred_worker
        or not claiming_worker
        or preferred_worker == claiming_worker
    ):
        return True
    if est_mem_mb <= 0 or preferred_worker_max_mb <= 0:
        return False
    return est_mem_mb > preferred_worker_max_mb


def get_pool(
    dsn: Optional[str] = None, minconn: int = 1, maxconn: int = 5
) -> psycopg2.pool.ThreadedConnectionPool:
    """Get or create the module-level connection pool."""
    global _pool
    with _pool_lock:
        if _pool is None or _pool.closed:
            candidates = [dsn] if dsn else _get_dsn_candidates()
            last_exc: Optional[Exception] = None
            for candidate in candidates:
                if not candidate:
                    continue
                candidate_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
                try:
                    candidate_pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn, maxconn, candidate
                    )
                    probe_conn = candidate_pool.getconn()
                    try:
                        with probe_conn.cursor() as cur:
                            cur.execute("SELECT to_regnamespace('exp_registry')")
                            row = cur.fetchone()
                            if not row or row[0] is None:
                                raise RuntimeError("exp_registry schema not found")
                    finally:
                        candidate_pool.putconn(probe_conn)
                    _pool = candidate_pool
                    break
                except Exception as e:
                    last_exc = e
                    if candidate_pool is not None:
                        try:
                            candidate_pool.closeall()
                        except Exception:
                            pass
                    _pool = None
            if _pool is None:
                if last_exc is not None:
                    raise last_exc
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn, maxconn, _get_dsn()
                )
        return _pool


def close_pool():
    """Shutdown the connection pool."""
    global _pool
    with _pool_lock:
        if _pool is not None and not _pool.closed:
            _pool.closeall()
            _pool = None


@contextmanager
def get_conn(dsn: Optional[str] = None):
    """Get a connection from the pool with auto-commit management."""
    max_attempts = 2
    pool = None
    conn = None
    last_exc: Optional[Exception] = None

    for _ in range(max_attempts):
        pool = get_pool(dsn)
        candidate = pool.getconn()
        try:
            if isinstance(candidate, psycopg2.extensions.connection):
                with candidate.cursor() as probe_cur:
                    probe_cur.execute("SELECT 1")
            conn = candidate
            break
        except Exception as e:
            last_exc = e
            try:
                pool.putconn(candidate, close=True)
            except Exception:
                pass
            close_pool()

    if pool is None or conn is None:
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Failed to acquire PostgreSQL connection")

    try:
        yield conn
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        if isinstance(e, psycopg2.Error):
            try:
                pool.putconn(conn, close=True)
            except Exception:
                pass
            close_pool()
            conn = None
        raise
    finally:
        if conn is not None:
            pool.putconn(conn)


# ---------------------------------------------------------------------------
# Snapshot sync: dump DB state to experiments.json for backward compat
# ---------------------------------------------------------------------------


def sync_snapshot_to_json(json_path: Path, dsn: Optional[str] = None):
    """Dump current DB state to experiments.json (best-effort, non-blocking)."""
    try:
        with get_conn(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT exp_registry.snapshot_as_json()")
                row = cur.fetchone()
                if not row:
                    return
                data = row[0]
                # data is already a dict from psycopg2's json handling
                if isinstance(data, str):
                    data = json.loads(data)
                if not isinstance(data, dict):
                    return
                data = enrich_progression_snapshot(data)

                next_experiments = data.get("experiments")
                next_archived = data.get("archived")
                if not isinstance(next_experiments, list) or not isinstance(
                    next_archived, list
                ):
                    return

                if not next_experiments and not next_archived and json_path.exists():
                    try:
                        prev = json.loads(json_path.read_text(encoding="utf-8"))
                        prev_experiments = prev.get("experiments")
                        prev_archived = prev.get("archived")
                        if (
                            isinstance(prev_experiments, list)
                            and isinstance(prev_archived, list)
                            and (prev_experiments or prev_archived)
                        ):
                            return
                    except Exception:
                        pass

        # Atomic write
        import tempfile

        fd, tmp_path = tempfile.mkstemp(
            dir=str(json_path.parent),
            prefix=f".{json_path.name}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            os.replace(tmp_path, json_path)
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except Exception:
                pass
    except Exception:
        pass  # Best-effort; never crash the runner for snapshot sync


# ---------------------------------------------------------------------------
# DBExperimentsDB: Drop-in replacement for ExperimentsDB
# ---------------------------------------------------------------------------


class DBExperimentsDB:
    """PostgreSQL-backed experiment registry.

    Provides the same interface as the original ExperimentsDB but backed by
    PostgreSQL with atomic operations and fencing tokens.
    """

    def __init__(self, dsn: Optional[str] = None, json_path: Optional[Path] = None):
        if json_path is None and isinstance(dsn, Path):
            json_path = dsn
            dsn = None
        self.dsn = dsn
        # For backward compat: path to experiments.json for snapshot sync
        self.json_path = json_path
        # Store run_ids for fencing (experiment_name -> run_id)
        self._run_ids: Dict[str, str] = {}
        self._run_ids_lock = threading.Lock()
        self._last_heartbeat_error = ""

    def _sync_snapshot(self):
        """Best-effort sync DB state to experiments.json."""
        if self.json_path:
            try:
                data = self.load()
                if not data.get("experiments") and not data.get("archived"):
                    if self.json_path.exists():
                        existing = json.loads(
                            self.json_path.read_text(encoding="utf-8")
                        )
                        if existing.get("experiments") or existing.get("archived"):
                            return
            except Exception:
                pass
            sync_snapshot_to_json(self.json_path, self.dsn)

    def get_connection_health(self) -> Dict[str, Any]:
        attempts: List[Dict[str, Any]] = []
        dsn_candidates = [self.dsn] if self.dsn else _get_dsn_candidates()
        started = time.monotonic()
        for candidate in dsn_candidates:
            if not candidate:
                continue
            conn = None
            attempt_started = time.monotonic()
            try:
                conn = psycopg2.connect(candidate)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1, to_regnamespace('exp_registry') IS NOT NULL"
                    )
                    row = cur.fetchone() or (0, False)
                healthy = bool(row[0] == 1)
                schema_ready = bool(row[1])
                latency_ms = int((time.monotonic() - attempt_started) * 1000)
                attempts.append(
                    {
                        "dsn": candidate,
                        "ok": healthy,
                        "schema_ready": schema_ready,
                        "latency_ms": latency_ms,
                        "error": "",
                    }
                )
                if healthy:
                    return {
                        "ok": True,
                        "schema_ready": schema_ready,
                        "active_dsn": candidate,
                        "duration_ms": int((time.monotonic() - started) * 1000),
                        "attempts": attempts,
                    }
            except Exception as exc:
                attempts.append(
                    {
                        "dsn": candidate,
                        "ok": False,
                        "schema_ready": False,
                        "latency_ms": int((time.monotonic() - attempt_started) * 1000),
                        "error": str(exc),
                    }
                )
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        return {
            "ok": False,
            "schema_ready": False,
            "active_dsn": "",
            "duration_ms": int((time.monotonic() - started) * 1000),
            "attempts": attempts,
        }

    def _ensure_runtime_settings_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exp_registry.runtime_settings (
                key text PRIMARY KEY,
                value_json jsonb NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT NOW()
            )
            """
        )

    def _ensure_buddy_reports_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exp_registry.buddy_reports (
                id BIGSERIAL PRIMARY KEY,
                reporter_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                target_process_alive BOOLEAN NOT NULL,
                target_db_reachable BOOLEAN NOT NULL,
                target_gpu_ok BOOLEAN NOT NULL,
                checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_buddy_reports_target_checked
            ON exp_registry.buddy_reports(target_id, checked_at DESC)
            """
        )

    def record_buddy_report(
        self,
        reporter_id: str,
        target_id: str,
        target_process_alive: bool,
        target_db_reachable: bool,
        target_gpu_ok: bool,
    ) -> bool:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    self._ensure_buddy_reports_table(cur)
                    cur.execute(
                        """
                        INSERT INTO exp_registry.buddy_reports(
                            reporter_id,
                            target_id,
                            target_process_alive,
                            target_db_reachable,
                            target_gpu_ok,
                            checked_at
                        )
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            str(reporter_id),
                            str(target_id),
                            bool(target_process_alive),
                            bool(target_db_reachable),
                            bool(target_gpu_ok),
                        ),
                    )
            return True
        except Exception as e:
            print(f"[DBExperimentsDB] record_buddy_report error: {e}")
            return False

    def get_latest_buddy_reports(self, ttl_sec: int = 90) -> Dict[str, Dict[str, Any]]:
        safe_ttl = max(1, int(ttl_sec or 90))
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    self._ensure_buddy_reports_table(cur)
                    cur.execute(
                        """
                        SELECT DISTINCT ON (target_id)
                            target_id,
                            reporter_id,
                            target_process_alive,
                            target_db_reachable,
                            target_gpu_ok,
                            checked_at,
                            EXTRACT(EPOCH FROM (NOW() - checked_at)) AS age_sec
                        FROM exp_registry.buddy_reports
                        WHERE checked_at >= NOW() - (%s * INTERVAL '1 second')
                        ORDER BY target_id, checked_at DESC
                        """,
                        (safe_ttl,),
                    )
                    rows = cur.fetchall() or []
            result: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                target_id = str(row.get("target_id") or "").strip()
                if not target_id:
                    continue
                result[target_id] = {
                    "target_id": target_id,
                    "reporter_id": str(row.get("reporter_id") or "").strip(),
                    "target_process_alive": bool(row.get("target_process_alive")),
                    "target_db_reachable": bool(row.get("target_db_reachable")),
                    "target_gpu_ok": bool(row.get("target_gpu_ok")),
                    "checked_at": row.get("checked_at"),
                    "age_sec": float(row.get("age_sec") or 999999),
                }
            return result
        except Exception as e:
            print(f"[DBExperimentsDB] get_latest_buddy_reports error: {e}")
            return {}

    def get_allocation_strategy(self, default: str = "distributed") -> str:
        allowed = {
            "distributed",
            "centralized",
            "round-robin",
            "fill-first",
            "manual",
        }
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    self._ensure_runtime_settings_table(cur)
                    cur.execute(
                        "SELECT value_json->>'value' FROM exp_registry.runtime_settings WHERE key = %s",
                        ("allocation_strategy",),
                    )
                    row = cur.fetchone()
                    value = str(row[0]).strip().lower() if row and row[0] else ""
                    if value in allowed:
                        return value
                    return default
        except Exception:
            return default

    def set_allocation_strategy(self, strategy: str) -> bool:
        value = str(strategy or "").strip().lower()
        if value not in {
            "distributed",
            "centralized",
            "round-robin",
            "fill-first",
            "manual",
        }:
            return False
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    self._ensure_runtime_settings_table(cur)
                    cur.execute(
                        """
                        INSERT INTO exp_registry.runtime_settings(key, value_json, updated_at)
                        VALUES (%s, %s::jsonb, NOW())
                        ON CONFLICT (key)
                        DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = NOW()
                        """,
                        ("allocation_strategy", json.dumps({"value": value})),
                    )
                    conn.commit()
            return True
        except Exception:
            return False

    # --- Run ID management (fencing tokens) ---

    def set_run_id(self, exp_name: str, run_id: str):
        with self._run_ids_lock:
            self._run_ids[exp_name] = run_id

    def get_run_id(self, exp_name: str) -> Optional[str]:
        with self._run_ids_lock:
            return self._run_ids.get(exp_name)

    def clear_run_id(self, exp_name: str):
        with self._run_ids_lock:
            self._run_ids.pop(exp_name, None)

    # --- Core read operations ---

    def load(self) -> Dict:
        """Load all experiments as a dict matching the old JSON format."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT exp_registry.snapshot_as_json()")
                    row = cur.fetchone()
                    if not row:
                        return {"experiments": [], "archived": []}
                    data = row[0]
                    if isinstance(data, str):
                        data = json.loads(data)
                    if isinstance(data, dict):
                        data = enrich_progression_snapshot(data)
                        experiments = data.get("experiments", [])
                        if not isinstance(experiments, list):
                            experiments = []
                        completed = data.get("completed")
                        if not isinstance(completed, list):
                            completed = [
                                e
                                for e in experiments
                                if str(e.get("status", "")).upper()
                                in ("DONE", "COMPLETED")
                            ]
                        active_experiments = [
                            e
                            for e in experiments
                            if str(e.get("status", "")).upper()
                            not in ("DONE", "COMPLETED")
                        ]
                        data["experiments"] = active_experiments
                        data["completed"] = completed
                    return data
        except Exception as e:
            if self.json_path and self.json_path.exists():
                try:
                    data = json.loads(self.json_path.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        experiments = data.get("experiments")
                        archived = data.get("archived")
                        completed = data.get("completed")
                        if isinstance(experiments, list) and isinstance(archived, list):
                            if not isinstance(completed, list):
                                completed = [
                                    exp
                                    for exp in experiments
                                    if str(exp.get("status", "")).upper()
                                    in ("DONE", "COMPLETED")
                                ]
                                data["experiments"] = [
                                    exp
                                    for exp in experiments
                                    if str(exp.get("status", "")).upper()
                                    not in ("DONE", "COMPLETED")
                                ]
                                data["completed"] = completed
                            print(
                                f"[DBExperimentsDB] DB unreachable, using JSON snapshot: {e}"
                            )
                            return data
                except Exception:
                    pass
            print(f"[DBExperimentsDB] load error: {e}")
            return {"experiments": [], "archived": []}

    def load_all_for_panel(self) -> List[Dict[str, Any]]:
        """Load ALL experiments from DB for panel display, including unclaimed queue entries."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    has_claimed_at = True
                    try:
                        cur.execute(
                            "SELECT name, status, preferred_worker, extra, display_order, "
                            "parent_experiment, peak_memory_mb, started_at, claimed_at "
                            "FROM exp_registry.experiments "
                            "ORDER BY display_order NULLS LAST, name"
                        )
                    except Exception as e:
                        if "claimed_at" not in str(e).lower():
                            raise
                        has_claimed_at = False
                        conn.rollback()
                        cur.execute(
                            "SELECT name, status, preferred_worker, extra, display_order, "
                            "parent_experiment, peak_memory_mb, started_at "
                            "FROM exp_registry.experiments "
                            "ORDER BY display_order NULLS LAST, name"
                        )
                    rows = cur.fetchall()
                    result = []
                    for row in rows:
                        exp = {"name": row[0], "status": row[1] or "NEEDS_RERUN"}
                        if row[2]:
                            exp["preferred_worker"] = row[2]
                        if row[3] and isinstance(row[3], dict):
                            exp.update(row[3])
                        if row[4] is not None:
                            exp["display_order"] = row[4]
                        if row[5]:
                            exp["parent_experiment"] = row[5]
                        if row[6]:
                            exp["peak_memory_mb"] = row[6]
                        if row[7]:
                            exp["started_at"] = (
                                row[7].isoformat()
                                if hasattr(row[7], "isoformat")
                                else str(row[7])
                            )
                        if has_claimed_at and row[8]:
                            exp["claimed_at"] = (
                                row[8].isoformat()
                                if hasattr(row[8], "isoformat")
                                else str(row[8])
                            )
                        result.append(exp)
                    return result
        except Exception as e:
            print(f"[DBExperimentsDB] load_all_for_panel error: {e}")
            return []

    def get_experiment(self, name: str) -> Optional[Dict]:
        """Get a single experiment as a dict."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT
                            e.*, 
                            p.status::text AS condition_parent_status
                        FROM exp_registry.experiments AS e
                        LEFT JOIN exp_registry.experiments AS p
                          ON p.name = NULLIF(COALESCE(e.extra->>'condition_parent', ''), '')
                        WHERE e.name = %s
                        """,
                        (name,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return None
                    return self._row_to_dict(row)
        except Exception as e:
            print(f"[DBExperimentsDB] get_experiment error: {e}")
            return None

    def get_panel_truth(self, name: str) -> Optional[Dict[str, Any]]:
        """Return DB-backed truth fields for panel rendering without artifact fallback."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT
                            name,
                            status::text AS status,
                            completed_at,
                            result_f1,
                            result_auc,
                            result_peak_mb,
                            error_type,
                            error_message,
                            is_true_oom,
                            error_peak_mb,
                            failed_at,
                            extra
                        FROM exp_registry.experiments
                        WHERE name = %s
                        """,
                        (name,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return None
                    extra = row.get("extra") or {}
                    if not isinstance(extra, dict):
                        extra = {}
                    canonical_result = extra.get("canonical_result") or {}
                    if not isinstance(canonical_result, dict):
                        canonical_result = {}
                    terminal_metadata = extra.get("terminal_metadata") or {}
                    if not isinstance(terminal_metadata, dict):
                        terminal_metadata = {}
                    return {
                        "name": row.get("name"),
                        "status": row.get("status"),
                        "completed_at": row.get("completed_at"),
                        "result": {
                            "f1_score": row.get("result_f1"),
                            "auc_score": row.get("result_auc"),
                            "peak_memory_mb": row.get("result_peak_mb", 0),
                        },
                        "error_info": {
                            "type": row.get("error_type"),
                            "message": row.get("error_message"),
                            "is_true_oom": row.get("is_true_oom", False),
                            "peak_memory_mb": row.get("error_peak_mb", 0),
                            "failed_at": row.get("failed_at").isoformat()
                            if row.get("failed_at")
                            else None,
                        }
                        if row.get("error_type")
                        else None,
                        "canonical_result": canonical_result,
                        "terminal_metadata": terminal_metadata,
                        "truth_source": "exp_registry",
                    }
        except Exception as e:
            print(f"[DBExperimentsDB] get_panel_truth error: {e}")
            return None

    def _get_preferred_worker_max_mb(self, cur, worker_id: Optional[str]) -> int:
        if not worker_id:
            return 0
        cur.execute(
            "SELECT gpu_info FROM exp_registry.worker_heartbeats WHERE worker_id = %s",
            (worker_id,),
        )
        row = cur.fetchone()
        gpu_info = []
        if row:
            if isinstance(row, dict):
                gpu_info = row.get("gpu_info") or []
            elif len(row) >= 1:
                gpu_info = row[0] or []
        return _max_allowed_gpu_total_mb(str(worker_id), gpu_info)

    def _claim_allowed_for_worker(self, cur, name: str, worker_id: str) -> bool:
        cur.execute(
            "SELECT preferred_worker, extra FROM exp_registry.experiments WHERE name = %s",
            (name,),
        )
        row = cur.fetchone()
        if not row:
            return False
        if isinstance(row, dict):
            preferred_worker = row.get("preferred_worker")
            extra = row.get("extra") or {}
        else:
            preferred_worker = row[0]
            extra = row[1] or {}
        if not isinstance(extra, dict):
            extra = {}
        memory_contract = extra.get("memory_contract") or {}
        est_mem_mb = int(memory_contract.get("est_mem_decision_mb", 0) or 0)
        preferred_max_mb = self._get_preferred_worker_max_mb(cur, preferred_worker)
        return _preferred_worker_fallback_allowed(
            str(preferred_worker) if preferred_worker else None,
            str(worker_id) if worker_id else None,
            est_mem_mb,
            preferred_max_mb,
        )

    # --- Status transitions ---

    def claim_experiment(
        self, name: str, worker_id: str, gpu_id: int, pid: int
    ) -> Optional[str]:
        """Claim an experiment: NEEDS_RERUN -> RUNNING. Returns run_id or None."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    if not self._claim_allowed_for_worker(cur, name, worker_id):
                        return None
                    cur.execute(
                        "SELECT exp_registry.claim_experiment(%s, %s, %s, %s)",
                        (name, worker_id, gpu_id, pid),
                    )
                    row = cur.fetchone()
                    run_id = row[0] if row else None
                    if run_id:
                        self.set_run_id(name, str(run_id))
                        return str(run_id)
                    return None
        except Exception as e:
            print(f"[DBExperimentsDB] claim_experiment error: {e}")
            return None
        finally:
            self._sync_snapshot()

    def update_experiment(self, name: str, updates: Dict) -> bool:
        """Generic update for a single experiment (for compat)."""
        try:
            # Build SET clause from updates
            set_parts = []
            params: list = []
            field_map = self._updates_to_columns(updates)
            for col, val in field_map.items():
                if col == "__extra_merge__":
                    set_parts.append(
                        "extra = COALESCE(extra, '{}'::jsonb) || %s::jsonb"
                    )
                    params.append(json.dumps(val))
                    continue
                set_parts.append(f"{col} = %s")
                params.append(val)

            if not set_parts:
                return False

            params.append(name)
            sql = f"UPDATE exp_registry.experiments SET {', '.join(set_parts)} WHERE name = %s"

            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    ok = cur.rowcount > 0
            if ok:
                self.clear_run_id(name)
            self._sync_snapshot()
            return ok
        except Exception as e:
            print(f"[DBExperimentsDB] update_experiment error: {e}")
            return False

    def mark_running(
        self,
        name: str,
        worker_id: str,
        gpu_id: int,
        pid: int,
        started_at: Optional[str] = None,
    ) -> Optional[str]:
        """Claim + mark running. Returns run_id."""
        run_id = self.claim_experiment(name, worker_id, gpu_id, pid)
        if run_id and started_at:
            # Override started_at if provided
            try:
                with get_conn(self.dsn) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE exp_registry.experiments SET started_at = %s WHERE name = %s AND run_id = %s::uuid",
                            (started_at, name, run_id),
                        )
                self._sync_snapshot()
            except Exception:
                pass
        return run_id

    def mark_done(
        self, name: str, result: Optional[Dict] = None, run_id: Optional[str] = None
    ) -> bool:
        """Complete an experiment: RUNNING -> COMPLETED with fencing."""
        if run_id is None:
            run_id = self.get_run_id(name)
        if not run_id:
            print(f"[DBExperimentsDB] mark_done: no run_id for {name}")
            return False
        try:
            f1 = None
            auc = None
            peak = 0
            if result:
                f1 = result.get("f1_score", result.get("test_f1"))
                auc = result.get("auc_score", result.get("test_auc"))
                peak = result.get("peak_memory_mb", 0)
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT exp_registry.complete_experiment(%s, %s::uuid, %s::double precision, %s::double precision, %s::integer)",
                        (name, run_id, f1, auc, int(peak or 0)),
                    )
                    row = cur.fetchone()
                    ok = row[0] if row else False
            if ok:
                self.clear_run_id(name)
            self._sync_snapshot()
            return bool(ok)
        except Exception as e:
            print(f"[DBExperimentsDB] mark_done error: {e}")
            return False

    def mark_error(
        self,
        name: str,
        error_type: str,
        message: str,
        is_true_oom: bool = False,
        peak_memory_mb: int = 0,
        run_id: Optional[str] = None,
    ) -> bool:
        """Fail an experiment: RUNNING -> NEEDS_RERUN with error info."""
        if run_id is None:
            run_id = self.get_run_id(name)
        if not run_id:
            print(f"[DBExperimentsDB] mark_error: no run_id for {name}")
            return False
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT exp_registry.fail_experiment(%s, %s::uuid, %s, %s, %s, %s)",
                        (
                            name,
                            run_id,
                            error_type,
                            message,
                            is_true_oom,
                            peak_memory_mb,
                        ),
                    )
                    row = cur.fetchone()
                    ok = row[0] if row else False
            if ok:
                self.clear_run_id(name)
            self._sync_snapshot()
            return bool(ok)
        except Exception as e:
            print(f"[DBExperimentsDB] mark_error error: {e}")
            return False

    def get_run_id_db(self, name: str) -> Optional[str]:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT run_id FROM exp_registry.experiments WHERE name = %s",
                        (name,),
                    )
                    row = cur.fetchone()
                    if not row or not row[0]:
                        return None
                    run_id = str(row[0])
                    self.set_run_id(name, run_id)
                    return run_id
        except Exception as e:
            print(f"[DBExperimentsDB] get_run_id_db error: {e}")
            return None

    def update_running_peak(self, name: str, peak_memory_mb: int):
        """Update peak memory for a running experiment."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE exp_registry.experiments SET peak_memory_mb = GREATEST(COALESCE(peak_memory_mb, 0), %s) "
                        "WHERE name = %s AND status = 'RUNNING'",
                        (peak_memory_mb, name),
                    )
            # Don't sync snapshot for every peak update (too frequent)
        except Exception as e:
            print(f"[DBExperimentsDB] update_running_peak error: {e}")

    # --- Batch operations ---

    def reset_failed_experiments(self) -> int:
        """Reset only terminal-failed experiments to NEEDS_RERUN.

        Terminal failures (per _get_db_terminal_reason):
          - true OOM:      error_type='OOM' AND is_true_oom=TRUE
          - script errors: error_type IN ('SCRIPT_ERROR','ZOMBIE','PID_MISSING')

        Excluded: RUNNING, COMPLETED, QUEUED_RETRY (soft OOM / normal retry),
        FROZEN (MANUAL_FREEZE).
        """
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE exp_registry.experiments
                        SET status = 'NEEDS_RERUN',
                            error_type = NULL,
                            error_message = NULL,
                            is_true_oom = FALSE,
                            failed_at = NULL,
                            retry_count = 0,
                            oom_retry_count = 0
                        WHERE status = 'NEEDS_RERUN'
                          AND (
                            (error_type = 'OOM' AND is_true_oom = TRUE)
                            OR error_type IN ('SCRIPT_ERROR', 'ZOMBIE', 'PID_MISSING')
                          )
                    """)
                    count = cur.rowcount
            self._sync_snapshot()
            return count
        except Exception as e:
            print(f"[DBExperimentsDB] reset_failed error: {e}")
            return 0

    def kill_experiments_on_worker(self, worker_id: str) -> int:
        """Reset all RUNNING experiments for a specific worker."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE exp_registry.experiments
                        SET status = 'NEEDS_RERUN',
                            run_id = NULL,
                            worker_id = NULL,
                            gpu_id = NULL,
                            pid = NULL,
                            started_at = NULL,
                            retry_count = 0,
                            oom_retry_count = 0
                        WHERE status = 'RUNNING'
                          AND worker_id = %s
                    """,
                        (worker_id,),
                    )
                    count = cur.rowcount
            self._sync_snapshot()
            return count
        except Exception as e:
            print(f"[DBExperimentsDB] kill_on_worker error: {e}")
            return 0

    def kill_experiment(self, name: str) -> bool:
        """Reset an experiment and move to end of queue."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Get max display_order
                    cur.execute(
                        "SELECT COALESCE(MAX(display_order), 0) + 1 FROM exp_registry.experiments"
                    )
                    max_order = cur.fetchone()[0]
                    cur.execute(
                        """
                        UPDATE exp_registry.experiments
                        SET status = 'NEEDS_RERUN',
                            run_id = NULL,
                            worker_id = NULL,
                            gpu_id = NULL,
                            pid = NULL,
                            started_at = NULL,
                            completed_at = NULL,
                            result_f1 = NULL,
                            result_auc = NULL,
                            error_type = 'MANUAL_STOP',
                            error_message = 'manually stop',
                            is_true_oom = FALSE,
                            failed_at = NOW(),
                            retry_count = 0,
                            oom_retry_count = 0,
                            display_order = %s
                        WHERE name = %s
                    """,
                        (max_order, name),
                    )
                    ok = cur.rowcount > 0
            self.clear_run_id(name)
            self._sync_snapshot()
            return ok
        except Exception as e:
            print(f"[DBExperimentsDB] kill_experiment error: {e}")
            return False

    def freeze_experiment(self, name: str) -> bool:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE exp_registry.experiments
                        SET status = 'NEEDS_RERUN',
                            run_id = NULL,
                            worker_id = NULL,
                            gpu_id = NULL,
                            pid = NULL,
                            started_at = NULL,
                            error_type = 'MANUAL_FREEZE',
                            error_message = 'manually frozen',
                            is_true_oom = FALSE,
                            failed_at = NOW()
                        WHERE name = %s
                        """,
                        (name,),
                    )
                    ok = cur.rowcount > 0
            if ok:
                self.clear_run_id(name)
            self._sync_snapshot()
            return ok
        except Exception as e:
            print(f"[DBExperimentsDB] freeze_experiment error: {e}")
            return False

    def rerun_experiment(self, name: str) -> bool:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(MIN(display_order), 0) - 1 FROM exp_registry.experiments"
                    )
                    min_order = cur.fetchone()[0]
                    cur.execute(
                        """
                        UPDATE exp_registry.experiments
                        SET status = 'NEEDS_RERUN',
                            run_id = NULL,
                            worker_id = NULL,
                            gpu_id = NULL,
                            pid = NULL,
                            started_at = NULL,
                            peak_memory_mb = 0,
                            completed_at = NULL,
                            result_f1 = NULL,
                            result_auc = NULL,
                            result_peak_mb = 0,
                            error_type = NULL,
                            error_message = NULL,
                            is_true_oom = FALSE,
                            error_peak_mb = 0,
                            failed_at = NULL,
                            retry_count = 0,
                            oom_retry_count = 0,
                            max_retries = GREATEST(COALESCE(max_retries, 0), 1),
                            doc_processed_at = NULL,
                            display_order = %s
                        WHERE name = %s
                    """,
                        (min_order, name),
                    )
                    ok = cur.rowcount > 0
            self.clear_run_id(name)
            self._sync_snapshot()
            return ok
        except Exception as e:
            print(f"[DBExperimentsDB] rerun_experiment error: {e}")
            return False

    def start_experiment_now(self, name: str) -> bool:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Get min display_order
                    cur.execute(
                        "SELECT COALESCE(MIN(display_order), 0) - 1 FROM exp_registry.experiments"
                    )
                    min_order = cur.fetchone()[0]
                    cur.execute(
                        """
                        UPDATE exp_registry.experiments
                        SET status = 'NEEDS_RERUN',
                            run_id = NULL,
                            worker_id = NULL,
                            gpu_id = NULL,
                            pid = NULL,
                            started_at = NULL,
                            peak_memory_mb = 0,
                            completed_at = NULL,
                            result_f1 = NULL,
                            result_auc = NULL,
                            result_peak_mb = 0,
                            display_order = %s,
                            error_type = NULL,
                            error_message = NULL,
                            is_true_oom = FALSE,
                            error_peak_mb = 0,
                            failed_at = NULL,
                            retry_count = 0,
                            oom_retry_count = 0,
                            max_retries = GREATEST(COALESCE(max_retries, 0), 1),
                            doc_processed_at = NULL
                        WHERE name = %s
                    """,
                        (min_order, name),
                    )
                    ok = cur.rowcount > 0
            if ok:
                self.clear_run_id(name)
            self._sync_snapshot()
            return ok
        except Exception as e:
            print(f"[DBExperimentsDB] start_now error: {e}")
            return False

    def move_experiment(self, name: str, direction: str) -> bool:
        """Swap display_order with adjacent experiment."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, display_order, parent_experiment FROM exp_registry.experiments ORDER BY display_order, id"
                    )
                    rows = cur.fetchall()
                    ids = [(r[0], r[1], r[2]) for r in rows]
                    target_id = self._get_id_by_name(cur, name)
                    idx = next(
                        (i for i, (eid, _, _) in enumerate(ids) if eid == target_id),
                        -1,
                    )
                    if idx < 0:
                        return False

                    swap_idx = _pick_frame_swap_index(ids, idx, direction)
                    if swap_idx is None:
                        return False

                    id_a, order_a, _ = ids[idx]
                    id_b, order_b, _ = ids[swap_idx]
                    cur.execute(
                        "UPDATE exp_registry.experiments SET display_order = %s WHERE id = %s",
                        (order_b, id_a),
                    )
                    cur.execute(
                        "UPDATE exp_registry.experiments SET display_order = %s WHERE id = %s",
                        (order_a, id_b),
                    )
            self._sync_snapshot()
            return True
        except Exception as e:
            print(f"[DBExperimentsDB] move error: {e}")
            return False

    def assign_experiment_worker(self, name: str, worker_id: Optional[str]) -> bool:
        try:
            normalized_worker = str(worker_id).strip() if worker_id is not None else ""
            db_worker = normalized_worker or None
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT status, worker_id FROM exp_registry.experiments WHERE name = %s",
                        (name,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return False
                    current_status, current_worker = row
                    cur.execute(
                        "UPDATE exp_registry.experiments SET preferred_worker = %s WHERE name = %s",
                        (db_worker, name),
                    )
                    if (
                        db_worker
                        and str(current_status) == STATUS_RUNNING
                        and current_worker
                        and str(current_worker) != db_worker
                    ):
                        cur.execute(
                            """
                            UPDATE exp_registry.experiments
                            SET status = 'NEEDS_RERUN',
                                run_id = NULL, worker_id = NULL,
                                gpu_id = NULL, pid = NULL,
                                started_at = NULL,
                                retry_count = 0, oom_retry_count = 0
                            WHERE name = %s AND status = 'RUNNING'
                        """,
                            (name,),
                        )
            self._sync_snapshot()
            return True
        except Exception as e:
            print(f"[DBExperimentsDB] assign_worker error: {e}")
            return False

    def queue_remote_termination(
        self,
        name: str,
        target_worker: str,
        pid: int,
        action: str,
        requester_worker: str,
    ) -> bool:
        try:
            payload = {
                "target_worker": str(target_worker),
                "pid": int(pid),
                "action": str(action),
                "requester_worker": str(requester_worker),
                "requested_at": datetime.now().isoformat(),
            }
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE exp_registry.experiments
                        SET extra = jsonb_set(
                            COALESCE(extra, '{}'::jsonb),
                            '{remote_termination}',
                            %s::jsonb,
                            true
                        )
                        WHERE name = %s
                          AND status = 'RUNNING'
                          AND worker_id = %s
                          AND pid = %s
                    """,
                        (json.dumps(payload), name, target_worker, int(pid)),
                    )
                    ok = cur.rowcount > 0
            if ok:
                self._sync_snapshot()
            return ok
        except Exception as e:
            print(f"[DBExperimentsDB] queue_remote_termination error: {e}")
            return False

    def fetch_remote_termination_requests(
        self, worker_id: str, limit: int = 16
    ) -> List[Dict]:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT name, pid, worker_id, extra
                        FROM exp_registry.experiments
                        WHERE status = 'RUNNING'
                          AND COALESCE(extra->'remote_termination'->>'target_worker', '') = %s
                        ORDER BY updated_at ASC, id ASC
                        LIMIT %s
                    """,
                        (str(worker_id), int(limit)),
                    )
                    rows = cur.fetchall()
            requests: List[Dict] = []
            for row in rows:
                extra = row.get("extra") or {}
                req = extra.get("remote_termination") or {}
                if not isinstance(req, dict):
                    continue
                requests.append(
                    {
                        "name": row.get("name"),
                        "worker_id": row.get("worker_id"),
                        "pid": row.get("pid"),
                        "target_worker": req.get("target_worker"),
                        "requested_pid": req.get("pid"),
                        "action": req.get("action"),
                        "requester_worker": req.get("requester_worker"),
                    }
                )
            return requests
        except Exception as e:
            print(f"[DBExperimentsDB] fetch_remote_termination_requests error: {e}")
            return []

    def clear_remote_termination_request(self, name: str, worker_id: str) -> bool:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE exp_registry.experiments
                        SET extra = COALESCE(extra, '{}'::jsonb) - 'remote_termination'
                        WHERE name = %s
                          AND COALESCE(extra->'remote_termination'->>'target_worker', '') = %s
                    """,
                        (name, str(worker_id)),
                    )
                    ok = cur.rowcount > 0
            if ok:
                self._sync_snapshot()
            return ok
        except Exception as e:
            print(f"[DBExperimentsDB] clear_remote_termination_request error: {e}")
            return False

    def heal_running_worker_owner(self, name: str, worker_id: str) -> bool:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE exp_registry.experiments
                        SET worker_id = %s,
                            gpu_id = NULL,
                            pid = NULL,
                            updated_at = NOW()
                        WHERE name = %s
                          AND status = 'RUNNING'
                    """,
                        (worker_id, name),
                    )
                    ok = cur.rowcount > 0
            if ok:
                self._sync_snapshot()
            return ok
        except Exception as e:
            print(f"[DBExperimentsDB] heal_running_worker_owner error: {e}")
            return False

    # --- Worker management ---

    def disable_worker(self, worker_id: str) -> bool:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO exp_registry.disabled_workers(worker_id) VALUES (%s) ON CONFLICT DO NOTHING",
                        (worker_id,),
                    )
            self._sync_snapshot()
            return True
        except Exception as e:
            print(f"[DBExperimentsDB] disable_worker error: {e}")
            return False

    def enable_worker(self, worker_id: str) -> bool:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM exp_registry.disabled_workers WHERE worker_id = %s",
                        (worker_id,),
                    )
            self._sync_snapshot()
            return True
        except Exception as e:
            print(f"[DBExperimentsDB] enable_worker error: {e}")
            return False

    def is_worker_disabled(self, worker_id: str) -> bool:
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM exp_registry.disabled_workers WHERE worker_id = %s",
                        (worker_id,),
                    )
                    return cur.fetchone() is not None
        except Exception:
            return False

    # --- Heartbeat ---

    def update_heartbeat(
        self,
        worker_id: str,
        pid: int,
        running_jobs: int,
        running_experiments: List[str],
        gpu_info: Any = None,
        cpu_info: Any = None,
    ) -> bool:
        """Write worker heartbeat to DB. Returns True on success."""
        worker_token = str(worker_id or "").strip()
        if not worker_token:
            self._last_heartbeat_error = "worker_id is empty"
            return False

        whitelist = set(_load_worker_whitelist())
        if whitelist and worker_token not in whitelist:
            self._last_heartbeat_error = (
                f"worker_id={worker_token} ignored: not in machines whitelist"
            )
            return True

        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT exp_registry.update_heartbeat(%s, %s, %s, %s, %s, %s)",
                        (
                            worker_token,
                            pid,
                            running_jobs,
                            running_experiments,
                            json.dumps(gpu_info or []),
                            json.dumps(cpu_info or {}),
                        ),
                    )
            self._last_heartbeat_error = ""
            return True
        except Exception as e:
            self._last_heartbeat_error = (
                f"worker_id={worker_token} pid={pid} running_jobs={running_jobs} "
                f"running_experiments={len(running_experiments)} err={type(e).__name__}: {e}"
            )
            print(f"[DBExperimentsDB] update_heartbeat error: {self._last_heartbeat_error}")
            try:
                close_pool()
            except Exception:
                pass
            try:
                with get_conn(self.dsn) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT exp_registry.update_heartbeat(%s, %s, %s, %s, %s, %s)",
                            (
                                worker_token,
                                pid,
                                running_jobs,
                                running_experiments,
                                json.dumps(gpu_info or []),
                                json.dumps(cpu_info or {}),
                            ),
                        )
                self._last_heartbeat_error = ""
                return True
            except Exception as retry_e:
                self._last_heartbeat_error = (
                    f"{self._last_heartbeat_error} retry_err={type(retry_e).__name__}: {retry_e}"
                )
                print(f"[DBExperimentsDB] update_heartbeat retry failed: {self._last_heartbeat_error}")
                return False

    def cleanup_worker_heartbeats(self, whitelist: List[str]) -> int:
        allowed = sorted({str(worker_id or "").strip() for worker_id in whitelist if str(worker_id or "").strip()})
        if not allowed:
            return 0
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM exp_registry.worker_heartbeats WHERE NOT (worker_id = ANY(%s))",
                        (allowed,),
                    )
                    return int(cur.rowcount or 0)
        except Exception as e:
            print(f"[DBExperimentsDB] cleanup_worker_heartbeats error: {e}")
            return 0

    def get_last_heartbeat_error(self) -> str:
        return str(self._last_heartbeat_error or "")

    def get_cluster_heartbeats(self) -> Dict[str, Dict]:
        """Read all heartbeats from DB."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM exp_registry.worker_heartbeats")
                    rows = cur.fetchall()
                    result = {}
                    for row in rows:
                        wid = row["worker_id"]
                        last_seen = row["last_seen"]
                        cpu_info = row.get("cpu_info", {})
                        if not isinstance(cpu_info, dict):
                            cpu_info = {}
                        seconds_ago = (
                            (datetime.now(last_seen.tzinfo) - last_seen).total_seconds()
                            if last_seen
                            else 999999
                        )
                        result[wid] = {
                            "worker_id": wid,
                            "timestamp": last_seen.isoformat() if last_seen else "",
                            "last_seen_sec": seconds_ago,
                            "pid": row.get("pid"),
                            "running_jobs": row.get("running_jobs", 0),
                            "running_experiments": row.get("running_experiments", []),
                            "gpus": row.get("gpu_info", []),
                            "cpu": cpu_info,
                            "gpu_probe_error": str(
                                cpu_info.get("_gpu_probe_error", "") or ""
                            ),
                        }
                    return result
        except Exception as e:
            print(f"[DBExperimentsDB] get_cluster_heartbeats error: {e}")
            return {}

    def get_filtered_cluster_heartbeats(
        self,
        whitelist: Optional[List[str]] = None,
        *,
        fail_closed: bool = True,
    ) -> Dict[str, Dict]:
        allowed = whitelist if whitelist is not None else _load_worker_whitelist()
        return filter_worker_heartbeats(
            self.get_cluster_heartbeats(),
            allowed,
            fail_closed=fail_closed,
        )

    def get_worker_heartbeat(
        self,
        worker_id: str,
        whitelist: Optional[List[str]] = None,
        *,
        fail_closed: bool = True,
    ) -> Dict[str, Any]:
        allowed = whitelist if whitelist is not None else _load_worker_whitelist()
        return _get_worker_heartbeat(
            self.get_cluster_heartbeats(),
            worker_id,
            allowed,
            fail_closed=fail_closed,
        )

    # --- Stale check (THE key improvement over file-based) ---

    def check_stale_experiments(
        self,
        stale_sec: int = DEFAULT_STALE_SEC,
        caller_worker: Optional[str] = None,
        buddy_report_ttl_sec: Optional[int] = None,
    ) -> List[Tuple[str, str]]:
        """Atomic stale detection + reset. Returns [(name, stale_worker)]."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    if buddy_report_ttl_sec is None:
                        cur.execute(
                            "SELECT experiment_name, stale_worker FROM exp_registry.check_stale_experiments(%s, %s)",
                            (stale_sec, caller_worker),
                        )
                        results = cur.fetchall()
                    else:
                        safe_ttl = max(1, int(buddy_report_ttl_sec))
                        self._ensure_buddy_reports_table(cur)
                        cur.execute(
                            """
                            WITH stale_candidates AS (
                                SELECT e.name, e.worker_id
                                FROM exp_registry.experiments AS e
                                LEFT JOIN exp_registry.worker_heartbeats AS h
                                  ON h.worker_id = e.worker_id
                                WHERE e.status = 'RUNNING'
                                  AND e.worker_id IS NOT NULL
                                  AND (%s IS NULL OR e.worker_id <> %s)
                                  AND (
                                    h.last_seen IS NULL
                                    OR NOW() - h.last_seen > (%s * INTERVAL '1 second')
                                  )
                                  AND NOT EXISTS (
                                    SELECT 1
                                    FROM exp_registry.buddy_reports AS b
                                    WHERE b.target_id = e.worker_id
                                      AND b.target_process_alive IS TRUE
                                      AND b.checked_at >= NOW() - (%s * INTERVAL '1 second')
                                  )
                                FOR UPDATE OF e
                            ),
                            updated AS (
                                UPDATE exp_registry.experiments AS e
                                SET status = 'NEEDS_RERUN',
                                    retry_count = e.retry_count + 1,
                                    error_type = 'STALE_LOCK',
                                    error_message = CONCAT('Heartbeat stale for worker ', c.worker_id),
                                    failed_at = NOW(),
                                    run_id = NULL,
                                    worker_id = NULL,
                                    gpu_id = NULL,
                                    pid = NULL,
                                    started_at = NULL
                                FROM stale_candidates AS c
                                WHERE e.name = c.name
                                  AND e.status = 'RUNNING'
                                RETURNING e.name, c.worker_id
                            )
                            SELECT name, worker_id FROM updated
                            """,
                            (caller_worker, caller_worker, stale_sec, safe_ttl),
                        )
                        results = cur.fetchall()
            if results:
                self._sync_snapshot()
            return [(r[0], r[1]) for r in results]
        except Exception as e:
            print(f"[DBExperimentsDB] check_stale error: {e}")
            return []

    # --- Zombie check (local PID liveness) ---

    def check_zombie_processes(
        self, worker_id: str, exclude_names: Optional[set[str]] = None
    ) -> List[Tuple[str, int]]:
        """Check and reset experiments whose local PID has died."""
        zombies = []
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT name, pid, run_id FROM exp_registry.experiments
                        WHERE status = 'RUNNING' AND worker_id = %s AND pid IS NOT NULL
                    """,
                        (worker_id,),
                    )
                    rows = cur.fetchall()

                    for name, pid, run_id in rows:
                        if exclude_names and name in exclude_names:
                            continue
                        if not pid:
                            continue
                        try:
                            os.kill(pid, 0)
                        except ProcessLookupError:
                            # Process dead -> reset
                            cur.execute(
                                """
                                UPDATE exp_registry.experiments
                                SET status = 'NEEDS_RERUN',
                                    retry_count = retry_count + 1,
                                    error_type = 'ZOMBIE',
                                    error_message = %s,
                                    failed_at = NOW(),
                                    run_id = NULL, worker_id = NULL,
                                    gpu_id = NULL, pid = NULL,
                                    started_at = NULL
                                WHERE name = %s AND status = 'RUNNING'
                                  AND (run_id = %s OR run_id IS NULL)
                            """,
                                (f"Process {pid} died unexpectedly", name, run_id),
                            )
                            zombies.append((name, pid))
                        except PermissionError:
                            pass  # Process exists, different user
                    conn.commit()
            if zombies:
                self._sync_snapshot()
        except Exception as e:
            print(f"[DBExperimentsDB] check_zombie error: {e}")
        return zombies

    # --- PID registration enforcement ---

    def enforce_running_pid_registration(self, grace_sec: int = 20) -> List[str]:
        """Reset RUNNING experiments that don't have a PID registered."""
        fixed = []
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE exp_registry.experiments
                        SET status = 'NEEDS_RERUN',
                            retry_count = retry_count + 1,
                            error_type = 'PID_MISSING',
                            error_message = 'RUNNING entry missing registered PID',
                            failed_at = NOW(),
                            run_id = NULL, worker_id = NULL,
                            gpu_id = NULL, pid = NULL,
                            started_at = NULL
                        WHERE status = 'RUNNING'
                          AND (pid IS NULL OR pid <= 1)
                          AND (started_at IS NULL
                               OR started_at < NOW() - (%s || ' seconds')::INTERVAL)
                        RETURNING name
                    """,
                        (str(grace_sec),),
                    )
                    fixed = [r[0] for r in cur.fetchall()]
            if fixed:
                self._sync_snapshot()
        except Exception as e:
            print(f"[DBExperimentsDB] enforce_pid error: {e}")
        return fixed

    # --- Heal registry from running processes ---

    def heal_from_running_process(
        self,
        name: str,
        worker_id: str,
        gpu_id: int,
        pid: int,
        started_at: Optional[str] = None,
    ) -> bool:
        """Re-mark an experiment as RUNNING if its process is alive but DB disagrees."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Check current state
                    cur.execute(
                        "SELECT status, worker_id, pid, run_id FROM exp_registry.experiments WHERE name = %s FOR UPDATE",
                        (name,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return False
                    current_status, current_worker, current_pid, current_run_id = row

                    # Already correct
                    if (
                        str(current_status) == STATUS_RUNNING
                        and str(current_worker or "") == worker_id
                        and current_pid == pid
                    ):
                        return True

                    # Heal: set back to RUNNING
                    if (
                        str(current_status) == STATUS_RUNNING
                        and current_run_id is not None
                    ):
                        cur.execute(
                            """
                            UPDATE exp_registry.experiments
                            SET worker_id = %s,
                                gpu_id = %s,
                                pid = %s,
                                started_at = COALESCE(%s::timestamptz, started_at, NOW())
                            WHERE name = %s
                              AND status = 'RUNNING'
                              AND run_id = %s::uuid
                        """,
                            (
                                worker_id,
                                gpu_id,
                                pid,
                                started_at,
                                name,
                                str(current_run_id),
                            ),
                        )
                        self.set_run_id(name, str(current_run_id))
                    else:
                        import uuid

                        new_run_id = str(uuid.uuid4())
                        cur.execute(
                            """
                            UPDATE exp_registry.experiments
                            SET status = 'RUNNING',
                                run_id = %s::uuid,
                                worker_id = %s,
                                gpu_id = %s,
                                pid = %s,
                                started_at = COALESCE(%s::timestamptz, started_at, NOW())
                            WHERE name = %s
                        """,
                            (new_run_id, worker_id, gpu_id, pid, started_at, name),
                        )
                        self.set_run_id(name, new_run_id)
            self._sync_snapshot()
            return True
        except Exception as e:
            print(f"[DBExperimentsDB] heal error: {e}")
            return False

    # --- Runnable experiments ---

    def get_runnable_experiments(
        self, local_gpu_total: int = 24000, worker_id: Optional[str] = None
    ) -> List[Dict]:
        """Get experiments eligible for execution."""
        try:
            with get_conn(self.dsn) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT
                            name,
                            status::text AS status,
                            parent_experiment,
                            COALESCE(extra->>'role', '') AS role,
                            error_type
                        FROM exp_registry.experiments
                        """
                    )
                    all_rows = cur.fetchall()
                    condition_status_lookup = {
                        str(r.get("name") or "").strip(): str(r.get("status") or "")
                        for r in all_rows
                        if str(r.get("name") or "").strip()
                    }
                    unfinished_children: Dict[str, int] = {}
                    for row in all_rows:
                        parent = str(row.get("parent_experiment") or "").strip()
                        if not parent:
                            continue
                        status = str(row.get("status") or "").upper()
                        error_type = str(row.get("error_type") or "").strip()
                        is_terminal = status == STATUS_COMPLETED or (
                            status == STATUS_NEEDS_RERUN and bool(error_type)
                        )
                        if not is_terminal:
                            unfinished_children[parent] = (
                                unfinished_children.get(parent, 0) + 1
                            )

                    cur.execute(
                        "SELECT * FROM exp_registry.get_runnable(%s, %s)",
                        (worker_id, local_gpu_total),
                    )
                    rows = cur.fetchall()
                    filtered: List[Dict] = []
                    for row in rows:
                        error_type = str(row.get("error_type") or "").strip().upper()
                        if error_type in {"MANUAL_STOP", "MANUAL_FREEZE"}:
                            continue
                        exp = self._row_to_dict(
                            row,
                            condition_status_lookup=condition_status_lookup,
                        )
                        role = str(exp.get("role") or "").strip().lower()
                        name = str(exp.get("name") or "")
                        progression_status = (
                            str(exp.get("progression_status") or "").strip().upper()
                        )
                        if progression_status == PROGRESSION_BLOCKED_CONDITION:
                            continue
                        if role == "main" and unfinished_children.get(name, 0) > 0:
                            continue
                        filtered.append(exp)

                    if worker_id:
                        cur.execute(
                            "SELECT * FROM exp_registry.get_runnable(%s, %s)",
                            (None, local_gpu_total),
                        )
                        fallback_rows = cur.fetchall()
                        for row in fallback_rows:
                            exp = self._row_to_dict(
                                row,
                                condition_status_lookup=condition_status_lookup,
                            )
                            error_info = exp.get("error_info")
                            if not isinstance(error_info, dict):
                                error_info = {}
                            error_type = str(error_info.get("type") or "").upper()
                            if error_type in {"MANUAL_STOP", "MANUAL_FREEZE"}:
                                continue
                            progression_status = (
                                str(exp.get("progression_status") or "").strip().upper()
                            )
                            if progression_status == PROGRESSION_BLOCKED_CONDITION:
                                continue
                            if any(
                                existing.get("name") == exp.get("name")
                                for existing in filtered
                            ):
                                continue
                            preferred_worker = exp.get("preferred_worker")
                            if not preferred_worker or str(preferred_worker) == str(
                                worker_id
                            ):
                                continue
                            est_mem_mb = int(
                                (
                                    (exp.get("memory_contract") or {}).get(
                                        "est_mem_decision_mb", 0
                                    )
                                    or 0
                                )
                            )
                            preferred_max_mb = self._get_preferred_worker_max_mb(
                                cur, preferred_worker
                            )
                            if _preferred_worker_fallback_allowed(
                                str(preferred_worker) if preferred_worker else None,
                                worker_id,
                                est_mem_mb,
                                preferred_max_mb,
                            ):
                                filtered.append(exp)
                    return filtered
        except Exception as e:
            print(f"[DBExperimentsDB] get_runnable error: {e}")
            return []

    # --- Helpers ---

    @staticmethod
    def _get_id_by_name(cur, name: str) -> Optional[int]:
        cur.execute("SELECT id FROM exp_registry.experiments WHERE name = %s", (name,))
        row = cur.fetchone()
        return row[0] if row else None

    @staticmethod
    def _row_to_dict(
        row: Dict, condition_status_lookup: Optional[Dict[str, str]] = None
    ) -> Dict:
        """Convert a DB row to the old JSON format dict."""
        status_raw = str(row.get("status", "NEEDS_RERUN"))
        extra = row.get("extra")
        if not isinstance(extra, dict):
            extra = {}
        memory_contract = extra.get("memory_contract")
        if not isinstance(memory_contract, dict):
            memory_contract = None
        running_on = None
        if status_raw == STATUS_RUNNING and row.get("worker_id"):
            running_on = {
                "worker": row.get("worker_id"),
                "gpu": row.get("gpu_id"),
                "pid": row.get("pid"),
                "started_at": row["started_at"].isoformat()
                if row.get("started_at")
                else None,
                "peak_memory_mb": row.get("peak_memory_mb", 0),
            }

        error_info = None
        if row.get("error_type"):
            error_info = {
                "type": row["error_type"],
                "is_true_oom": row.get("is_true_oom", False),
                "message": row.get("error_message", ""),
                "peak_memory_mb": row.get("error_peak_mb", 0),
                "failed_at": row["failed_at"].isoformat()
                if row.get("failed_at")
                else None,
            }

        result = None
        if row.get("result_f1") is not None or row.get("result_auc") is not None:
            result = {
                "f1_score": row.get("result_f1"),
                "auc_score": row.get("result_auc"),
                "peak_memory_mb": row.get("result_peak_mb", 0),
            }

        condition_parent = str(extra.get("condition_parent") or "").strip() or None
        gate_type = str(extra.get("gate_type") or "").strip() or None
        gate_evidence_ref = str(extra.get("gate_evidence_ref") or "").strip() or None
        if not condition_parent:
            condition_parent = _clean_optional_text(row.get("condition_parent"))
        parent_status = _clean_optional_text(row.get("condition_parent_status"))
        if not parent_status and condition_parent and condition_status_lookup:
            parent_status = condition_status_lookup.get(condition_parent)
        progression_status, progression_block_reason = derive_progression_status(
            status_raw,
            condition_parent=condition_parent,
            condition_parent_status=parent_status,
            warmup_hint=False,
        )

        return {
            "name": row["name"],
            "batch_id": row.get("batch_id", ""),
            "status": "COMPLETED" if status_raw == "COMPLETED" else status_raw,
            "running_on": running_on,
            "completed_at": row["completed_at"].isoformat()
            if row.get("completed_at")
            else None,
            "result": result,
            "retry_count": row.get("retry_count", 0),
            "oom_retry_count": row.get("oom_retry_count", 0),
            "max_retries": row.get("max_retries", MAX_RETRY_COUNT),
            "error_info": error_info,
            "preferred_worker": row.get("preferred_worker"),
            "group_id": row.get("group_id"),
            "depends_on_group": row.get("depends_on_group"),
            "parent_experiment": row.get("parent_experiment"),
            "condition_parent": condition_parent,
            "gate_type": gate_type,
            "gate_evidence_ref": gate_evidence_ref,
            "progression_status": progression_status,
            "block_reason": progression_block_reason,
            "progression_block_reason": progression_block_reason,
            "role": str(extra.get("role", "") or ""),
            "main_experiment": str(extra.get("main_experiment", "") or ""),
            "memory_contract": memory_contract,
            "doc_processed_at": row["doc_processed_at"].isoformat()
            if row.get("doc_processed_at")
            else None,
            "display_order": row.get("display_order", 0),
            "script": row.get("script_path"),
        }

    @staticmethod
    def _updates_to_columns(updates: Dict) -> Dict[str, Any]:
        """Map old-style update dict keys to DB column names."""
        col_map: Dict[str, Any] = {}
        extra_merge: Dict[str, Any] = {}
        for key, val in updates.items():
            if key == "status":
                # Normalize status for DB enum
                s = str(val).upper()
                if s in ("DONE", "SKIPPED", "COMPLETED"):
                    col_map["status"] = "COMPLETED"
                elif s in ("READY", "ERROR", "OOM", "NEEDS_RERUN"):
                    col_map["status"] = "NEEDS_RERUN"
                elif s == "RUNNING":
                    col_map["status"] = "RUNNING"
            elif key == "running_on":
                if val is None:
                    col_map["worker_id"] = None
                    col_map["gpu_id"] = None
                    col_map["pid"] = None
                    col_map["started_at"] = None
                elif isinstance(val, dict):
                    col_map["worker_id"] = val.get("worker")
                    col_map["gpu_id"] = val.get("gpu")
                    col_map["pid"] = val.get("pid")
                    col_map["started_at"] = val.get("started_at")
                    if "peak_memory_mb" in val:
                        col_map["peak_memory_mb"] = val["peak_memory_mb"]
            elif key == "error_info":
                if val is None:
                    col_map["error_type"] = None
                    col_map["error_message"] = None
                    col_map["is_true_oom"] = False
                    col_map["error_peak_mb"] = 0
                    col_map["failed_at"] = None
                elif isinstance(val, dict):
                    col_map["error_type"] = val.get("type")
                    col_map["error_message"] = val.get("message")
                    col_map["is_true_oom"] = val.get("is_true_oom", False)
                    col_map["error_peak_mb"] = val.get("peak_memory_mb", 0)
                    col_map["failed_at"] = val.get("failed_at")
            elif key == "result":
                if isinstance(val, dict):
                    col_map["result_f1"] = val.get("f1_score")
                    col_map["result_auc"] = val.get("auc_score")
                    col_map["result_peak_mb"] = val.get("peak_memory_mb", 0)
            elif key == "completed_at":
                col_map["completed_at"] = val
            elif key == "retry_count":
                col_map["retry_count"] = val
            elif key == "max_retries":
                col_map["max_retries"] = val
            elif key == "oom_retry_count":
                col_map["oom_retry_count"] = val
            elif key == "preferred_worker":
                col_map["preferred_worker"] = val
            elif key == "group_id":
                col_map["group_id"] = val
            elif key == "depends_on_group":
                col_map["depends_on_group"] = val
            elif key == "parent_experiment":
                col_map["parent_experiment"] = val
            elif key == "doc_processed_at":
                col_map["doc_processed_at"] = val
            elif key == "script":
                col_map["script_path"] = val
            elif key == "batch_id":
                col_map["batch_id"] = val
            elif key == "memory_contract":
                if isinstance(val, dict) and val:
                    extra_merge["memory_contract"] = val
            elif key == "role":
                extra_merge["role"] = str(val or "")
            elif key == "main_experiment":
                extra_merge["main_experiment"] = str(val or "")
            elif key in {"condition_parent", "gate_type", "gate_evidence_ref"}:
                cleaned = str(val or "").strip()
                extra_merge[key] = cleaned
            elif key == "description":
                extra_merge["description"] = str(val or "")
            elif key == "priority":
                try:
                    extra_merge["priority"] = int(val)
                except (TypeError, ValueError):
                    extra_merge["priority"] = 0
            elif key == "extra":
                if isinstance(val, dict) and val:
                    extra_merge.update(val)
        if extra_merge:
            col_map["__extra_merge__"] = extra_merge
        return col_map

    # --- Save (for compat: full data dump not needed with DB, but keep interface) ---

    def save(self, data: Dict):
        """Compat: not used with DB backend. Snapshot sync only."""
        self._sync_snapshot()


# ---------------------------------------------------------------------------
# Archive helpers (T4: CSV export + DB archive)
# ---------------------------------------------------------------------------

_ARCHIVE_CSV_COLUMNS = [
    "name",
    "status",
    "f1",
    "auc",
    "description",
    "config_summary",
    "archived_at",
    "batch_id",
]


def _extract_csv_fields(name: str, data: Dict, archived_at: str) -> Dict[str, Any]:
    """Pull flat CSV-friendly fields from an archived experiment JSONB blob."""
    result_block = data.get("result") or {}
    if not isinstance(result_block, dict):
        result_block = {}
    extra = data.get("extra") or {}
    if not isinstance(extra, dict):
        extra = {}
    return {
        "name": name,
        "status": str(data.get("status", "COMPLETED")),
        "f1": result_block.get("f1_score") or data.get("result_f1"),
        "auc": result_block.get("auc_score") or data.get("result_auc"),
        "description": str(
            extra.get("description", "") or data.get("description", "") or ""
        ),
        "config_summary": json.dumps(
            {
                k: data.get(k)
                for k in (
                    "hidden_dim",
                    "lr",
                    "max_epochs",
                    "features",
                    "script",
                    "batch_size",
                )
                if data.get(k) is not None
            },
            ensure_ascii=False,
        ),
        "archived_at": archived_at,
        "batch_id": str(data.get("batch_id", "") or ""),
    }


def archive_experiment_to_db(
    exp_data: Dict[str, Any], dsn: Optional[str] = None
) -> bool:
    """Insert one experiment into ``exp_registry.archived_experiments``.

    *exp_data* must contain at least ``"name"``; the entire dict is stored as
    the ``data`` JSONB column.  Returns ``True`` on success.
    """
    name = str(exp_data.get("name", "")).strip()
    if not name:
        return False
    try:
        with get_conn(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO exp_registry.archived_experiments(name, data, archived_at) "
                    "VALUES (%s, %s, NOW())",
                    (name, json.dumps(exp_data, default=str, ensure_ascii=False)),
                )
        return True
    except Exception as e:
        print(f"[archive] archive_experiment_to_db error: {e}")
        return False


def export_archive_csv(
    output_path: Path,
    dsn: Optional[str] = None,
) -> int:
    """Export all rows from ``exp_registry.archived_experiments`` to *output_path* as CSV.

    Returns the number of rows written (excluding header).
    """
    import psycopg2.extras as _extras

    rows_written = 0
    try:
        with get_conn(dsn) as conn:
            with conn.cursor(cursor_factory=_extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT name, data, archived_at FROM exp_registry.archived_experiments "
                    "ORDER BY archived_at, id"
                )
                db_rows = cur.fetchall()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_ARCHIVE_CSV_COLUMNS)
            writer.writeheader()
            for row in db_rows:
                data = row["data"]
                if isinstance(data, str):
                    data = json.loads(data)
                if not isinstance(data, dict):
                    data = {}
                archived_at = (
                    row["archived_at"].isoformat()
                    if hasattr(row["archived_at"], "isoformat")
                    else str(row["archived_at"] or "")
                )
                writer.writerow(_extract_csv_fields(row["name"], data, archived_at))
                rows_written += 1
        return rows_written
    except Exception as e:
        print(f"[archive] export_archive_csv error: {e}")
        return rows_written


def archive_and_export(
    exp_data: Dict[str, Any],
    csv_path: Path,
    dsn: Optional[str] = None,
) -> bool:
    """Archive one experiment to DB then re-export the full CSV.

    Returns ``True`` if both the insert and the export succeed.
    """
    ok = archive_experiment_to_db(exp_data, dsn=dsn)
    if not ok:
        return False
    export_archive_csv(csv_path, dsn=dsn)
    return True
