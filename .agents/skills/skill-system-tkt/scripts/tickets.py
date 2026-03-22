#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[3]
CONFIG_DIR = ROOT_DIR / "config"
NOTE_TASKS_PATH = ROOT_DIR / "note" / "note_tasks.md"
CANDIDATE_NOTE_TASKS_PATHS = [
    ROOT_DIR / "note" / "note_tasks.md",
    ROOT_DIR / "Phase3" / "note" / "note_tasks.md",
]
MEMORY_MEM_PATH = ROOT_DIR / "skills" / "skill-system-memory" / "scripts" / "mem.py"

# Import shared error codes
try:
    from errors import SUBPROCESS_TIMEOUT, DB_CONNECT_TIMEOUT, SkillError
except ImportError:
    # Fallback if errors.py not on path — define minimal versions
    SUBPROCESS_TIMEOUT = 30
    DB_CONNECT_TIMEOUT = 10

    class SkillError(RuntimeError):  # type: ignore[no-redef]
        def __init__(self, code: str, message: str | None = None, *, details: dict[str, Any] | None = None):
            self.code = code
            self.details = details or {}
            super().__init__(message or code)

# Ticket ID format validation (H13)
TICKET_ID_PATTERN = re.compile(r'^[A-Za-z][A-Za-z0-9_-]{0,50}$')


def validate_ticket_id(ticket_id: str) -> str:
    """Validate and normalize a ticket ID."""
    tid = ticket_id.strip()
    if not TICKET_ID_PATTERN.match(tid):
        raise TicketError(
            "SK-TKT-014",
            f"Invalid ticket_id format: {ticket_id!r}",
            details={"ticket_id": ticket_id, "pattern": TICKET_ID_PATTERN.pattern},
        )
    return tid


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep merge override into base. Override values win for non-dict leaves."""
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


# Default schemas for each config file — new keys auto-populate on load
_CONFIG_DEFAULTS: dict[str, dict[str, Any]] = {
    "tkt.yaml": {
        "bundle": {"min_tickets": 2, "max_tickets": 6, "auto_audit": True},
        "ticket": {"states": ["open", "claimed", "in_progress", "done", "blocked", "failed"], "lock_timeout": 30},
        "roadmap": {"data_dir": ".tkt", "valid_stages": ["planning", "active", "review", "blocked", "done", "archived"]},
        "stale": {"idle_timeout_minutes": 60, "auto_revert": False},
        "enforcement": {"tdd_required": True, "run_structural": True, "run_pytest": True, "block_on_failure": True},
        "isolation": {"worktree_enabled": False, "worktree_dir": ".tkt/worktrees", "auto_merge_on_close": True, "merge_strategy": "no-ff"},
        "policy": {
            "collaboration": {
                "question_tool_first": True,
                "subagent_scope_enforced": True,
                "cross_ticket_loophole": "forbidden",
            },
            "method_laws": {
                "one_ticket_at_a_time": "One session may hold at most one active claimed ticket at a time.",
                "keep_picking_open_stale": "Before ending the round, if OPEN or STALE worker tickets remain claimable, continue claiming the next one.",
                "integrator_only_closure": "Only the integrator ticket may declare batch closure; worker tickets may close themselves but may never declare the batch complete.",
            },
            "question_tool": {"preferred": True, "shape": "2-4 options, one recommended option, single choice.", "fallback": "structured_single_choice_prompt"},
            "subagent": {"encouraged_when": "ticket decomposes cleanly", "scope_rule": "ticket-scoped", "cross_ticket_loophole": "forbidden"},
        },
    },
    "insight.yaml": {
        "observe": {"max_facets_per_day": 3, "confidence_threshold": 0.3},
        "evolve": {"max_passes_per_day": 1, "approval_drift_threshold": 0.5},
        "memory": {"half_life_days": 30, "embedding_dim": 1536, "rrf_k": 60, "decay_applies_to": "episodic"},
    },
    "cli.yaml": {
        "output": {"default_format": "json"},
        "init": {"auto_scaffold": False, "skip_postgres": False},
    },
}


def load_config(name: str) -> dict[str, Any]:
    """Load a YAML config file from config/, merged with schema defaults.

    New keys from _CONFIG_DEFAULTS auto-populate so existing configs
    always have complete schemas without manual migration.
    Returns defaults if file does not exist (normal fallback).
    Raises SkillError if file exists but cannot be parsed (C4 fix).
    """
    defaults = _CONFIG_DEFAULTS.get(name, {})
    cfg_path = CONFIG_DIR / name
    if not cfg_path.exists():
        return defaults.copy() if defaults else {}
    try:
        import yaml  # type: ignore

        user_cfg = yaml.safe_load(cfg_path.read_text()) or {}
    except ImportError:
        raise SkillError("SK-SYS-002", "PyYAML is required: pip install pyyaml")
    except Exception as exc:
        if "yaml" in type(exc).__module__.lower():
            raise SkillError("SK-CFG-001", f"Config file {name} has invalid YAML: {exc}")
        raise SkillError("SK-SYS-004", f"Cannot read config file {name}: {exc}")

    if not defaults:
        return user_cfg
    return _deep_merge(defaults, user_cfg)


def _get_policy(key: str, default: Any) -> Any:
    """Read a policy value from config/tkt.yaml, falling back to default."""
    cfg = load_config("tkt.yaml")
    policy = cfg.get("policy", {})
    return policy.get(key, default)


# --- Structural constants (not user-tunable) ---

STATUS_MAP = {
    "open": "OPEN",
    "in_progress": "CLAIMED",
    "blocked": "BLOCKED",
    "closed": "CLOSED",
    "stale": "STALE",
}
ACTIVE_TICKET_STATUSES = {"CLAIMED", "BLOCKED"}

# --- Policy constants (config/tkt.yaml is the single source of truth) ---

_DEFAULT_COLLABORATION_POLICY = {
    "question_tool_first": True,
    "subagent_scope_enforced": True,
    "cross_ticket_loophole": "forbidden",
}
_DEFAULT_METHOD_LAWS = {
    "one_ticket_at_a_time": "One session may hold at most one active claimed ticket at a time.",
    "keep_picking_open_stale": "Before ending the round, if OPEN or STALE worker tickets remain claimable, continue claiming the next one.",
    "integrator_only_closure": "Only the integrator ticket may declare batch closure; worker tickets may close themselves but may never declare the batch complete.",
}
COLLABORATION_POLICY = _get_policy("collaboration", _DEFAULT_COLLABORATION_POLICY)
METHOD_LAWS = _get_policy("method_laws", _DEFAULT_METHOD_LAWS)
TICKET_SCOPE_RULES = {
    "TKT-003": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "spec/verify_batch_ticket_queue.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "claim semantics, claim reporting, and batch verification surfaces",
    },
    "TKT-004": {
        "allowed_prefixes": [
            "skills/skill-system-cockpit/",
            "skills/skill-system-review/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
            "spec/verify_batch_ticket_queue.py",
        ],
        "description": "batch-aware cockpit/review visibility and UI evidence surfaces",
    },
    "TKT-005": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-review/",
            "skills/skill-system-cockpit/",
            "spec/verify_startup_flow.py",
            "skills/skills-index.json",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "startup flow wiring across workflow, review, and cockpit",
    },
    "TKT-006": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-review/",
            "spec/verify_batch_ticket_queue.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "question-tool-first branching policy and subagent policy reporting",
    },
    "TKT-007": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "spec/verify_batch_ticket_queue.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "claim-loop semantics, stale handling, and worker versus integrator closure rules",
    },
    "TKT-008": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "spec/verify_batch_ticket_queue.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "integrator-only closure reporting and honest batch final status",
    },
    "TKT-009": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-review/",
            "review/templates/REVIEW_BUNDLE.md",
            "review/REVIEW_BUNDLE.md",
            "spec/verify_batch_ticket_queue.py",
            "note/note_feedback.md",
        ],
        "description": "workflow law codification, question-tool policy, and bounded subagent reporting",
    },
    "TKT-010": {
        "allowed_prefixes": [
            "skills/skill-system-cockpit/",
            "review/artifacts/tkt-010/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "fresh cockpit and startup UI evidence capture",
    },
    "TKT-011": {
        "allowed_prefixes": [
            "skills/skill-system-review/",
            "review/templates/REVIEW_BUNDLE.md",
            "review/REVIEW_BUNDLE.md",
            "review/REVIEW_AGENT_PROTOCOL.md",
            "spec/verify_batch_ticket_queue.py",
            "note/note_feedback.md",
        ],
        "description": "git status and source-of-truth reporting semantics",
    },
    "TKT-012": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-review/",
            "review/templates/REVIEW_BUNDLE.md",
            "review/REVIEW_BUNDLE.md",
            "spec/verify_batch_ticket_queue.py",
            "note/note_feedback.md",
        ],
        "description": "integrator acceptance and legal batch closure reporting",
    },
    "TKT-013": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "spec/verify_batch_ticket_queue.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "canonical note_tasks path resolution and refresh-new-tasks operation",
    },
    "TKT-014": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "spec/verify_batch_ticket_queue.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "pre-stop and pre-close refresh enforcement",
    },
    "TKT-015": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
            "spec/verify_batch_ticket_queue.py",
        ],
        "description": "integrator acceptance after post-refresh zero-claimable state",
    },
    "TKT-016": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "spec/verify_batch_ticket_queue.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "workflow-owned session loop runner and machine-readable loop state",
    },
    "TKT-017": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-review/",
            "review/REVIEW_BUNDLE.md",
            "spec/verify_batch_ticket_queue.py",
            "note/note_feedback.md",
        ],
        "description": "question-tool-first and bounded-subagent policy enforcement/reporting",
    },
    "TKT-018": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-review/",
            "review/REVIEW_BUNDLE.md",
            "spec/verify_batch_ticket_queue.py",
            "note/note_feedback.md",
        ],
        "description": "integrator closure through workflow-owned session loop legality",
    },
    "TKT-019": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-cockpit/",
            "spec/verify_batch_ticket_queue.py",
            "spec/verify_ticket_workflow_round.py",
            "spec/verify_cockpit_round.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "task provenance model, trusted default cockpit filtering, and honest hidden-count reporting",
    },
    "TKT-020": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-cockpit/",
            "spec/verify_batch_ticket_queue.py",
            "spec/verify_ticket_workflow_round.py",
            "spec/verify_startup_flow.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "verification/test task isolation and durable provenance tagging for verification-created fixtures",
    },
    "TKT-021": {
        "allowed_prefixes": [
            "skills/skill-system-cockpit/",
            "review/artifacts/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
            "spec/verify_batch_ticket_queue.py",
            "spec/verify_cockpit_round.py",
        ],
        "description": "batch-aware trusted task panel rendering and fresh UI evidence capture",
    },
    "TKT-022": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-review/",
            "skills/skill-system-cockpit/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
            "spec/verify_batch_ticket_queue.py",
            "spec/verify_cockpit_round.py",
        ],
        "description": "integrator acceptance and legal batch closure for cockpit task trustworthiness",
    },
    "TKT-023": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "spec/verify_batch_ticket_queue.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "canonical review inbox path resolution and parser for the actual current inbox shape",
    },
    "TKT-024": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "spec/verify_batch_ticket_queue.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "review inbox refresh and durable ticket ingestion with minimal dedup reporting",
    },
    "TKT-025": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "spec/verify_batch_ticket_queue.py",
            "spec/verify_startup_flow.py",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "dual-intake refresh law for session loop, startup, and integrator closure",
    },
    "TKT-026": {
        "allowed_prefixes": [
            "skills/skill-system-cockpit/",
            "skills/skill-system-review/",
            "review/artifacts/tkt-026/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "cockpit and review visibility for workflow-owned review inbox state",
    },
    "TKT-027": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-cockpit/",
            "skills/skill-system-review/",
            "review/artifacts/tkt-026/",
            "review/artifacts/tkt-027/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
            "spec/verify_batch_ticket_queue.py",
            "spec/verify_startup_flow.py",
        ],
        "description": "integrator acceptance and final bundle for legal dual-intake batch closure",
    },
    "TKT-028": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-cockpit/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "generic profile-adapter contract and machine-readable adapter gap model",
    },
    "TKT-029": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-cockpit/",
            "skills/skill-system-review/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "structured onboarding decision surface for profile/watcher attachment gaps",
    },
    "TKT-030": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-review/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "bounded subagent playbook and ticket-level subagent decision reporting",
    },
    "TKT-031": {
        "allowed_prefixes": [
            "skills/skill-system-tkt/",
            "skills/skill-system-cockpit/",
            "skills/skill-system-review/",
            "review/artifacts/tkt-031/",
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
            "spec/verify_startup_flow.py",
            "spec/verify_cockpit_round.py",
        ],
        "description": "integrator acceptance, final artifacts, and git/source-of-truth sync for adoption-readiness batch",
    },
}
QUESTION_TOOL_POLICY = _get_policy("question_tool", {
    "preferred": True,
    "shape": "2-4 options, one recommended option, single choice.",
    "fallback": "structured_single_choice_prompt",
})
SUBAGENT_POLICY = _get_policy("subagent", {
    "encouraged_when": "ticket decomposes cleanly",
    "scope_rule": "every subagent must stay inside the currently claimed ticket scope",
    "cross_ticket_loophole": "forbidden",
})
SUBAGENT_PLAYBOOK = _get_policy("subagent_playbook", {
    "preferred_use_cases": [
        "parallel independent subtasks inside the currently claimed ticket",
        "narrow research or grep work that unblocks a claimed ticket decision",
        "isolated read-only verification slices for a claimed ticket",
    ],
    "non_use_cases": [
        "single cohesive code path in one local module",
        "cross-ticket work or anything that widens scope beyond the claimed ticket",
        "performative delegation when the parent session already has enough context",
    ],
    "reporting_contract": [
        "subagent_name",
        "purpose",
        "ticket_scope",
        "write_capability_used",
        "why_subagent_was_needed_or_not_needed",
    ],
})
TASK_PROVENANCE_WORKFLOW = "workflow"
TASK_PROVENANCE_VERIFICATION = "verification"
TASK_PROVENANCE_LEGACY = "legacy"
TASK_SURFACE_VISIBILITY_DEFAULT = "default"
TASK_SURFACE_VISIBILITY_HIDDEN = "hidden_by_default"


# Map legacy error codes to unified SK- codes
_LEGACY_CODE_MAP: dict[str, str] = {
    "MISSING_SESSION_ID": "SK-TKT-013",
    "SESSION_ALREADY_HAS_CLAIMED_TICKET": "SK-TKT-001",
    "UNKNOWN_TICKET": "SK-TKT-002",
    "TICKET_ALREADY_CLOSED": "SK-TKT-003",
    "TICKET_CLAIMED_BY_OTHER_SESSION": "SK-TKT-004",
    "TICKET_BLOCKED": "SK-TKT-005",
    "CLAIM_FAILED": "SK-TKT-006",
    "BLOCK_FAILED": "SK-TKT-007",
    "CLOSE_FAILED": "SK-TKT-008",
    "NO_ACTIVE_CLAIMED_TICKET": "SK-TKT-009",
    "TICKET_NOT_OWNED_BY_SESSION": "SK-TKT-010",
    "PRE_CLOSE_VERIFICATION_FAILED": "SK-TKT-011",
    "INTEGRATOR_CLOSE_NOT_READY": "SK-TKT-012",
    "MISSING_TICKET_FIELDS": "SK-TKT-014",
    "NOTE_TASKS_NOT_FOUND": "SK-TKT-015",
    "NO_TICKET_BLOCK": "SK-TKT-016",
    "NO_TICKET_BATCH": "SK-TKT-016",
    "INVALID_TICKET_BLOCK": "SK-TKT-016",
    "EMPTY_TICKET_BATCH": "SK-TKT-025",
    "PATH_ALIAS_CONFLICT": "SK-TKT-022",
    "REVIEW_INBOX_NOT_FOUND": "SK-TKT-023",
    "REVIEW_INBOX_PATH_CONFLICT": "SK-TKT-022",
    "REVIEW_INBOX_FORMAT_UNSUPPORTED": "SK-TKT-024",
}


class TicketError(RuntimeError):
    def __init__(
        self, code: str, message: str, *, details: dict[str, Any] | None = None
    ):
        super().__init__(message)
        self.code = code
        self.error_code = _LEGACY_CODE_MAP.get(code, code)  # unified SK- code
        self.details = details or {}


def load_module(module_path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_memory_module():
    return load_module(MEMORY_MEM_PATH, "workflow_mem")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stable_lock_keys(value: str) -> tuple[int, int]:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return (
        int.from_bytes(digest[:4], "big", signed=True),
        int.from_bytes(digest[4:8], "big", signed=True),
    )


def advisory_lock(conn: Any, scope: str, value: str) -> None:
    key_one, key_two = stable_lock_keys(f"{scope}:{value}")
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (key_one, key_two))


def parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    return {}


def infer_task_provenance(
    *,
    ticket_id: str,
    source: str | None,
    batch_id: str | None,
    explicit: str | None = None,
) -> str:
    normalized = (explicit or "").strip().lower()
    if normalized in {
        TASK_PROVENANCE_WORKFLOW,
        TASK_PROVENANCE_VERIFICATION,
        TASK_PROVENANCE_LEGACY,
    }:
        return normalized

    source_text = (source or "").strip().lower()
    ticket_text = ticket_id.strip().upper()
    batch_text = (batch_id or "").strip().upper()
    if (
        "verify" in source_text
        or ticket_text.endswith("-VERIFY")
        or "-VERIFY-" in ticket_text
        or batch_text.startswith("BATCH-VERIFY")
    ):
        return TASK_PROVENANCE_VERIFICATION
    return TASK_PROVENANCE_WORKFLOW


def normalize_task_surface_visibility(
    explicit: str | None, *, task_provenance: str
) -> str:
    normalized = (explicit or "").strip().lower()
    if normalized in {
        TASK_SURFACE_VISIBILITY_DEFAULT,
        TASK_SURFACE_VISIBILITY_HIDDEN,
    }:
        return normalized
    if task_provenance == TASK_PROVENANCE_VERIFICATION:
        return TASK_SURFACE_VISIBILITY_HIDDEN
    return TASK_SURFACE_VISIBILITY_DEFAULT


def infer_batch_task_provenance(
    existing_tickets: list[dict[str, Any]], *, batch_id: str | None
) -> str:
    for ticket in existing_tickets:
        metadata = ticket.get("metadata", {})
        explicit = metadata.get("task_provenance") or ticket.get("task_provenance")
        if explicit:
            return infer_task_provenance(
                ticket_id=ticket.get("ticket_id", batch_id or "BATCH"),
                source=ticket.get("source"),
                batch_id=ticket.get("batch_id") or batch_id,
                explicit=str(explicit),
            )
    return infer_task_provenance(
        ticket_id=batch_id or "BATCH",
        source="note_tasks_batch",
        batch_id=batch_id,
    )


def connect_workflow_db():
    mem = load_memory_module()
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:
        raise RuntimeError("psycopg2 is required") from exc

    db_target, target_source = mem.resolve_db_target()
    kwargs: dict[str, Any] = {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": db_target,
    }
    user = os.environ.get("PGUSER", "").strip()
    if user:
        kwargs["user"] = user
    kwargs["connect_timeout"] = DB_CONNECT_TIMEOUT
    try:
        conn = psycopg2.connect(**kwargs)
    except Exception as exc:
        raise TicketError(
            "SK-MEM-001",
            f"Database connection failed: {exc}",
            details={"host": kwargs.get("host"), "port": kwargs.get("port"), "dbname": db_target},
        )
    conn.autocommit = False
    return conn, db_target, target_source


def normalize_ticket_row(row: tuple[Any, ...]) -> dict[str, Any]:
    metadata = parse_metadata(row[8])
    status = STATUS_MAP.get(str(row[4]), str(row[4]).upper())
    task_provenance = infer_task_provenance(
        ticket_id=str(row[1]),
        source=metadata.get("source"),
        batch_id=metadata.get("batch_id"),
        explicit=metadata.get("task_provenance"),
    )
    return {
        "task_id": int(row[0]),
        "ticket_id": row[1],
        "title": row[2],
        "summary": row[3],
        "status": status,
        "created_at": str(row[5]),
        "claimed_at": metadata.get("claimed_at"),
        "closed_at": str(row[6]) if row[6] is not None else None,
        "claimed_by_session": metadata.get("claimed_by_session"),
        "source": metadata.get("source", "manual"),
        "ticket_type": metadata.get("ticket_type", "WORKER"),
        "workflow_state": metadata.get("workflow_state", status),
        "batch_id": metadata.get("batch_id"),
        "queue_order": metadata.get("queue_order"),
        "task_provenance": task_provenance,
        "task_surface_visibility": normalize_task_surface_visibility(
            metadata.get("task_surface_visibility"),
            task_provenance=task_provenance,
        ),
        "assignee": row[7],
        "metadata": metadata,
    }


def fetch_ticket(conn: Any, ticket_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_key, title, description, status, created_at, closed_at, assignee, metadata
            FROM agent_tasks
            WHERE deleted_at IS NULL
              AND metadata->>'kind' = 'workflow_ticket'
              AND task_key = %s
            LIMIT 1
            """,
            (ticket_id,),
        )
        row = cur.fetchone()
    return normalize_ticket_row(row) if row else None


def list_tickets(
    conn: Any, *, unresolved_only: bool = False, batch_id: str | None = None
) -> list[dict[str, Any]]:
    query = """
        SELECT id, task_key, title, description, status, created_at, closed_at, assignee, metadata
        FROM agent_tasks
        WHERE deleted_at IS NULL
          AND metadata->>'kind' = 'workflow_ticket'
    """
    params: list[Any] = []
    if unresolved_only:
        query += " AND status IN ('open', 'in_progress', 'blocked')"
    if batch_id is not None:
        query += " AND metadata->>'batch_id' = %s"
        params.append(batch_id)
    query += " ORDER BY COALESCE((metadata->>'queue_order')::int, 2147483647), created_at ASC"
    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
    return [normalize_ticket_row(row) for row in rows]


def require_session_id(explicit: str | None) -> str:
    for value in (
        explicit,
        os.environ.get("OPENCODE_SESSION_ID"),
        os.environ.get("SESSION_ID"),
        os.environ.get("OMO_SESSION_ID"),
        os.environ.get("POSH_SESSION_ID"),
        os.environ.get("XDG_SESSION_ID"),
    ):
        if value and value.strip():
            return value.strip()
    raise TicketError(
        "MISSING_SESSION_ID",
        "session_id is required for ticket ownership operations.",
    )


def parse_note_tasks_ticket(note_tasks_text: str) -> dict[str, str]:
    if "# TICKET" not in note_tasks_text:
        raise TicketError(
            "NO_TICKET_BLOCK", "note/note_tasks.md does not contain a # TICKET block."
        )

    lines = note_tasks_text.splitlines()
    fields: dict[str, str] = {}
    objective_lines: list[str] = []
    in_objective = False
    for line in lines:
        stripped = line.strip()
        if stripped == "# TICKET":
            continue
        if stripped.startswith("## "):
            in_objective = stripped == "## OBJECTIVE"
            continue
        if in_objective:
            if stripped:
                objective_lines.append(stripped)
            continue
        if ":" in stripped and not stripped.startswith("- "):
            key, value = stripped.split(":", 1)
            fields[key.strip()] = value.strip()

    if not fields.get("ticket_id") or not fields.get("title"):
        raise TicketError(
            "INVALID_TICKET_BLOCK", "Ticket block is missing ticket_id or title."
        )
    return {
        "ticket_id": fields["ticket_id"],
        "title": fields["title"],
        "summary": " ".join(objective_lines),
        "source": "note_tasks",
    }


def parse_note_tasks_batch(note_tasks_text: str) -> dict[str, Any]:
    if "# TICKET_BATCH" not in note_tasks_text:
        raise TicketError(
            "NO_TICKET_BATCH",
            "note/note_tasks.md does not contain a # TICKET_BATCH block.",
        )

    batch_id_match = re_search(r"^batch_id:\s*(.+)$", note_tasks_text)
    mode_match = re_search(r"^mode:\s*(.+)$", note_tasks_text)
    status_match = re_search(r"^status:\s*(.+)$", note_tasks_text)
    ticket_pattern = re.compile(
        r"^###\s+(TKT-[A-Za-z0-9-]+)\n(?P<body>.*?)(?=^###\s+TKT-[A-Za-z0-9-]+|^##\s+[A-Z_]+|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    tickets: list[dict[str, Any]] = []
    for index, match in enumerate(ticket_pattern.finditer(note_tasks_text), start=1):
        body = match.group("body")
        title_match = re_search(r"^title:\s*(.+)$", body)
        type_match = re_search(r"^type:\s*(.+)$", body)
        status_match_ticket = re_search(r"^status:\s*(.+)$", body)
        objective = extract_block(body, "objective")
        summary = " ".join(
            line[1:].strip() if line.startswith("-") else line.strip()
            for line in objective
            if line.strip()
        )
        tickets.append(
            {
                "ticket_id": match.group(1).strip(),
                "title": title_match.strip() if title_match else "<unknown>",
                "summary": summary,
                "source": "note_tasks_batch",
                "ticket_type": type_match.strip() if type_match else "WORKER",
                "batch_id": batch_id_match.strip() if batch_id_match else None,
                "queue_order": index,
                "requested_status": status_match_ticket.strip()
                if status_match_ticket
                else "OPEN",
            }
        )
    if not tickets:
        raise TicketError(
            "EMPTY_TICKET_BATCH", "No tickets were found in # TICKET_BATCH."
        )
    return {
        "batch_id": batch_id_match.strip() if batch_id_match else "<unknown>",
        "mode": mode_match.strip() if mode_match else "<unknown>",
        "status": status_match.strip() if status_match else "<unknown>",
        "tickets": tickets,
    }


def re_search(pattern: str, text: str) -> str | None:
    import re

    match = re.search(pattern, text, re.MULTILINE)
    return match.group(1) if match else None


def extract_block(body: str, key: str) -> list[str]:
    lines = body.splitlines()
    collected: list[str] = []
    in_block = False
    prefix = f"{key}:"
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(prefix):
            in_block = True
            remainder = stripped[len(prefix) :].strip()
            if remainder:
                collected.append(remainder)
            continue
        if in_block:
            if stripped.startswith("required_work:") or stripped.startswith(
                "acceptance:"
            ):
                break
            if stripped:
                collected.append(stripped)
    return collected


def resolve_note_tasks_path(
    candidate_paths: list[Path] | None = None,
) -> dict[str, Any]:
    candidates = candidate_paths or list(CANDIDATE_NOTE_TASKS_PATHS)
    existing = [path for path in candidates if path.exists()]
    if not existing:
        raise TicketError(
            "NOTE_TASKS_NOT_FOUND",
            "No canonical note_tasks path was found.",
            details={"candidate_paths": [str(path) for path in candidates]},
        )
    if len(existing) == 1:
        return {
            "canonical_path": existing[0],
            "path_resolution_status": "RESOLVED_SINGLE_PATH",
            "candidate_paths": [str(path) for path in candidates],
            "existing_paths": [str(path) for path in existing],
        }

    first, second = existing[:2]
    try:
        same_file = first.samefile(second)
    except OSError:
        same_file = first.resolve() == second.resolve()
    if same_file:
        canonical = first.resolve()
        return {
            "canonical_path": canonical,
            "path_resolution_status": "RESOLVED_SHARED_ALIAS",
            "candidate_paths": [str(path) for path in candidates],
            "existing_paths": [str(path) for path in existing],
        }
    raise TicketError(
        "PATH_ALIAS_CONFLICT",
        "Multiple distinct note_tasks paths exist; refusing to guess the canonical intake file.",
        details={
            "candidate_paths": [str(path) for path in candidates],
            "existing_paths": [str(path) for path in existing],
        },
    )


def read_note_tasks_document(
    candidate_paths: list[Path] | None = None,
) -> tuple[dict[str, Any], str]:
    resolution = resolve_note_tasks_path(candidate_paths)
    canonical_path = Path(resolution["canonical_path"])
    return resolution, canonical_path.read_text(encoding="utf-8")


def extract_review_agent_inbox_section(note_tasks_text: str) -> str:
    start_match = re.search(
        r"^##\s+Review Agent Inbox\s*$", note_tasks_text, re.MULTILINE
    )
    if not start_match:
        return ""
    return note_tasks_text[start_match.end() :].strip()


def resolve_review_inbox_path(
    candidate_paths: list[Path] | None = None,
) -> dict[str, Any]:
    candidates = candidate_paths or list(CANDIDATE_NOTE_TASKS_PATHS)
    inbox_sources: list[Path] = []
    for path in candidates:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        if extract_review_agent_inbox_section(text):
            inbox_sources.append(path)
    if not inbox_sources:
        raise TicketError(
            "REVIEW_INBOX_NOT_FOUND",
            "No canonical review inbox path was found.",
            details={"candidate_paths": [str(path) for path in candidates]},
        )
    if len(inbox_sources) == 1:
        return {
            "canonical_path": inbox_sources[0],
            "path_resolution_status": "RESOLVED_SINGLE_INBOX_PATH",
            "candidate_paths": [str(path) for path in candidates],
            "existing_paths": [str(path) for path in inbox_sources],
        }

    first, second = inbox_sources[:2]
    try:
        same_file = first.samefile(second)
    except OSError:
        same_file = first.resolve() == second.resolve()
    if same_file:
        canonical = first.resolve()
        return {
            "canonical_path": canonical,
            "path_resolution_status": "RESOLVED_SHARED_INBOX_ALIAS",
            "candidate_paths": [str(path) for path in candidates],
            "existing_paths": [str(path) for path in inbox_sources],
        }

    raise TicketError(
        "REVIEW_INBOX_PATH_CONFLICT",
        "Multiple distinct review inbox paths exist; refusing to guess the canonical inbox source.",
        details={
            "candidate_paths": [str(path) for path in candidates],
            "existing_paths": [str(path) for path in inbox_sources],
        },
    )


def parse_review_agent_inbox(note_tasks_text: str) -> dict[str, Any]:
    inbox_section = extract_review_agent_inbox_section(note_tasks_text)
    if not inbox_section:
        raise TicketError(
            "REVIEW_INBOX_NOT_FOUND",
            "The note_tasks document does not contain a Review Agent Inbox section.",
        )
    if inbox_section.startswith("# TICKET_BATCH"):
        return {
            "parser_shape": "embedded_ticket_batch_in_note_tasks",
            "source_kind": "ticket_batch",
            "batch": parse_note_tasks_batch(inbox_section),
        }
    raise TicketError(
        "REVIEW_INBOX_FORMAT_UNSUPPORTED",
        "Review Agent Inbox exists but its current shape is not supported by the deterministic parser.",
        details={"preview": inbox_section.splitlines()[:5]},
    )


def extract_new_section(note_tasks_text: str) -> str:
    start_match = re.search(r"^###\s+New\s*$", note_tasks_text, re.MULTILINE)
    if not start_match:
        return ""
    start = start_match.end()
    end_positions = [
        match.start()
        for pattern in (
            r"^###\s+Request\s*$",
            r"^###\s+Asking Question",
            r"^##\s+Review Agent Inbox\s*$",
            r"^#\s+TICKET_BATCH\s*$",
            r"^#\s+TICKET\s*$",
        )
        for match in re.finditer(pattern, note_tasks_text[start:], re.MULTILINE)
    ]
    end = start + min(end_positions) if end_positions else len(note_tasks_text)
    return note_tasks_text[start:end].strip()


def build_simple_new_ticket_id(batch_id: str | None, item: str, index: int) -> str:
    digest = (
        hashlib.sha1(
            f"{batch_id or 'NO_BATCH'}::{index}::{item.strip()}".encode("utf-8")
        )
        .hexdigest()[:8]
        .upper()
    )
    return f"TKT-NEW-{digest}"


def parse_new_section_items(
    note_tasks_text: str,
    *,
    batch_id: str | None,
    starting_queue_order: int,
) -> list[dict[str, Any]]:
    new_section = extract_new_section(note_tasks_text)
    if not new_section:
        return []

    explicit_ticket_pattern = re.compile(
        r"^###\s+(TKT-[A-Za-z0-9-]+)\n(?P<body>.*?)(?=^###\s+TKT-[A-Za-z0-9-]+|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    parsed: list[dict[str, Any]] = []
    consumed_ranges: list[tuple[int, int]] = []
    queue_order = starting_queue_order

    for match in explicit_ticket_pattern.finditer(new_section):
        consumed_ranges.append(match.span())
        body = match.group("body")
        title_match = re_search(r"^title:\s*(.+)$", body)
        type_match = re_search(r"^type:\s*(.+)$", body)
        status_match = re_search(r"^status:\s*(.+)$", body)
        objective = extract_block(body, "objective")
        summary = " ".join(
            line[1:].strip() if line.startswith("-") else line.strip()
            for line in objective
            if line.strip()
        )
        parsed.append(
            {
                "ticket_id": match.group(1).strip(),
                "title": title_match.strip() if title_match else "<unknown>",
                "summary": summary,
                "source": "note_tasks_new_section",
                "ticket_type": type_match.strip() if type_match else "WORKER",
                "batch_id": batch_id,
                "queue_order": queue_order,
                "requested_status": status_match.strip() if status_match else "OPEN",
                "item_type": "explicit_ticket",
            }
        )
        queue_order += 1

    residual_lines: list[str] = []
    cursor = 0
    for start, end in consumed_ranges:
        residual_lines.append(new_section[cursor:start])
        cursor = end
    residual_lines.append(new_section[cursor:])
    residual_text = "\n".join(residual_lines)
    bullet_items = [
        line[1:].strip() if line.strip().startswith("-") else line.strip()
        for line in residual_text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    for index, item in enumerate(bullet_items, start=1):
        parsed.append(
            {
                "ticket_id": build_simple_new_ticket_id(batch_id, item, index),
                "title": item[:120],
                "summary": item,
                "source": "note_tasks_new_section",
                "ticket_type": "WORKER",
                "batch_id": batch_id,
                "queue_order": queue_order,
                "requested_status": "OPEN",
                "item_type": "simple_task",
            }
        )
        queue_order += 1
    return parsed


def persist_refresh_result(
    conn: Any, *, batch_id: str | None, refresh_result: dict[str, Any]
) -> None:
    payload = json.dumps(refresh_result)
    with conn.cursor() as cur:
        if batch_id is not None:
            cur.execute(
                """
                UPDATE agent_tasks
                SET metadata = metadata
                    || jsonb_build_object('latest_refresh', %s::jsonb)
                    || jsonb_build_object(
                        'refresh_history',
                        COALESCE(metadata->'refresh_history', '[]'::jsonb) || jsonb_build_array(%s::jsonb)
                    )
                WHERE deleted_at IS NULL
                  AND metadata->>'kind' = 'workflow_ticket'
                  AND metadata->>'batch_id' = %s
                """,
                (payload, payload, batch_id),
            )
        else:
            cur.execute(
                """
                UPDATE agent_tasks
                SET metadata = metadata
                    || jsonb_build_object('latest_refresh', %s::jsonb)
                    || jsonb_build_object(
                        'refresh_history',
                        COALESCE(metadata->'refresh_history', '[]'::jsonb) || jsonb_build_array(%s::jsonb)
                    )
                WHERE deleted_at IS NULL
                  AND metadata->>'kind' = 'workflow_ticket'
                """,
                (payload, payload),
            )


def persist_ticket_refresh_snapshot(
    conn: Any, *, ticket_id: str, field_name: str, refresh_status: dict[str, Any]
) -> None:
    payload = json.dumps(refresh_status)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE agent_tasks
            SET metadata = metadata || jsonb_build_object(%s, %s::jsonb)
            WHERE deleted_at IS NULL
              AND metadata->>'kind' = 'workflow_ticket'
              AND task_key = %s
            """,
            (field_name, payload, ticket_id),
        )


def persist_review_inbox_refresh_result(
    conn: Any, *, batch_id: str | None, refresh_result: dict[str, Any]
) -> None:
    payload = json.dumps(refresh_result)
    with conn.cursor() as cur:
        if batch_id is not None:
            cur.execute(
                """
                UPDATE agent_tasks
                SET metadata = metadata
                    || jsonb_build_object('latest_review_inbox_refresh', %s::jsonb)
                    || jsonb_build_object(
                        'review_inbox_refresh_history',
                        COALESCE(metadata->'review_inbox_refresh_history', '[]'::jsonb) || jsonb_build_array(%s::jsonb)
                    )
                WHERE deleted_at IS NULL
                  AND metadata->>'kind' = 'workflow_ticket'
                  AND metadata->>'batch_id' = %s
                """,
                (payload, payload, batch_id),
            )
        else:
            cur.execute(
                """
                UPDATE agent_tasks
                SET metadata = metadata
                    || jsonb_build_object('latest_review_inbox_refresh', %s::jsonb)
                    || jsonb_build_object(
                        'review_inbox_refresh_history',
                        COALESCE(metadata->'review_inbox_refresh_history', '[]'::jsonb) || jsonb_build_array(%s::jsonb)
                    )
                WHERE deleted_at IS NULL
                  AND metadata->>'kind' = 'workflow_ticket'
                """,
                (payload, payload),
            )


def refresh_review_inbox(
    conn: Any,
    *,
    trigger_point: str = "manual",
    batch_id: str | None = None,
    candidate_paths: list[Path] | None = None,
) -> dict[str, Any]:
    resolution = resolve_review_inbox_path(candidate_paths)
    canonical_path = Path(resolution["canonical_path"])
    note_tasks_text = canonical_path.read_text(encoding="utf-8")
    parsed = parse_review_agent_inbox(note_tasks_text)
    batch = parsed["batch"]
    resolved_batch_id = batch_id or batch.get("batch_id")

    consumed_or_pending: list[dict[str, Any]] = []
    newly_ingested: list[dict[str, Any]] = []
    for item in batch["tickets"]:
        existing_ticket = fetch_ticket(conn, item["ticket_id"])
        if existing_ticket is not None:
            consumed_or_pending.append(
                {
                    "inbox_item_id": item["ticket_id"],
                    "ticket_id": item["ticket_id"],
                    "state": "already_present",
                }
            )
            continue

        ticket = intake_ticket(
            conn,
            ticket_id=item["ticket_id"],
            title=item["title"],
            summary=item["summary"],
            source="review_agent_inbox",
            ticket_type=item.get("ticket_type"),
            workflow_state=item.get("requested_status"),
            batch_id=resolved_batch_id,
            queue_order=item.get("queue_order"),
        )
        newly_ingested.append(ticket)
        consumed_or_pending.append(
            {
                "inbox_item_id": item["ticket_id"],
                "ticket_id": ticket["ticket_id"],
                "state": "ingested",
            }
        )

    refresh_result = {
        "canonical_inbox_path": str(canonical_path),
        "path_resolution_status": resolution["path_resolution_status"],
        "candidate_paths": resolution["candidate_paths"],
        "existing_paths": resolution["existing_paths"],
        "parser_shape": parsed["parser_shape"],
        "refresh_entrypoint": "python3 skills/skill-system-workflow/scripts/tickets.py refresh-review-inbox",
        "refresh_trigger_point": trigger_point,
        "latest_review_inbox_check_at": now_utc_iso(),
        "new_inbox_items_detected": [
            {
                "inbox_item_id": item["ticket_id"],
                "title": item["title"],
                "ticket_type": item.get("ticket_type", "WORKER"),
            }
            for item in batch["tickets"]
        ],
        "new_tickets_ingested": [ticket["ticket_id"] for ticket in newly_ingested],
        "consumed_or_pending_inbox_items": consumed_or_pending,
    }
    persist_review_inbox_refresh_result(
        conn, batch_id=resolved_batch_id, refresh_result=refresh_result
    )
    return refresh_result


def review_inbox_ticket_ids(refresh_status: dict[str, Any]) -> set[str]:
    ticket_ids = {
        str(item.get("ticket_id"))
        for item in refresh_status.get("consumed_or_pending_inbox_items", [])
        if item.get("ticket_id")
    }
    ticket_ids.update(
        str(item.get("inbox_item_id"))
        for item in refresh_status.get("new_inbox_items_detected", [])
        if item.get("inbox_item_id")
    )
    return ticket_ids


def derive_review_inbox_claimable_worker_tickets(
    summary: dict[str, Any], refresh_status: dict[str, Any]
) -> list[dict[str, Any]]:
    inbox_ticket_ids = review_inbox_ticket_ids(refresh_status)
    return [
        ticket
        for ticket in summary.get("claimable_worker_tickets", [])
        if ticket.get("ticket_id") in inbox_ticket_ids
    ]


def refresh_new_tasks(
    conn: Any,
    *,
    trigger_point: str = "manual",
    batch_id: str | None = None,
    candidate_paths: list[Path] | None = None,
) -> dict[str, Any]:
    resolution, note_tasks_text = read_note_tasks_document(candidate_paths)
    canonical_path = Path(resolution["canonical_path"])
    base_payload = ensure_note_tasks_tickets(
        conn,
        note_tasks_text=note_tasks_text,
        note_tasks_path=canonical_path,
    )
    resolved_batch_id = batch_id or base_payload.get("batch_id")
    existing_tickets = list_tickets(
        conn, unresolved_only=False, batch_id=resolved_batch_id
    )
    batch_task_provenance = infer_batch_task_provenance(
        existing_tickets, batch_id=resolved_batch_id
    )
    batch_task_surface_visibility = normalize_task_surface_visibility(
        None, task_provenance=batch_task_provenance
    )
    next_queue_order = (
        max((ticket.get("queue_order") or 0) for ticket in existing_tickets) + 1
        if existing_tickets
        else 1
    )
    new_items = parse_new_section_items(
        note_tasks_text,
        batch_id=resolved_batch_id,
        starting_queue_order=next_queue_order,
    )
    ingested: list[dict[str, Any]] = []
    newly_ingested: list[dict[str, Any]] = []
    for item in new_items:
        existed_before = fetch_ticket(conn, item["ticket_id"]) is not None
        ticket = intake_ticket(
            conn,
            ticket_id=item["ticket_id"],
            title=item["title"],
            summary=item["summary"],
            source=item["source"],
            ticket_type=item.get("ticket_type"),
            workflow_state=item.get("requested_status"),
            batch_id=resolved_batch_id,
            queue_order=item.get("queue_order"),
            task_provenance=batch_task_provenance,
            task_surface_visibility=batch_task_surface_visibility,
        )
        ingested.append(ticket)
        if not existed_before:
            newly_ingested.append(ticket)
    refreshed_summary = summarize_claim_ownership(conn, batch_id=resolved_batch_id)
    ingested_claimable_worker_tickets = [
        ticket["ticket_id"]
        for ticket in newly_ingested
        if ticket.get("ticket_type", "WORKER") == "WORKER"
        and ticket.get("workflow_state", ticket.get("status")) in {"OPEN", "STALE"}
    ]
    refresh_result = {
        "canonical_note_tasks_path": str(canonical_path),
        "path_resolution_status": resolution["path_resolution_status"],
        "candidate_paths": resolution["candidate_paths"],
        "existing_paths": resolution["existing_paths"],
        "refresh_entrypoint": "python3 skills/skill-system-workflow/scripts/tickets.py refresh-new-tasks",
        "refresh_trigger_point": trigger_point,
        "latest_new_check_at": now_utc_iso(),
        "new_items_detected": [
            {
                "ticket_id": item["ticket_id"],
                "title": item["title"],
                "item_type": item["item_type"],
            }
            for item in new_items
        ],
        "new_tickets_ingested": [ticket["ticket_id"] for ticket in newly_ingested],
        "ingested_claimable_worker_tickets": ingested_claimable_worker_tickets,
        "continue_due_to_new_policy": bool(ingested_claimable_worker_tickets),
    }
    persist_refresh_result(
        conn, batch_id=resolved_batch_id, refresh_result=refresh_result
    )
    return refresh_result


def build_method_laws_snapshot() -> dict[str, str]:
    return {
        **METHOD_LAWS,
        "question_tool_policy": COLLABORATION_POLICY["question_tool_policy"],
        "question_tool_prompt_shape": COLLABORATION_POLICY[
            "question_tool_prompt_shape"
        ],
        "fallback_policy": COLLABORATION_POLICY["fallback_policy"],
        "subagent_policy": COLLABORATION_POLICY["subagent_policy"],
        "cross_ticket_loophole_policy": COLLABORATION_POLICY[
            "cross_ticket_loophole_policy"
        ],
    }


def collect_subagent_usage(
    tickets: list[dict[str, Any]], *, scope_ticket_id: str | None = None
) -> list[dict[str, Any]]:
    usage_sources = (
        [ticket for ticket in tickets if ticket.get("ticket_id") == scope_ticket_id]
        if scope_ticket_id
        else tickets
    )
    collected: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for ticket in usage_sources:
        for item in ticket.get("metadata", {}).get("subagent_usage", []):
            normalized = {
                "subagent_name": item.get("subagent_name", "<unknown>"),
                "purpose": item.get("purpose", "<unknown>"),
                "ticket_scope": item.get(
                    "ticket_scope",
                    scope_ticket_id or ticket.get("ticket_id", "<unknown>"),
                ),
                "write_capability_used": item.get("write_capability_used", "no"),
            }
            key = (
                normalized["subagent_name"],
                normalized["purpose"],
                normalized["ticket_scope"],
                normalized["write_capability_used"],
            )
            if key not in seen:
                seen.add(key)
                collected.append(normalized)
    return collected


def build_usage_reporting(
    tickets: list[dict[str, Any]], scope_ticket_id: str | None
) -> dict[str, Any]:
    subagent_usage = collect_subagent_usage(tickets, scope_ticket_id=scope_ticket_id)
    return {
        "question_tool_used": "no-branch-decision-required",
        "question_tool_prompt_shape": COLLABORATION_POLICY[
            "question_tool_prompt_shape"
        ],
        "scope_ticket_id": scope_ticket_id,
        "subagent_usage": subagent_usage,
        "subagent_playbook": dict(SUBAGENT_PLAYBOOK),
        "why_subagent_was_needed_or_not_needed": (
            "Subagents were needed because the claimed ticket was decomposed into bounded helper tasks inside the same ticket scope."
            if subagent_usage
            else "No subagent was needed because the claimed ticket work stayed inside one cohesive code path."
        ),
    }


def ensure_note_tasks_ticket(
    conn: Any,
    *,
    note_tasks_text: str | None = None,
    note_tasks_path: Path | None = None,
    task_provenance: str | None = None,
    task_surface_visibility: str | None = None,
) -> dict[str, Any]:
    if note_tasks_text is None:
        _, note_tasks_text = read_note_tasks_document()
    ticket = parse_note_tasks_ticket(note_tasks_text)
    return intake_ticket(
        conn,
        ticket_id=ticket["ticket_id"],
        title=ticket["title"],
        summary=ticket["summary"],
        source=ticket["source"],
        task_provenance=task_provenance,
        task_surface_visibility=task_surface_visibility,
    )


def ensure_note_tasks_tickets(
    conn: Any,
    *,
    note_tasks_text: str | None = None,
    note_tasks_path: Path | None = None,
    task_provenance: str | None = None,
    task_surface_visibility: str | None = None,
) -> dict[str, Any]:
    if note_tasks_text is None:
        _, note_tasks_text = read_note_tasks_document()
    if "# TICKET_BATCH" in note_tasks_text:
        batch = parse_note_tasks_batch(note_tasks_text)
        tickets = [
            intake_ticket(
                conn,
                ticket_id=ticket["ticket_id"],
                title=ticket["title"],
                summary=ticket["summary"],
                source=ticket["source"],
                ticket_type=ticket.get("ticket_type"),
                workflow_state=ticket.get("requested_status"),
                batch_id=batch["batch_id"],
                queue_order=ticket["queue_order"],
                task_provenance=task_provenance,
                task_surface_visibility=task_surface_visibility,
            )
            for ticket in batch["tickets"]
        ]
        return {
            "batch_id": batch["batch_id"],
            "mode": batch["mode"],
            "status": batch["status"],
            "tickets": tickets,
        }
    ticket = ensure_note_tasks_ticket(
        conn,
        note_tasks_text=note_tasks_text,
        note_tasks_path=note_tasks_path,
        task_provenance=task_provenance,
        task_surface_visibility=task_surface_visibility,
    )
    return {
        "batch_id": None,
        "mode": "single-ticket",
        "status": ticket["status"],
        "tickets": [ticket],
    }


def intake_ticket(
    conn: Any,
    *,
    ticket_id: str,
    title: str,
    summary: str,
    source: str,
    ticket_type: str | None = None,
    workflow_state: str | None = None,
    batch_id: str | None = None,
    queue_order: int | None = None,
    task_provenance: str | None = None,
    task_surface_visibility: str | None = None,
) -> dict[str, Any]:
    effective_task_provenance = infer_task_provenance(
        ticket_id=ticket_id,
        source=source,
        batch_id=batch_id,
        explicit=task_provenance,
    )
    effective_task_surface_visibility = normalize_task_surface_visibility(
        task_surface_visibility,
        task_provenance=effective_task_provenance,
    )
    existing = fetch_ticket(conn, ticket_id)
    if existing is not None:
        if batch_id is None and queue_order is None:
            return existing
        effective_workflow_state = (
            existing["status"]
            if existing["status"] in {"CLAIMED", "BLOCKED", "CLOSED"}
            else workflow_state or existing.get("workflow_state") or existing["status"]
        )
        effective_existing_provenance = infer_task_provenance(
            ticket_id=ticket_id,
            source=source,
            batch_id=batch_id,
            explicit=existing.get("metadata", {}).get("task_provenance")
            or task_provenance,
        )
        effective_existing_visibility = normalize_task_surface_visibility(
            existing.get("metadata", {}).get("task_surface_visibility")
            or task_surface_visibility,
            task_provenance=effective_existing_provenance,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agent_tasks
                SET metadata = metadata || jsonb_build_object(
                    'kind', 'workflow_ticket',
                    'ticket_id', task_key,
                    'source', %s,
                    'ticket_type', %s,
                    'workflow_state', %s,
                    'batch_id', %s,
                    'queue_order', %s,
                    'task_provenance', %s,
                    'task_surface_visibility', %s
                )
                WHERE deleted_at IS NULL
                  AND metadata->>'kind' = 'workflow_ticket'
                  AND task_key = %s
                RETURNING id, task_key, title, description, status, created_at, closed_at, assignee, metadata
                """,
                (
                    source,
                    ticket_type,
                    effective_workflow_state,
                    batch_id,
                    queue_order,
                    effective_existing_provenance,
                    effective_existing_visibility,
                    ticket_id,
                ),
            )
            row = cur.fetchone()
        return normalize_ticket_row(row)

    metadata = json.dumps(
        {
            "kind": "workflow_ticket",
            "ticket_id": ticket_id,
            "source": source,
            "ticket_type": ticket_type,
            "workflow_state": workflow_state or "OPEN",
            "batch_id": batch_id,
            "queue_order": queue_order,
            "claimed_at": None,
            "claimed_by_session": None,
            "task_provenance": effective_task_provenance,
            "task_surface_visibility": effective_task_surface_visibility,
        }
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_tasks (task_key, title, description, status, created_by, metadata)
            VALUES (%s, %s, %s, 'open', 'workflow-ticket', %s::jsonb)
            RETURNING id, task_key, title, description, status, created_at, closed_at, assignee, metadata
            """,
            (ticket_id, title, summary, metadata),
        )
        row = cur.fetchone()
    return normalize_ticket_row(row)


def claim_ticket(conn: Any, *, ticket_id: str, session_id: str) -> dict[str, Any]:
    session_id = require_session_id(session_id)
    advisory_lock(conn, "session", session_id)
    advisory_lock(conn, "ticket", ticket_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT task_key
            FROM agent_tasks
            WHERE deleted_at IS NULL
              AND metadata->>'kind' = 'workflow_ticket'
              AND status IN ('in_progress', 'blocked')
              AND metadata->>'claimed_by_session' = %s
              AND task_key <> %s
            LIMIT 1
            """,
            (session_id, ticket_id),
        )
        conflicting = cur.fetchone()
        if conflicting is not None:
            raise TicketError(
                "SESSION_ALREADY_HAS_CLAIMED_TICKET",
                f"Session {session_id} already owns ticket {conflicting[0]}.",
                details={
                    "session_id": session_id,
                    "existing_ticket_id": conflicting[0],
                },
            )

        cur.execute(
            """
            SELECT id, task_key, title, description, status, created_at, closed_at, assignee, metadata
            FROM agent_tasks
            WHERE deleted_at IS NULL
              AND metadata->>'kind' = 'workflow_ticket'
              AND task_key = %s
            FOR UPDATE
            """,
            (ticket_id,),
        )
        target_row = cur.fetchone()
        if target_row is None:
            raise TicketError("UNKNOWN_TICKET", f"Ticket {ticket_id} was not found.")
        current_ticket = normalize_ticket_row(target_row)
        if current_ticket["status"] == "CLOSED":
            raise TicketError(
                "TICKET_ALREADY_CLOSED",
                f"Ticket {ticket_id} is already closed.",
                details={"ticket_id": ticket_id},
            )
        if (
            current_ticket["claimed_by_session"] == session_id
            and current_ticket["status"] in ACTIVE_TICKET_STATUSES
        ):
            return current_ticket
        if (
            current_ticket["claimed_by_session"] not in {None, session_id}
            and current_ticket["status"] in ACTIVE_TICKET_STATUSES
        ):
            raise TicketError(
                "TICKET_CLAIMED_BY_OTHER_SESSION",
                f"Ticket {ticket_id} is already claimed by another session.",
                details={
                    "ticket_id": ticket_id,
                    "claimed_by_session": current_ticket["claimed_by_session"],
                    "status": current_ticket["status"],
                },
            )
        if current_ticket["status"] == "BLOCKED":
            raise TicketError(
                "TICKET_BLOCKED",
                f"Ticket {ticket_id} is blocked and cannot be claimed.",
                details={"ticket_id": ticket_id},
            )

        cur.execute(
            """
            UPDATE agent_tasks
            SET assignee = %s,
                status = 'in_progress',
                metadata = metadata || jsonb_build_object(
                    'kind', 'workflow_ticket',
                    'ticket_id', task_key,
                    'claimed_by_session', %s,
                    'claimed_at', %s,
                    'workflow_state', 'CLAIMED'
                )
            WHERE deleted_at IS NULL
              AND metadata->>'kind' = 'workflow_ticket'
              AND task_key = %s
              AND status = 'open'
            RETURNING id, task_key, title, description, status, created_at, closed_at, assignee, metadata
            """,
            (session_id, session_id, now_utc_iso(), ticket_id),
        )
        row = cur.fetchone()

    if row is None:
        raise TicketError(
            "CLAIM_FAILED",
            f"Ticket {ticket_id} could not be claimed.",
            details={"ticket_id": ticket_id, "session_id": session_id},
        )
    ticket = normalize_ticket_row(row)

    # Create git worktree for isolation (Superpowers-inspired)
    # C2 fix: if worktree creation fails, rollback the claim
    worktree_result = create_worktree(ticket_id)
    if worktree_result.get("error_code") == "SK-GIT-001":
        # Rollback: revert ticket to open
        with conn.cursor() as rollback_cur:
            rollback_cur.execute(
                """UPDATE agent_tasks SET status = 'open',
                   metadata = metadata - 'claimed_by_session' - 'claimed_at' - 'workflow_state'
                   WHERE task_key = %s AND deleted_at IS NULL""",
                (ticket_id,),
            )
        raise TicketError(
            "SK-GIT-001",
            f"Worktree creation failed for {ticket_id}; claim rolled back.",
            details=worktree_result,
        )
    ticket["worktree"] = worktree_result

    return ticket


def block_ticket(
    conn: Any,
    *,
    ticket_id: str,
    session_id: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    session_value = require_session_id(session_id)
    owned_ticket = require_owned_active_ticket(
        conn, session_id=session_value, ticket_id=ticket_id
    )
    metadata_reason = reason or "blocked"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE agent_tasks
            SET status = 'blocked',
                metadata = metadata || jsonb_build_object(
                    'kind', 'workflow_ticket',
                    'ticket_id', task_key,
                    'claimed_by_session', %s,
                    'blocked_reason', %s,
                    'blocked_at', %s,
                    'workflow_state', 'BLOCKED'
                )
            WHERE deleted_at IS NULL
              AND metadata->>'kind' = 'workflow_ticket'
              AND task_key = %s
              AND status IN ('in_progress', 'blocked')
            RETURNING id, task_key, title, description, status, created_at, closed_at, assignee, metadata
            """,
            (session_value, metadata_reason, now_utc_iso(), owned_ticket["ticket_id"]),
        )
        row = cur.fetchone()
    if row is None:
        raise TicketError("BLOCK_FAILED", f"Ticket {ticket_id} could not be blocked.")
    return normalize_ticket_row(row)


def resolve_current_owned_ticket(conn: Any, *, session_id: str) -> str:
    session_id = require_session_id(session_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT task_key
            FROM agent_tasks
            WHERE deleted_at IS NULL
              AND metadata->>'kind' = 'workflow_ticket'
              AND status IN ('in_progress', 'blocked')
              AND metadata->>'claimed_by_session' = %s
            ORDER BY updated_at DESC
            LIMIT 2
            """,
            (session_id,),
        )
        rows = cur.fetchall()
    if not rows:
        raise TicketError(
            "NO_ACTIVE_CLAIMED_TICKET",
            f"Session {session_id} does not own an active claimed ticket.",
        )
    return rows[0][0]


def require_owned_active_ticket(
    conn: Any, *, session_id: str, ticket_id: str | None = None
) -> dict[str, Any]:
    resolved_ticket_id = ticket_id or resolve_current_owned_ticket(
        conn, session_id=session_id
    )
    session_id = require_session_id(session_id)
    advisory_lock(conn, "session", session_id)
    advisory_lock(conn, "ticket", resolved_ticket_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_key, title, description, status, created_at, closed_at, assignee, metadata
            FROM agent_tasks
            WHERE deleted_at IS NULL
              AND metadata->>'kind' = 'workflow_ticket'
              AND task_key = %s
            FOR UPDATE
            """,
            (resolved_ticket_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise TicketError(
            "UNKNOWN_TICKET", f"Ticket {resolved_ticket_id} was not found."
        )
    ticket = normalize_ticket_row(row)
    if (
        ticket["claimed_by_session"] != session_id
        or ticket["status"] not in ACTIVE_TICKET_STATUSES
    ):
        raise TicketError(
            "TICKET_NOT_OWNED_BY_SESSION",
            f"Ticket {resolved_ticket_id} is not actively owned by session {session_id}.",
            details={
                "ticket_id": resolved_ticket_id,
                "session_id": session_id,
                "claimed_by_session": ticket["claimed_by_session"],
                "status": ticket["status"],
            },
        )
    return ticket


def close_ticket(
    conn: Any,
    *,
    session_id: str,
    ticket_id: str | None = None,
    resolution: str | None = None,
) -> dict[str, Any]:
    owned_ticket = require_owned_active_ticket(
        conn, session_id=session_id, ticket_id=ticket_id
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE agent_tasks
            SET status = 'closed',
                closed_at = NOW(),
                metadata = metadata || jsonb_build_object(
                    'kind', 'workflow_ticket',
                    'ticket_id', task_key,
                    'closed_by_session', %s,
                    'resolution', %s,
                    'workflow_state', 'CLOSED'
                )
            WHERE deleted_at IS NULL
              AND metadata->>'kind' = 'workflow_ticket'
              AND task_key = %s
              AND status IN ('in_progress', 'blocked')
            RETURNING id, task_key, title, description, status, created_at, closed_at, assignee, metadata
            """,
            (session_id, resolution or "", owned_ticket["ticket_id"]),
        )
        row = cur.fetchone()
    if row is None:
        raise TicketError(
            "CLOSE_FAILED", f"Ticket {owned_ticket['ticket_id']} could not be closed."
        )
    return normalize_ticket_row(row)


def close_ticket_with_refresh(
    conn: Any,
    *,
    session_id: str,
    ticket_id: str | None = None,
    resolution: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_ticket_id = ticket_id or resolve_current_owned_ticket(
        conn, session_id=session_id
    )
    ticket = fetch_ticket(conn, resolved_ticket_id)
    closure_report = None
    review_inbox_refresh = None
    if ticket and ticket.get("ticket_type") == "INTEGRATOR":
        closure_report, refresh_status = build_integrator_closure_report_with_refresh(
            conn, session_id=session_id, ticket_id=resolved_ticket_id
        )
        review_inbox_refresh = closure_report.get("latest_review_inbox_refresh")
        if not closure_report.get("can_close_batch"):
            raise TicketError(
                "INTEGRATOR_CLOSE_NOT_READY",
                "Integrator closure is illegal because worker tickets remain unresolved after refresh.",
                details={
                    "ticket_id": resolved_ticket_id,
                    "refresh_status": refresh_status,
                    "closure_report": closure_report,
                },
            )
    else:
        refresh_status = refresh_new_tasks(
            conn,
            batch_id=(ticket or {}).get("batch_id"),
            trigger_point="close-ticket",
        )
    # Pre-close TDD verification (Superpowers-inspired)
    verification = run_pre_close_verification(resolved_ticket_id)
    cfg = load_config("tkt.yaml")
    enforcement = cfg.get("enforcement", {})
    if not verification.get("passed", True) and enforcement.get("block_on_failure", True):
        raise TicketError(
            "PRE_CLOSE_VERIFICATION_FAILED",
            f"Ticket {resolved_ticket_id} cannot be closed: tests did not pass.",
            details={"verification": verification},
        )

    closed = close_ticket(
        conn,
        session_id=session_id,
        ticket_id=resolved_ticket_id,
        resolution=resolution,
    )
    persist_ticket_refresh_snapshot(
        conn,
        ticket_id=resolved_ticket_id,
        field_name="close_refresh",
        refresh_status=refresh_status,
    )
    if ticket and ticket.get("ticket_type") == "INTEGRATOR":
        persist_ticket_refresh_snapshot(
            conn,
            ticket_id=resolved_ticket_id,
            field_name="integrator_close_refresh",
            refresh_status=refresh_status,
        )
        if review_inbox_refresh is not None:
            persist_ticket_refresh_snapshot(
                conn,
                ticket_id=resolved_ticket_id,
                field_name="integrator_close_review_inbox_refresh",
                refresh_status=review_inbox_refresh,
            )
    # Merge and cleanup git worktree (Superpowers-inspired)
    worktree_result = merge_and_cleanup_worktree(resolved_ticket_id)
    closed["worktree"] = worktree_result

    closed["latest_refresh"] = refresh_status
    closed["verification"] = verification
    if review_inbox_refresh is not None:
        closed["latest_review_inbox_refresh"] = review_inbox_refresh
    return closed, refresh_status


def check_open_tickets(
    conn: Any, *, session_id: str | None = None, batch_id: str | None = None
) -> dict[str, Any]:
    tickets = list_tickets(conn, unresolved_only=True, batch_id=batch_id)
    active_ticket = None
    if session_id:
        for ticket in tickets:
            if ticket["claimed_by_session"] == session_id and ticket["status"] in {
                "CLAIMED",
                "BLOCKED",
            }:
                active_ticket = ticket
                break
    return {
        "open_ticket_count": len(tickets),
        "active_ticket": active_ticket,
        "unresolved_tickets": tickets,
    }


def check_open_tickets_with_refresh(
    conn: Any, *, session_id: str | None = None, batch_id: str | None = None
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    refresh_status = refresh_new_tasks(
        conn, batch_id=batch_id, trigger_point="check-open-tickets"
    )
    review_inbox_refresh = refresh_review_inbox(
        conn, batch_id=batch_id, trigger_point="check-open-tickets"
    )
    report = check_open_tickets(conn, session_id=session_id, batch_id=batch_id)
    summary = summarize_claim_ownership(conn, session_id=session_id, batch_id=batch_id)
    review_inbox_claimable = derive_review_inbox_claimable_worker_tickets(
        summary, review_inbox_refresh
    )
    report["latest_refresh"] = refresh_status
    report["latest_review_inbox_refresh"] = review_inbox_refresh
    report["latest_review_inbox_check_at"] = review_inbox_refresh.get(
        "latest_review_inbox_check_at"
    )
    report["latest_inbox_items_detected"] = review_inbox_refresh.get(
        "new_inbox_items_detected", []
    )
    report["continue_due_to_inbox_items"] = bool(review_inbox_claimable)
    summary["latest_refresh"] = refresh_status
    summary["latest_review_inbox_refresh"] = review_inbox_refresh
    summary["latest_review_inbox_check_at"] = review_inbox_refresh.get(
        "latest_review_inbox_check_at"
    )
    summary["latest_inbox_items_detected"] = review_inbox_refresh.get(
        "new_inbox_items_detected", []
    )
    summary["review_inbox_claimable_worker_tickets"] = review_inbox_claimable
    summary["continue_due_to_inbox_items"] = bool(review_inbox_claimable)
    return report, summary, refresh_status


def summarize_claim_ownership(
    conn: Any, *, session_id: str | None = None, batch_id: str | None = None
) -> dict[str, Any]:
    tickets = list_tickets(conn, unresolved_only=False, batch_id=batch_id)
    current_session = (
        require_session_id(session_id)
        if session_id
        or any(
            os.environ.get(name)
            for name in (
                "OPENCODE_SESSION_ID",
                "SESSION_ID",
                "OMO_SESSION_ID",
                "POSH_SESSION_ID",
                "XDG_SESSION_ID",
            )
        )
        else None
    )
    claimed_by_this_session = [
        ticket
        for ticket in tickets
        if current_session
        and ticket["claimed_by_session"] == current_session
        and ticket["status"] in ACTIVE_TICKET_STATUSES
    ]
    claimed_by_other = [
        ticket
        for ticket in tickets
        if ticket["status"] in ACTIVE_TICKET_STATUSES
        and ticket["claimed_by_session"]
        and ticket["claimed_by_session"] != current_session
    ]
    blocked = [ticket for ticket in tickets if ticket["status"] == "BLOCKED"]
    closed = [ticket for ticket in tickets if ticket["status"] == "CLOSED"]
    claimable = [ticket for ticket in tickets if ticket["status"] in {"OPEN", "STALE"}]
    next_claimable = claimable[0] if claimable else None
    worker_tickets = [
        ticket for ticket in tickets if ticket.get("ticket_type", "WORKER") == "WORKER"
    ]
    stale_worker_tickets = [
        ticket for ticket in worker_tickets if ticket["status"] == "STALE"
    ]
    claimable_worker_tickets = [
        ticket for ticket in worker_tickets if ticket["status"] in {"OPEN", "STALE"}
    ]
    next_claimable_worker_ticket = (
        claimable_worker_tickets[0] if claimable_worker_tickets else None
    )
    integrator_ticket = next(
        (ticket for ticket in tickets if ticket.get("ticket_type") == "INTEGRATOR"),
        None,
    )
    scope_ticket_id = (
        claimed_by_this_session[0]["ticket_id"] if claimed_by_this_session else None
    )
    return {
        "batch_id": batch_id,
        "session_id": current_session,
        "total_tickets": len(tickets),
        "claimed_by_this_session": claimed_by_this_session,
        "currently_claimed_by_this_session": [
            ticket.get("ticket_id")
            for ticket in claimed_by_this_session
            if ticket.get("ticket_id")
        ],
        "claimable_tickets": claimable,
        "claimed_by_other_sessions": claimed_by_other,
        "blocked_tickets": blocked,
        "closed_tickets": closed,
        "next_claimable_ticket": next_claimable,
        "claimable_worker_tickets": claimable_worker_tickets,
        "stale_worker_tickets": stale_worker_tickets,
        "next_claimable_worker_ticket": next_claimable_worker_ticket,
        "integrator_ticket_status": integrator_ticket["status"]
        if integrator_ticket
        else None,
        "worker_closure_rule": "Worker tickets may close themselves but cannot declare batch complete.",
        "integrator_closure_rule": "only the integrator ticket may declare batch closure after no OPEN or STALE worker tickets remain; blocked tickets must be reported honestly.",
        "stale_handling_model": "STALE is an explicit workflow state and stays claimable without hidden time-threshold magic.",
        "collaboration_policy": dict(COLLABORATION_POLICY),
        "method_laws": build_method_laws_snapshot(),
        **build_usage_reporting(tickets, scope_ticket_id),
    }


def summarize_claim_ownership_with_refresh(
    conn: Any, *, session_id: str | None = None, batch_id: str | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    refresh_status = refresh_new_tasks(
        conn, batch_id=batch_id, trigger_point="claim-summary"
    )
    review_inbox_refresh = refresh_review_inbox(
        conn, batch_id=batch_id, trigger_point="claim-summary"
    )
    summary = summarize_claim_ownership(conn, session_id=session_id, batch_id=batch_id)
    review_inbox_claimable = derive_review_inbox_claimable_worker_tickets(
        summary, review_inbox_refresh
    )
    summary["currently_claimed_by_this_session"] = [
        ticket.get("ticket_id")
        for ticket in summary.get("claimed_by_this_session", [])
        if ticket.get("ticket_id")
    ]
    summary["latest_refresh"] = refresh_status
    summary["latest_review_inbox_refresh"] = review_inbox_refresh
    summary["latest_review_inbox_check_at"] = review_inbox_refresh.get(
        "latest_review_inbox_check_at"
    )
    summary["latest_inbox_items_detected"] = review_inbox_refresh.get(
        "new_inbox_items_detected", []
    )
    summary["review_inbox_claimable_worker_tickets"] = review_inbox_claimable
    summary["continue_due_to_inbox_items"] = bool(review_inbox_claimable)
    return summary, refresh_status


def session_sequences_from_summary(
    summary: dict[str, Any], *, session_id: str
) -> tuple[list[str], list[str]]:
    claimed_sequence: list[str] = []
    closed_sequence: list[str] = []
    for ticket in list(summary.get("closed_tickets", [])) + list(
        summary.get("claimed_by_this_session", [])
    ):
        if ticket.get("claimed_by_session") != session_id:
            continue
        ticket_id = ticket.get("ticket_id")
        if not ticket_id:
            continue
        if ticket_id not in claimed_sequence:
            claimed_sequence.append(ticket_id)
        if (
            ticket in summary.get("closed_tickets", [])
            and ticket_id not in closed_sequence
        ):
            closed_sequence.append(ticket_id)
    return claimed_sequence, closed_sequence


def session_ticket_sequences(
    conn: Any, *, session_id: str, batch_id: str | None = None
) -> tuple[list[str], list[str]]:
    tickets = list_tickets(conn, unresolved_only=False, batch_id=batch_id)
    ordered = sorted(
        tickets,
        key=lambda ticket: (
            ticket.get("queue_order")
            if ticket.get("queue_order") is not None
            else 2**31,
            ticket.get("created_at") or "",
        ),
    )
    claimed = [
        ticket["ticket_id"]
        for ticket in ordered
        if ticket.get("metadata", {}).get("claimed_by_session") == session_id
    ]
    closed = [
        ticket["ticket_id"]
        for ticket in ordered
        if ticket.get("metadata", {}).get("closed_by_session") == session_id
    ]
    return claimed, closed


def build_session_loop_state(
    conn: Any, *, session_id: str, batch_id: str | None = None
) -> dict[str, Any]:
    resolved_session_id = require_session_id(session_id)
    summary, refresh_status = summarize_claim_ownership_with_refresh(
        conn, session_id=resolved_session_id, batch_id=batch_id
    )
    active_ticket = (
        summary["claimed_by_this_session"][0]
        if summary.get("claimed_by_this_session")
        else None
    )
    claimed_ticket = None
    stop_reason = ""
    next_action = ""

    if active_ticket is not None:
        current_ticket = active_ticket
        stop_reason = "resume_claimed_ticket"
        next_action = "resume_claimed_ticket"
    elif summary.get("claimable_worker_tickets"):
        claimed_ticket = claim_ticket(
            conn,
            ticket_id=summary["next_claimable_worker_ticket"]["ticket_id"],
            session_id=resolved_session_id,
        )
        summary = summarize_claim_ownership(
            conn, session_id=resolved_session_id, batch_id=batch_id
        )
        current_ticket = claimed_ticket
        stop_reason = "claimed_next_worker_ticket"
        next_action = "work_claimed_ticket"
    elif any(
        ticket.get("ticket_type", "WORKER") == "WORKER"
        for ticket in summary.get("claimed_by_other_sessions", [])
    ):
        current_ticket = None
        stop_reason = "waiting_on_other_session_workers"
        next_action = "wait_for_other_session"
    elif summary.get("integrator_ticket_status") in {"OPEN", "CLAIMED", "CLOSED"}:
        current_ticket = None
        stop_reason = "ready_for_integrator_review"
        next_action = "handoff_or_integrator_review"
    else:
        current_ticket = None
        stop_reason = "no_worker_action_available"
        next_action = "idle"

    claimed_sequence, closed_sequence = session_ticket_sequences(
        conn, session_id=resolved_session_id, batch_id=batch_id
    )
    scope_ticket_id = (
        current_ticket["ticket_id"]
        if current_ticket is not None
        else summary.get("scope_ticket_id")
    )
    return {
        "session_loop_entrypoint": "python3 skills/skill-system-workflow/scripts/tickets.py session-loop",
        "session_id": resolved_session_id,
        "batch_id": summary.get("batch_id") or batch_id,
        "current_ticket": current_ticket,
        "claimed_ticket": claimed_ticket,
        "claim_summary": summary,
        "latest_refresh": refresh_status,
        "latest_refresh_at": refresh_status.get("latest_new_check_at"),
        "latest_new_items_detected": refresh_status.get("new_items_detected", []),
        "latest_new_tickets_ingested": refresh_status.get("new_tickets_ingested", []),
        "latest_review_inbox_refresh": summary.get("latest_review_inbox_refresh", {}),
        "latest_review_inbox_refresh_at": summary.get("latest_review_inbox_check_at"),
        "latest_inbox_items_detected": summary.get("latest_inbox_items_detected", []),
        "continue_due_to_inbox_items": summary.get(
            "continue_due_to_inbox_items", False
        ),
        "currently_claimed_by_this_session": summary.get(
            "currently_claimed_by_this_session", []
        ),
        "tickets_claimed_sequentially": claimed_sequence,
        "tickets_closed_this_session": closed_sequence,
        "stop_reason": stop_reason,
        "next_action": next_action,
        "open_or_stale_remaining": len(summary.get("claimable_worker_tickets", [])),
        "integrator_eligible": bool(
            not summary.get("claimable_worker_tickets")
            and summary.get("integrator_ticket_status") in {"OPEN", "CLAIMED", "CLOSED"}
        ),
        **build_usage_reporting(
            list_tickets(conn, unresolved_only=False, batch_id=batch_id),
            scope_ticket_id,
        ),
        "method_laws": build_method_laws_snapshot(),
    }


def run_session_loop(
    conn: Any, *, session_id: str, batch_id: str | None = None
) -> dict[str, Any]:
    resolved_session_id = require_session_id(session_id)
    summary, refresh_status = summarize_claim_ownership_with_refresh(
        conn, session_id=resolved_session_id, batch_id=batch_id
    )
    active_ticket = (
        summary["claimed_by_this_session"][0]
        if summary.get("claimed_by_this_session")
        else None
    )
    claimed_ticket = None

    if active_ticket is not None:
        updated_summary = summary
        stop_reason = "resume_claimed_ticket"
    elif summary.get("claimable_worker_tickets"):
        claimed_ticket = claim_ticket(
            conn,
            ticket_id=summary["next_claimable_worker_ticket"]["ticket_id"],
            session_id=resolved_session_id,
        )
        updated_summary = summarize_claim_ownership(
            conn, session_id=resolved_session_id, batch_id=batch_id
        )
        active_ticket = claimed_ticket
        stop_reason = "claimed_next_worker_ticket"
    elif any(
        ticket.get("ticket_type", "WORKER") == "WORKER"
        for ticket in summary.get("claimed_by_other_sessions", [])
    ):
        updated_summary = summary
        stop_reason = "await_other_session_worker_resolution"
    else:
        updated_summary = summary
        stop_reason = "await_integrator_closure_after_refresh"

    closed_by_session = [
        ticket["ticket_id"]
        for ticket in updated_summary.get("closed_tickets", [])
        if ticket.get("claimed_by_session") == resolved_session_id
        or ticket.get("metadata", {}).get("closed_by_session") == resolved_session_id
    ]
    claimed_sequence = list(closed_by_session)
    if active_ticket is not None and active_ticket["ticket_id"] not in claimed_sequence:
        claimed_sequence.append(active_ticket["ticket_id"])

    return {
        "session_loop_entrypoint": "python3 skills/skill-system-workflow/scripts/tickets.py session-loop",
        "batch_id": updated_summary.get("batch_id") or batch_id,
        "session_id": resolved_session_id,
        "active_ticket": active_ticket,
        "current_ticket": active_ticket,
        "claimed_ticket": claimed_ticket,
        "claim_summary": updated_summary,
        "latest_refresh": refresh_status,
        "latest_refresh_at": refresh_status.get("latest_new_check_at"),
        "latest_new_items_detected": refresh_status.get("new_items_detected", []),
        "latest_new_tickets_ingested": refresh_status.get("new_tickets_ingested", []),
        "latest_review_inbox_refresh": updated_summary.get(
            "latest_review_inbox_refresh", {}
        ),
        "latest_review_inbox_refresh_at": updated_summary.get(
            "latest_review_inbox_check_at"
        ),
        "latest_inbox_items_detected": updated_summary.get(
            "latest_inbox_items_detected", []
        ),
        "continue_due_to_inbox_items": updated_summary.get(
            "continue_due_to_inbox_items", False
        ),
        "currently_claimed_by_this_session": updated_summary.get(
            "currently_claimed_by_this_session", []
        ),
        "tickets_claimed_sequentially": claimed_sequence,
        "tickets_closed_this_session": closed_by_session,
        "stop_reason": stop_reason,
        "open_or_stale_remaining": len(
            updated_summary.get("claimable_worker_tickets", [])
        ),
        "integrator_eligible": bool(
            not updated_summary.get("claimable_worker_tickets")
            and active_ticket is None
            and updated_summary.get("integrator_ticket_status")
            in {"OPEN", "CLAIMED", "CLOSED"}
        ),
        **build_usage_reporting(
            list_tickets(conn, unresolved_only=False, batch_id=batch_id),
            active_ticket["ticket_id"] if active_ticket else None,
        ),
        "method_laws": build_method_laws_snapshot(),
    }


def build_startup_context(
    conn: Any, *, session_id: str | None = None, batch_id: str | None = None
) -> dict[str, Any]:
    _, note_tasks_text = read_note_tasks_document()
    ensure_note_tasks_tickets(conn, note_tasks_text=note_tasks_text)
    batch = parse_note_tasks_batch(note_tasks_text)
    resolved_batch_id = batch_id or batch["batch_id"]
    ticket_defs = {ticket["ticket_id"]: ticket for ticket in batch["tickets"]}
    all_tickets = list_tickets(conn, unresolved_only=False, batch_id=resolved_batch_id)
    summary, refresh_status = summarize_claim_ownership_with_refresh(
        conn, session_id=session_id, batch_id=resolved_batch_id
    )
    unresolved = list_tickets(conn, unresolved_only=True, batch_id=resolved_batch_id)
    active_ticket = (
        summary["claimed_by_this_session"][0]
        if summary["claimed_by_this_session"]
        else None
    )
    worker_tickets = [
        ticket
        for ticket in unresolved
        if ticket_defs.get(ticket["ticket_id"], {}).get("ticket_type", "WORKER")
        == "WORKER"
    ]
    stale_worker_tickets = [
        ticket
        for ticket in worker_tickets
        if ticket["status"] == "STALE"
        or (
            ticket.get("workflow_state", ticket["status"]) == "STALE"
            and ticket["status"] == "OPEN"
        )
    ]
    claimable_worker_tickets = [
        ticket
        for ticket in worker_tickets
        if ticket["status"] == "OPEN"
        or (
            ticket.get("workflow_state", ticket["status"]) == "STALE"
            and ticket["status"] == "OPEN"
        )
    ]
    next_claimable_worker_ticket = (
        claimable_worker_tickets[0] if claimable_worker_tickets else None
    )
    integrator_ticket = next(
        (
            ticket
            for ticket in all_tickets
            if ticket.get("ticket_type", "WORKER") == "INTEGRATOR"
        ),
        None,
    )
    current_ticket = active_ticket or next_claimable_worker_ticket
    usage_reporting = build_usage_reporting(
        all_tickets, current_ticket["ticket_id"] if current_ticket else None
    )
    recommended_action = (
        "continue_claimed_ticket"
        if active_ticket is not None
        else "claim_next_worker_ticket"
        if next_claimable_worker_ticket is not None
        else "handoff_or_integrator_review"
    )
    review_prompt_command = (
        f"python3 skills/skill-system-review/scripts/review_prompt.py generate-review-prompt --ticket-id {current_ticket['ticket_id']}"
        if current_ticket is not None
        else "python3 skills/skill-system-review/scripts/review_prompt.py generate-startup-review-prompt"
    )
    return {
        "batch_id": resolved_batch_id,
        "workflow_owner": "skill-system-workflow",
        "review_owner": "skill-system-review",
        "cockpit_owner": "skill-system-cockpit",
        "total_tickets": summary.get("total_tickets", 0),
        "active_ticket": active_ticket,
        "current_ticket_context": current_ticket,
        "next_claimable_worker_ticket": next_claimable_worker_ticket,
        "claimable_worker_tickets": claimable_worker_tickets,
        "stale_worker_tickets": stale_worker_tickets,
        "claimed_by_other_sessions": summary.get("claimed_by_other_sessions", []),
        "blocked_tickets": summary.get("blocked_tickets", []),
        "closed_tickets": summary.get("closed_tickets", []),
        "integrator_ticket_status": integrator_ticket["workflow_state"]
        if integrator_ticket
        else None,
        "worker_closure_rule": "Worker tickets may close themselves but cannot declare batch complete.",
        "integrator_closure_rule": "only the integrator ticket may declare batch closure after no OPEN or STALE worker tickets remain; blocked tickets must be reported honestly.",
        "stale_handling_model": "STALE is an explicit workflow state and stays claimable without hidden time-threshold magic.",
        "current_review_handoff_available": current_ticket is not None,
        "startup_entrypoint": "python3 skills/skill-system-workflow/scripts/tickets.py session-loop",
        "startup_outputs": {
            "current_batch_context": resolved_batch_id,
            "current_ticket_context": current_ticket["ticket_id"]
            if current_ticket
            else None,
            "next_claimable_worker_ticket": next_claimable_worker_ticket["ticket_id"]
            if next_claimable_worker_ticket
            else None,
            "current_review_handoff_available": current_ticket is not None,
            "what_to_send_to_review_agent_next": review_prompt_command,
        },
        "review_prompt_command": review_prompt_command,
        "recommended_action": recommended_action,
        "latest_refresh": refresh_status,
        "latest_review_inbox_refresh": summary.get("latest_review_inbox_refresh", {}),
        "latest_review_inbox_check_at": summary.get("latest_review_inbox_check_at"),
        "latest_inbox_items_detected": summary.get("latest_inbox_items_detected", []),
        "continue_due_to_inbox_items": summary.get(
            "continue_due_to_inbox_items", False
        ),
        "question_tool_policy": COLLABORATION_POLICY["question_tool_policy"],
        "fallback_policy": COLLABORATION_POLICY["fallback_policy"],
        "subagent_policy": COLLABORATION_POLICY["subagent_policy"],
        "cross_ticket_loophole_policy": COLLABORATION_POLICY[
            "cross_ticket_loophole_policy"
        ],
        "method_laws": build_method_laws_snapshot(),
        **usage_reporting,
    }


def build_integrator_closure_report(
    conn: Any, *, session_id: str, ticket_id: str | None = None
) -> dict[str, Any]:
    integrator_ticket = require_owned_active_ticket(
        conn, session_id=session_id, ticket_id=ticket_id
    )
    if integrator_ticket.get("ticket_type") != "INTEGRATOR":
        raise TicketError(
            "TICKET_NOT_INTEGRATOR",
            f"Ticket {integrator_ticket['ticket_id']} is not an integrator ticket.",
            details={"ticket_id": integrator_ticket["ticket_id"]},
        )
    tickets = list_tickets(
        conn, unresolved_only=False, batch_id=integrator_ticket["batch_id"]
    )
    worker_tickets = [
        ticket for ticket in tickets if ticket.get("ticket_type", "WORKER") == "WORKER"
    ]
    closure_blocking_workers = [
        ticket["ticket_id"]
        for ticket in worker_tickets
        if ticket.get("workflow_state", ticket["status"])
        in {"OPEN", "STALE", "CLAIMED"}
    ]
    blocked_workers = [
        ticket["ticket_id"]
        for ticket in worker_tickets
        if ticket.get("workflow_state", ticket["status"]) == "BLOCKED"
    ]
    closed_workers = [
        ticket["ticket_id"]
        for ticket in worker_tickets
        if ticket.get("workflow_state", ticket["status"]) == "CLOSED"
    ]
    final_status = (
        "NOT_READY_UNRESOLVED_WORKERS"
        if closure_blocking_workers
        else "READY_WITH_BLOCKED_WORKERS"
        if blocked_workers
        else "READY_TO_CLOSE"
    )
    return {
        "ticket_id": integrator_ticket["ticket_id"],
        "batch_id": integrator_ticket["batch_id"],
        "ticket_role_model": "WORKER vs INTEGRATOR; only integrator tickets may declare batch closure.",
        "worker_closure_rule": "Worker tickets may close themselves but cannot declare batch complete.",
        "integrator_closure_rule": "only the integrator ticket may declare batch closure after no OPEN or STALE worker tickets remain; blocked tickets must be reported honestly.",
        "stale_handling_model": "STALE is an explicit workflow state and stays claimable without hidden time-threshold magic.",
        "closed_worker_tickets": closed_workers,
        "unresolved_blocked_tickets": blocked_workers,
        "unresolved_claimed_worker_tickets": [
            ticket_id
            for ticket_id in closure_blocking_workers
            if ticket_id not in blocked_workers
        ],
        "final_batch_closure_status": final_status,
        "closure_legality": "LEGAL" if final_status == "READY_TO_CLOSE" else "ILLEGAL",
        "can_close_batch": final_status == "READY_TO_CLOSE",
        **build_usage_reporting(tickets, integrator_ticket["ticket_id"]),
    }


def build_integrator_closure_report_with_refresh(
    conn: Any, *, session_id: str, ticket_id: str | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_ticket_id = ticket_id or resolve_current_owned_ticket(
        conn, session_id=session_id
    )
    ticket = fetch_ticket(conn, resolved_ticket_id)
    refresh_status = refresh_new_tasks(
        conn,
        batch_id=(ticket or {}).get("batch_id"),
        trigger_point="integrator-closure-report",
    )
    if refresh_status.get("continue_due_to_new_policy"):
        raise TicketError(
            "CONTINUE_DUE_TO_NEW_TASKS",
            "Integrator closure remains illegal because refresh-new-tasks detected new claimable worker tasks.",
            details={"ticket_id": resolved_ticket_id, "refresh_status": refresh_status},
        )
    review_inbox_refresh = refresh_review_inbox(
        conn,
        batch_id=(ticket or {}).get("batch_id"),
        trigger_point="integrator-closure-report",
    )
    summary = summarize_claim_ownership(
        conn, session_id=session_id, batch_id=(ticket or {}).get("batch_id")
    )
    review_inbox_claimable = derive_review_inbox_claimable_worker_tickets(
        summary, review_inbox_refresh
    )
    if review_inbox_claimable:
        raise TicketError(
            "CONTINUE_DUE_TO_REVIEW_INBOX_ITEMS",
            "Integrator closure remains illegal because review inbox still implies claimable worker work.",
            details={
                "ticket_id": resolved_ticket_id,
                "refresh_status": refresh_status,
                "review_inbox_refresh": review_inbox_refresh,
                "review_inbox_claimable_worker_tickets": [
                    ticket["ticket_id"] for ticket in review_inbox_claimable
                ],
            },
        )
    closure_report = build_integrator_closure_report(
        conn, session_id=session_id, ticket_id=resolved_ticket_id
    )
    closure_report["latest_refresh"] = refresh_status
    closure_report["latest_review_inbox_refresh"] = review_inbox_refresh
    closure_report["latest_review_inbox_check_at"] = review_inbox_refresh.get(
        "latest_review_inbox_check_at"
    )
    closure_report["latest_inbox_items_detected"] = review_inbox_refresh.get(
        "new_inbox_items_detected", []
    )
    closure_report["continue_due_to_inbox_items"] = False
    return closure_report, refresh_status


def get_ticket_scope(ticket_id: str) -> dict[str, Any]:
    for prefix, rule in TICKET_SCOPE_RULES.items():
        if ticket_id == prefix or ticket_id.startswith(f"{prefix}-"):
            return rule
    return {
        "allowed_prefixes": [
            "review/REVIEW_BUNDLE.md",
            "note/note_feedback.md",
        ],
        "description": "default operational reporting surfaces only",
    }


SKILLS_DIR = ROOT_DIR / "skills"


# ---------------------------------------------------------------------------
# Safe subprocess execution (C1 fix — always with timeout)
# ---------------------------------------------------------------------------


def _run_git(args: list[str], *, cwd: str | Path | None = None, timeout: int = SUBPROCESS_TIMEOUT) -> subprocess.CompletedProcess[str]:
    """Run a git command with timeout. Raises SkillError on timeout."""
    try:
        return subprocess.run(
            args, cwd=str(cwd or ROOT_DIR),
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise TicketError(
            "SK-SYS-001",
            f"Git command timed out after {timeout}s: {' '.join(args[:3])}",
            details={"command": args, "timeout": timeout},
        )
    except FileNotFoundError:
        raise TicketError(
            "SK-GIT-007",
            "Git is not available on PATH",
            details={"command": args},
        )


# ---------------------------------------------------------------------------
# Git worktree isolation (Superpowers-inspired)
# ---------------------------------------------------------------------------


def _worktree_enabled() -> bool:
    cfg = load_config("tkt.yaml")
    return cfg.get("isolation", {}).get("worktree_enabled", False)


def _worktree_dir() -> Path:
    cfg = load_config("tkt.yaml")
    rel = cfg.get("isolation", {}).get("worktree_dir", ".tkt/worktrees")
    return ROOT_DIR / rel


def _detect_base_branch() -> str:
    """Detect the base branch with multiple fallbacks (H15 fix)."""
    for args in [
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        ["git", "config", "init.defaultBranch"],
        ["git", "symbolic-ref", "refs/remotes/origin/HEAD", "--short"],
    ]:
        try:
            r = _run_git(args, timeout=5)
            branch = r.stdout.strip()
            if r.returncode == 0 and branch and branch != "HEAD":
                return branch
        except TicketError:
            continue
    return "main"


def create_worktree(ticket_id: str) -> dict[str, Any]:
    """Create an isolated git worktree for a ticket.

    Returns {created: bool, worktree_path: str, branch: str} or {skipped: True}.
    """
    if not _worktree_enabled():
        return {"skipped": True, "reason": "worktree_enabled is false"}

    branch = f"tkt/{ticket_id}"
    wt_dir = _worktree_dir() / ticket_id

    if wt_dir.exists():
        return {"created": False, "worktree_path": str(wt_dir), "branch": branch, "already_exists": True}

    wt_dir.parent.mkdir(parents=True, exist_ok=True)
    base_branch = _detect_base_branch()

    # Check if branch already exists (stale from previous crash — H5/SK-GIT-005)
    branch_check = _run_git(["git", "branch", "--list", branch])
    if branch_check.stdout.strip():
        # Delete stale branch before recreating
        _run_git(["git", "branch", "-D", branch])

    result = _run_git(["git", "worktree", "add", str(wt_dir), "-b", branch])
    if result.returncode != 0:
        return {
            "created": False,
            "error_code": "SK-GIT-001",
            "error": result.stderr.strip(),
            "branch": branch,
            "worktree_path": str(wt_dir),
        }

    return {
        "created": True,
        "worktree_path": str(wt_dir),
        "branch": branch,
        "base_branch": base_branch,
    }


def merge_and_cleanup_worktree(ticket_id: str) -> dict[str, Any]:
    """Merge worktree branch back to base and clean up.

    Returns {merged: bool, branch: str} or {skipped: True}.
    C3 fix: abort merge on conflict, keep worktree for manual resolution.
    """
    if not _worktree_enabled():
        return {"skipped": True, "reason": "worktree_enabled is false"}

    cfg = load_config("tkt.yaml")
    isolation = cfg.get("isolation", {})
    branch = f"tkt/{ticket_id}"
    wt_dir = _worktree_dir() / ticket_id

    if not wt_dir.exists():
        return {"skipped": True, "reason": f"worktree not found: {wt_dir}"}

    result: dict[str, Any] = {"branch": branch, "worktree_path": str(wt_dir)}

    # 1. Check if there are uncommitted changes in the worktree
    status_r = _run_git(["git", "status", "--porcelain"], cwd=wt_dir)
    if status_r.stdout.strip():
        _run_git(["git", "add", "-A"], cwd=wt_dir)
        commit_r = _run_git(
            ["git", "commit", "-m", f"auto-commit: {ticket_id} work before merge"],
            cwd=wt_dir,
        )
        result["auto_committed"] = commit_r.returncode == 0

    # 2. Merge if auto_merge_on_close is enabled
    if isolation.get("auto_merge_on_close", True):
        strategy = isolation.get("merge_strategy", "no-ff")
        merge_args = ["git", "merge"]
        if strategy == "no-ff":
            merge_args += ["--no-ff", branch]
        elif strategy == "squash":
            merge_args += ["--squash", branch]
        else:
            merge_args += [branch]

        merge_r = _run_git(merge_args)
        result["merged"] = merge_r.returncode == 0

        if merge_r.returncode != 0:
            # C3 fix: abort the failed merge and keep worktree for manual resolution
            _run_git(["git", "merge", "--abort"])
            result["error_code"] = "SK-GIT-002"
            result["conflict"] = True
            result["merge_error"] = merge_r.stderr.strip()
            result["resolution_hint"] = (
                f"Merge conflict in {branch}. Worktree kept at {wt_dir} for manual resolution. "
                f"Resolve conflicts, then run: git worktree remove {wt_dir}"
            )
            return result

        if strategy == "squash":
            _run_git(["git", "commit", "-m", f"squash merge: {ticket_id}"])

    # 3. Remove worktree
    _run_git(["git", "worktree", "remove", str(wt_dir)])

    # 4. Delete branch
    _run_git(["git", "branch", "-d", branch])

    result["cleaned_up"] = True
    return result


def find_tests_in_scope(allowed_prefixes: list[str]) -> list[Path]:
    """Find test files (test_*.py) under allowed scope prefixes."""
    test_files: list[Path] = []
    for prefix in allowed_prefixes:
        search_dir = ROOT_DIR / prefix.rstrip("/")
        if search_dir.is_dir():
            test_files.extend(search_dir.rglob("test_*.py"))
        elif search_dir.is_file() and search_dir.name.startswith("test_"):
            test_files.append(search_dir)
    return sorted(set(test_files))


def run_pre_close_verification(ticket_id: str) -> dict[str, Any]:
    """Run acceptance tests before allowing ticket closure.

    Reads config/tkt.yaml enforcement section for settings.
    Returns {passed: bool, structural: bool|None, tests: bool|None, details: ...}
    """
    cfg = load_config("tkt.yaml")
    enforcement = cfg.get("enforcement", {})
    if not enforcement.get("tdd_required", False):
        return {"passed": True, "skipped": True, "reason": "tdd_required is false"}

    results: dict[str, Any] = {"structural": None, "tests": None, "details": {}}

    # 1. Structural verification
    if enforcement.get("run_structural", True):
        verify_script = (
            SKILLS_DIR / "skill-system-behavior" / "scripts" / "verify_structural.py"
        )
        if verify_script.exists():
            try:
                r = subprocess.run(
                    [sys.executable, str(verify_script)],
                    capture_output=True, text=True,
                    cwd=str(ROOT_DIR), timeout=SUBPROCESS_TIMEOUT,
                )
                results["structural"] = r.returncode == 0
                results["details"]["structural_output"] = r.stdout[:500] if r.stdout else ""
                if r.returncode != 0:
                    results["details"]["structural_error"] = r.stderr[:500] if r.stderr else ""
            except subprocess.TimeoutExpired:
                results["structural"] = False
                results["details"]["structural_error"] = f"Timed out after {SUBPROCESS_TIMEOUT}s"

    # 2. Pytest for tests in ticket scope
    if enforcement.get("run_pytest", True):
        scope = get_ticket_scope(ticket_id)
        test_files = find_tests_in_scope(scope.get("allowed_prefixes", []))
        if test_files:
            try:
                r = subprocess.run(
                    [sys.executable, "-m", "pytest", "-x", "--tb=short"]
                    + [str(f) for f in test_files],
                    capture_output=True, text=True,
                    cwd=str(ROOT_DIR), timeout=SUBPROCESS_TIMEOUT * 2,
                )
                results["tests"] = r.returncode == 0
                results["details"]["test_files"] = [str(f) for f in test_files]
                results["details"]["test_output"] = r.stdout[-500:] if r.stdout else ""
                if r.returncode != 0:
                    results["details"]["test_error"] = r.stderr[:500] if r.stderr else ""
            except subprocess.TimeoutExpired:
                results["tests"] = False
                results["details"]["test_error"] = f"Timed out after {SUBPROCESS_TIMEOUT * 2}s"
        else:
            results["tests"] = None  # no tests found — not a failure

    results["passed"] = all(v is not False for v in [results["structural"], results["tests"]])
    return results


def list_changed_files(repo_root: Path = ROOT_DIR) -> list[str]:
    result = _run_git(
        ["git", "status", "--short", "--untracked-files=all"],
        cwd=repo_root,
    )
    changed: list[str] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        path = line[3:].strip()
        if path:
            changed.append(path)
    return changed


def evaluate_ticket_scope(ticket_id: str, changed_files: list[str]) -> dict[str, Any]:
    scope = get_ticket_scope(ticket_id)
    allowed_prefixes = scope["allowed_prefixes"]
    violations = [
        path
        for path in changed_files
        if not any(
            path == prefix.rstrip("/") or path.startswith(prefix)
            for prefix in allowed_prefixes
        )
    ]
    return {
        "ticket_id": ticket_id,
        "scope_ticket_id": ticket_id,
        "scope_description": scope["description"],
        "allowed_prefixes": allowed_prefixes,
        "changed_files": changed_files,
        "out_of_scope_files": violations,
        "subagent_scope_rule": SUBAGENT_POLICY["scope_rule"],
        "cross_ticket_loophole": SUBAGENT_POLICY["cross_ticket_loophole"],
        "scope_breach_status": "SCOPE_BREACH" if violations else "CLEAN",
    }


def emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=True, default=str))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="tickets.py")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_intake = sub.add_parser("intake-ticket")
    p_intake.add_argument("--ticket-id")
    p_intake.add_argument("--title")
    p_intake.add_argument("--summary")
    p_intake.add_argument("--source", default="manual")
    p_intake.add_argument("--from-note-tasks", action="store_true")

    p_list = sub.add_parser("list-tickets")
    p_list.add_argument("--unresolved-only", action="store_true")
    p_list.add_argument("--batch-id")

    p_claim = sub.add_parser("claim-ticket")
    p_claim.add_argument("--ticket-id", required=True)
    p_claim.add_argument("--session-id")

    p_block = sub.add_parser("block-ticket")
    p_block.add_argument("--ticket-id", required=True)
    p_block.add_argument("--session-id")
    p_block.add_argument("--reason")

    p_close = sub.add_parser("close-ticket")
    p_close.add_argument("--ticket-id")
    p_close.add_argument("--session-id")
    p_close.add_argument("--resolution")

    p_check = sub.add_parser("check-open-tickets")
    p_check.add_argument("--session-id")
    p_check.add_argument("--batch-id")

    p_summary = sub.add_parser("claim-summary")
    p_summary.add_argument("--session-id")
    p_summary.add_argument("--batch-id")

    p_loop = sub.add_parser("session-loop")
    p_loop.add_argument("--session-id", required=True)
    p_loop.add_argument("--batch-id")

    p_refresh = sub.add_parser("refresh-new-tasks")
    p_refresh.add_argument("--batch-id")
    p_refresh.add_argument("--trigger-point", default="manual")

    p_refresh_inbox = sub.add_parser("refresh-review-inbox")
    p_refresh_inbox.add_argument("--batch-id")
    p_refresh_inbox.add_argument("--trigger-point", default="manual")

    p_startup = sub.add_parser("startup-flow")
    p_startup.add_argument("--session-id")
    p_startup.add_argument("--batch-id")

    p_integrator = sub.add_parser("integrator-closure-report")
    p_integrator.add_argument("--session-id", required=True)
    p_integrator.add_argument("--ticket-id")

    p_scope = sub.add_parser("check-ticket-scope")
    p_scope.add_argument("--ticket-id", required=True)
    p_scope.add_argument("--changed-file", action="append", default=[])

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        conn, db_target, target_source = connect_workflow_db()
    except TicketError as exc:
        emit(
            {
                "status": "error",
                "error_code": exc.error_code,
                "legacy_code": exc.code,
                "message": str(exc),
                "details": exc.details,
            }
        )
        return 1
    try:
        if args.cmd == "intake-ticket":
            if args.from_note_tasks:
                payload = ensure_note_tasks_tickets(conn)
            else:
                if not args.ticket_id or not args.title or not args.summary:
                    raise TicketError(
                        "MISSING_TICKET_FIELDS",
                        "ticket_id, title, and summary are required unless --from-note-tasks is used.",
                    )
                ticket = intake_ticket(
                    conn,
                    ticket_id=args.ticket_id,
                    title=args.title,
                    summary=args.summary,
                    source=args.source,
                )
                payload = {
                    "batch_id": None,
                    "mode": "single-ticket",
                    "status": ticket["status"],
                    "tickets": [ticket],
                }
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "ticket": payload["tickets"][0]
                    if len(payload["tickets"]) == 1
                    else None,
                    "batch": payload,
                }
            )
            return 0

        if args.cmd == "list-tickets":
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "tickets": list_tickets(
                        conn,
                        unresolved_only=args.unresolved_only,
                        batch_id=args.batch_id,
                    ),
                }
            )
            conn.rollback()
            return 0

        if args.cmd == "claim-ticket":
            ticket = claim_ticket(
                conn,
                ticket_id=args.ticket_id,
                session_id=require_session_id(args.session_id),
            )
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "ticket": ticket,
                }
            )
            return 0

        if args.cmd == "block-ticket":
            ticket = block_ticket(
                conn,
                ticket_id=args.ticket_id,
                session_id=args.session_id,
                reason=args.reason,
            )
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "ticket": ticket,
                }
            )
            return 0

        if args.cmd == "close-ticket":
            ticket, refresh_status = close_ticket_with_refresh(
                conn,
                ticket_id=args.ticket_id,
                session_id=require_session_id(args.session_id),
                resolution=args.resolution,
            )
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "ticket": ticket,
                    "refresh_status": refresh_status,
                }
            )
            return 0

        if args.cmd == "check-open-tickets":
            report, summary, refresh_status = check_open_tickets_with_refresh(
                conn, session_id=args.session_id, batch_id=args.batch_id
            )
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "report": report,
                    "claim_summary": summary,
                    "refresh_status": refresh_status,
                }
            )
            return 0

        if args.cmd == "claim-summary":
            summary, refresh_status = summarize_claim_ownership_with_refresh(
                conn, session_id=args.session_id, batch_id=args.batch_id
            )
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "claim_summary": summary,
                    "refresh_status": refresh_status,
                }
            )
            return 0

        if args.cmd == "session-loop":
            loop_state = run_session_loop(
                conn,
                session_id=require_session_id(args.session_id),
                batch_id=args.batch_id,
            )
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "loop_state": loop_state,
                }
            )
            return 0

        if args.cmd == "refresh-new-tasks":
            refresh_status = refresh_new_tasks(
                conn,
                batch_id=args.batch_id,
                trigger_point=args.trigger_point,
            )
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "refresh_status": refresh_status,
                }
            )
            return 0

        if args.cmd == "refresh-review-inbox":
            refresh_status = refresh_review_inbox(
                conn,
                batch_id=args.batch_id,
                trigger_point=args.trigger_point,
            )
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "refresh_status": refresh_status,
                }
            )
            return 0

        if args.cmd == "startup-flow":
            startup_context = build_startup_context(
                conn, session_id=args.session_id, batch_id=args.batch_id
            )
            conn.rollback()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "startup_context": startup_context,
                }
            )
            return 0

        if args.cmd == "integrator-closure-report":
            closure_report, refresh_status = (
                build_integrator_closure_report_with_refresh(
                    conn,
                    session_id=require_session_id(args.session_id),
                    ticket_id=args.ticket_id,
                )
            )
            conn.commit()
            emit(
                {
                    "status": "ok",
                    "db_target": db_target,
                    "target_source": target_source,
                    "closure_report": closure_report,
                    "refresh_status": refresh_status,
                }
            )
            return 0

        conn.rollback()
        emit(
            {
                "status": "ok",
                "db_target": db_target,
                "target_source": target_source,
                "scope_report": evaluate_ticket_scope(
                    args.ticket_id,
                    list(args.changed_file)
                    if args.changed_file
                    else list_changed_files(),
                ),
            }
        )
        return 0
    except TicketError as exc:
        conn.rollback()
        emit(
            {
                "status": "error",
                "error_code": exc.error_code,
                "legacy_code": exc.code,
                "message": str(exc),
                "details": exc.details,
            }
        )
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
