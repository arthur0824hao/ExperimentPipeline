"""Unified error code system for the Skill System.

All error codes follow SK-{DOMAIN}-{NUMBER} format.
This module is the single source of truth for error codes and shared error utilities.

Domains:
  SYS = system-level (subprocess, deps, disk)
  CFG = config operations
  TKT = ticket lifecycle
  GIT = git operations (worktree, scope)
  MEM = memory operations
  CLI = CLI dispatcher
"""

from __future__ import annotations

import json
import sys
from typing import Any


# ---------------------------------------------------------------------------
# Error code registry
# ---------------------------------------------------------------------------

ERRORS: dict[str, dict[str, str]] = {
    # SK-SYS: System-level
    "SK-SYS-001": {"severity": "critical", "message": "Subprocess timeout exceeded"},
    "SK-SYS-002": {"severity": "critical", "message": "Required dependency missing"},
    "SK-SYS-003": {"severity": "high", "message": "Subprocess execution failed"},
    "SK-SYS-004": {"severity": "high", "message": "File I/O error"},
    "SK-SYS-005": {"severity": "medium", "message": "Environment variable missing"},
    # SK-CFG: Config
    "SK-CFG-001": {
        "severity": "critical",
        "message": "Config file corrupt (invalid YAML)",
    },
    "SK-CFG-002": {"severity": "high", "message": "Config type validation failed"},
    "SK-CFG-003": {
        "severity": "medium",
        "message": "Config file not found (using defaults)",
    },
    "SK-CFG-004": {"severity": "medium", "message": "Config key not found"},
    "SK-CFG-005": {"severity": "medium", "message": "Config value out of valid range"},
    # SK-TKT: Ticket lifecycle
    "SK-TKT-001": {
        "severity": "high",
        "message": "Session already has a claimed ticket",
    },
    "SK-TKT-002": {"severity": "high", "message": "Ticket not found"},
    "SK-TKT-003": {"severity": "high", "message": "Ticket already closed"},
    "SK-TKT-004": {"severity": "high", "message": "Ticket claimed by another session"},
    "SK-TKT-005": {"severity": "high", "message": "Ticket is blocked"},
    "SK-TKT-006": {"severity": "high", "message": "Claim failed (race condition)"},
    "SK-TKT-007": {"severity": "high", "message": "Block failed"},
    "SK-TKT-008": {"severity": "high", "message": "Close failed"},
    "SK-TKT-009": {
        "severity": "high",
        "message": "No active claimed ticket for session",
    },
    "SK-TKT-010": {"severity": "high", "message": "Ticket not owned by session"},
    "SK-TKT-011": {"severity": "critical", "message": "Pre-close verification failed"},
    "SK-TKT-012": {"severity": "high", "message": "Integrator closure not ready"},
    "SK-TKT-013": {"severity": "high", "message": "Session ID required but missing"},
    "SK-TKT-014": {"severity": "high", "message": "Invalid or missing ticket fields"},
    "SK-TKT-015": {"severity": "high", "message": "Note tasks file not found"},
    "SK-TKT-016": {"severity": "high", "message": "Note tasks parse error"},
    "SK-TKT-017": {"severity": "high", "message": "Bundle locked (concurrent access)"},
    "SK-TKT-018": {"severity": "high", "message": "Bundle not found"},
    "SK-TKT-019": {"severity": "high", "message": "Ticket state invalid for operation"},
    "SK-TKT-020": {
        "severity": "medium",
        "message": "Stale ticket detected (session idle)",
    },
    "SK-TKT-021": {
        "severity": "high",
        "message": "Roadmap already exists (use --force)",
    },
    "SK-TKT-022": {"severity": "high", "message": "Invalid roadmap stage"},
    "SK-TKT-023": {
        "severity": "high",
        "message": "Roadmap stage transition not allowed",
    },
    "SK-TKT-024": {
        "severity": "high",
        "message": "Roadmap gate check failed (bundles not closed)",
    },
    "SK-TKT-025": {
        "severity": "high",
        "message": "Roadmap gate check failed (no bundles for active)",
    },
    "SK-TKT-026": {"severity": "high", "message": "Bundle dependency not found"},
    "SK-TKT-027": {
        "severity": "high",
        "message": "Bundle dependency not satisfied (still open)",
    },
    "SK-TKT-028": {
        "severity": "medium",
        "message": "Note tasks path conflict (ambiguous)",
    },
    "SK-TKT-029": {"severity": "medium", "message": "Review inbox not found"},
    "SK-TKT-030": {
        "severity": "high",
        "message": "Evidence required when closing done work",
    },
    "SK-TKT-031": {"severity": "high", "message": "Audit independence gate failed"},
    "SK-TKT-032": {
        "severity": "high",
        "message": "Structural validation failed before close",
    },
    "SK-TKT-033": {
        "severity": "high",
        "message": "Configured close gate command failed",
    },
    "SK-TKT-034": {
        "severity": "high",
        "message": "Executable acceptance criteria failed",
    },
    "SK-TKT-035": {
        "severity": "high",
        "message": "Ticket state transition not allowed",
    },
    "SK-TKT-036": {
        "severity": "high",
        "message": "Worktree creation failed for bundle",
    },
    "SK-TKT-037": {
        "severity": "high",
        "message": "Worktree has uncommitted changes",
    },
    "SK-TKT-038": {
        "severity": "high",
        "message": "Carryover file not found",
    },
    # SK-GIT: Git operations
    "SK-GIT-001": {"severity": "critical", "message": "Worktree creation failed"},
    "SK-GIT-002": {"severity": "critical", "message": "Worktree merge conflict"},
    "SK-GIT-003": {"severity": "high", "message": "Worktree cleanup failed"},
    "SK-GIT-004": {"severity": "high", "message": "Scope breach detected"},
    "SK-GIT-005": {
        "severity": "medium",
        "message": "Worktree branch already exists (stale)",
    },
    "SK-GIT-006": {"severity": "medium", "message": "Base branch detection failed"},
    "SK-GIT-007": {
        "severity": "high",
        "message": "Git not available or repo not initialized",
    },
    # SK-MEM: Memory operations
    "SK-MEM-001": {"severity": "high", "message": "Database connection failed"},
    "SK-MEM-002": {"severity": "high", "message": "Database query error"},
    "SK-MEM-003": {
        "severity": "medium",
        "message": "pgvector not available (degraded mode)",
    },
    "SK-MEM-004": {"severity": "medium", "message": "No memories found"},
    "SK-MEM-005": {"severity": "high", "message": "Memory validation failed"},
    "SK-MEM-006": {
        "severity": "medium",
        "message": "HNSW index not available (sequential scan)",
    },
    "SK-MEM-007": {"severity": "high", "message": "Database connection timeout"},
    "SK-MEM-008": {
        "severity": "medium",
        "message": "Memory schema missing (degraded no-op mode)",
    },
    "SK-MEM-009": {
        "severity": "medium",
        "message": "Database unavailable, local file fallback active",
    },
    # SK-CLI: CLI dispatcher
    "SK-CLI-001": {
        "severity": "high",
        "message": "Downstream script returned non-JSON output",
    },
    "SK-CLI-002": {"severity": "high", "message": "Downstream script execution failed"},
    "SK-CLI-003": {"severity": "medium", "message": "Config directory not found"},
}

# ---------------------------------------------------------------------------
# Timeout constants (configurable via config/cli.yaml)
# ---------------------------------------------------------------------------

SUBPROCESS_TIMEOUT = 30  # seconds
DB_CONNECT_TIMEOUT = 10  # seconds


# ---------------------------------------------------------------------------
# SkillError — unified exception
# ---------------------------------------------------------------------------


class SkillError(RuntimeError):
    """Unified error for all skill system operations."""

    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        details: dict[str, Any] | None = None,
    ):
        default = ERRORS.get(code, {})
        self.code = code
        self.severity = default.get("severity", "high")
        self.details = details or {}
        resolved_message = message or default.get("message", code)
        super().__init__(resolved_message)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": "error",
            "error_code": self.code,
            "message": str(self),
            "severity": self.severity,
        }
        if self.details:
            result["details"] = self.details
        return result

    def emit(self) -> None:
        """Print error as JSON to stdout (last-line JSON contract)."""
        print(json.dumps(self.to_dict(), ensure_ascii=True, default=str))


def emit_error(code: str, message: str | None = None, **details: Any) -> None:
    """Emit a JSON error without raising an exception."""
    default = ERRORS.get(code, {})
    payload: dict[str, Any] = {
        "status": "error",
        "error_code": code,
        "message": message or default.get("message", code),
        "severity": default.get("severity", "high"),
    }
    if details:
        payload["details"] = details
    print(json.dumps(payload, ensure_ascii=True, default=str))


def die_with_code(code: str, message: str | None = None, **details: Any) -> None:
    """Emit error JSON and exit with code 1."""
    emit_error(code, message, **details)
    sys.exit(1)
