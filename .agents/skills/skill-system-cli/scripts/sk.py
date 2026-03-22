#!/usr/bin/env python3
"""sk — Unified CLI for the Skill System.

Thin dispatcher that routes to existing skill scripts.
All output is JSON by default (agent-native). Use --human for readable output.
"""

from __future__ import annotations

import json
import importlib.util
import hashlib
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import click
except ImportError:
    print(
        json.dumps(
            {
                "status": "error",
                "error_code": "SK-SYS-002",
                "message": "click is required: pip install click",
                "severity": "critical",
            }
        )
    )
    sys.exit(1)

SUBPROCESS_TIMEOUT = 30  # seconds

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
SKILLS_DIR = SCRIPT_DIR.parents[1]
ROOT_DIR = SKILLS_DIR.parent
CONFIG_DIR = ROOT_DIR / "config"
NOTE_DIR = ROOT_DIR / "note"
TKT_DIR = ROOT_DIR / ".tkt"

# Skill script paths
TKT_SH = SKILLS_DIR / "skill-system-tkt" / "scripts" / "tkt.sh"
TICKETS_PY = SKILLS_DIR / "skill-system-tkt" / "scripts" / "tickets.py"
MEM_PY = SKILLS_DIR / "skill-system-memory" / "scripts" / "mem.py"
SKILLS_SH = SKILLS_DIR / "skill-system-installer" / "scripts" / "skills.sh"
MEMORY_INIT_SQL = SKILLS_DIR / "skill-system-memory" / "init.sql"
BOOTSTRAP_TEMPLATES = SKILLS_DIR / "skill-system-router" / "templates"
BUILD_INDEX_SH = SKILLS_DIR / "skill-system-router" / "scripts" / "build-index.sh"
VALIDATE_REPO_STRUCTURAL_PY = ROOT_DIR / "spec" / "validate_repo_structural.py"


def emit(payload: dict[str, Any]) -> None:
    """Emit JSON to stdout (last-line JSON contract)."""
    click.echo(json.dumps(payload, ensure_ascii=False, default=str))


def run_script(
    cmd: list[str], *, check: bool = False, timeout: int = SUBPROCESS_TIMEOUT
) -> subprocess.CompletedProcess[str]:
    """Run a subprocess with timeout (C1 fix). Returns result."""
    try:
        return subprocess.run(
            cmd, capture_output=True, text=True, check=check, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        emit(
            {
                "status": "error",
                "error_code": "SK-SYS-001",
                "message": f"Command timed out after {timeout}s: {cmd[0]}",
                "severity": "critical",
            }
        )
        sys.exit(1)


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep merge override into base. Override values win for non-dict leaves."""
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


# Config schema defaults — auto-populate missing keys on load
_CONFIG_DEFAULTS: dict[str, dict[str, Any]] = {
    "tkt.yaml": {
        "bundle": {"min_tickets": 2, "max_tickets": 6, "auto_audit": True},
        "ticket": {
            "states": ["open", "claimed", "in_progress", "done", "blocked", "failed"],
            "lock_timeout": 30,
        },
        "roadmap": {
            "data_dir": ".tkt",
            "valid_stages": [
                "planning",
                "active",
                "review",
                "blocked",
                "done",
                "archived",
            ],
        },
        "stale": {"idle_timeout_minutes": 60, "auto_revert": False},
        "enforcement": {
            "tdd_required": True,
            "run_structural": True,
            "run_pytest": True,
            "block_on_failure": True,
        },
        "isolation": {
            "worktree_enabled": False,
            "worktree_dir": ".tkt/worktrees",
            "auto_merge_on_close": True,
            "merge_strategy": "no-ff",
        },
    },
    "insight.yaml": {
        "observe": {"max_facets_per_day": 3, "confidence_threshold": 0.3},
        "evolve": {"max_passes_per_day": 1, "approval_drift_threshold": 0.5},
        "memory": {
            "half_life_days": 30,
            "embedding_dim": 1536,
            "rrf_k": 60,
            "decay_all_types": False,
        },
    },
    "cli.yaml": {
        "output": {"default_format": "json"},
        "init": {"auto_scaffold": False, "skip_postgres": False},
    },
    "router.yaml": {
        "routing": {
            "workflow_threshold": 3,
            "log_workflows": True,
            "skip_trivial_logging": True,
        },
        "policy": {"default_allowed_effects": ["fs.read", "db.read"]},
        "call_chain": {"tkt_integration": True, "workflow_planning": True},
    },
    "workflow.yaml": {
        "planning": {"max_tasks_per_wave": 6, "prefer_parallel": True},
        "recipes": {"recipe_dir": "recipes", "match_recipes_first": True},
        "mermaid": {
            "direction": "TD",
            "styles": {
                "pending": "fill:#fef3c7,stroke:#f59e0b",
                "running": "fill:#dbeafe,stroke:#3b82f6",
                "done": "fill:#d1fae5,stroke:#10b981",
                "failed": "fill:#fee2e2,stroke:#ef4444",
            },
        },
    },
}


def _format_yaml_scalar(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace('"', '\\"')
        if (
            any(ch in value for ch in [":", "#", "[", "]", "{", "}", "\n"])
            or value.strip() != value
            or value == ""
        ):
            return f'"{escaped}"'
        return value
    return json.dumps(value, ensure_ascii=False)


def _dump_simple_yaml(value: Any, *, indent: int = 0) -> str:
    prefix = "  " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}{key}:")
                lines.append(_dump_simple_yaml(item, indent=indent + 1))
            else:
                lines.append(f"{prefix}{key}: {_format_yaml_scalar(item)}")
        return "\n".join(lines)
    if isinstance(value, list):
        lines = []
        for item in value:
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}-")
                lines.append(_dump_simple_yaml(item, indent=indent + 1))
            else:
                lines.append(f"{prefix}- {_format_yaml_scalar(item)}")
        return "\n".join(lines)
    return f"{prefix}{_format_yaml_scalar(value)}"


def _render_config_default(name: str) -> str | None:
    defaults = _CONFIG_DEFAULTS.get(name)
    if not defaults:
        return None
    try:
        import yaml  # type: ignore

        rendered = yaml.safe_dump(defaults, sort_keys=False, allow_unicode=True)
    except ImportError:
        rendered = _dump_simple_yaml(defaults)
        if not rendered.endswith("\n"):
            rendered += "\n"
    return rendered


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file merged with schema defaults. Returns defaults if missing. Raises on corrupt (C4 fix)."""
    defaults = _CONFIG_DEFAULTS.get(path.name, {})
    if not path.exists():
        return defaults.copy() if defaults else {}
    try:
        import yaml  # type: ignore

        user_cfg = yaml.safe_load(path.read_text()) or {}
    except ImportError:
        emit(
            {
                "status": "error",
                "error_code": "SK-SYS-002",
                "message": "PyYAML is required: pip install pyyaml",
                "severity": "critical",
            }
        )
        sys.exit(1)
    except Exception as exc:
        emit(
            {
                "status": "error",
                "error_code": "SK-CFG-001",
                "message": f"Config file {path.name} has invalid YAML: {exc}",
                "severity": "critical",
            }
        )
        sys.exit(1)
    if not defaults:
        return user_cfg
    return _deep_merge(defaults, user_cfg)


def flatten_dict(d: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten nested dict with dot-separated keys."""
    items: dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, key))
        else:
            items[key] = v
    return items


def set_nested(d: dict[str, Any], key: str, value: str) -> None:
    """Set a value in a nested dict using dot-separated key."""
    parts = key.split(".")
    for part in parts[:-1]:
        d = d.setdefault(part, {})
    # Try to parse value as YAML literal
    try:
        import yaml  # type: ignore

        parsed = yaml.safe_load(value)
        d[parts[-1]] = parsed
    except Exception:
        d[parts[-1]] = value


# ===========================================================================
# Root group
# ===========================================================================


@click.group(invoke_without_command=True)
@click.pass_context
def sk(ctx: click.Context) -> None:
    """sk — Unified CLI for the Skill System."""
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# ===========================================================================
# sk init
# ===========================================================================


@sk.command()
@click.option("--check", is_flag=True, help="Only detect, don't create anything.")
@click.option(
    "--mode",
    type=click.Choice(["minimal", "local", "full"]),
    default="full",
    show_default=True,
    help="Dependency preflight profile.",
)
def init(check: bool, mode: str) -> None:
    """Bootstrap project structure: config/, note/, .tkt/, PostgreSQL."""
    report: dict[str, Any] = {}
    checks: dict[str, Any] = {}
    actions_taken: list[str] = []

    def _compute_skill_hash(skill_path: Path) -> str:
        digest = hashlib.sha256()
        files: list[Path] = []
        for root, _, fnames in os.walk(skill_path):
            root_path = Path(root)
            for fname in fnames:
                files.append(root_path / fname)
        for fp in sorted(files, key=lambda p: os.path.relpath(str(p), str(skill_path))):
            with fp.open("rb") as fh:
                while True:
                    chunk = fh.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
                digest.update(b"\0")
        return digest.hexdigest()

    # --- dependency preflight ---
    dep_checks: list[dict[str, Any]] = []
    for cmd in ["bash", "python3"]:
        dep_checks.append(
            {
                "name": cmd,
                "kind": "command",
                "required": True,
                "available": bool(shutil.which(cmd)),
            }
        )

    if mode in ("local", "full"):
        dep_checks.append(
            {
                "name": "click",
                "kind": "python_module",
                "required": True,
                "available": importlib.util.find_spec("click") is not None,
            }
        )
        dep_checks.append(
            {
                "name": "yaml",
                "kind": "python_module",
                "required": True,
                "available": importlib.util.find_spec("yaml") is not None,
            }
        )

    missing_required = [
        d["name"] for d in dep_checks if d["required"] and not d["available"]
    ]
    report["dependencies"] = {
        "profile": mode,
        "status": "ok" if not missing_required else "missing_required",
        "missing_required": missing_required,
        "checks": dep_checks,
    }
    checks["dependencies"] = report["dependencies"]["status"]

    # --- config/ ---
    config_files = [
        "README.md",
        "cli.yaml",
        "insight.yaml",
        "router.yaml",
        "workflow.yaml",
        "tkt.yaml",
    ]
    config_defaults = {
        name: f"{name}.default" for name in config_files if name.endswith(".yaml")
    }
    if CONFIG_DIR.exists():
        missing = [f for f in config_files if not (CONFIG_DIR / f).exists()]
        report["config"] = {
            "exists": True,
            "missing_files": missing,
            "status": "ok" if not missing else "incomplete",
        }
        if not check and missing:
            restored = []
            for name in missing:
                default_name = config_defaults.get(name)
                if not default_name:
                    continue
                src = CONFIG_DIR / default_name
                dst = CONFIG_DIR / name
                if src.exists() and not dst.exists():
                    shutil.copy2(src, dst)
                    restored.append(name)
            remaining = [f for f in config_files if not (CONFIG_DIR / f).exists()]
            if restored:
                report["config"]["restored_from_default"] = restored
                report["config"]["missing_files"] = remaining
                report["config"]["status"] = "ok" if not remaining else "incomplete"
                actions_taken.append("restore_missing_config_defaults")
    else:
        report["config"] = {"exists": False, "status": "missing"}
        if not check:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            seeded = []
            for name, default_name in config_defaults.items():
                src = CONFIG_DIR / default_name
                dst = CONFIG_DIR / name
                if src.exists() and not dst.exists():
                    shutil.copy2(src, dst)
                    seeded.append(name)
                    continue
                fallback_text = _render_config_default(name)
                if fallback_text and not dst.exists():
                    dst.write_text(fallback_text)
                    seeded.append(name)
            local_dir = CONFIG_DIR / "local"
            local_dir.mkdir(exist_ok=True)
            gitignore = local_dir / ".gitignore"
            if not gitignore.exists():
                gitignore.write_text("*\n")
            if seeded:
                report["config"]["restored_from_default"] = seeded
            report["config"]["status"] = "created"
            actions_taken.append("scaffold_config")
    checks["config"] = report["config"]["status"]

    # --- note/ ---
    note_files = ["note_rules.md", "note_tasks.md", "note_feedback.md"]
    if NOTE_DIR.exists():
        missing = [f for f in note_files if not (NOTE_DIR / f).exists()]
        report["note"] = {
            "exists": True,
            "missing_files": missing,
            "status": "ok" if not missing else "incomplete",
        }
        if not check and missing:
            for f in missing:
                tmpl = BOOTSTRAP_TEMPLATES / f
                dst = NOTE_DIR / f
                if tmpl.exists():
                    shutil.copy2(tmpl, dst)
            report["note"]["status"] = "completed"
            actions_taken.append("complete_note_templates")
    else:
        report["note"] = {"exists": False, "status": "missing"}
        if not check:
            NOTE_DIR.mkdir(parents=True, exist_ok=True)
            for f in note_files:
                tmpl = BOOTSTRAP_TEMPLATES / f
                dst = NOTE_DIR / f
                if tmpl.exists():
                    shutil.copy2(tmpl, dst)
            report["note"]["status"] = "created"
            actions_taken.append("scaffold_note")
    checks["note"] = report["note"]["status"]

    # --- PostgreSQL (profile-aware) ---
    psql = shutil.which("psql")
    if mode == "minimal":
        report["postgres"] = {"required": False, "status": "skipped"}
    elif mode == "local":
        report["postgres"] = {
            "required": False,
            "psql_found": bool(psql),
            "status": "optional_available" if psql else "optional_missing",
        }
    elif not psql:
        report["postgres"] = {
            "required": True,
            "psql_found": False,
            "status": "unavailable",
        }
    else:
        # Check if agent_memory DB exists
        r = run_script([psql, "-lqt"])
        db_exists = "agent_memory" in (r.stdout or "")
        if not db_exists:
            report["postgres"] = {
                "required": True,
                "psql_found": True,
                "db_exists": False,
                "status": "db_missing",
                "hint": "Run: psql -c 'CREATE DATABASE agent_memory;'",
            }
        else:
            # Check if tables exist
            r2 = run_script(
                [
                    psql,
                    "-w",
                    "-d",
                    "agent_memory",
                    "-c",
                    "SELECT 1 FROM agent_memories LIMIT 0;",
                ]
            )
            tables_ok = r2.returncode == 0
            report["postgres"] = {
                "required": True,
                "psql_found": True,
                "db_exists": True,
                "schema_ready": tables_ok,
                "status": "ready" if tables_ok else "schema_missing",
            }
            if not tables_ok:
                report["postgres"]["hint"] = (
                    f"Run: psql -d agent_memory -f {MEMORY_INIT_SQL}"
                )
    checks["postgres"] = report["postgres"]["status"]

    # --- .tkt/ ---
    if TKT_DIR.exists():
        report["tkt"] = {"exists": True, "status": "ok"}
    else:
        report["tkt"] = {"exists": False, "status": "missing"}
        if not check:
            project_name = ROOT_DIR.name
            r = run_script(
                ["bash", str(TKT_SH), "init-roadmap", "--project", project_name]
            )
            report["tkt"]["status"] = "created" if r.returncode == 0 else "error"
            if r.returncode != 0:
                report["tkt"]["error"] = r.stderr.strip()
            else:
                actions_taken.append("scaffold_tkt")
    checks["tkt"] = report["tkt"]["status"]

    # --- lockfile bootstrap ---
    lockfile_path = ROOT_DIR / "skills-lock.json"
    lock_payload: dict[str, Any] | None = None
    lockfile_report: dict[str, Any]
    if lockfile_path.exists():
        lockfile_report = {"status": "present", "path": str(lockfile_path)}
    else:
        lockfile_report = {"status": "missing", "path": str(lockfile_path)}
        if not check:
            skills_obj: dict[str, dict[str, str]] = {}
            if SKILLS_DIR.exists():
                for entry in sorted(SKILLS_DIR.iterdir()):
                    if not entry.is_dir():
                        continue
                    if not (entry / "SKILL.md").is_file():
                        continue
                    skills_obj[entry.name] = {
                        "source": "local",
                        "sourceType": "local",
                        "computedHash": _compute_skill_hash(entry),
                    }
            lock_payload = {"version": 1, "skills": skills_obj}
            lockfile_path.write_text(json.dumps(lock_payload, indent=2) + "\n")
            lockfile_report["status"] = "created"
            lockfile_report["created_skill_entries"] = len(skills_obj)
            actions_taken.append("create_lockfile")

    report["lockfile"] = lockfile_report

    if lock_payload is None and lockfile_path.exists():
        try:
            lock_payload = json.loads(lockfile_path.read_text())
        except Exception as exc:
            lockfile_report["status"] = "invalid"
            lockfile_report["error"] = str(exc)

    if lock_payload and isinstance(lock_payload.get("skills"), dict):
        pending_names: list[str] = []
        skills_data = lock_payload.get("skills", {})
        for name, entry in skills_data.items():
            if not isinstance(entry, dict):
                continue
            hash_value = str(entry.get("computedHash", ""))
            if hash_value and hash_value != "pending":
                continue
            if not (SKILLS_DIR / name).is_dir():
                continue
            pending_names.append(name)

        lockfile_report["pending_hashes"] = len(pending_names)
        if pending_names and not check:
            fixed = 0
            for name in pending_names:
                skill_path = SKILLS_DIR / name
                if not skill_path.is_dir():
                    continue
                entry = skills_data.get(name)
                if not isinstance(entry, dict):
                    continue
                entry["computedHash"] = _compute_skill_hash(skill_path)
                fixed += 1
            if fixed > 0:
                lockfile_path.write_text(json.dumps(lock_payload, indent=2) + "\n")
                lockfile_report["pending_hashes_fixed"] = fixed
                actions_taken.append("computed_pending_hashes")
                checks["pending_hashes_fixed"] = fixed
        elif pending_names:
            checks["pending_hashes_detected"] = len(pending_names)

    checks["lockfile"] = lockfile_report["status"]

    # --- skill validity scan ---
    invalid_skills: list[str] = []
    installed_skills = 0
    if SKILLS_DIR.exists():
        for skill_dir in sorted(SKILLS_DIR.iterdir()):
            if not skill_dir.is_dir():
                continue
            if (skill_dir / "SKILL.md").is_file():
                installed_skills += 1
            else:
                invalid_skills.append(skill_dir.name)
    report["skills"] = {
        "installed_skills": installed_skills,
        "invalid_skills": invalid_skills,
        "status": "ok" if not invalid_skills else "invalid",
    }
    checks["installed_skills"] = installed_skills
    if invalid_skills:
        checks["invalid_skills"] = invalid_skills

    report["mode"] = "check" if check else "scaffold"
    emit(
        {
            "status": "ok",
            "phase": "check" if check else "complete",
            "checks": checks,
            "actions_taken": actions_taken,
            "init_report": report,
        }
    )


# ===========================================================================
# sk status
# ===========================================================================


@sk.command()
def status() -> None:
    """Show global project status."""
    report: dict[str, Any] = {}

    # config
    config_files = ["insight.yaml", "router.yaml", "workflow.yaml", "tkt.yaml"]
    report["config"] = {
        "exists": CONFIG_DIR.is_dir(),
        "files": sum(1 for f in config_files if (CONFIG_DIR / f).exists()),
    }

    # note
    note_files = ["note_rules.md", "note_tasks.md", "note_feedback.md"]
    report["note"] = {
        "exists": NOTE_DIR.is_dir(),
        "files": sum(1 for f in note_files if (NOTE_DIR / f).exists()),
    }

    # tkt
    report["tkt"] = {"initialized": TKT_DIR.is_dir(), "bundles": 0}
    if TKT_DIR.is_dir():
        bundles_dir = TKT_DIR / "bundles"
        if bundles_dir.is_dir():
            report["tkt"]["bundles"] = sum(
                1 for d in bundles_dir.iterdir() if d.is_dir()
            )

    # postgres
    psql = shutil.which("psql")
    if psql:
        r = run_script(
            [
                psql,
                "-w",
                "-d",
                "agent_memory",
                "-tAc",
                "SELECT count(*) FROM agent_memories;",
            ]
        )
        if r.returncode == 0:
            report["postgres"] = {
                "connected": True,
                "memory_count": int(r.stdout.strip())
                if r.stdout.strip().isdigit()
                else 0,
            }
        else:
            report["postgres"] = {"connected": False}
    else:
        report["postgres"] = {"connected": False, "psql_found": False}

    # skills
    index_path = SKILLS_DIR / "skills-index.json"
    report["skills"] = {
        "index_exists": index_path.exists(),
        "installed": sum(
            1
            for d in SKILLS_DIR.iterdir()
            if d.is_dir() and d.name.startswith("skill-system-")
        ),
    }

    emit({"status": "ok", **report})


# ===========================================================================
# sk health
# ===========================================================================


@sk.command()
def health() -> None:
    """Aggregate health dashboard: stale tickets, scope breaches, test failures, config gaps."""
    report: dict[str, Any] = {"status": "ok", "checks": {}}

    # --- Config completeness ---
    config_files = [
        "tkt.yaml",
        "insight.yaml",
        "cli.yaml",
        "router.yaml",
        "workflow.yaml",
    ]
    config_present = []
    config_missing = []
    for f in config_files:
        if (CONFIG_DIR / f).exists():
            config_present.append(f)
        else:
            config_missing.append(f)
    report["checks"]["config"] = {
        "present": config_present,
        "missing": config_missing,
        "status": "ok" if not config_missing else "incomplete",
    }

    # --- Note files ---
    note_files = ["note_rules.md", "note_tasks.md", "note_feedback.md"]
    note_missing = [f for f in note_files if not (NOTE_DIR / f).exists()]
    report["checks"]["notes"] = {
        "status": "ok" if not note_missing else "incomplete",
        "missing": note_missing,
    }

    # --- TKT / Bundle health ---
    tkt_health: dict[str, Any] = {"initialized": TKT_DIR.is_dir()}
    if TKT_DIR.is_dir():
        bundles_dir = TKT_DIR / "bundles"
        bundle_stats = {"total": 0, "open": 0, "in_progress": 0, "closed": 0}
        stale_tickets: list[str] = []
        open_tickets: list[str] = []

        if bundles_dir.is_dir():
            for bdir in sorted(bundles_dir.iterdir()):
                if not bdir.is_dir() or not bdir.name.startswith("B-"):
                    continue
                bundle_stats["total"] += 1
                byaml = bdir / "bundle.yaml"
                if byaml.exists():
                    bdata = load_yaml(byaml)
                    bstatus = bdata.get("status", "unknown")
                    if bstatus in bundle_stats:
                        bundle_stats[bstatus] += 1

                # Scan tickets for stale/open
                for tf in sorted(bdir.glob("TKT-*.yaml")):
                    tdata = load_yaml(tf)
                    ts = tdata.get("status", "")
                    tid = tdata.get("id", tf.stem)
                    if ts == "open":
                        open_tickets.append(f"{bdir.name}/{tid}")
                    elif ts == "claimed":
                        # Check staleness via claimed_at
                        claimed_at = tdata.get("claimed_at")
                        if claimed_at and claimed_at != "null":
                            try:
                                claimed_dt = datetime.fromisoformat(
                                    claimed_at.replace("Z", "+00:00")
                                )
                                idle_minutes = (
                                    datetime.now(timezone.utc) - claimed_dt
                                ).total_seconds() / 60
                                tkt_cfg = load_yaml(CONFIG_DIR / "tkt.yaml")
                                threshold = tkt_cfg.get("stale", {}).get(
                                    "idle_timeout_minutes", 60
                                )
                                if idle_minutes > threshold:
                                    stale_tickets.append(
                                        f"{bdir.name}/{tid} (idle {int(idle_minutes)}m)"
                                    )
                            except (ValueError, TypeError):
                                pass

        tkt_health["bundles"] = bundle_stats
        tkt_health["stale_tickets"] = stale_tickets
        tkt_health["open_tickets_count"] = len(open_tickets)
        tkt_health["status"] = "warning" if stale_tickets else "ok"

        # Roadmap stage
        rm_path = TKT_DIR / "roadmap.yaml"
        if rm_path.exists():
            rmdata = load_yaml(rm_path)
            tkt_health["roadmap_stage"] = rmdata.get("stage", "unknown")
    else:
        tkt_health["status"] = "not_initialized"

    report["checks"]["tkt"] = tkt_health

    # --- PostgreSQL health ---
    psql = shutil.which("psql")
    pg_health: dict[str, Any] = {"psql_found": bool(psql)}
    if psql:
        r = run_script(
            [
                psql,
                "-w",
                "-d",
                "agent_memory",
                "-tAc",
                "SELECT count(*) FROM agent_memories;",
            ]
        )
        if r.returncode == 0:
            count = int(r.stdout.strip()) if r.stdout.strip().isdigit() else 0
            pg_health["connected"] = True
            pg_health["memory_count"] = count
            pg_health["status"] = "ok"
        else:
            pg_health["connected"] = False
            pg_health["status"] = "unreachable"
    else:
        pg_health["status"] = "unavailable"
    report["checks"]["postgres"] = pg_health

    # --- Overall verdict ---
    issues = []
    for name, check in report["checks"].items():
        s = check.get("status", "ok")
        if s not in ("ok", "ready"):
            issues.append(f"{name}: {s}")
    report["overall"] = "healthy" if not issues else "degraded"
    report["issues"] = issues

    emit(report)


# ===========================================================================
# sk contracts — index/contract sync
# ===========================================================================


@sk.group("contracts")
def contracts_group() -> None:
    """Contract/index maintenance commands."""
    pass


@contracts_group.command("sync")
def contracts_sync() -> None:
    """Rebuild skills index and run structural contract validation."""
    index_path = SKILLS_DIR / "skills-index.json"
    build = run_script(["bash", str(BUILD_INDEX_SH), str(index_path)])
    if build.returncode != 0:
        emit(
            {
                "status": "error",
                "error_code": "SK-CLI-002",
                "message": "contracts sync failed while rebuilding skills index",
                "details": {
                    "stdout": build.stdout.strip()[-500:] if build.stdout else "",
                    "stderr": build.stderr.strip()[-500:] if build.stderr else "",
                },
            }
        )
        return

    validate = run_script([sys.executable, str(VALIDATE_REPO_STRUCTURAL_PY)])
    if validate.returncode != 0:
        emit(
            {
                "status": "error",
                "error_code": "SK-CLI-002",
                "message": "contracts sync failed during structural validation",
                "details": {
                    "stdout": validate.stdout.strip()[-500:] if validate.stdout else "",
                    "stderr": validate.stderr.strip()[-500:] if validate.stderr else "",
                },
            }
        )
        return

    emit(
        {
            "status": "ok",
            "message": "contracts sync completed",
            "steps": {
                "build_index": "ok",
                "validate_repo_structural": "ok",
            },
            "index_path": str(index_path),
        }
    )


# ===========================================================================
# sk config
# ===========================================================================


@sk.group()
def config() -> None:
    """Read/write config/*.yaml values."""
    pass


@config.command("list")
def config_list() -> None:
    """List all config files."""
    files = []
    if CONFIG_DIR.is_dir():
        for f in sorted(CONFIG_DIR.iterdir()):
            if f.suffix in (".yaml", ".yml") and f.is_file():
                files.append(f.name)
    emit({"status": "ok", "config_dir": str(CONFIG_DIR), "files": files})


@config.command("show")
@click.argument("name")
def config_show(name: str) -> None:
    """Show full contents of a config file (e.g. 'tkt')."""
    filename = name if name.endswith((".yaml", ".yml")) else f"{name}.yaml"
    path = CONFIG_DIR / filename
    if not path.exists():
        emit(
            {
                "status": "error",
                "error_code": "SK-CFG-003",
                "message": f"Config file not found: {path}",
            }
        )
        return
    data = load_yaml(path)
    emit({"status": "ok", "file": filename, "config": data})


@config.command("get")
@click.argument("key")
def config_get(key: str) -> None:
    """Get a config value by dot-path (e.g. 'tkt.bundle.max_tickets')."""
    parts = key.split(".", 1)
    if len(parts) < 2:
        emit(
            {
                "status": "error",
                "error_code": "SK-CFG-004",
                "message": "Key format: <file>.<path> (e.g. tkt.bundle.max_tickets)",
            }
        )
        return
    filename = f"{parts[0]}.yaml"
    data = load_yaml(CONFIG_DIR / filename)
    flat = flatten_dict(data)
    lookup = parts[1]
    if lookup in flat:
        emit({"status": "ok", "key": key, "value": flat[lookup]})
    else:
        emit(
            {
                "status": "error",
                "error_code": "SK-CFG-004",
                "message": f"Key not found: {key}",
                "available_keys": list(flat.keys()),
            }
        )


@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key: str, value: str) -> None:
    """Set a config value by dot-path (e.g. 'tkt.bundle.max_tickets 8')."""
    parts = key.split(".", 1)
    if len(parts) < 2:
        emit(
            {
                "status": "error",
                "error_code": "SK-CFG-004",
                "message": "Key format: <file>.<path>",
            }
        )
        return
    filename = f"{parts[0]}.yaml"
    path = CONFIG_DIR / filename
    if not path.exists():
        emit(
            {
                "status": "error",
                "error_code": "SK-CFG-003",
                "message": f"Config file not found: {path}",
            }
        )
        return
    try:
        import yaml  # type: ignore
    except ImportError:
        emit(
            {
                "status": "error",
                "error_code": "SK-SYS-002",
                "message": "PyYAML required: pip install pyyaml",
            }
        )
        return

    data = yaml.safe_load(path.read_text()) or {}
    set_nested(data, parts[1], value)
    path.write_text(yaml.dump(data, default_flow_style=False, allow_unicode=True))
    emit({"status": "ok", "key": key, "value": value, "file": filename})


# ===========================================================================
# sk tkt — ticket lifecycle
# ===========================================================================


@sk.group()
def tkt() -> None:
    """Ticket lifecycle operations (bundles + DB tickets)."""
    pass


def _run_downstream(
    cmd: list[str],
    label: str = "script",
    *,
    allow_plaintext_success: bool = False,
) -> None:
    """Run downstream command and emit JSON or allowed plaintext output."""
    r = run_script(cmd)
    output = r.stdout.strip()

    # Try to find valid JSON in output (search from last line backwards)
    if output:
        for line in reversed(output.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                click.echo(line)
                return
            except json.JSONDecodeError:
                continue

    # No valid JSON found
    if r.returncode != 0:
        emit(
            {
                "status": "error",
                "error_code": "SK-CLI-002",
                "message": f"{label} failed (exit {r.returncode})",
                "details": {
                    "stdout": output[-500:] if output else "",
                    "stderr": r.stderr.strip()[:500] if r.stderr else "",
                },
            }
        )
    elif output and allow_plaintext_success:
        click.echo(output)
    elif output:
        emit(
            {
                "status": "error",
                "error_code": "SK-CLI-001",
                "message": f"{label} returned non-JSON output",
                "details": {"stdout": output[-500:]},
            }
        )
    else:
        emit({"status": "ok", "output": ""})


def _run_tkt_sh(args: list[str]) -> None:
    """Run tkt.sh with args and emit result."""
    _run_downstream(["bash", str(TKT_SH)] + args, label="tkt.sh")


def _run_tickets_py(args: list[str]) -> None:
    """Run tickets.py with args and emit result."""
    _run_downstream([sys.executable, str(TICKETS_PY)] + args, label="tickets.py")


# --- Filesystem bundle ops ---


@tkt.command("init-roadmap")
@click.option("--project", required=True, help="Project name.")
def tkt_init_roadmap(project: str) -> None:
    """Initialize .tkt/ directory and roadmap."""
    _run_tkt_sh(["init-roadmap", "--project", project])


@tkt.command("create-bundle")
@click.option("--goal", required=True, help="Bundle goal description.")
@click.option(
    "--depends-on", default=None, help="Comma-separated bundle IDs this depends on."
)
@click.option("--priority", default=None, help="Priority (low|normal|high|critical).")
def tkt_create_bundle(goal: str, depends_on: str | None, priority: str | None) -> None:
    """Create a new TKT bundle from a goal."""
    args = ["create-bundle", "--goal", goal]
    if depends_on:
        args += ["--depends-on", depends_on]
    if priority:
        args += ["--priority", priority]
    _run_tkt_sh(args)


@tkt.command("bundle-status")
@click.option("--bundle", required=True, help="Bundle ID (e.g. B-001).")
def tkt_bundle_status(bundle: str) -> None:
    """Show bundle status with all tickets."""
    _run_tkt_sh(["status", "--bundle", bundle])


@tkt.command("close-bundle")
@click.option("--bundle", required=True, help="Bundle ID.")
def tkt_close_bundle(bundle: str) -> None:
    """Close a bundle (all worker TKTs must be done/failed)."""
    _run_tkt_sh(["close", "--bundle", bundle])


@tkt.command("list-bundles")
def tkt_list_bundles() -> None:
    """List all bundles."""
    _run_tkt_sh(["list"])


@tkt.command("express", context_settings={"ignore_unknown_options": True})
@click.argument("express_args", nargs=-1)
@click.option("--title", help="Express ticket title (create mode).")
@click.option("--acceptance", help="Express acceptance criteria (create mode).")
@click.option("--category", help="Ticket category/agent class (create mode).")
@click.option("--agent", help="Agent id (claim mode).")
@click.option("--files-changed", type=int, help="Files changed count (close mode).")
def tkt_express(
    express_args: tuple[str, ...],
    title: str | None,
    acceptance: str | None,
    category: str | None,
    agent: str | None,
    files_changed: int | None,
) -> None:
    """Manage express tickets: create, list, claim, close."""
    # Create mode: sk tkt express --title ... --acceptance ...
    if not express_args:
        if not title or not acceptance:
            emit(
                {
                    "status": "error",
                    "error_code": "SK-TKT-014",
                    "message": "Create mode requires --title and --acceptance",
                    "usage": [
                        'sk tkt express --title "..." --acceptance "..."',
                        "sk tkt express list",
                        "sk tkt express claim EXP-001 --agent <agent-id>",
                        "sk tkt express close EXP-001 --files-changed 2",
                    ],
                }
            )
            return
        args = ["express", "--title", title, "--acceptance", acceptance]
        if category:
            args += ["--category", category]
        _run_tkt_sh(args)
        return

    action = express_args[0]

    if action == "list":
        _run_tkt_sh(["express", "list"])
        return

    if action == "close":
        if len(express_args) < 2:
            emit(
                {
                    "status": "error",
                    "error_code": "SK-TKT-014",
                    "message": "Close mode requires express ticket id (e.g. EXP-001)",
                }
            )
            return
        args = ["express", "close", express_args[1]]
        if files_changed is not None:
            args += ["--files-changed", str(files_changed)]
        _run_tkt_sh(args)
        return

    if action == "claim":
        if len(express_args) < 2:
            emit(
                {
                    "status": "error",
                    "error_code": "SK-TKT-014",
                    "message": "Claim mode requires express ticket id (e.g. EXP-001)",
                }
            )
            return
        if not agent:
            emit(
                {
                    "status": "error",
                    "error_code": "SK-TKT-014",
                    "message": "Claim mode requires --agent",
                }
            )
            return
        _run_tkt_sh(["express", "claim", express_args[1], "--agent", agent])
        return

    emit(
        {
            "status": "error",
            "error_code": "SK-CLI-002",
            "message": f"Unknown express action: {action}",
            "usage": [
                'sk tkt express --title "..." --acceptance "..."',
                "sk tkt express list",
                "sk tkt express claim EXP-001 --agent <agent-id>",
                "sk tkt express close EXP-001 --files-changed 2",
            ],
        }
    )


@tkt.command("roadmap-transition")
@click.option(
    "--stage",
    required=True,
    help="Target stage (planning|active|review|blocked|done|archived).",
)
@click.option("--reason", required=True, help="Reason for the transition.")
@click.option("--force", is_flag=True, help="Override gate checks.")
def tkt_roadmap_transition(stage: str, reason: str, force: bool) -> None:
    """Transition roadmap to a new stage (gated)."""
    args = ["roadmap-transition", "--stage", stage, "--reason", reason]
    if force:
        args.append("--force")
    _run_tkt_sh(args)


@tkt.command("roadmap-status")
def tkt_roadmap_status() -> None:
    """Show roadmap stage, bundle summary, and allowed transitions."""
    _run_tkt_sh(["roadmap-status"])


# --- DB ticket ops ---


@tkt.command("intake")
@click.option("--from-note-tasks", is_flag=True, help="Intake from note/note_tasks.md.")
@click.option("--ticket-id", help="Manual ticket ID.")
@click.option("--title", help="Ticket title.")
@click.option("--summary", help="Ticket summary.")
def tkt_intake(
    from_note_tasks: bool, ticket_id: str | None, title: str | None, summary: str | None
) -> None:
    """Create or sync durable tickets."""
    args = ["intake-ticket"]
    if from_note_tasks:
        args.append("--from-note-tasks")
    else:
        if ticket_id:
            args += ["--ticket-id", ticket_id]
        if title:
            args += ["--title", title]
        if summary:
            args += ["--summary", summary]
    _run_tickets_py(args)


@tkt.command("list-tickets")
@click.option("--unresolved-only", is_flag=True)
@click.option("--batch-id")
def tkt_list_tickets(unresolved_only: bool, batch_id: str | None) -> None:
    """List durable workflow tickets."""
    args = ["list-tickets"]
    if unresolved_only:
        args.append("--unresolved-only")
    if batch_id:
        args += ["--batch-id", batch_id]
    _run_tickets_py(args)


@tkt.command("claim")
@click.option("--ticket-id", required=True)
@click.option("--session-id")
def tkt_claim(ticket_id: str, session_id: str | None) -> None:
    """Claim a ticket for the current session."""
    args = ["claim-ticket", "--ticket-id", ticket_id]
    if session_id:
        args += ["--session-id", session_id]
    _run_tickets_py(args)


@tkt.command("block")
@click.option("--ticket-id", required=True)
@click.option("--reason")
def tkt_block(ticket_id: str, reason: str | None) -> None:
    """Mark a ticket as blocked."""
    args = ["block-ticket", "--ticket-id", ticket_id]
    if reason:
        args += ["--reason", reason]
    _run_tickets_py(args)


@tkt.command("close")
@click.option("--session-id", required=True)
@click.option("--ticket-id")
@click.option("--resolution")
def tkt_close(session_id: str, ticket_id: str | None, resolution: str | None) -> None:
    """Close the currently claimed ticket."""
    args = ["close-ticket", "--session-id", session_id]
    if ticket_id:
        args += ["--ticket-id", ticket_id]
    if resolution:
        args += ["--resolution", resolution]
    _run_tickets_py(args)


@tkt.command("check-open")
@click.option("--session-id")
@click.option("--batch-id")
def tkt_check_open(session_id: str | None, batch_id: str | None) -> None:
    """Report unresolved tickets."""
    args = ["check-open-tickets"]
    if session_id:
        args += ["--session-id", session_id]
    if batch_id:
        args += ["--batch-id", batch_id]
    _run_tickets_py(args)


@tkt.command("summary")
@click.option("--session-id")
@click.option("--batch-id")
def tkt_summary(session_id: str | None, batch_id: str | None) -> None:
    """Batch/session claim ownership summary."""
    args = ["claim-summary"]
    if session_id:
        args += ["--session-id", session_id]
    if batch_id:
        args += ["--batch-id", batch_id]
    _run_tickets_py(args)


@tkt.command("loop")
@click.option("--session-id", required=True)
@click.option("--batch-id")
def tkt_loop(session_id: str, batch_id: str | None) -> None:
    """Run the legal worker loop for one session."""
    args = ["session-loop", "--session-id", session_id]
    if batch_id:
        args += ["--batch-id", batch_id]
    _run_tickets_py(args)


@tkt.command("refresh-new")
@click.option("--batch-id")
@click.option("--trigger-point", default="manual")
def tkt_refresh_new(batch_id: str | None, trigger_point: str) -> None:
    """Re-read note_tasks ### New and ingest new work."""
    args = ["refresh-new-tasks", "--trigger-point", trigger_point]
    if batch_id:
        args += ["--batch-id", batch_id]
    _run_tickets_py(args)


@tkt.command("refresh-inbox")
@click.option("--batch-id")
@click.option("--trigger-point", default="manual")
def tkt_refresh_inbox(batch_id: str | None, trigger_point: str) -> None:
    """Parse review inbox and ingest inbox-derived tickets."""
    args = ["refresh-review-inbox", "--trigger-point", trigger_point]
    if batch_id:
        args += ["--batch-id", batch_id]
    _run_tickets_py(args)


@tkt.command("startup")
@click.option("--session-id")
@click.option("--batch-id")
def tkt_startup(session_id: str | None, batch_id: str | None) -> None:
    """Startup/claim-loop context."""
    args = ["startup-flow"]
    if session_id:
        args += ["--session-id", session_id]
    if batch_id:
        args += ["--batch-id", batch_id]
    _run_tickets_py(args)


@tkt.command("closure-report")
@click.option("--session-id", required=True)
@click.option("--ticket-id")
def tkt_closure_report(session_id: str, ticket_id: str | None) -> None:
    """Report whether integrator can legally close the batch."""
    args = ["integrator-closure-report", "--session-id", session_id]
    if ticket_id:
        args += ["--ticket-id", ticket_id]
    _run_tickets_py(args)


@tkt.command("scope")
@click.option("--ticket-id", required=True)
def tkt_scope(ticket_id: str) -> None:
    """Check if changed files stay inside ticket scope."""
    args = ["check-ticket-scope", "--ticket-id", ticket_id]
    _run_tickets_py(args)


# ===========================================================================
# sk mem — memory operations
# ===========================================================================


@sk.group()
def mem() -> None:
    """Memory operations (PostgreSQL agent_memories)."""
    pass


def _run_mem_py(args: list[str]) -> None:
    """Run mem.py and emit."""
    _run_downstream([sys.executable, str(MEM_PY)] + args, label="mem.py")


@mem.command("search")
@click.argument("query")
@click.option("--limit", default=10, type=int)
@click.option("--scope", type=click.Choice(["global", "project", "session"]))
def mem_search(query: str, limit: int, scope: str | None) -> None:
    """Search agent memories."""
    args = ["search", query, "--limit", str(limit)]
    if scope:
        args += ["--scope", scope]
    _run_mem_py(args)


@mem.command("store")
@click.option(
    "--type",
    "memory_type",
    required=True,
    help="Memory type (working/episodic/semantic/procedural).",
)
@click.option("--category", required=True)
@click.option("--title", required=True)
@click.option("--content", required=True)
@click.option("--tags", default="")
@click.option(
    "--scope", type=click.Choice(["global", "project", "session"]), default="session"
)
@click.option("--importance", default=5.0, type=float)
def mem_store(
    memory_type: str,
    category: str,
    title: str,
    content: str,
    tags: str,
    scope: str,
    importance: float,
) -> None:
    """Store a new memory."""
    _run_mem_py(
        [
            "store",
            "--type",
            memory_type,
            "--category",
            category,
            "--title",
            title,
            "--content",
            content,
            "--tags",
            tags,
            "--scope",
            scope,
            "--importance",
            str(importance),
        ]
    )


@mem.command("list")
@click.option("--scope", type=click.Choice(["global", "project", "session"]))
@click.option("--category")
@click.option("--limit", default=20, type=int)
def mem_list(scope: str | None, category: str | None, limit: int) -> None:
    """List stored memories."""
    args = ["list", "--limit", str(limit)]
    if scope:
        args += ["--scope", scope]
    if category:
        args += ["--category", category]
    _run_mem_py(args)


@mem.command("compact")
@click.option("--scope", type=click.Choice(["global", "project", "session"]))
def mem_compact(scope: str | None) -> None:
    """Compact duplicate memories."""
    args = ["compact"]
    if scope:
        args += ["--scope", scope]
    _run_mem_py(args)


@mem.command("export")
@click.option("--format", "fmt", type=click.Choice(["json", "csv"]), default="json")
@click.option("--scope", type=click.Choice(["global", "project", "session"]))
@click.option("--limit", default=1000, type=int)
def mem_export(fmt: str, scope: str | None, limit: int) -> None:
    """Export memories."""
    args = ["export", "--format", fmt, "--limit", str(limit)]
    if scope:
        args += ["--scope", scope]
    _run_mem_py(args)


@mem.command("hybrid-search")
@click.argument("query")
@click.option("--limit", default=10, type=int)
@click.option(
    "--half-life", default=30.0, type=float, help="Temporal decay half-life in days."
)
def mem_hybrid_search(query: str, limit: int, half_life: float) -> None:
    """Hybrid search: text + vector (RRF) with temporal decay."""
    _run_mem_py(
        ["hybrid-search", query, "--limit", str(limit), "--half-life", str(half_life)]
    )


@mem.command("status")
def mem_status() -> None:
    """Memory system health status."""
    _run_mem_py(["status"])


@mem.command("tags")
def mem_tags() -> None:
    """List all memory tags."""
    _run_mem_py(["tags"])


@mem.command("categories")
def mem_categories() -> None:
    """List all memory categories."""
    _run_mem_py(["categories"])


# ===========================================================================
# sk install — skill management
# ===========================================================================


@sk.group("install")
def install_group() -> None:
    """Skill installation and management."""
    pass


def _run_skills_sh(args: list[str]) -> None:
    """Run skills.sh and emit."""
    _run_downstream(
        ["bash", str(SKILLS_SH)] + args,
        label="skills.sh",
        allow_plaintext_success=True,
    )


@install_group.command("bootstrap")
def install_bootstrap() -> None:
    """First-run setup: detect missing structure, scaffold, compute lockfile hashes."""
    _run_skills_sh(["bootstrap"])


@install_group.command("list")
def install_list() -> None:
    """List installed skills."""
    _run_skills_sh(["list"])


@install_group.command("add")
@click.argument("skill_name")
@click.option("--force", is_flag=True, help="Overwrite existing (backup first).")
def install_add(skill_name: str, force: bool) -> None:
    """Install a skill by name."""
    args = ["install"]
    if skill_name.startswith("skill-system-"):
        args += [
            "--repo",
            "arthur0824hao/skills",
            "--path",
            f"skills/{skill_name}",
        ]
    else:
        args.append(skill_name)
    if force:
        args.append("--force")
    _run_skills_sh(args)


@install_group.command("update")
@click.option("--all", "update_all", is_flag=True, help="Check all skills.")
@click.option("--skill", default=None, help="Check a specific skill.")
@click.option("--apply", is_flag=True, help="Reinstall drifted/missing skills.")
@click.option("--dry-run", is_flag=True, help="Report only, no changes.")
def install_update(
    update_all: bool, skill: str | None, apply: bool, dry_run: bool
) -> None:
    """Check for skill drift and optionally update."""
    args = ["update"]
    if update_all:
        args.append("--all")
    elif skill:
        args += ["--skill", skill]
    else:
        args.append("--all")  # default to all
    if apply:
        args.append("--update")
    if dry_run:
        args.append("--dry-run")
    _run_skills_sh(args)


@install_group.command("sync")
@click.option("--strategy", type=click.Choice(["copy", "symlink"]), default="copy")
@click.option("--force", is_flag=True, help="Overwrite existing local skills.")
@click.argument("skills", required=False)
def install_sync(strategy: str, force: bool, skills: str | None) -> None:
    """Sync global skills into local workspace."""
    args = ["sync", "--strategy", strategy]
    if force:
        args.append("--force")
    if skills:
        args.append(skills)
    _run_skills_sh(args)


@install_group.command("hash")
@click.argument("skill_name", required=False)
def install_hash(skill_name: str | None) -> None:
    """Compute and display SHA256 hash for installed skill(s)."""
    lockfile = ROOT_DIR / "skills-lock.json"
    skills_dir = SKILLS_DIR

    import hashlib

    def compute_hash(skill_path: Path) -> str:
        digest = hashlib.sha256()
        files = []
        for root, _, fnames in os.walk(skill_path):
            for fname in fnames:
                files.append(os.path.join(root, fname))
        for fp in sorted(files, key=lambda v: os.path.relpath(v, str(skill_path))):
            with open(fp, "rb") as fh:
                while True:
                    chunk = fh.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
                digest.update(b"\0")
        return digest.hexdigest()

    results: list[dict[str, str]] = []
    if skill_name:
        sp = skills_dir / skill_name
        if sp.is_dir():
            results.append({"skill": skill_name, "hash": compute_hash(sp)})
        else:
            emit(
                {
                    "status": "error",
                    "error_code": "SK-CLI-002",
                    "message": f"Skill not found: {skill_name}",
                }
            )
            return
    else:
        for d in sorted(skills_dir.iterdir()):
            if d.is_dir() and (d / "SKILL.md").exists():
                results.append({"skill": d.name, "hash": compute_hash(d)})

    emit({"status": "ok", "hashes": results, "count": len(results)})


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    sk()
