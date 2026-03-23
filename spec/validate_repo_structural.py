#!/usr/bin/env python3
"""Validate ExperimentPipeline repository structural integrity.

This script runs before each bundle close to ensure the repo is in a
consistent state. Checks cover:
  - Required directories exist
  - Key files are present
  - Python syntax on critical files
  - No leftover merge conflicts
"""

import json
import os
import subprocess
import sys
from pathlib import Path


def run(*args, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(*args, capture_output=True, text=True, **kwargs)


def check_dirs(root: Path) -> list[dict]:
    results = []
    required_dirs = ["pipeline", "pipeline/tests", ".agents/skills", ".tkt/bundles"]
    for d in required_dirs:
        p = root / d
        if p.is_dir():
            results.append(
                {"id": f"dir-{d}", "status": "PASS", "detail": f"exists: {d}"}
            )
        else:
            results.append(
                {"id": f"dir-{d}", "status": "FAIL", "detail": f"missing: {d}"}
            )
    return results


def check_key_files(root: Path) -> list[dict]:
    results = []
    key_files = [
        "pipeline/experiments.py",
        "pipeline/db_registry.py",
        "pipeline/preprocess.py",
        "AGENTS.md",
        "pyrightconfig.json",
        "pytest.ini",
    ]
    for f in key_files:
        p = root / f
        if p.is_file():
            results.append(
                {"id": f"file-{f}", "status": "PASS", "detail": f"exists: {f}"}
            )
        else:
            results.append(
                {"id": f"file-{f}", "status": "FAIL", "detail": f"missing: {f}"}
            )
    return results


def check_no_conflicts(root: Path) -> list[dict]:
    results = []
    marker_files = []
    for py_file in root.rglob("*.py"):
        if ".agents" not in py_file.parts and "__pycache__" not in str(py_file):
            try:
                lines = py_file.read_text(
                    encoding="utf-8", errors="ignore"
                ).splitlines()
                for i, line in enumerate(lines):
                    if line.startswith("<<<<<<<") or line.startswith(">>>>>>>"):
                        marker_files.append(
                            f"{py_file.relative_to(root)}:{i + 1}: {line[:60]}"
                        )
            except Exception:
                pass
    if marker_files:
        results.append(
            {
                "id": "no-conflicts",
                "status": "FAIL",
                "detail": f"Unresolved merge conflicts: {marker_files}",
            }
        )
    else:
        results.append(
            {"id": "no-conflicts", "status": "PASS", "detail": "No merge conflicts"}
        )
    return results


def check_python_syntax(root: Path) -> list[dict]:
    """Check Python syntax on critical pipeline files."""
    results = []
    critical = [
        "pipeline/experiments.py",
        "pipeline/db_registry.py",
        "pipeline/preprocess.py",
    ]
    for f in critical:
        p = root / f
        if p.is_file():
            r = run([sys.executable, "-m", "py_compile", str(p)])
            if r.returncode == 0:
                results.append(
                    {"id": f"syntax-{f}", "status": "PASS", "detail": f"OK: {f}"}
                )
            else:
                results.append(
                    {
                        "id": f"syntax-{f}",
                        "status": "FAIL",
                        "detail": f"Syntax error in {f}: {r.stderr.strip()}",
                    }
                )
    return results


def find_project_root() -> Path:
    p = Path.cwd()
    for parent in [p] + list(p.parents):
        if (parent / ".tkt").is_dir() and (parent / "pipeline").is_dir():
            return parent
    return p


def main():
    root = find_project_root()

    all_results = []
    all_results.extend(check_dirs(root))
    all_results.extend(check_key_files(root))
    all_results.extend(check_no_conflicts(root))
    all_results.extend(check_python_syntax(root))

    failures = [r for r in all_results if r["status"] == "FAIL"]
    overall = "PASS" if not failures else "FAIL"

    print(f"Structural validation: {overall}")
    for r in all_results:
        icon = "✓" if r["status"] == "PASS" else "✗"
        print(f"  {icon} {r['id']}: {r['detail']}")
    print(json.dumps({"status": overall, "results": all_results}))

    sys.exit(0 if overall == "PASS" else 1)


if __name__ == "__main__":
    main()
