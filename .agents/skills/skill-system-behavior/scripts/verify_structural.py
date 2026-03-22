#!/usr/bin/env python3
"""Run structural acceptance tests: manifest ↔ spec consistency, script existence."""

import json
import os
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None


def _load_yaml(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if yaml:
        return yaml.safe_load(text)
    raise RuntimeError("PyYAML not installed. Install with: pip install pyyaml")


def _extract_manifest(skill_md: Path) -> dict | None:
    """Extract JSON from ```skill-manifest block in SKILL.md."""
    text = skill_md.read_text(encoding="utf-8")
    pattern = r"```skill-manifest\s*\n(.*?)```"
    match = re.search(pattern, text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def verify(skill_dir: Path) -> dict:
    """Run structural tests. Returns {status, results}."""
    results = []
    skill_dir = Path(skill_dir)

    # 1. Load spec
    spec_path = skill_dir / "SKILL.spec.yaml"
    if not spec_path.exists():
        return {
            "status": "FAIL",
            "results": [
                {
                    "id": "spec-exists",
                    "status": "FAIL",
                    "detail": "SKILL.spec.yaml not found",
                }
            ],
        }

    try:
        spec = _load_yaml(spec_path)
    except Exception as e:
        return {
            "status": "FAIL",
            "results": [
                {"id": "spec-parse", "status": "FAIL", "detail": f"Parse error: {e}"}
            ],
        }

    spec_ops = {op["name"] for op in spec.get("operations", []) if isinstance(op, dict)}

    # 2. Load manifest from SKILL.md
    skill_md = skill_dir / "SKILL.md"
    if not skill_md.exists():
        results.append(
            {"id": "skill-md-exists", "status": "FAIL", "detail": "SKILL.md not found"}
        )
        return {"status": "FAIL", "results": results}

    manifest = _extract_manifest(skill_md)
    if not manifest:
        results.append(
            {
                "id": "manifest-parse",
                "status": "FAIL",
                "detail": "Could not parse skill-manifest from SKILL.md",
            }
        )
        return {"status": "FAIL", "results": results}

    manifest_ops = set(manifest.get("operations", {}).keys())

    # Test: manifest-valid — all spec operations present in manifest
    missing_ops = spec_ops - manifest_ops
    if missing_ops:
        results.append(
            {
                "id": "manifest-valid",
                "status": "FAIL",
                "detail": f"Operations in spec but not in manifest: {sorted(missing_ops)}",
            }
        )
    else:
        results.append(
            {
                "id": "manifest-valid",
                "status": "PASS",
                "detail": f"All {len(spec_ops)} operations present",
            }
        )

    # Test: scripts-exist — all entrypoint scripts exist on disk
    missing_scripts = []
    executable_names = {
        "python",
        "python3",
        "bash",
        "sh",
        "powershell",
        "powershell.exe",
        "pwsh",
        "node",
    }
    for op_name, op_def in manifest.get("operations", {}).items():
        entrypoints = op_def.get("entrypoints", {})
        for platform, ep in entrypoints.items():
            if platform == "agent":
                continue  # Agent-executed, no script file
            if isinstance(ep, list):
                # Find script-like args (not python3/bash/powershell)
                for arg in ep:
                    if (
                        isinstance(arg, str)
                        and not arg.startswith("{")
                        and arg not in executable_names
                        and ("." in arg or "/" in arg or "\\" in arg)
                    ):
                        normalized = arg.replace("\\", os.sep)
                        script_path = skill_dir / normalized
                        if not script_path.exists() and not arg.startswith("-"):
                            missing_scripts.append(f"{op_name}: {arg}")

    if missing_scripts:
        results.append(
            {
                "id": "scripts-exist",
                "status": "FAIL",
                "detail": f"Missing scripts: {missing_scripts}",
            }
        )
    else:
        results.append(
            {
                "id": "scripts-exist",
                "status": "PASS",
                "detail": "All entrypoint scripts found",
            }
        )

    # Test: spec-manifest-consistency — manifest doesn't have extra undeclared ops
    extra_ops = manifest_ops - spec_ops
    if extra_ops:
        results.append(
            {
                "id": "spec-manifest-consistency",
                "status": "WARN",
                "detail": f"Operations in manifest but not in spec: {sorted(extra_ops)}",
            }
        )
    else:
        results.append(
            {
                "id": "spec-manifest-consistency",
                "status": "PASS",
                "detail": "No extra operations",
            }
        )

    overall = (
        "PASS" if all(r["status"] in ("PASS", "WARN") for r in results) else "FAIL"
    )
    return {"status": overall, "results": results}


def main():
    if len(sys.argv) < 2:
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "results": [
                        {
                            "id": "usage",
                            "status": "FAIL",
                            "detail": "Usage: verify_structural.py <skill-dir>",
                        }
                    ],
                }
            )
        )
        sys.exit(1)

    skill_dir = Path(sys.argv[1])
    result = verify(skill_dir)

    # Human-readable
    print(f"Structural verification: {result['status']}")
    for r in result["results"]:
        icon = "✓" if r["status"] == "PASS" else ("⚠" if r["status"] == "WARN" else "✗")
        print(f"  {icon} {r['id']}: {r['detail']}")

    # Last line JSON
    print(json.dumps(result))
    sys.exit(0 if result["status"] in ("PASS",) else 1)


if __name__ == "__main__":
    main()
