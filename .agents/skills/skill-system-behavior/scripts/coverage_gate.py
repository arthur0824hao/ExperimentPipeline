#!/usr/bin/env python3

import argparse
import fnmatch
import json
import sys
from pathlib import Path


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _is_excluded(path: Path, root: Path, exclude_patterns: list[str]) -> bool:
    rel = path.relative_to(root)
    rel_str = rel.as_posix()
    parts = rel.parts

    for pattern in exclude_patterns:
        if fnmatch.fnmatch(path.name, pattern):
            return True
        if fnmatch.fnmatch(rel_str, pattern):
            return True
        for part in parts:
            if fnmatch.fnmatch(part, pattern):
                return True
    return False


def _collect_files(
    root: Path, patterns: list[str], exclude_patterns: list[str]
) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for candidate in root.glob(pattern):
            if not candidate.is_file():
                continue
            if _is_excluded(candidate, root, exclude_patterns):
                continue
            if candidate not in seen:
                files.append(candidate)
                seen.add(candidate)
    return sorted(files)


def coverage_report(
    project_dir: Path,
    patterns: list[str],
    exclude_patterns: list[str],
) -> dict:
    scripts = _collect_files(project_dir, patterns, exclude_patterns)
    behavior_specs = _collect_files(
        project_dir, ["**/*.behavior.yaml"], exclude_patterns
    )

    behavior_by_name: dict[str, list[Path]] = {}
    skill_behavior_dirs: set[Path] = set()
    for spec in behavior_specs:
        behavior_by_name.setdefault(spec.name, []).append(spec)
        if spec.name == "SKILL.behavior.yaml":
            skill_behavior_dirs.add(spec.parent)

    covered = []
    uncovered = []

    for script in scripts:
        matched_spec: Path | None = None

        expected_name = f"{script.stem}.behavior.yaml"
        named_specs = behavior_by_name.get(expected_name, [])
        if named_specs:
            same_dir = [spec for spec in named_specs if spec.parent == script.parent]
            matched_spec = sorted(same_dir or named_specs)[0]
        else:
            skill_spec_path = script.parent / "SKILL.behavior.yaml"
            if script.parent in skill_behavior_dirs and skill_spec_path.exists():
                matched_spec = skill_spec_path

        rel_script = script.relative_to(project_dir).as_posix()
        if matched_spec is None:
            uncovered.append(rel_script)
            continue

        covered.append(
            {
                "script": rel_script,
                "spec": matched_spec.relative_to(project_dir).as_posix(),
            }
        )

    total = len(scripts)
    coverage_pct = round((len(covered) / total * 100.0), 2) if total else 100.0

    return {
        "covered": covered,
        "uncovered": uncovered,
        "total": total,
        "coverage_pct": coverage_pct,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan a project for scripts missing behavior specs."
    )
    parser.add_argument("project_dir", type=Path)
    parser.add_argument("--patterns", default="**/*.py,**/*.sh")
    parser.add_argument("--exclude", default="test_*,__pycache__,.*")
    args = parser.parse_args()

    project_dir = args.project_dir.resolve()
    patterns = _split_csv(args.patterns)
    exclude_patterns = _split_csv(args.exclude)

    if not project_dir.exists() or not project_dir.is_dir():
        result = {
            "covered": [],
            "uncovered": [],
            "total": 0,
            "coverage_pct": 0.0,
            "error": f"Project directory not found: {project_dir}",
        }
        print(f"Coverage scan failed: {result['error']}")
        print(json.dumps(result))
        sys.exit(0)

    result = coverage_report(project_dir, patterns, exclude_patterns)

    print(f"Coverage scan complete: {project_dir}")
    print(f"Patterns: {', '.join(patterns) if patterns else '(none)'}")
    print(f"Excluded: {', '.join(exclude_patterns) if exclude_patterns else '(none)'}")
    print(
        f"Covered: {len(result['covered'])}/{result['total']} ({result['coverage_pct']}%)"
    )
    print(f"Uncovered scripts: {len(result['uncovered'])}")

    print(json.dumps(result))
    sys.exit(0)


if __name__ == "__main__":
    main()
