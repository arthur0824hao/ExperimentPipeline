#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml


TARGET_KEYS = {"summary", "evidence"}
TOP_LEVEL_KEYS = {
    "id",
    "bundle",
    "type",
    "title",
    "description",
    "acceptance_criteria",
    "status",
    "claimed_by",
    "claimed_at",
    "started_at",
    "completed_at",
    "category",
    "effort_estimate",
    "agent_type",
    "skills",
    "wave",
    "qa_scenarios",
    "source_plan",
    "source_ticket_index",
    "exact_changes",
    "diagnostic_steps",
    "result",
    "transition_log",
    "depends_on",
}


def _sanitize_string(value: str) -> str:
    if "```" not in value:
        return value
    return value.replace("```", "'''")


def _sanitize_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        sanitized: Dict[Any, Any] = {}
        for key, value in payload.items():
            if key in TARGET_KEYS and isinstance(value, str):
                sanitized[key] = _sanitize_string(value)
            else:
                sanitized[key] = _sanitize_payload(value)
        return sanitized
    if isinstance(payload, list):
        return [_sanitize_payload(item) for item in payload]
    return payload


def _load_yaml(text: str) -> Any:
    return yaml.safe_load(text) if text.strip() else {}


def _fallback_text_sanitize(text: str) -> str:
    sanitized = text.replace("```", "'''").replace("`", "'")
    lines = sanitized.splitlines()
    repaired: List[str] = []
    idx = 0
    block_re = re.compile(r"^[A-Za-z0-9_\-]+:\s*[|>][-+0-9]*\s*$")
    key_re = re.compile(r"^([A-Za-z0-9_\-]+):\s*")

    while idx < len(lines):
        line = lines[idx]
        repaired.append(line)
        idx += 1
        if not block_re.match(line):
            continue

        while idx < len(lines):
            candidate = lines[idx]
            key_match = key_re.match(candidate)
            if key_match and key_match.group(1) in TOP_LEVEL_KEYS:
                break
            if candidate and not candidate.startswith(" "):
                repaired.append(f"  {candidate}")
            else:
                repaired.append(candidate)
            idx += 1

    return "\n".join(repaired) + ("\n" if sanitized.endswith("\n") else "")


def sanitize_file(path: Path) -> Tuple[bool, str]:
    raw = path.read_text(encoding="utf-8")
    candidate = raw

    try:
        payload = _load_yaml(candidate)
    except yaml.YAMLError:
        candidate = _fallback_text_sanitize(candidate)
        payload = _load_yaml(candidate)

    sanitized_payload = _sanitize_payload(payload)
    rendered = yaml.safe_dump(
        sanitized_payload,
        sort_keys=False,
        allow_unicode=False,
        default_flow_style=False,
    )
    changed = rendered != raw
    return changed, rendered


def collect_yaml_files(root: Path) -> List[Path]:
    tkt_dir = root / ".tkt"
    if not tkt_dir.exists():
        return []
    return sorted(path for path in tkt_dir.rglob("*.yaml") if path.is_file())


def main() -> int:
    parser = argparse.ArgumentParser(description="Sanitize and lint .tkt YAML files")
    parser.add_argument("--target", type=Path, default=Path("."))
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Apply sanitization changes in place",
    )
    args = parser.parse_args()

    target = args.target.resolve()
    files = collect_yaml_files(target)
    if not files:
        print(f"No .tkt YAML files found under {target}")
        return 0

    changed_files: List[Path] = []
    for file_path in files:
        try:
            changed, rendered = sanitize_file(file_path)
        except yaml.YAMLError as exc:
            print(f"YAML parse error: {file_path}: {exc}")
            return 1
        if changed:
            changed_files.append(file_path)
            if args.fix:
                file_path.write_text(rendered, encoding="utf-8")

    if changed_files and not args.fix:
        print("Sanitization needed for:")
        for path in changed_files:
            print(path)
        print("Run with --fix to apply changes.")
        return 1

    if changed_files and args.fix:
        print(f"Sanitized {len(changed_files)} file(s).")
    else:
        print("All .tkt YAML files are valid and sanitized.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
