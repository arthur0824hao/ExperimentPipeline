#!/usr/bin/env python3
"""Check installed skills for drift and optionally reinstall them."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
import subprocess
import sys


@dataclass
class Args:
    lockfile: str | None = None
    skill: str | None = None
    all: bool = False
    update: bool = False
    dry_run: bool = False
    skills_dir: str | None = None


@dataclass
class SkillState:
    name: str
    local_hash: str
    lock_hash: str
    status: str


class UpdateError(Exception):
    pass


def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _find_lockfile(start_dir: str) -> str:
    current = os.path.abspath(start_dir)
    while True:
        candidate = os.path.join(current, "skills-lock.json")
        if os.path.isfile(candidate):
            return candidate
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    raise UpdateError("Could not find skills-lock.json from script directory.")


def _load_lockfile(lockfile_path: str) -> dict:
    try:
        with open(lockfile_path, "r", encoding="utf-8") as file_handle:
            payload = json.load(file_handle)
    except FileNotFoundError as exc:
        raise UpdateError(f"Lockfile not found: {lockfile_path}") from exc
    except json.JSONDecodeError as exc:
        raise UpdateError(f"Invalid JSON in lockfile: {lockfile_path}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("skills"), dict):
        raise UpdateError("Lockfile must contain a top-level 'skills' object.")
    return payload


def _compute_skill_hash(skill_dir: str) -> str:
    digest = hashlib.sha256()
    files: list[str] = []
    for root, _, file_names in os.walk(skill_dir):
        for file_name in file_names:
            files.append(os.path.join(root, file_name))
    for file_path in sorted(files, key=lambda value: os.path.relpath(value, skill_dir)):
        with open(file_path, "rb") as file_handle:
            while True:
                chunk = file_handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
            digest.update(b"\0")
    return digest.hexdigest()


def _status_for_skill(skill_dir: str, lock_hash: str) -> SkillState:
    if not os.path.isdir(skill_dir):
        return SkillState(
            name=os.path.basename(skill_dir),
            local_hash="-",
            lock_hash=lock_hash,
            status="missing",
        )
    try:
        local_hash = _compute_skill_hash(skill_dir)
    except OSError as exc:
        print(f"Error: failed to hash {skill_dir}: {exc}", file=sys.stderr)
        return SkillState(
            name=os.path.basename(skill_dir),
            local_hash="-",
            lock_hash=lock_hash,
            status="drifted",
        )
    status = "ok" if local_hash == lock_hash else "drifted"
    return SkillState(
        name=os.path.basename(skill_dir),
        local_hash=local_hash,
        lock_hash=lock_hash,
        status=status,
    )


def _print_table(rows: list[SkillState]) -> None:
    headers = ("skill_name", "local_hash", "lock_hash", "status")
    widths = [len(value) for value in headers]
    for row in rows:
        widths[0] = max(widths[0], len(row.name))
        widths[1] = max(widths[1], len(row.local_hash))
        widths[2] = max(widths[2], len(row.lock_hash))
        widths[3] = max(widths[3], len(row.status))
    line = " | ".join(
        header.ljust(widths[index]) for index, header in enumerate(headers)
    )
    divider = "-+-".join("-" * width for width in widths)
    print(line)
    print(divider)
    for row in rows:
        values = (row.name, row.local_hash, row.lock_hash, row.status)
        print(
            " | ".join(value.ljust(widths[index]) for index, value in enumerate(values))
        )


def _reinstall_skill(skill_name: str, entry: dict, skills_dir: str) -> bool:
    source = entry.get("source")
    source_type = entry.get("sourceType")
    if source_type != "github":
        print(
            f"Error: unsupported sourceType for {skill_name}: {source_type}",
            file=sys.stderr,
        )
        return False
    if not source or not isinstance(source, str):
        print(f"Error: missing source for {skill_name}", file=sys.stderr)
        return False

    install_script = os.path.join(_script_dir(), "install-skill-from-github.py")
    command = [
        sys.executable,
        install_script,
        "--repo",
        source,
        "--path",
        f"skills/{skill_name}",
        "--dest",
        skills_dir,
        "--force",
    ]
    result = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    if result.returncode != 0:
        if result.stdout.strip():
            print(result.stdout.strip())
        if result.stderr.strip():
            print(result.stderr.strip(), file=sys.stderr)
        print(f"Error: reinstall failed for {skill_name}", file=sys.stderr)
        return False
    if result.stdout.strip():
        print(result.stdout.strip())
    return True


def _write_lockfile(lockfile_path: str, payload: dict) -> None:
    with open(lockfile_path, "w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, indent=2)
        file_handle.write("\n")


def _select_skills(args: Args, skills_map: dict) -> list[str]:
    if args.skill:
        if args.skill not in skills_map:
            raise UpdateError(f"Skill not found in lockfile: {args.skill}")
        return [args.skill]
    if args.all:
        return sorted(skills_map)
    raise UpdateError("Provide either --skill <name> or --all.")


def _parse_args(argv: list[str]) -> Args:
    parser = argparse.ArgumentParser(
        description="Check and update installed skills from lockfile."
    )
    parser.add_argument("--lockfile", help="Path to skills-lock.json")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--skill", help="Update or check one skill name")
    group.add_argument("--all", action="store_true", help="Update or check all skills")
    parser.add_argument(
        "--update", action="store_true", help="Reinstall missing or drifted skills"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report only, do not modify files"
    )
    parser.add_argument(
        "--skills-dir",
        help="Installed skills directory (defaults to <lockfile-dir>/skills)",
    )
    return parser.parse_args(argv, namespace=Args())


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    try:
        lockfile_path = (
            os.path.abspath(args.lockfile)
            if args.lockfile
            else _find_lockfile(_script_dir())
        )
        lock_payload = _load_lockfile(lockfile_path)
        skills_map = lock_payload["skills"]
        selected = _select_skills(args, skills_map)
        skills_dir = (
            os.path.abspath(args.skills_dir)
            if args.skills_dir
            else os.path.join(os.path.dirname(lockfile_path), "skills")
        )

        rows: list[SkillState] = []
        for skill_name in selected:
            entry = skills_map.get(skill_name) or {}
            lock_hash = entry.get("computedHash")
            if not isinstance(lock_hash, str) or not lock_hash:
                print(f"Error: missing computedHash for {skill_name}", file=sys.stderr)
                rows.append(SkillState(skill_name, "-", "-", "drifted"))
                continue
            skill_dir = os.path.join(skills_dir, skill_name)
            state = _status_for_skill(skill_dir, lock_hash)
            state.name = skill_name
            rows.append(state)

        _print_table(rows)

        if not args.update:
            return 0

        changed_lockfile = False
        for row in rows:
            if row.status not in ("drifted", "missing"):
                continue
            print(f"Updating {row.name}...")
            if args.dry_run:
                print(f"Dry-run: skipped reinstall for {row.name}")
                continue
            entry = skills_map.get(row.name) or {}
            if not _reinstall_skill(row.name, entry, skills_dir):
                continue

            refreshed_dir = os.path.join(skills_dir, row.name)
            if not os.path.isdir(refreshed_dir):
                print(
                    f"Error: skill directory missing after reinstall: {refreshed_dir}",
                    file=sys.stderr,
                )
                continue

            refreshed_hash = _compute_skill_hash(refreshed_dir)
            entry["computedHash"] = refreshed_hash
            skills_map[row.name] = entry
            changed_lockfile = True
            print(f"Updated lockfile hash for {row.name}: {refreshed_hash}")

        if changed_lockfile:
            _write_lockfile(lockfile_path, lock_payload)
            print(f"Wrote lockfile: {lockfile_path}")
        return 0
    except UpdateError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: unexpected failure: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
