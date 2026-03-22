#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[3]
EVOLUTION_PROPOSAL_PATH = (
    ROOT_DIR / "skills" / "skill-system-evolution" / "scripts" / "evolution_proposal.py"
)


def load_module(module_path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_evolution_module():
    return load_module(EVOLUTION_PROPOSAL_PATH, "insight_evolution_proposal")


def utc_iso_minute_now() -> str:
    return datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()


def stable_proposal_id(round_version: str, summary: str) -> str:
    digest = hashlib.sha256(f"{round_version}|{summary}".encode("utf-8")).hexdigest()
    return f"cockpit-{digest[:12]}"


def build_workflow_proposal(
    base_state: dict[str, Any],
    *,
    kind: str,
    summary: str,
    rationale: str,
    suggested_change: str,
    evidence_refs: list[str],
) -> dict[str, Any]:
    evolution_mod = load_evolution_module()
    round_version = base_state.get("active_round", {}).get("version", "cockpit-round")
    return evolution_mod.make_proposal(
        proposal_id=stable_proposal_id(round_version, summary),
        kind=kind,
        summary=summary,
        rationale=rationale,
        suggested_change=suggested_change,
        evidence_refs=sorted({ref for ref in evidence_refs if ref}),
        created_at=utc_iso_minute_now(),
        status="PROPOSED",
    )


def generate_humane_workflow_proposals(
    base_state: dict[str, Any], *, limit: int = 3
) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    source_refs = base_state.get("source_refs", {})
    note_tasks_ref = source_refs.get("note_tasks", "note/note_tasks.md")
    note_feedback_ref = source_refs.get("note_feedback", "note/note_feedback.md")
    runtime_doctor_ref = source_refs.get(
        "runtime_doctor", "skills/skill-system-memory/scripts/runtime_doctor.py"
    )

    watcher_gaps = base_state.get("watcher_gaps", [])
    if watcher_gaps:
        gap = watcher_gaps[0]
        proposals.append(
            build_workflow_proposal(
                base_state,
                kind="next_step",
                summary=f"Attach {gap['watcher']} as the next cockpit profile adapter",
                rationale=(
                    "Cockpit currently exposes a generic watcher gap and needs a humane workflow-oriented adapter recommendation."
                ),
                suggested_change=gap["recommended_next"],
                evidence_refs=[note_tasks_ref, runtime_doctor_ref],
            )
        )

    frictions = base_state.get("frictions", [])
    if frictions:
        friction = frictions[0]
        proposals.append(
            build_workflow_proposal(
                base_state,
                kind="friction",
                summary="Turn the top open friction into a tracked cockpit workflow fix",
                rationale=friction["summary"],
                suggested_change=(
                    "Promote the open friction into one explicit workflow task or renderer rule so it stops resurfacing as ad-hoc process debt."
                ),
                evidence_refs=[note_feedback_ref, note_tasks_ref],
            )
        )

    if not base_state.get("active_tasks"):
        proposals.append(
            build_workflow_proposal(
                base_state,
                kind="next_step",
                summary="Materialize the next cockpit step as an active task",
                rationale=(
                    "The cockpit round still has remaining work, but there is no active task in the canonical task authority to keep progress visible."
                ),
                suggested_change=(
                    "Create one explicit task for the next unmet cockpit requirement so the cockpit and review surfaces stay aligned."
                ),
                evidence_refs=[note_tasks_ref],
            )
        )

    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for proposal in proposals:
        if proposal["proposal_id"] in seen:
            continue
        seen.add(proposal["proposal_id"])
        deduped.append(proposal)
    return deduped[:limit]


def summarize_workflow_proposals(
    proposals: list[dict[str, Any]], *, limit: int = 2
) -> list[str]:
    summary_lines = [
        f"[{proposal['kind']}] {proposal['summary']} (id={proposal['proposal_id']})"
        for proposal in proposals[:limit]
    ]
    return summary_lines or ["(none)"]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="cockpit_proposals.py")
    parser.add_argument("command", choices=("generate-proposals",))
    parser.add_argument("--state-json")
    parser.add_argument("--state-file")
    parser.add_argument("--limit", type=int, default=3)
    return parser.parse_args(argv)


def load_state_from_args(args: argparse.Namespace) -> dict[str, Any]:
    if args.state_json:
        return json.loads(args.state_json)
    if args.state_file:
        return json.loads(Path(args.state_file).read_text(encoding="utf-8"))
    raise SystemExit("Missing cockpit state. Use --state-json or --state-file.")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    state = load_state_from_args(args)
    payload = {
        "status": "ok",
        "pending_storage_model": "non_durable_in_memory_only",
        "proposal_scope": "humane_agent_workflow_improvements_only",
        "proposals": generate_humane_workflow_proposals(state, limit=args.limit),
    }
    payload["summary"] = summarize_workflow_proposals(payload["proposals"])
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
