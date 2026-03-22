#!/usr/bin/env python3
"""
skill-system-review: Bridge between OMO Prometheus and TKT ticket lifecycle.

Commands:
  generate-review-prompt       Build structured review context for Prometheus
  generate-startup-review-prompt  Build startup overview for Prometheus
  plan-to-bundle               Convert Prometheus plan.md → TKT bundle commands
  write-review-inbox           Write review feedback into Review Agent Inbox
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

SKILLS_ROOT = Path(__file__).resolve().parent.parent.parent
PROJECT_ROOT = SKILLS_ROOT.parent
TKT_DIR = PROJECT_ROOT / ".tkt"
ROADMAP_PATH = TKT_DIR / "roadmap.yaml"
BUNDLES_DIR = TKT_DIR / "bundles"
NOTE_TASKS_CANDIDATES = [
    PROJECT_ROOT / "note" / "note_tasks.md",
    SKILLS_ROOT / "note" / "note_tasks.md",
]
TKT_SCRIPT = SKILLS_ROOT / "skill-system-tkt" / "scripts" / "tkt.sh"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read_yaml_simple(path: Path) -> dict:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    if yaml is not None:
        loaded = yaml.safe_load(text)
        return loaded if isinstance(loaded, dict) else {}
    result: dict = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            key, _, val = line.partition(":")
            result[key.strip()] = val.strip().strip('"').strip("'")
    return result


def _find_bundle_for_ticket(ticket_id: str) -> Path | None:
    """Find which bundle directory contains a given ticket."""
    if not BUNDLES_DIR.exists():
        return None
    for bundle_dir in sorted(BUNDLES_DIR.iterdir()):
        if not bundle_dir.is_dir():
            continue
        for ticket_file in bundle_dir.glob("TKT-*.yaml"):
            if ticket_file.stem == ticket_id:
                return bundle_dir
        for ticket_file in bundle_dir.glob("TKT-*.md"):
            if ticket_file.stem == ticket_id:
                return bundle_dir
    return None


def _read_review_yaml(bundle_dir: Path) -> dict:
    """Read review.yaml from a bundle directory."""
    review_path = bundle_dir / "review.yaml"
    if review_path.exists():
        return _read_yaml_simple(review_path)
    return {}


def _read_ticket_yaml(path: Path) -> dict:
    data = _read_yaml_simple(path)
    return data if isinstance(data, dict) else {}


def _ticket_wave(ticket: dict, wave_map: dict[str, int]) -> int:
    explicit_wave = ticket.get("wave")
    if isinstance(explicit_wave, int):
        return explicit_wave
    depends_on = ticket.get("depends_on") or []
    if not depends_on:
        return 1
    return max(wave_map.get(dep, 1) for dep in depends_on) + 1


def _dispatch_summary(title: str) -> str:
    cleaned = re.sub(r"\s+", " ", title).strip()
    return cleaned[:120]


def cmd_generate_dispatch(args: argparse.Namespace) -> None:
    bundle_dir = BUNDLES_DIR / args.bundle
    if not bundle_dir.exists():
        print(
            json.dumps({"error": f"Bundle not found: {args.bundle}"}), file=sys.stderr
        )
        sys.exit(1)

    requested = []
    if args.tickets:
        requested = [item.strip() for item in args.tickets.split(",") if item.strip()]

    tickets: list[dict] = []
    for ticket_file in sorted(bundle_dir.glob("TKT-*.yaml")):
        data = _read_ticket_yaml(ticket_file)
        ticket_id = data.get("id", ticket_file.stem)
        if ticket_id in {"TKT-000", "TKT-A00"}:
            continue
        if requested and ticket_id not in requested:
            continue
        if not requested and data.get("status") != "open":
            continue
        tickets.append(
            {
                "ticket_id": ticket_id,
                "title": data.get("title", ticket_id),
                "depends_on": data.get("depends_on") or [],
                "effort_estimate": data.get("effort_estimate"),
                "wave": data.get("wave"),
                "status": data.get("status"),
            }
        )

    wave_map: dict[str, int] = {}
    ordered = sorted(tickets, key=lambda item: item["ticket_id"])
    for ticket in ordered:
        wave_map[ticket["ticket_id"]] = _ticket_wave(ticket, wave_map)
        ticket["dispatch_wave"] = wave_map[ticket["ticket_id"]]
        ticket["summary"] = _dispatch_summary(ticket["title"])

    ordered.sort(key=lambda item: (item["dispatch_wave"], item["ticket_id"]))
    waves: dict[int, list[dict]] = {}
    for ticket in ordered:
        waves.setdefault(ticket["dispatch_wave"], []).append(ticket)

    human_lines = [f"Bundle {args.bundle} Dispatch"]
    for wave, wave_tickets in sorted(waves.items()):
        human_lines.append(f"Wave {wave}")
        for ticket in wave_tickets:
            effort = ticket.get("effort_estimate") or "unspecified"
            human_lines.append(
                f"- {ticket['ticket_id']}: {ticket['summary']} (effort={effort})"
            )

    output = {
        "bundle": args.bundle,
        "tickets": ordered,
        "waves": [
            {"wave": wave, "tickets": [ticket["ticket_id"] for ticket in wave_tickets]}
            for wave, wave_tickets in sorted(waves.items())
        ],
        "generated_at": _now_iso(),
    }
    print("\n".join(human_lines))
    print(json.dumps(output, ensure_ascii=False))


def _collect_ticket_results(bundle_dir: Path) -> list[dict]:
    """Collect all ticket result summaries from a bundle."""
    results = []
    for ticket_file in sorted(bundle_dir.glob("TKT-*.yaml")):
        data = _read_yaml_simple(ticket_file)
        results.append(
            {
                "ticket_id": ticket_file.stem,
                "file": str(ticket_file.relative_to(PROJECT_ROOT)),
                **data,
            }
        )
    for ticket_file in sorted(bundle_dir.glob("TKT-*.md")):
        text = ticket_file.read_text(encoding="utf-8")
        results.append(
            {
                "ticket_id": ticket_file.stem,
                "file": str(ticket_file.relative_to(PROJECT_ROOT)),
                "content_preview": text[:500],
            }
        )
    return results


def _read_roadmap() -> dict:
    """Read roadmap.yaml summary."""
    if not ROADMAP_PATH.exists():
        return {"status": "no_roadmap", "path": str(ROADMAP_PATH)}
    return {
        "status": "exists",
        "path": str(ROADMAP_PATH),
        **_read_yaml_simple(ROADMAP_PATH),
    }


def _list_bundles() -> list[dict]:
    """List all bundles with basic metadata."""
    bundles = []
    if not BUNDLES_DIR.exists():
        return bundles
    for bundle_dir in sorted(BUNDLES_DIR.iterdir(), key=lambda p: p.name, reverse=True):
        if not bundle_dir.is_dir():
            continue
        review = _read_review_yaml(bundle_dir)
        ticket_count = len(list(bundle_dir.glob("TKT-*")))
        bundles.append(
            {
                "bundle_id": bundle_dir.name,
                "path": str(bundle_dir.relative_to(PROJECT_ROOT)),
                "ticket_count": ticket_count,
                "has_review": bool(review),
                "review_summary": review.get("summary", ""),
            }
        )
    return bundles


# ---------------------------------------------------------------------------
# Command: generate-review-prompt
# ---------------------------------------------------------------------------


def cmd_generate_review_prompt(args: argparse.Namespace) -> None:
    ticket_id = args.ticket_id
    bundle_dir = _find_bundle_for_ticket(ticket_id)

    if bundle_dir is None:
        # Fallback: provide what we can without bundle context
        output = {
            "ticket_id": ticket_id,
            "bundle_found": False,
            "roadmap": _read_roadmap(),
            "review_context": {
                "note": f"Bundle for {ticket_id} not found in filesystem. "
                "Ticket may be DB-only. Use tickets.py for full context.",
            },
            "prometheus_prompt": _build_review_prompt_text(ticket_id, None, [], {}),
            "expected_output_format": _review_output_schema(),
            "generated_at": _now_iso(),
        }
    else:
        review_yaml = _read_review_yaml(bundle_dir)
        ticket_results = _collect_ticket_results(bundle_dir)
        audit_results = [
            t for t in ticket_results if t["ticket_id"].startswith("TKT-A")
        ]

        output = {
            "ticket_id": ticket_id,
            "bundle_id": bundle_dir.name,
            "bundle_found": True,
            "roadmap": _read_roadmap(),
            "review_context": {
                "review_yaml": review_yaml,
                "ticket_results": ticket_results,
                "audit_results": audit_results,
                "ticket_count": len(ticket_results),
                "audit_count": len(audit_results),
            },
            "prometheus_prompt": _build_review_prompt_text(
                ticket_id, bundle_dir.name, ticket_results, review_yaml
            ),
            "expected_output_format": _review_output_schema(),
            "generated_at": _now_iso(),
        }

    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    print()


def _build_review_prompt_text(
    ticket_id: str,
    bundle_id: str | None,
    ticket_results: list[dict],
    review_yaml: dict,
) -> str:
    lines = [
        "You are reviewing completed work as Prometheus (Review Agent).",
        "",
        f"Ticket: {ticket_id}",
    ]
    if bundle_id:
        lines.append(f"Bundle: {bundle_id}")
    if review_yaml.get("summary"):
        lines.extend(["", "## Existing Review Summary", review_yaml["summary"]])
    if ticket_results:
        lines.extend(["", "## Completed Tickets"])
        for t in ticket_results:
            tid = t.get("ticket_id", "?")
            title = t.get("title", t.get("content_preview", "")[:80])
            lines.append(f"- {tid}: {title}")
    lines.extend(
        [
            "",
            "## Your Task",
            "Evaluate the completed work and produce:",
            "1. **summary**: One paragraph — what was accomplished and the outcome",
            "2. **discussion_points[]**: Trade-offs, concerns, deferred work, questions for the user",
            "3. **next_actions[]**: Follow-up improvements, technical debt, natural extensions",
            "4. **quality_assessment**: { checked_items[], findings[], quality_score (1-5) }",
            "",
            "Output as JSON matching the expected_output_format.",
        ]
    )
    return "\n".join(lines)


def _review_output_schema() -> dict:
    return {
        "summary": "string — one paragraph",
        "discussion_points": ["string — each a question or concern"],
        "next_actions": ["string — each a follow-up action item"],
        "quality_assessment": {
            "checked_items": ["string"],
            "findings": ["string"],
            "quality_score": "integer 1-5",
        },
    }


# ---------------------------------------------------------------------------
# Command: generate-startup-review-prompt
# ---------------------------------------------------------------------------


def cmd_generate_startup_review_prompt(_args: argparse.Namespace) -> None:
    roadmap = _read_roadmap()
    bundles = _list_bundles()
    active_bundles = [b for b in bundles if not b.get("review_summary")]

    prompt_lines = [
        "You are Prometheus, starting a new session as Review Agent.",
        "",
        "## Roadmap Status",
        f"Stage: {roadmap.get('stage', 'unknown')}",
        f"Project: {roadmap.get('project', 'unknown')}",
        "",
        f"## Bundles ({len(bundles)} total, {len(active_bundles)} active)",
    ]
    for b in bundles:
        status = "reviewed" if b["has_review"] else "active"
        prompt_lines.append(
            f"- {b['bundle_id']}: {b['ticket_count']} tickets [{status}]"
        )

    prompt_lines.extend(
        [
            "",
            "## Your Task",
            "Review the current project state and identify:",
            "1. Bundles that need review attention",
            "2. Roadmap goals that may need re-prioritization",
            "3. Any stale or blocked work that needs intervention",
            "",
            "Provide your assessment as a structured JSON response.",
        ]
    )

    output = {
        "roadmap_summary": roadmap,
        "bundles": bundles,
        "prometheus_prompt": "\n".join(prompt_lines),
        "generated_at": _now_iso(),
    }
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    print()


# ---------------------------------------------------------------------------
# Command: plan-to-bundle
# ---------------------------------------------------------------------------

_CHECKBOX_RE = re.compile(r"^(\s*)- \[[ x]\]\s+(.+)$", re.MULTILINE)
_HEADING_RE = re.compile(r"^(#{1,4})\s+(.+)$", re.MULTILINE)
_DEPENDENCY_RE = re.compile(
    r"(?:depends\s+on|after|requires|blocked\s+by)\s+[\"']?([A-Za-z][A-Za-z0-9_\- ]*[A-Za-z0-9])(?=[\"')\],;\s]|$)",
    re.IGNORECASE,
)
_EFFORT_RE = re.compile(
    r"(?:effort|estimate|time)[:\s]+(\d+[\-–]\d+\s*[mhd]|\d+\s*[mhd])",
    re.IGNORECASE,
)
_CATEGORY_RE = re.compile(
    r"\b(visual-engineering|ultrabrain|deep|quick|explore|librarian|oracle|build)\b",
    re.IGNORECASE,
)


_INLINE_META_RE = re.compile(
    r"\s*\((?:effort|estimate|time)[:\s][^)]+\)\s*|\s*\[(?:build|deep|quick|explore|librarian|oracle|ultrabrain|visual-engineering)\]\s*",
    re.IGNORECASE,
)

_WAVE_TITLE_RE = re.compile(r"\bwave\s+(\d+)\b", re.IGNORECASE)

_STRUCTURED_META_RE = re.compile(
    r"^\s*(?:-|\*)\s*(category|agent|agent_type|effort|estimate|time|depends_on|depends on|acceptance|acceptance criteria|source_plan|source plan|source_ticket_index|source ticket index|skills|wave|qa_scenarios|qa scenarios)\s*:\s*(.+)$",
    re.IGNORECASE,
)


def _clean_title(raw_title: str) -> str:
    """Strip inline metadata (effort, agent type, dependency markers) from title."""
    title = _INLINE_META_RE.sub("", raw_title)
    title = re.sub(r"\s*\(depends\s+on[^)]*\)\s*", "", title, flags=re.IGNORECASE)
    return title.strip()


def _split_meta_list(value: str) -> list[str]:
    return [item.strip() for item in re.split(r"\s*[|,;]\s*", value) if item.strip()]


def _is_obviously_tiny_plan(text: str, tickets: list[dict]) -> bool:
    if len(tickets) < 1 or len(tickets) > 2:
        return False

    if len(text) > 1200:
        return False

    nonempty_lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(nonempty_lines) > 20:
        return False

    if "```" in text:
        return False

    if _DEPENDENCY_RE.search(text) or _EFFORT_RE.search(text):
        return False

    subheadings = [m for m in _HEADING_RE.finditer(text) if len(m.group(1)) >= 2]
    if len(subheadings) > 1:
        return False

    for ticket in tickets:
        if ticket.get("depends_on") or ticket.get("effort_estimate"):
            return False

    return True


def _parse_checkbox_tickets(text: str, plan_path: Path) -> list[dict]:
    lines = text.splitlines()
    tickets: list[dict] = []
    line_waves: list[int | None] = []
    current_wave: int | None = None
    for line in lines:
        heading = _HEADING_RE.match(line)
        if heading:
            wave_match = _WAVE_TITLE_RE.search(heading.group(2))
            if wave_match:
                current_wave = int(wave_match.group(1))
        line_waves.append(current_wave)

    i = 0
    ticket_index = 1
    while i < len(lines):
        line = lines[i]
        match = _CHECKBOX_RE.match(line)
        if not match:
            i += 1
            continue

        raw_title = match.group(2).strip()
        title = _clean_title(raw_title)
        ticket: dict = {
            "index": ticket_index,
            "title": title,
            "type": "worker",
            "source_plan": str(plan_path.name),
            "source_ticket_index": ticket_index,
        }
        if line_waves[i] is not None:
            ticket["wave"] = line_waves[i]

        effort_match = _EFFORT_RE.search(raw_title)
        cat_match = _CATEGORY_RE.search(raw_title)
        dep_match = _DEPENDENCY_RE.search(raw_title)
        if dep_match:
            ticket["depends_on"] = [
                d.strip() for d in dep_match.group(1).split(",") if d.strip()
            ]
        if effort_match:
            ticket["effort_estimate"] = effort_match.group(1).strip()
        if cat_match:
            ticket["category"] = cat_match.group(1).lower()

        j = i + 1
        while j < len(lines):
            next_line = lines[j]
            if _CHECKBOX_RE.match(next_line):
                break
            heading = _HEADING_RE.match(next_line)
            if heading and (
                len(heading.group(1)) <= 2
                or _WAVE_TITLE_RE.search(heading.group(2)) is not None
            ):
                break

            meta_match = _STRUCTURED_META_RE.match(next_line)
            if meta_match:
                key = meta_match.group(1).lower().replace(" ", "_")
                value = meta_match.group(2).strip()
                if key in {"agent", "agent_type", "category"}:
                    ticket["category"] = value.lower()
                elif key in {"effort", "estimate", "time"}:
                    ticket["effort_estimate"] = value
                elif key == "depends_on":
                    ticket["depends_on"] = [
                        d.strip() for d in value.split(",") if d.strip()
                    ]
                elif key == "skills":
                    ticket["skills"] = _split_meta_list(value)
                elif key == "wave":
                    if value.isdigit():
                        ticket["wave"] = int(value)
                elif key == "qa_scenarios":
                    ticket.setdefault("qa_scenarios", []).extend(
                        _split_meta_list(value)
                    )
                elif key == "acceptance" or key == "acceptance_criteria":
                    ticket.setdefault("acceptance_criteria", []).append(value)
                elif key == "source_plan":
                    ticket["source_plan"] = value
                elif key == "source_ticket_index":
                    if value.isdigit():
                        ticket["source_ticket_index"] = int(value)
            j += 1

        tickets.append(ticket)
        ticket_index += 1
        i = j

    return tickets


def cmd_plan_to_bundle(args: argparse.Namespace) -> None:
    plan_path = Path(args.plan_file)
    if not plan_path.exists():
        print(
            json.dumps({"error": f"Plan file not found: {plan_path}"}), file=sys.stderr
        )
        sys.exit(1)

    text = plan_path.read_text(encoding="utf-8")

    # Extract goal from first heading
    first_heading = _HEADING_RE.search(text)
    goal = first_heading.group(2).strip() if first_heading else plan_path.stem

    tickets: list[dict] = _parse_checkbox_tickets(text, plan_path)

    # If no checkboxes found, try headings as tasks
    if not tickets:
        for i, match in enumerate(_HEADING_RE.finditer(text), start=1):
            level = len(match.group(1))
            if level >= 2:  # Skip the main title
                title = match.group(2).strip()
                # Skip headings that look like section headers
                if any(
                    kw in title.lower()
                    for kw in ["goal", "context", "note", "summary", "reference"]
                ):
                    continue
                tickets.append(
                    {
                        "index": i,
                        "title": title,
                        "type": "worker",
                    }
                )

    route = "express" if _is_obviously_tiny_plan(text, tickets) else "bundle"
    tkt_commands = []
    tkt_sh = str(TKT_SCRIPT)
    if route == "express" and tickets:
        for t in tickets:
            acceptance_parts = list(t.get("acceptance_criteria") or [t["title"]])
            if t.get("effort_estimate"):
                acceptance_parts.append(f"Effort: {t['effort_estimate']}")
            if t.get("category"):
                acceptance_parts.append(f"Category: {t['category']}")
            if t.get("skills"):
                acceptance_parts.append(f"Skills: {', '.join(t['skills'])}")
            if t.get("qa_scenarios"):
                acceptance_parts.append(f"QA: {', '.join(t['qa_scenarios'])}")
            if t.get("depends_on"):
                acceptance_parts.append(f"Depends on: {', '.join(t['depends_on'])}")
            acceptance = " | ".join(acceptance_parts)
            cmd = f'bash "{tkt_sh}" express --title "{t["title"]}" --acceptance "{acceptance}"'
            if t.get("category"):
                cmd += f' --category "{t["category"]}"'
            if t.get("effort_estimate"):
                cmd += f' --effort-estimate "{t["effort_estimate"]}"'
            if t.get("wave") is not None:
                cmd += f" --wave {t['wave']}"
            if t.get("source_plan"):
                cmd += f' --source-plan "{t["source_plan"]}"'
            if t.get("source_ticket_index") is not None:
                cmd += f" --source-ticket-index {t['source_ticket_index']}"
            tkt_commands.append(cmd)
        note = "Express route: create one express ticket per parsed task (no bundle ID substitution)."
    else:
        tkt_commands.append(f'bash "{tkt_sh}" create-bundle --goal "{goal}"')
        for t in tickets:
            desc_parts = [t["title"]]
            if t.get("effort_estimate"):
                desc_parts.append(f"Effort: {t['effort_estimate']}")
            if t.get("category"):
                desc_parts.append(f"Category: {t['category']}")
            if t.get("skills"):
                desc_parts.append(f"Skills: {', '.join(t['skills'])}")
            if t.get("wave") is not None:
                desc_parts.append(f"Wave: {t['wave']}")
            if t.get("qa_scenarios"):
                desc_parts.append(f"QA: {', '.join(t['qa_scenarios'])}")
            if t.get("depends_on"):
                desc_parts.append(f"Depends on: {', '.join(t['depends_on'])}")
            desc = " | ".join(desc_parts)
            acceptance = " | ".join(t.get("acceptance_criteria") or [t["title"]])
            cmd = f'bash "{tkt_sh}" add --bundle <B-NNN> --type worker --title "{t["title"]}" --description "{desc}" --acceptance "{acceptance}"'
            if t.get("category"):
                cmd += f' --category "{t["category"]}"'
            if t.get("effort_estimate"):
                cmd += f' --effort-estimate "{t["effort_estimate"]}"'
            if t.get("skills"):
                cmd += f' --skills "{", ".join(t["skills"])}"'
            if t.get("wave") is not None:
                cmd += f" --wave {t['wave']}"
            if t.get("qa_scenarios"):
                cmd += f' --qa-scenarios "{", ".join(t["qa_scenarios"])}"'
            if t.get("depends_on"):
                cmd += f' --depends-on "{", ".join(t["depends_on"])}"'
            if t.get("source_plan"):
                cmd += f' --source-plan "{t["source_plan"]}"'
            if t.get("source_ticket_index") is not None:
                cmd += f" --source-ticket-index {t['source_ticket_index']}"
            tkt_commands.append(cmd)
        note = "Replace <B-NNN> with the actual bundle ID from create-bundle output."

    output = {
        "source_file": str(plan_path),
        "route": route,
        "goal": goal,
        "tickets": tickets,
        "ticket_count": len(tickets),
        "tkt_commands": tkt_commands,
        "note": note,
        "generated_at": _now_iso(),
    }
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    print()


# ---------------------------------------------------------------------------
# Command: write-review-inbox
# ---------------------------------------------------------------------------


def cmd_write_review_inbox(args: argparse.Namespace) -> None:
    input_path = Path(args.input)
    if not input_path.exists():
        print(
            json.dumps({"error": f"Input file not found: {input_path}"}),
            file=sys.stderr,
        )
        sys.exit(1)

    data = json.loads(input_path.read_text(encoding="utf-8"))
    batch_id = data.get("batch_id", f"REVIEW-{_now_iso()[:10]}")
    discussion_points = data.get("discussion_points", [])
    next_actions = data.get("next_actions", [])

    # Build TICKET_BATCH format matching parse_review_agent_inbox() expectations
    tickets_md: list[str] = []
    ticket_idx = 1

    for point in discussion_points:
        tid = f"RV-{batch_id[-3:]}-{ticket_idx:03d}"
        tickets_md.append(f"### {tid}")
        tickets_md.append(f"- title: {point[:120]}")
        tickets_md.append(f"- summary: {point}")
        tickets_md.append(f"- ticket_type: WORKER")
        tickets_md.append(f"- requested_status: OPEN")
        tickets_md.append(f"- queue_order: {ticket_idx}")
        tickets_md.append("")
        ticket_idx += 1

    for action in next_actions:
        tid = f"RV-{batch_id[-3:]}-{ticket_idx:03d}"
        tickets_md.append(f"### {tid}")
        tickets_md.append(f"- title: {action[:120]}")
        tickets_md.append(f"- summary: {action}")
        tickets_md.append(f"- ticket_type: WORKER")
        tickets_md.append(f"- requested_status: OPEN")
        tickets_md.append(f"- queue_order: {ticket_idx}")
        tickets_md.append("")
        ticket_idx += 1

    inbox_section = "\n".join(
        [
            "",
            "## Review Agent Inbox",
            f"# TICKET_BATCH",
            f"batch_id: {batch_id}",
            f"generated_at: {_now_iso()}",
            f"source: prometheus-review",
            "",
            *tickets_md,
        ]
    )

    # Find or create note_tasks.md
    target_path: Path | None = None
    for candidate in NOTE_TASKS_CANDIDATES:
        if candidate.exists():
            target_path = candidate
            break

    if target_path is None:
        # Create in first candidate location
        target_path = NOTE_TASKS_CANDIDATES[0]
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(f"# Tasks\n{inbox_section}\n", encoding="utf-8")
    else:
        existing = target_path.read_text(encoding="utf-8")
        # Remove existing Review Agent Inbox section if present
        inbox_marker = "## Review Agent Inbox"
        if inbox_marker in existing:
            idx = existing.index(inbox_marker)
            # Find next ## heading after inbox
            rest = existing[idx + len(inbox_marker) :]
            next_section = re.search(r"^## ", rest, re.MULTILINE)
            if next_section:
                existing = existing[:idx] + rest[next_section.start() :]
            else:
                existing = existing[:idx].rstrip()
        target_path.write_text(
            existing.rstrip() + "\n" + inbox_section + "\n", encoding="utf-8"
        )

    output = {
        "inbox_path": str(target_path),
        "tickets_written": ticket_idx - 1,
        "batch_id": batch_id,
        "generated_at": _now_iso(),
    }
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    print()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="skill-system-review: Prometheus ↔ TKT bridge"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_review = sub.add_parser("generate-review-prompt")
    p_review.add_argument("--ticket-id", required=True)
    p_review.set_defaults(func=cmd_generate_review_prompt)

    p_startup = sub.add_parser("generate-startup-review-prompt")
    p_startup.set_defaults(func=cmd_generate_startup_review_prompt)

    p_plan = sub.add_parser("plan-to-bundle")
    p_plan.add_argument("--plan-file", required=True)
    p_plan.set_defaults(func=cmd_plan_to_bundle)

    p_dispatch = sub.add_parser("generate-dispatch")
    p_dispatch.add_argument("--bundle", required=True)
    p_dispatch.add_argument("--tickets")
    p_dispatch.set_defaults(func=cmd_generate_dispatch)

    p_inbox = sub.add_parser("write-review-inbox")
    p_inbox.add_argument("--input", required=True)
    p_inbox.set_defaults(func=cmd_write_review_inbox)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
