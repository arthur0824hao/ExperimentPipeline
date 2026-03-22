#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import re
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[3]
WORKFLOW_TICKETS_PATH = (
    ROOT_DIR / "skills" / "skill-system-workflow" / "scripts" / "tickets.py"
)
ARCHITECTURE_MAP_PATH = ROOT_DIR / "note" / "architecture_map.md"
LAYER_NODES = [
    ("runtime:L0", "runtime", "L0 Runtime"),
    ("runtime:L1", "runtime", "L1 Hooks"),
    ("runtime:L2", "runtime", "L2 Skills"),
    ("runtime:L3", "runtime", "L3 Workflow"),
    ("runtime:L4", "runtime", "L4 Human Interface"),
]
ALLOWED_SUFFIXES = {".md", ".py", ".yaml", ".yml", ".sql", ".json"}


def load_workflow_module():
    spec = importlib.util.spec_from_file_location(
        "architecture_graph_workflow", WORKFLOW_TICKETS_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {WORKFLOW_TICKETS_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def connect_graph_db():
    workflow_mod = load_workflow_module()
    return workflow_mod.connect_workflow_db()


def display_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def collect_project_paths(repo_root: Path = ROOT_DIR) -> list[Path]:
    collected: list[Path] = []
    for directory in ("skills", "note", "review", "spec"):
        base = repo_root / directory
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if path.is_file() and path.suffix in ALLOWED_SUFFIXES:
                collected.append(path)
    return collected


def classify_node_type(relative_path: str) -> str:
    if relative_path.startswith("skills/skill-system-workflow/"):
        return "workflow"
    if relative_path.startswith("skills/"):
        return "skill"
    return "doc"


def extract_referenced_paths(text: str) -> list[str]:
    pattern = re.compile(r"(?:skills|note|review|spec)/[A-Za-z0-9_./-]+")
    found: list[str] = []
    for match in pattern.findall(text):
        candidate = match.rstrip("`.,:)")
        if candidate not in found:
            found.append(candidate)
    return found


def guess_relation_type(source_path: str, target_path: str) -> str:
    if source_path.endswith("cockpit.py") and target_path.endswith("note_feedback.md"):
        return "renders"
    if source_path.endswith("rule_projection.py") and target_path.endswith("_rules.md"):
        return "writes"
    if source_path.endswith("architecture_graph.py") and target_path.endswith(
        "architecture_map.md"
    ):
        return "writes"
    if source_path.endswith("tickets.py") and target_path.endswith("note_tasks.md"):
        return "reads"
    if target_path.endswith(".md"):
        return "reads"
    return "depends_on"


def build_architecture_graph(repo_root: Path = ROOT_DIR) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    seen_paths: set[str] = set()

    for source_path, node_type, name in LAYER_NODES:
        nodes.append({"node_type": node_type, "name": name, "source_path": source_path})
        seen_paths.add(source_path)

    project_paths = collect_project_paths(repo_root)
    for path in project_paths:
        relative_path = display_path(path, repo_root)
        if relative_path in seen_paths:
            continue
        nodes.append(
            {
                "node_type": classify_node_type(relative_path),
                "name": path.stem,
                "source_path": relative_path,
            }
        )
        seen_paths.add(relative_path)

    node_lookup = {node["source_path"]: node for node in nodes}

    for source_path, target_path in [
        ("runtime:L0", "runtime:L1"),
        ("runtime:L1", "runtime:L2"),
        ("runtime:L2", "runtime:L3"),
        ("runtime:L3", "runtime:L4"),
    ]:
        edges.append(
            {
                "source_path": source_path,
                "target_path": target_path,
                "relation_type": "depends_on",
            }
        )

    for node in nodes:
        source_path = node["source_path"]
        if node["node_type"] == "skill":
            edges.append(
                {
                    "source_path": "runtime:L2",
                    "target_path": source_path,
                    "relation_type": "depends_on",
                }
            )
        elif node["node_type"] == "workflow":
            edges.append(
                {
                    "source_path": "runtime:L3",
                    "target_path": source_path,
                    "relation_type": "depends_on",
                }
            )
        elif node["node_type"] == "doc":
            edges.append(
                {
                    "source_path": "runtime:L4",
                    "target_path": source_path,
                    "relation_type": "depends_on",
                }
            )

    for path in project_paths:
        relative_path = display_path(path, repo_root)
        text = path.read_text(encoding="utf-8")
        for referenced_path in extract_referenced_paths(text):
            if referenced_path in node_lookup:
                edges.append(
                    {
                        "source_path": relative_path,
                        "target_path": referenced_path,
                        "relation_type": guess_relation_type(
                            relative_path, referenced_path
                        ),
                    }
                )

    deduped_edges: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str]] = set()
    for edge in edges:
        key = (edge["source_path"], edge["target_path"], edge["relation_type"])
        if key in seen_edges:
            continue
        seen_edges.add(key)
        deduped_edges.append(edge)

    nodes = sorted(nodes, key=lambda item: (item["node_type"], item["source_path"]))
    deduped_edges = sorted(
        deduped_edges,
        key=lambda item: (
            item["relation_type"],
            item["source_path"],
            item["target_path"],
        ),
    )
    layer_map = {
        "L0 Runtime": len(
            [node for node in nodes if node["source_path"] == "runtime:L0"]
        ),
        "L1 Hooks": len(
            [node for node in nodes if node["source_path"] == "runtime:L1"]
        ),
        "L2 Skills": len([node for node in nodes if node["node_type"] == "skill"]),
        "L3 Workflow": len(
            [node for node in nodes if node["node_type"] in {"workflow", "runtime"}]
        ),
        "L4 Human Interface": len(
            [node for node in nodes if node["node_type"] == "doc"]
        ),
    }
    return {"nodes": nodes, "edges": deduped_edges, "layer_map": layer_map}


def sync_architecture_graph(conn: Any, graph: dict[str, Any]) -> dict[str, Any]:
    node_ids: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute("DELETE FROM skill_system.project_edges")
        cur.execute("DELETE FROM skill_system.project_nodes")
        for node in graph["nodes"]:
            cur.execute(
                """
                INSERT INTO skill_system.project_nodes (node_type, name, source_path)
                VALUES (%s, %s, %s)
                RETURNING node_id
                """,
                (node["node_type"], node["name"], node["source_path"]),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("Failed to insert project node")
            node_ids[node["source_path"]] = int(row[0])
        for edge in graph["edges"]:
            cur.execute(
                """
                INSERT INTO skill_system.project_edges (source_node, target_node, relation_type)
                VALUES (%s, %s, %s)
                """,
                (
                    node_ids[edge["source_path"]],
                    node_ids[edge["target_path"]],
                    edge["relation_type"],
                ),
            )
    return graph


def read_architecture_graph(conn: Any) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT node_id, node_type, name, source_path
            FROM skill_system.project_nodes
            ORDER BY node_type ASC, source_path ASC
            """
        )
        nodes = [
            {
                "node_id": int(row[0]),
                "node_type": row[1],
                "name": row[2],
                "source_path": row[3],
            }
            for row in cur.fetchall()
        ]
        cur.execute(
            """
            SELECT pe.edge_id, src.source_path, dst.source_path, pe.relation_type
            FROM skill_system.project_edges pe
            JOIN skill_system.project_nodes src ON src.node_id = pe.source_node
            JOIN skill_system.project_nodes dst ON dst.node_id = pe.target_node
            ORDER BY pe.relation_type ASC, src.source_path ASC, dst.source_path ASC
            """
        )
        edges = [
            {
                "edge_id": int(row[0]),
                "source_path": row[1],
                "target_path": row[2],
                "relation_type": row[3],
            }
            for row in cur.fetchall()
        ]
    layer_map = {
        "L0 Runtime": len(
            [node for node in nodes if node["source_path"] == "runtime:L0"]
        ),
        "L1 Hooks": len(
            [node for node in nodes if node["source_path"] == "runtime:L1"]
        ),
        "L2 Skills": len([node for node in nodes if node["node_type"] == "skill"]),
        "L3 Workflow": len(
            [node for node in nodes if node["node_type"] in {"workflow", "runtime"}]
        ),
        "L4 Human Interface": len(
            [node for node in nodes if node["node_type"] == "doc"]
        ),
    }
    return {"nodes": nodes, "edges": edges, "layer_map": layer_map}


def build_architecture_graph_summary(graph: dict[str, Any]) -> dict[str, Any]:
    return {
        "node_count": len(graph["nodes"]),
        "edge_count": len(graph["edges"]),
        "layer_map": graph["layer_map"],
    }


def render_architecture_map(graph: dict[str, Any]) -> str:
    skill_nodes = [node for node in graph["nodes"] if node["node_type"] == "skill"]
    doc_nodes = [node for node in graph["nodes"] if node["node_type"] == "doc"]
    workflow_nodes = [
        node for node in graph["nodes"] if node["node_type"] in {"workflow", "runtime"}
    ]
    lines = [
        "# ARCHITECTURE_MAP",
        "",
        "- source_of_truth: `skill_system.project_nodes` + `skill_system.project_edges`",
        f"- node_count: `{len(graph['nodes'])}`",
        f"- edge_count: `{len(graph['edges'])}`",
        "",
        "## Runtime Layers",
        "",
    ]
    for name, count in graph["layer_map"].items():
        lines.append(f"- {name}: {count}")
    lines.extend(["", "## Skill Graph", ""])
    for node in skill_nodes[:10]:
        lines.append(f"- {node['source_path']}")
    lines.extend(["", "## Document Graph", ""])
    for node in doc_nodes[:10]:
        lines.append(f"- {node['source_path']}")
    lines.extend(["", "## Workflow Graph", ""])
    for node in workflow_nodes[:10]:
        lines.append(f"- {node['source_path']}")
    lines.append("")
    return "\n".join(lines)


def write_architecture_map(
    graph: dict[str, Any], repo_root: Path = ROOT_DIR, output_path: Path | None = None
) -> str:
    path = output_path or ARCHITECTURE_MAP_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_architecture_map(graph), encoding="utf-8")
    return display_path(path, repo_root)


def run_sync(
    write_file: bool, repo_root: Path = ROOT_DIR, output_path: Path | None = None
) -> dict[str, Any]:
    conn, db_target, target_source = connect_graph_db()
    try:
        graph = build_architecture_graph(repo_root)
        sync_architecture_graph(conn, graph)
        stored = read_architecture_graph(conn)
        written_file = (
            write_architecture_map(stored, repo_root, output_path)
            if write_file
            else None
        )
        conn.commit()
        return {
            "status": "ok",
            "db_target": db_target,
            "target_source": target_source,
            "architecture_graph": build_architecture_graph_summary(stored),
            "written_file": written_file,
        }
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync canonical project architecture graph into PostgreSQL and note/architecture_map.md."
    )
    parser.add_argument(
        "--write-file",
        action="store_true",
        help="Write note/architecture_map.md after syncing.",
    )
    parser.add_argument(
        "--output-path", help="Optional override for the architecture_map output path."
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = run_sync(
        write_file=args.write_file,
        output_path=Path(args.output_path).resolve() if args.output_path else None,
    )
    print(json.dumps(payload, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
