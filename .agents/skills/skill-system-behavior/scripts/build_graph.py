#!/usr/bin/env python3

import argparse
import json
import re
import sys
from pathlib import Path

import yaml


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build a skill dependency graph from SKILL.spec.yaml files."
    )
    parser.add_argument(
        "--skills-dir",
        default="./skills",
        help="Root directory to scan recursively for SKILL.spec.yaml files.",
    )
    parser.add_argument(
        "--format",
        choices=["mermaid", "json", "sql"],
        default="mermaid",
        help="Output format.",
    )
    parser.add_argument(
        "--db-write",
        action="store_true",
        help="Emit SQL statements suitable for piping to psql.",
    )
    return parser.parse_args()


def load_yaml(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_index(skills_dir):
    candidates = [
        skills_dir / "skills-index.json",
        skills_dir.parent / "skills-index.json",
    ]
    for path in candidates:
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                return data.get("skills", {})
            except Exception:
                return {}
    return {}


def sanitize_mermaid_id(value):
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    if not cleaned:
        cleaned = "skill"
    if cleaned[0].isdigit():
        cleaned = f"s_{cleaned}"
    return cleaned


def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_text_array(values):
    if not values:
        return "ARRAY[]::TEXT[]"
    parts = [sql_literal(v) for v in values]
    return f"ARRAY[{', '.join(parts)}]::TEXT[]"


def sql_json(value):
    payload = json.dumps(value, ensure_ascii=False)
    return sql_literal(payload) + "::jsonb"


def build_graph(skills_dir):
    specs = sorted(skills_dir.rglob("SKILL.spec.yaml"))
    index_skills = load_index(skills_dir)

    nodes = []
    edges = []
    node_names = set()

    for spec_path in specs:
        spec = load_yaml(spec_path) or {}
        skill_name = spec.get("skill_name")
        if not isinstance(skill_name, str) or not skill_name.strip():
            continue

        skill_name = skill_name.strip()
        node_names.add(skill_name)
        index_meta = (
            index_skills.get(skill_name, {}) if isinstance(index_skills, dict) else {}
        )

        node = {
            "skill_name": skill_name,
            "description": spec.get("description", ""),
            "version": index_meta.get("version"),
            "capabilities": index_meta.get("capabilities", []) or [],
            "effects": index_meta.get("effects", []) or [],
            "metadata": {
                "spec_path": str(spec_path),
                "dir": index_meta.get("dir"),
            },
        }
        nodes.append(node)

        for dep in spec.get("depends_on", []) or []:
            if isinstance(dep, str) and dep.strip():
                edges.append(
                    {
                        "from_skill": skill_name,
                        "to_skill": dep.strip(),
                        "edge_type": "depends_on",
                        "metadata": {},
                    }
                )

        for delegate in spec.get("delegates_to", []) or []:
            if isinstance(delegate, str) and delegate.strip():
                edges.append(
                    {
                        "from_skill": skill_name,
                        "to_skill": delegate.strip(),
                        "edge_type": "delegates_to",
                        "metadata": {},
                    }
                )

    for edge in edges:
        node_names.add(edge["to_skill"])

    existing = {n["skill_name"] for n in nodes}
    for name in sorted(node_names - existing):
        index_meta = (
            index_skills.get(name, {}) if isinstance(index_skills, dict) else {}
        )
        nodes.append(
            {
                "skill_name": name,
                "description": index_meta.get("description", ""),
                "version": index_meta.get("version"),
                "capabilities": index_meta.get("capabilities", []) or [],
                "effects": index_meta.get("effects", []) or [],
                "metadata": {
                    "spec_path": None,
                    "dir": index_meta.get("dir"),
                    "stub": True,
                },
            }
        )

    nodes = sorted(nodes, key=lambda n: n["skill_name"])

    dedup = set()
    unique_edges = []
    for edge in edges:
        key = (edge["from_skill"], edge["to_skill"], edge["edge_type"])
        if key in dedup:
            continue
        dedup.add(key)
        unique_edges.append(edge)
    unique_edges = sorted(
        unique_edges,
        key=lambda e: (e["from_skill"], e["edge_type"], e["to_skill"]),
    )

    return {"nodes": nodes, "edges": unique_edges}


def to_mermaid(graph):
    lines = ["flowchart TD"]

    for node in graph["nodes"]:
        node_id = sanitize_mermaid_id(node["skill_name"])
        lines.append(f'  {node_id}["{node["skill_name"]}"]')

    if graph["edges"]:
        lines.append("")

    for edge in graph["edges"]:
        src = sanitize_mermaid_id(edge["from_skill"])
        dst = sanitize_mermaid_id(edge["to_skill"])
        label = edge["edge_type"]
        if edge["edge_type"] == "depends_on":
            lines.append(f"  {src} -->|{label}| {dst}")
        else:
            lines.append(f"  {src} -.->|{label}| {dst}")

    return "\n".join(lines)


def to_sql(graph):
    lines = ["BEGIN;"]
    for node in graph["nodes"]:
        lines.append(
            "SELECT skill_system.upsert_graph_node("
            f"{sql_literal(node['skill_name'])}, "
            f"{sql_literal(node.get('description'))}, "
            f"{sql_literal(node.get('version'))}, "
            f"{sql_text_array(node.get('capabilities', []))}, "
            f"{sql_text_array(node.get('effects', []))}, "
            f"{sql_json(node.get('metadata', {}))}"
            ");"
        )

    for edge in graph["edges"]:
        lines.append(
            "SELECT skill_system.upsert_graph_edge("
            f"{sql_literal(edge['from_skill'])}, "
            f"{sql_literal(edge['to_skill'])}, "
            f"{sql_literal(edge['edge_type'])}, "
            f"{sql_json(edge.get('metadata', {}))}"
            ");"
        )
    lines.append("COMMIT;")
    return "\n".join(lines)


def main():
    args = parse_args()
    skills_dir = Path(args.skills_dir).resolve()

    graph = build_graph(skills_dir)

    if args.db_write or args.format == "sql":
        print(to_sql(graph))
        return 0

    if args.format == "json":
        print(json.dumps(graph, indent=2, ensure_ascii=False))
        return 0

    print(to_mermaid(graph))
    return 0


if __name__ == "__main__":
    sys.exit(main())
