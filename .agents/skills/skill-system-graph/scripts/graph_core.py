#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path
from typing import Any

import yaml


ROOT_DIR = Path(__file__).resolve().parents[3]
DEFAULT_DB_TARGET = "agent_memory"
DB_CONNECT_TIMEOUT = 10
ALLOWED_EDGE_TYPES = {"depends_on", "delegates_to"}


def resolve_db_target() -> tuple[str, str]:
    explicit = os.environ.get("SKILL_PGDATABASE", "").strip()
    ambient = os.environ.get("PGDATABASE", "").strip()

    if explicit:
        if ambient and ambient != explicit:
            return explicit, f"SKILL_PGDATABASE(overrides:{ambient})"
        return explicit, "SKILL_PGDATABASE"

    if ambient:
        return ambient, "PGDATABASE"

    return DEFAULT_DB_TARGET, f"default:{DEFAULT_DB_TARGET}"


def connect_db():
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:
        raise RuntimeError("psycopg2 is required") from exc

    db_target, _target_source = resolve_db_target()
    kwargs: dict[str, Any] = {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": db_target,
        "connect_timeout": DB_CONNECT_TIMEOUT,
    }
    user = os.environ.get("PGUSER", "").strip()
    if user:
        kwargs["user"] = user

    conn = psycopg2.connect(**kwargs)
    conn.autocommit = False
    return conn


def _load_yaml(path: Path) -> dict:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Spec root must be a mapping: {path}")
    return data


def compute_content_hash(spec_path: str | Path) -> str:
    spec_path = Path(spec_path)
    content = spec_path.read_text(encoding="utf-8")
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def _as_schema_version(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return 1


def _parse_v1_spec(spec: dict[str, Any], spec_path: Path) -> dict[str, Any]:
    skill_name = spec.get("skill_name")
    if not isinstance(skill_name, str) or not skill_name.strip():
        raise ValueError(f"Missing or invalid skill_name in {spec_path}")
    skill_name = skill_name.strip()

    description = spec.get("description")
    if not isinstance(description, str):
        description = ""

    operations = spec.get("operations", [])
    operations_count = 0
    if isinstance(operations, list):
        for operation in operations:
            if isinstance(operation, dict):
                operations_count += 1

    node = {
        "skill_name": skill_name,
        "description": description,
        "spec_path": str(spec_path),
        "content_hash": compute_content_hash(spec_path),
        "operations_count": operations_count,
    }

    edges = []
    for dep in spec.get("depends_on", []) or []:
        if isinstance(dep, str) and dep.strip():
            edges.append(
                {
                    "from_skill": skill_name,
                    "to_skill": dep.strip(),
                    "edge_type": "depends_on",
                }
            )

    for delegate in spec.get("delegates_to", []) or []:
        if isinstance(delegate, str) and delegate.strip():
            edges.append(
                {
                    "from_skill": skill_name,
                    "to_skill": delegate.strip(),
                    "edge_type": "delegates_to",
                }
            )

    return {"node": node, "nodes": [node], "edges": edges}


def _parse_v2_spec(spec: dict[str, Any], spec_path: Path) -> dict[str, Any]:
    spec_id = spec.get("spec_id")
    if not isinstance(spec_id, str) or not spec_id.strip():
        raise ValueError(f"Missing or invalid spec_id in {spec_path}")
    spec_id = spec_id.strip()

    spec_description = spec.get("description")
    if not isinstance(spec_description, str):
        spec_description = ""

    content_hash = compute_content_hash(spec_path)
    node_aliases: dict[str, str] = {}
    nodes: list[dict[str, Any]] = []

    raw_nodes = spec.get("nodes", [])
    if isinstance(raw_nodes, list):
        for raw_node in raw_nodes:
            if not isinstance(raw_node, dict):
                continue
            node_id = raw_node.get("id")
            if not isinstance(node_id, str) or not node_id.strip():
                continue
            node_id = node_id.strip()
            graph_node_id = f"{spec_id}::{node_id}"
            node_aliases[node_id] = graph_node_id

            node_description = raw_node.get("description")
            if not isinstance(node_description, str):
                node_description = ""

            behaviors = raw_node.get("behaviors", [])
            operations_count = 0
            if isinstance(behaviors, list):
                for behavior in behaviors:
                    if isinstance(behavior, dict):
                        operations_count += 1

            nodes.append(
                {
                    "skill_name": graph_node_id,
                    "description": node_description,
                    "spec_path": str(spec_path),
                    "content_hash": content_hash,
                    "operations_count": operations_count,
                }
            )

    if not nodes:
        nodes.append(
            {
                "skill_name": spec_id,
                "description": spec_description,
                "spec_path": str(spec_path),
                "content_hash": content_hash,
                "operations_count": 0,
            }
        )

    edges = []
    raw_relationships = spec.get("relationships", [])
    if isinstance(raw_relationships, list):
        for relationship in raw_relationships:
            if not isinstance(relationship, dict):
                continue
            from_node = relationship.get("from")
            to_node = relationship.get("to")
            if (
                not isinstance(from_node, str)
                or not from_node.strip()
                or not isinstance(to_node, str)
                or not to_node.strip()
            ):
                continue

            from_key = node_aliases.get(from_node.strip(), from_node.strip())
            to_key = node_aliases.get(to_node.strip(), to_node.strip())
            relationship_type = relationship.get("type")
            edge_type = (
                "delegates_to"
                if isinstance(relationship_type, str)
                and relationship_type.strip() == "delegates_to"
                else "depends_on"
            )

            edges.append(
                {
                    "from_skill": from_key,
                    "to_skill": to_key,
                    "edge_type": edge_type,
                }
            )

    return {"node": nodes[0], "nodes": nodes, "edges": edges}


def parse_spec(spec_path: str | Path) -> dict:
    spec_path = Path(spec_path)
    spec = _load_yaml(spec_path)

    schema_version = _as_schema_version(spec.get("schema_version"))
    if schema_version >= 2:
        return _parse_v2_spec(spec, spec_path)
    return _parse_v1_spec(spec, spec_path)


def infer_node_type(node_key: str, spec_path: Any, stub: bool) -> str:
    if stub:
        return "reference"
    if isinstance(spec_path, str) and spec_path.endswith(".behavior.yaml"):
        return "behavior-node"
    if "::" in node_key:
        return "behavior-node"
    return "skill"


def _node_key_from_record(node: dict[str, Any]) -> str | None:
    for field_name in ("node_key", "skill_name", "id", "name"):
        value = node.get(field_name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _edge_fields_from_record(edge: dict[str, Any]) -> tuple[str, str, str] | None:
    source_node = edge.get("source_node")
    if not isinstance(source_node, str) or not source_node.strip():
        source_node = edge.get("from_skill")

    target_node = edge.get("target_node")
    if not isinstance(target_node, str) or not target_node.strip():
        target_node = edge.get("to_skill")

    relation_type = edge.get("relation_type")
    if not isinstance(relation_type, str) or not relation_type.strip():
        relation_type = edge.get("edge_type")

    if (
        not isinstance(source_node, str)
        or not source_node.strip()
        or not isinstance(target_node, str)
        or not target_node.strip()
        or not isinstance(relation_type, str)
        or not relation_type.strip()
    ):
        return None

    return source_node.strip(), target_node.strip(), relation_type.strip()


def _coerce_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _normalized_node_metadata(node: dict[str, Any], node_key: str) -> dict[str, Any]:
    raw_metadata = node.get("metadata")
    metadata = dict(raw_metadata) if isinstance(raw_metadata, dict) else {}
    stub = bool(node.get("stub"))
    metadata.setdefault("stub", stub)
    metadata.setdefault("node_key", node_key)
    metadata.setdefault(
        "node_type", infer_node_type(node_key, node.get("spec_path"), stub)
    )
    return metadata


def _json_param(value: dict[str, Any]) -> Any:
    try:
        from psycopg2.extras import Json  # type: ignore
    except ImportError:
        return value
    return Json(value)


def _commit_if_supported(conn: Any) -> None:
    commit = getattr(conn, "commit", None)
    if callable(commit):
        commit()


def _rollback_if_supported(conn: Any) -> None:
    rollback = getattr(conn, "rollback", None)
    if callable(rollback):
        rollback()


def scan_all_specs(skills_dir: str | Path) -> list[dict]:
    skills_dir = Path(skills_dir)
    discovered: set[Path] = set()
    for pattern in ("SKILL.spec.yaml", "*.behavior.yaml"):
        discovered.update(skills_dir.rglob(pattern))

    specs = sorted(discovered)
    parsed = []
    for spec_path in specs:
        try:
            parsed.append(parse_spec(spec_path))
        except Exception as exc:
            print(f"Warning: failed to parse {spec_path}: {exc}", file=sys.stderr)
    return parsed


def build_graph(specs: list[dict]) -> dict:
    node_map: dict[str, dict[str, Any]] = {}
    edge_keys: set[tuple[str, str, str]] = set()
    edges: list[dict[str, str]] = []

    for item in specs:
        raw_nodes = item.get("nodes")
        nodes = raw_nodes if isinstance(raw_nodes, list) else [item.get("node", {})]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            skill_name = _node_key_from_record(node)
            if skill_name is None:
                continue
            node_type = infer_node_type(skill_name, node.get("spec_path"), False)

            node_map[skill_name] = {
                "skill_name": skill_name,
                "node_key": skill_name,
                "description": str(node.get("description") or ""),
                "spec_path": node.get("spec_path"),
                "content_hash": node.get("content_hash"),
                "operations_count": int(node.get("operations_count", 0) or 0),
                "node_type": node_type,
                "stub": False,
            }

    for item in specs:
        for edge in item.get("edges", []) or []:
            if not isinstance(edge, dict):
                continue
            from_skill = edge.get("from_skill")
            to_skill = edge.get("to_skill")
            edge_type = edge.get("edge_type")
            if (
                not isinstance(from_skill, str)
                or not from_skill
                or not isinstance(to_skill, str)
                or not to_skill
                or edge_type not in ALLOWED_EDGE_TYPES
            ):
                continue
            edge_key = (from_skill, to_skill, edge_type)
            if edge_key in edge_keys:
                continue
            edge_keys.add(edge_key)
            edges.append(
                {
                    "from_skill": from_skill,
                    "to_skill": to_skill,
                    "edge_type": edge_type,
                    "source_node": from_skill,
                    "target_node": to_skill,
                    "relation_type": edge_type,
                }
            )

    for from_skill, to_skill, _edge_type in edge_keys:
        if from_skill not in node_map:
            node_map[from_skill] = {
                "skill_name": from_skill,
                "node_key": from_skill,
                "description": "",
                "spec_path": None,
                "content_hash": None,
                "operations_count": 0,
                "node_type": infer_node_type(from_skill, None, True),
                "stub": True,
            }
        if to_skill not in node_map:
            node_map[to_skill] = {
                "skill_name": to_skill,
                "node_key": to_skill,
                "description": "",
                "spec_path": None,
                "content_hash": None,
                "operations_count": 0,
                "node_type": infer_node_type(to_skill, None, True),
                "stub": True,
            }

    ordered_nodes = sorted(node_map.values(), key=lambda item: item["skill_name"])
    ordered_edges = sorted(
        edges,
        key=lambda item: (item["from_skill"], item["edge_type"], item["to_skill"]),
    )

    return {"nodes": ordered_nodes, "edges": ordered_edges}


def sync_graph_to_db(graph: dict[str, Any], conn: Any) -> dict[str, int]:
    inserted = 0
    updated = 0
    skipped = 0
    removed = 0
    edges_removed = 0
    edges_inserted = 0

    nodes = graph.get("nodes", []) if isinstance(graph, dict) else []
    edges = graph.get("edges", []) if isinstance(graph, dict) else []

    seen_names: set[str] = set()
    ordered_names: list[str] = []

    try:
        with conn.cursor() as cur:
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                skill_name = _node_key_from_record(node)
                if skill_name is None:
                    continue
                if skill_name in seen_names:
                    continue
                seen_names.add(skill_name)
                ordered_names.append(skill_name)

                metadata = _normalized_node_metadata(node, skill_name)

                cur.execute(
                    """
                    SELECT content_hash, spec_path
                    FROM skill_system.skill_graph_nodes
                    WHERE skill_name = %s
                    """,
                    (skill_name,),
                )
                existing = cur.fetchone()

                cur.execute(
                    """
                    SELECT node_id, skipped
                    FROM skill_system.refresh_graph_node(
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        skill_name,
                        str(node.get("description") or ""),
                        str(node.get("version") or ""),
                        _coerce_text_list(node.get("capabilities")),
                        _coerce_text_list(node.get("effects")),
                        _json_param(metadata),
                        node.get("content_hash"),
                        node.get("spec_path"),
                        int(node.get("operations_count") or 0),
                    ),
                )
                refreshed = cur.fetchone()
                if refreshed is None:
                    raise RuntimeError(
                        f"refresh_graph_node returned no row for {skill_name}"
                    )

                was_skipped = bool(refreshed[1])
                if was_skipped:
                    skipped += 1
                elif existing is None:
                    inserted += 1
                else:
                    updated += 1

            cur.execute("DELETE FROM skill_system.skill_graph_edges")
            edges_removed = int(cur.rowcount or 0)

            seen_edges: set[tuple[str, str, str]] = set()
            for edge in edges:
                if not isinstance(edge, dict):
                    continue
                normalized_edge = _edge_fields_from_record(edge)
                if normalized_edge is None:
                    continue
                from_skill, to_skill, edge_type = normalized_edge
                edge_key = (from_skill, to_skill, edge_type)
                if edge_key in seen_edges:
                    continue
                seen_edges.add(edge_key)
                cur.execute(
                    """
                    INSERT INTO skill_system.skill_graph_edges (from_skill, to_skill, edge_type, metadata)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (from_skill, to_skill, edge_type, _json_param({})),
                )
                edges_inserted += int(cur.rowcount or 1)

            if ordered_names:
                cur.execute(
                    "DELETE FROM skill_system.skill_graph_nodes WHERE skill_name <> ALL(%s)",
                    (ordered_names,),
                )
            else:
                cur.execute("DELETE FROM skill_system.skill_graph_nodes")
            removed = int(cur.rowcount or 0)
    except Exception:
        _rollback_if_supported(conn)
        raise

    _commit_if_supported(conn)

    return {
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "removed": removed,
        "edges_removed": edges_removed,
        "edges_inserted": edges_inserted,
    }
