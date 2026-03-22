#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import inspect
import json
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Any, NoReturn


SCRIPT_DIR = Path(__file__).resolve().parent
SKILLS_DIR = SCRIPT_DIR.parents[1]


class CLIError(Exception):
    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        exit_code: int = 1,
        output_format: str = "json",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.exit_code = exit_code
        self.output_format = output_format
        self.details = details or {}


class GraphArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.raw_argv: list[str] = []

    def error(self, message: str) -> NoReturn:
        raise CLIError(
            message,
            error_code="GRAPH-CLI-ARG",
            exit_code=2,
            output_format=_extract_requested_format(self.raw_argv),
        )


def _set_parser_argv(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    if isinstance(parser, GraphArgumentParser):
        parser.raw_argv = list(argv)

    for action in parser._actions:
        choices = getattr(action, "choices", None)
        if isinstance(choices, dict):
            for subparser in choices.values():
                _set_parser_argv(subparser, argv)


def _extract_requested_format(argv: list[str]) -> str:
    for index, arg in enumerate(argv):
        if arg == "--format" and index + 1 < len(argv):
            value = argv[index + 1].strip().lower()
            if value in {"json", "text"}:
                return value
        if arg.startswith("--format="):
            value = arg.split("=", 1)[1].strip().lower()
            if value in {"json", "text"}:
                return value
    return "json"


@lru_cache(maxsize=None)
def _load_local_module(module_name: str) -> ModuleType:
    module_path = SCRIPT_DIR / f"{module_name}.py"
    if not module_path.exists():
        raise CLIError(
            f"Required module is missing: {module_path.name}",
            error_code="GRAPH-CLI-MODULE",
            details={"module": module_name, "path": str(module_path)},
        )

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise CLIError(
            f"Could not load module spec for {module_path.name}",
            error_code="GRAPH-CLI-MODULE",
            details={"module": module_name, "path": str(module_path)},
        )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _require_callable(module: ModuleType, attribute: str, *, module_name: str) -> Any:
    value = getattr(module, attribute, None)
    if callable(value):
        return value
    raise CLIError(
        f"{module_name}.{attribute}() is not available yet",
        error_code="GRAPH-CLI-UNAVAILABLE",
        details={"module": module_name, "attribute": attribute},
    )


def _emit_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _emit_text(lines: list[str], *, stream: Any = None) -> None:
    target = stream or sys.stdout
    target.write("\n".join(lines) + "\n")


def _emit_success(
    output_format: str, payload: dict[str, Any], text_lines: list[str]
) -> None:
    if output_format == "text":
        _emit_text(text_lines)
        return
    _emit_json(payload)


def _emit_error(error: CLIError) -> None:
    payload: dict[str, Any] = {
        "status": "error",
        "error_code": error.error_code,
        "message": error.message,
    }
    if error.details:
        payload["details"] = error.details

    if error.output_format == "text":
        lines = [f"Error [{error.error_code}]", error.message]
        if error.details:
            for key in sorted(error.details):
                lines.append(f"{key}: {error.details[key]}")
        _emit_text(lines, stream=sys.stderr)
        return

    _emit_json(payload)


def _graph_public_payload(graph: dict[str, Any]) -> dict[str, Any]:
    nodes = []
    for node in graph.get("nodes", []):
        nodes.append(
            {
                "skill_name": node.get("skill_name"),
                "description": node.get("description", ""),
                "spec_path": node.get("spec_path"),
                "operations_count": int(node.get("operations_count", 0) or 0),
                "content_hash": node.get("content_hash"),
                "stub": bool(node.get("stub", False)),
            }
        )

    edges = []
    for edge in graph.get("edges", []):
        edges.append(
            {
                "source": edge.get("from_skill"),
                "target": edge.get("to_skill"),
                "edge_type": edge.get("edge_type"),
            }
        )

    return {
        "status": "ok",
        "nodes": nodes,
        "edges": edges,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


def _load_graph_from_specs(skills_dir: Path) -> dict[str, Any]:
    graph_core = _load_local_module("graph_core")
    scan_all_specs = _require_callable(
        graph_core, "scan_all_specs", module_name="graph_core"
    )
    build_graph = _require_callable(graph_core, "build_graph", module_name="graph_core")
    return build_graph(scan_all_specs(skills_dir))


def _normalize_max_depth(value: Any, default: int = 10) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else 1


def _find_impact_in_graph(
    graph: dict[str, Any], node_name: str, max_depth: int
) -> dict[str, Any]:
    normalized_depth = _normalize_max_depth(max_depth)

    reverse_adj: dict[str, list[str]] = {}
    for edge in graph.get("edges", []):
        if not isinstance(edge, dict):
            continue
        source = edge.get("from_skill")
        target = edge.get("to_skill")
        if (
            not isinstance(source, str)
            or not source
            or not isinstance(target, str)
            or not target
        ):
            continue
        reverse_adj.setdefault(target, []).append(source)

    queue: list[tuple[str, int, list[str]]] = [(node_name, 0, [node_name])]
    visited_depth: dict[str, int] = {node_name: 0}
    impacts: dict[str, dict[str, Any]] = {}

    while queue:
        current, depth, path = queue.pop(0)
        if depth >= normalized_depth:
            continue

        for parent in reverse_adj.get(current, []):
            next_depth = depth + 1
            next_path = [*path, parent]
            previous_depth = visited_depth.get(parent)
            if previous_depth is not None and previous_depth <= next_depth:
                continue
            visited_depth[parent] = next_depth
            queue.append((parent, next_depth, next_path))
            impacts[parent] = {
                "impact_skill": parent,
                "depth": next_depth,
                "path": next_path,
            }

    impact_rows = sorted(
        impacts.values(), key=lambda item: (item["depth"], item["impact_skill"])
    )
    return {
        "status": "ok",
        "skill": node_name,
        "node": node_name,
        "max_depth": normalized_depth,
        "count": len(impact_rows),
        "impact_count": len(impact_rows),
        "impacts": impact_rows,
        "impact": impact_rows,
    }


def _show_text_lines(payload: dict[str, Any]) -> list[str]:
    lines = [
        "Skill graph",
        f"Nodes: {payload['node_count']}",
        f"Edges: {payload['edge_count']}",
        "",
        "Nodes",
    ]
    for node in payload["nodes"]:
        stub_label = " stub" if node.get("stub") else ""
        lines.append(
            f"- {node['skill_name']} (ops={node['operations_count']}{stub_label})"
        )
    lines.extend(["", "Edges"])
    for edge in payload["edges"]:
        lines.append(f"- {edge['source']} -[{edge['edge_type']}]-> {edge['target']}")
    return lines


def _normalize_neighbors_payload(
    skill_name: str, result: dict[str, Any]
) -> dict[str, Any]:
    raw_outgoing = list(result.get("outgoing") or [])
    raw_incoming = list(result.get("incoming") or [])
    raw_neighbors = list(result.get("neighbors") or [])

    outgoing = [
        {
            **item,
            "skill_name": item.get("skill_name") or item.get("neighbor_skill"),
        }
        for item in raw_outgoing
    ]
    incoming = [
        {
            **item,
            "skill_name": item.get("skill_name") or item.get("neighbor_skill"),
        }
        for item in raw_incoming
    ]

    if raw_neighbors and not outgoing and not incoming:
        for item in raw_neighbors:
            normalized = {
                **item,
                "skill_name": item.get("skill_name") or item.get("neighbor_skill"),
            }
            direction = str(item.get("direction") or "").lower()
            if direction == "incoming":
                incoming.append(normalized)
            else:
                outgoing.append(normalized)

    neighbors = []
    for item in outgoing:
        neighbors.append(
            {
                **item,
                "skill_name": item.get("skill_name") or item.get("neighbor_skill"),
                "direction": item.get("direction") or "outgoing",
            }
        )
    for item in incoming:
        neighbors.append(
            {
                **item,
                "skill_name": item.get("skill_name") or item.get("neighbor_skill"),
                "direction": item.get("direction") or "incoming",
            }
        )

    payload = {
        "status": result.get("status", "ok"),
        "skill": result.get("skill", skill_name),
        "outgoing": outgoing,
        "incoming": incoming,
        "neighbors": neighbors,
        "neighbor_count": len(neighbors),
    }
    for key, value in result.items():
        if key not in payload:
            payload[key] = value
    return payload


def _neighbors_text_lines(payload: dict[str, Any]) -> list[str]:
    lines = [f"Neighbors for {payload['skill']}"]
    lines.append(f"Total: {payload['neighbor_count']}")
    lines.append("")
    lines.append("Outgoing")
    if payload["outgoing"]:
        for item in payload["outgoing"]:
            lines.append(f"- {item.get('skill_name')} ({item.get('edge_type')})")
    else:
        lines.append("- (none)")
    lines.append("")
    lines.append("Incoming")
    if payload["incoming"]:
        for item in payload["incoming"]:
            lines.append(f"- {item.get('skill_name')} ({item.get('edge_type')})")
    else:
        lines.append("- (none)")
    return lines


def _normalize_path_payload(
    from_skill: str,
    to_skill: str,
    max_depth: int,
    result: dict[str, Any],
) -> dict[str, Any]:
    path = list(result.get("path") or [])
    found = bool(result.get("found", bool(path)))
    payload = {
        "status": result.get("status", "ok"),
        "from_skill": result.get("from_skill", from_skill),
        "to_skill": result.get("to_skill", to_skill),
        "path": path,
        "found": found,
        "max_depth": int(result.get("max_depth", max_depth)),
    }
    for key, value in result.items():
        if key not in payload:
            payload[key] = value
    return payload


def _path_text_lines(payload: dict[str, Any]) -> list[str]:
    if payload["found"] and payload["path"]:
        return [
            f"Path from {payload['from_skill']} to {payload['to_skill']}",
            " -> ".join(payload["path"]),
            f"Max depth: {payload['max_depth']}",
        ]
    return [
        f"Path from {payload['from_skill']} to {payload['to_skill']}",
        "No path found.",
        f"Max depth: {payload['max_depth']}",
    ]


def _normalize_impact_payload(
    skill_name: str, max_depth: int, result: dict[str, Any]
) -> dict[str, Any]:
    impact = list(result.get("impact") or result.get("impacts") or [])
    payload = {
        "status": result.get("status", "ok"),
        "skill": result.get("skill", skill_name),
        "impact": impact,
        "impact_count": int(
            result.get("impact_count", result.get("count", len(impact)))
        ),
        "max_depth": int(result.get("max_depth", max_depth)),
    }
    for key, value in result.items():
        if key not in payload:
            payload[key] = value
    return payload


def _impact_text_lines(payload: dict[str, Any]) -> list[str]:
    lines = [f"Impact for {payload['skill']}", f"Count: {payload['impact_count']}"]
    if payload["impact"]:
        lines.append("")
        for item in payload["impact"]:
            lines.append(f"- {item}")
    return lines


def _refresh_text_lines(payload: dict[str, Any]) -> list[str]:
    lines = ["Graph refresh"]
    for key in ("parsed", "inserted", "updated", "skipped", "removed"):
        if key in payload:
            lines.append(f"{key}: {payload[key]}")
    return lines


def _known_skill_names(skills_dir: Path) -> set[str]:
    graph = _load_graph_from_specs(skills_dir)
    return {
        str(node.get("skill_name"))
        for node in graph.get("nodes", [])
        if isinstance(node.get("skill_name"), str)
    }


def _validate_skill_name(skill_name: str, skills_dir: Path) -> None:
    known = _known_skill_names(skills_dir)
    if skill_name not in known:
        raise CLIError(
            f"Unknown skill: {skill_name}",
            error_code="GRAPH-CLI-SKILL",
            details={"skill": skill_name, "known_skills": len(known)},
        )


def _open_graph_connection() -> Any:
    graph_core = _load_local_module("graph_core")
    connect_db = _require_callable(graph_core, "connect_db", module_name="graph_core")
    return connect_db()


def _call_sync_graph(
    sync_graph_to_db: Any, graph: dict[str, Any], conn: Any, force: bool
) -> dict[str, Any]:
    parameters = inspect.signature(sync_graph_to_db).parameters
    if "conn" in parameters:
        kwargs = {"conn": conn}
        if "force" in parameters:
            kwargs["force"] = force
        return sync_graph_to_db(graph, **kwargs)
    if "connection" in parameters:
        kwargs = {"connection": conn}
        if "force" in parameters:
            kwargs["force"] = force
        return sync_graph_to_db(graph, **kwargs)
    if "force" in parameters:
        return sync_graph_to_db(graph, conn, force=force)
    return sync_graph_to_db(graph, conn)


def handle_show(args: argparse.Namespace) -> int:
    graph = _load_graph_from_specs(args.skills_dir)
    payload = _graph_public_payload(graph)
    _emit_success(args.output_format, payload, _show_text_lines(payload))
    return 0


def handle_scan(args: argparse.Namespace) -> int:
    graph = _load_graph_from_specs(args.project_dir)
    payload = _graph_public_payload(graph)
    payload["project_dir"] = str(args.project_dir)
    _emit_success(args.output_format, payload, _show_text_lines(payload))
    return 0


def handle_neighbors(args: argparse.Namespace) -> int:
    _validate_skill_name(args.skill_name, args.skills_dir)
    graph_queries = _load_local_module("graph_queries")
    get_neighbors = _require_callable(
        graph_queries, "get_neighbors", module_name="graph_queries"
    )

    conn = _open_graph_connection()
    try:
        result = get_neighbors(conn, args.skill_name)
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()

    if not isinstance(result, dict):
        raise CLIError(
            "graph_queries.get_neighbors() returned a non-dict result",
            error_code="GRAPH-CLI-RESULT",
            details={"command": "neighbors"},
        )

    payload = _normalize_neighbors_payload(args.skill_name, result)
    _emit_success(args.output_format, payload, _neighbors_text_lines(payload))
    return 0


def handle_path(args: argparse.Namespace) -> int:
    _validate_skill_name(args.from_skill, args.skills_dir)
    _validate_skill_name(args.to_skill, args.skills_dir)
    graph_queries = _load_local_module("graph_queries")
    find_path = _require_callable(
        graph_queries, "find_path", module_name="graph_queries"
    )

    conn = _open_graph_connection()
    try:
        result = find_path(conn, args.from_skill, args.to_skill, args.max_depth)
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()

    if not isinstance(result, dict):
        raise CLIError(
            "graph_queries.find_path() returned a non-dict result",
            error_code="GRAPH-CLI-RESULT",
            details={"command": "path"},
        )

    payload = _normalize_path_payload(
        args.from_skill, args.to_skill, args.max_depth, result
    )
    _emit_success(args.output_format, payload, _path_text_lines(payload))
    return 0


def handle_impact(args: argparse.Namespace) -> int:
    _validate_skill_name(args.skill_name, args.skills_dir)
    graph = _load_graph_from_specs(args.skills_dir)
    result = _find_impact_in_graph(graph, args.skill_name, args.max_depth)

    payload = _normalize_impact_payload(args.skill_name, args.max_depth, result)
    _emit_success(args.output_format, payload, _impact_text_lines(payload))
    return 0


def handle_refresh(args: argparse.Namespace) -> int:
    graph_core = _load_local_module("graph_core")
    scan_all_specs = _require_callable(
        graph_core, "scan_all_specs", module_name="graph_core"
    )
    build_graph = _require_callable(graph_core, "build_graph", module_name="graph_core")
    sync_graph_to_db = _require_callable(
        graph_core, "sync_graph_to_db", module_name="graph_core"
    )

    specs = scan_all_specs(args.skills_dir)
    graph = build_graph(specs)
    conn = _open_graph_connection()
    try:
        result = _call_sync_graph(sync_graph_to_db, graph, conn, args.force)
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()

    if not isinstance(result, dict):
        raise CLIError(
            "graph_core.sync_graph_to_db() returned a non-dict result",
            error_code="GRAPH-CLI-RESULT",
            details={"command": "refresh"},
        )

    payload = {
        "status": result.get("status", "ok"),
        "parsed": len(specs),
        "inserted": int(result.get("inserted", 0)),
        "updated": int(result.get("updated", 0)),
        "skipped": int(result.get("skipped", 0)),
        "removed": int(result.get("removed", 0)),
    }
    if args.force:
        payload["force"] = True
    _emit_success(args.output_format, payload, _refresh_text_lines(payload))
    return 0


def build_parser() -> GraphArgumentParser:
    shared = GraphArgumentParser(add_help=False)
    shared.add_argument(
        "--format",
        dest="output_format",
        choices=("json", "text"),
        default="json",
        help="Output format (default: json)",
    )

    parser = GraphArgumentParser(
        prog="graph_cli.py",
        description="Skill graph CLI with JSON-first output and text fallback.",
        parents=[shared],
    )
    subparsers = parser.add_subparsers(
        dest="command", required=True, parser_class=GraphArgumentParser
    )

    show_parser = subparsers.add_parser(
        "show",
        parents=[shared],
        help="Show nodes and edges from the graph model.",
    )
    show_parser.add_argument(
        "--skills-dir",
        type=Path,
        default=SKILLS_DIR,
        help=f"Directory to scan for SKILL.spec.yaml files (default: {SKILLS_DIR})",
    )
    show_parser.set_defaults(handler=handle_show)

    scan_parser = subparsers.add_parser(
        "scan",
        parents=[shared],
        help="Scan a project directory and emit graph JSON.",
    )
    scan_parser.add_argument(
        "project_dir",
        type=Path,
        help="Project directory to scan recursively for SKILL.spec.yaml files.",
    )
    scan_parser.set_defaults(handler=handle_scan)

    neighbors_parser = subparsers.add_parser(
        "neighbors",
        parents=[shared],
        help="Show direct incoming and outgoing neighbors for one skill.",
    )
    neighbors_parser.add_argument("skill_name", help="Skill name to inspect.")
    neighbors_parser.add_argument(
        "--skills-dir",
        type=Path,
        default=SKILLS_DIR,
        help=f"Directory to scan for known node ids (default: {SKILLS_DIR})",
    )
    neighbors_parser.set_defaults(handler=handle_neighbors)

    path_parser = subparsers.add_parser(
        "path",
        parents=[shared],
        help="Find the shortest path between two skills.",
    )
    path_parser.add_argument("from_skill", help="Starting skill.")
    path_parser.add_argument("to_skill", help="Destination skill.")
    path_parser.add_argument(
        "--skills-dir",
        type=Path,
        default=SKILLS_DIR,
        help=f"Directory to scan for known node ids (default: {SKILLS_DIR})",
    )
    path_parser.add_argument(
        "--max-depth",
        type=int,
        default=10,
        help="Maximum traversal depth (default: 10).",
    )
    path_parser.set_defaults(handler=handle_path)

    impact_parser = subparsers.add_parser(
        "impact",
        parents=[shared],
        help="Show transitive dependents for one graph node.",
    )
    impact_parser.add_argument("skill_name", help="Graph node id to inspect.")
    impact_parser.add_argument(
        "--skills-dir",
        type=Path,
        default=SKILLS_DIR,
        help=f"Directory to scan for graph nodes (default: {SKILLS_DIR})",
    )
    impact_parser.add_argument(
        "--max-depth",
        type=int,
        default=10,
        help="Maximum traversal depth (default: 10).",
    )
    impact_parser.set_defaults(handler=handle_impact)

    refresh_parser = subparsers.add_parser(
        "refresh",
        parents=[shared],
        help="Rebuild and sync the graph from SKILL.spec.yaml files.",
    )
    refresh_parser.add_argument(
        "--skills-dir",
        type=Path,
        default=SKILLS_DIR,
        help=f"Directory to scan for SKILL.spec.yaml files (default: {SKILLS_DIR})",
    )
    refresh_parser.add_argument(
        "--force",
        action="store_true",
        help="Request a full refresh even if hashes match.",
    )
    refresh_parser.set_defaults(handler=handle_refresh)

    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    _set_parser_argv(parser, argv)

    try:
        args = parser.parse_args(argv)
        return int(args.handler(args))
    except CLIError as error:
        _emit_error(error)
        return error.exit_code
    except Exception as exc:
        fallback = _extract_requested_format(argv)
        _emit_error(
            CLIError(
                str(exc),
                error_code="GRAPH-CLI-UNEXPECTED",
                output_format=fallback,
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
