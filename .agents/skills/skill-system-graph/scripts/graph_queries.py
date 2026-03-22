#!/usr/bin/env python3

from __future__ import annotations


def _to_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _as_int(value, default=10, minimum=1):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    if parsed < minimum:
        return minimum
    return parsed


def get_neighbors(conn, skill_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT neighbor_skill, depth, path, edge_type
            FROM skill_system.find_neighbors(%s)
            """,
            (skill_name,),
        )
        rows = cur.fetchall()

    neighbors = []
    for neighbor_skill, depth, path, edge_type in rows:
        norm_path = _to_list(path)
        direction = "unknown"
        if len(norm_path) >= 2:
            if norm_path[0] == skill_name and norm_path[1] == neighbor_skill:
                direction = "outgoing"
            elif norm_path[0] == neighbor_skill and norm_path[1] == skill_name:
                direction = "incoming"

        neighbors.append(
            {
                "neighbor_skill": neighbor_skill,
                "direction": direction,
                "edge_type": edge_type,
                "depth": int(depth),
                "path": norm_path,
            }
        )

    neighbors.sort(
        key=lambda item: (
            item["depth"],
            item["neighbor_skill"],
            item["edge_type"],
            item["direction"],
        )
    )
    return {
        "skill": skill_name,
        "count": len(neighbors),
        "neighbors": neighbors,
    }


def find_path(conn, from_skill, to_skill, max_depth=10):
    max_depth = _as_int(max_depth, default=10, minimum=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT path, depth
            FROM skill_system.find_path(%s::text, %s::text, NULL::text[], %s::integer)
            """,
            (from_skill, to_skill, max_depth),
        )
        rows = cur.fetchall()

    if not rows:
        return {
            "from_skill": from_skill,
            "to_skill": to_skill,
            "max_depth": max_depth,
            "found": False,
            "depth": None,
            "path_length": 0,
            "path": [],
        }

    path, depth = rows[0]
    norm_path = _to_list(path)
    return {
        "from_skill": from_skill,
        "to_skill": to_skill,
        "max_depth": max_depth,
        "found": True,
        "depth": int(depth),
        "path_length": len(norm_path),
        "path": norm_path,
    }


def find_impact(conn, skill_name, max_depth=10):
    max_depth = _as_int(max_depth, default=10, minimum=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT impact_skill, depth, path
            FROM skill_system.find_impact(%s::text, NULL::text[], %s::integer)
            """,
            (skill_name, max_depth),
        )
        rows = cur.fetchall()

    seen = set()
    impacts = []
    for impact_skill, depth, path in rows:
        if impact_skill in seen:
            continue
        seen.add(impact_skill)
        impacts.append(
            {
                "impact_skill": impact_skill,
                "depth": int(depth),
                "path": _to_list(path),
            }
        )

    impacts.sort(key=lambda item: (item["depth"], item["impact_skill"]))
    return {
        "skill": skill_name,
        "max_depth": max_depth,
        "count": len(impacts),
        "impacts": impacts,
        "impact_count": len(impacts),
        "impact": impacts,
    }
