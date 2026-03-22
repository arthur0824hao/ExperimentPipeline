#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("graph_queries.py")


def load_module():
    spec = importlib.util.spec_from_file_location(
        "skill_system_graph_queries", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeCursor:
    def __init__(self, rows, recorder):
        self._rows = rows
        self._recorder = recorder

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self._recorder.append((query, params))

    def fetchall(self):
        return list(self._rows)


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def cursor(self):
        return FakeCursor(self.rows, self.executed)


class GraphQueriesTests(unittest.TestCase):
    def test_get_neighbors_returns_empty_payload_when_none_found(self):
        mod = load_module()
        conn = FakeConnection([])

        result = mod.get_neighbors(conn, "skill-system-router")

        self.assertEqual(result["skill"], "skill-system-router")
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["neighbors"], [])

    def test_get_neighbors_returns_incoming_and_outgoing_with_metadata(self):
        mod = load_module()
        conn = FakeConnection(
            [
                (
                    "skill-system-memory",
                    1,
                    ["skill-system-memory", "skill-system-router"],
                    "depends_on",
                ),
                (
                    "skill-system-insight",
                    1,
                    ["skill-system-router", "skill-system-insight"],
                    "delegates_to",
                ),
            ]
        )

        result = mod.get_neighbors(conn, "skill-system-router")

        self.assertEqual(result["skill"], "skill-system-router")
        self.assertEqual(result["count"], 2)
        self.assertEqual(len(result["neighbors"]), 2)

        neighbors_by_name = {
            item["neighbor_skill"]: item for item in result["neighbors"]
        }
        memory_neighbor = neighbors_by_name["skill-system-memory"]
        self.assertEqual(memory_neighbor["direction"], "incoming")
        self.assertEqual(memory_neighbor["edge_type"], "depends_on")
        self.assertEqual(memory_neighbor["depth"], 1)

        insight_neighbor = neighbors_by_name["skill-system-insight"]
        self.assertEqual(insight_neighbor["direction"], "outgoing")
        self.assertEqual(insight_neighbor["edge_type"], "delegates_to")

        self.assertEqual(len(conn.executed), 1)
        self.assertIn("skill_system.find_neighbors", conn.executed[0][0])
        self.assertEqual(conn.executed[0][1], ("skill-system-router",))

    def test_get_neighbors_marks_unknown_direction_for_non_pair_path(self):
        mod = load_module()
        conn = FakeConnection(
            [
                (
                    "skill-system-memory",
                    1,
                    ["skill-system-memory"],
                    "depends_on",
                )
            ]
        )

        result = mod.get_neighbors(conn, "skill-system-router")

        self.assertEqual(result["neighbors"][0]["direction"], "unknown")

    def test_get_neighbors_returns_stably_sorted_neighbors(self):
        mod = load_module()
        conn = FakeConnection(
            [
                ("skill-c", 1, ["skill-system-router", "skill-c"], "delegates_to"),
                ("skill-a", 1, ["skill-a", "skill-system-router"], "depends_on"),
                ("skill-b", 1, ["skill-system-router", "skill-b"], "delegates_to"),
            ]
        )

        result = mod.get_neighbors(conn, "skill-system-router")

        self.assertEqual(
            [item["neighbor_skill"] for item in result["neighbors"]],
            ["skill-a", "skill-b", "skill-c"],
        )

    def test_find_path_returns_not_found_payload_when_sql_returns_none(self):
        mod = load_module()
        conn = FakeConnection([])

        result = mod.find_path(
            conn,
            "skill-system-github",
            "skill-system-memory",
            max_depth=4,
        )

        self.assertEqual(result["from_skill"], "skill-system-github")
        self.assertEqual(result["to_skill"], "skill-system-memory")
        self.assertEqual(result["max_depth"], 4)
        self.assertFalse(result["found"])
        self.assertEqual(result["path"], [])
        self.assertIsNone(result["depth"])
        self.assertEqual(result["path_length"], 0)

        self.assertEqual(len(conn.executed), 1)
        path_query = conn.executed[0][0]
        self.assertIn("skill_system.find_path", path_query)
        self.assertNotIn("DEFAULT", path_query.upper())
        self.assertIn(
            "find_path(%s::text, %s::text, NULL::text[], %s::integer)", path_query
        )
        self.assertEqual(
            conn.executed[0][1],
            ("skill-system-github", "skill-system-memory", 4),
        )

    def test_find_path_returns_shortest_path_payload_when_found(self):
        mod = load_module()
        conn = FakeConnection(
            [
                (
                    [
                        "skill-system-insight",
                        "skill-system-postgres",
                        "skill-system-memory",
                    ],
                    2,
                )
            ]
        )

        result = mod.find_path(
            conn,
            "skill-system-insight",
            "skill-system-memory",
            max_depth=5,
        )

        self.assertTrue(result["found"])
        self.assertEqual(result["depth"], 2)
        self.assertEqual(result["path_length"], 3)
        self.assertEqual(
            result["path"],
            ["skill-system-insight", "skill-system-postgres", "skill-system-memory"],
        )
        self.assertIn("skill_system.find_path", conn.executed[0][0])

    def test_find_path_clamps_non_positive_max_depth(self):
        mod = load_module()
        conn = FakeConnection([])

        result = mod.find_path(
            conn,
            "skill-system-insight",
            "skill-system-memory",
            max_depth=0,
        )

        self.assertEqual(result["max_depth"], 1)
        self.assertEqual(
            conn.executed[0][1], ("skill-system-insight", "skill-system-memory", 1)
        )

    def test_find_path_uses_default_depth_when_invalid_input(self):
        mod = load_module()
        conn = FakeConnection([])

        result = mod.find_path(
            conn,
            "skill-system-insight",
            "skill-system-memory",
            max_depth="bad",
        )

        self.assertEqual(result["max_depth"], 10)
        self.assertEqual(
            conn.executed[0][1], ("skill-system-insight", "skill-system-memory", 10)
        )

    def test_find_path_same_skill_payload_when_sql_returns_depth_zero(self):
        mod = load_module()
        conn = FakeConnection([(["skill-system-router"], 0)])

        result = mod.find_path(
            conn,
            "skill-system-router",
            "skill-system-router",
            max_depth=3,
        )

        self.assertTrue(result["found"])
        self.assertEqual(result["depth"], 0)
        self.assertEqual(result["path"], ["skill-system-router"])
        self.assertEqual(result["path_length"], 1)

    def test_find_impact_returns_transitive_dependents_without_duplicates(self):
        mod = load_module()
        conn = FakeConnection(
            [
                (
                    "skill-system-router",
                    1,
                    ["skill-system-memory", "skill-system-router"],
                ),
                (
                    "skill-system-insight",
                    2,
                    [
                        "skill-system-memory",
                        "skill-system-router",
                        "skill-system-insight",
                    ],
                ),
                (
                    "skill-system-router",
                    1,
                    ["skill-system-memory", "skill-system-router"],
                ),
            ]
        )

        result = mod.find_impact(conn, "skill-system-memory", max_depth=3)

        self.assertEqual(result["skill"], "skill-system-memory")
        self.assertEqual(result["max_depth"], 3)
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["impact_count"], 2)
        self.assertEqual(
            [item["impact_skill"] for item in result["impacts"]],
            ["skill-system-router", "skill-system-insight"],
        )
        self.assertEqual(result["impact"], result["impacts"])
        self.assertEqual(result["impacts"][1]["depth"], 2)
        impact_query = conn.executed[0][0]
        self.assertIn("skill_system.find_impact", impact_query)
        self.assertNotIn("DEFAULT", impact_query.upper())
        self.assertIn("find_impact(%s::text, NULL::text[], %s::integer)", impact_query)
        self.assertEqual(conn.executed[0][1], ("skill-system-memory", 3))

    def test_find_impact_is_cycle_safe_when_sql_returns_looping_paths(self):
        mod = load_module()
        conn = FakeConnection(
            [
                (
                    "skill-system-router",
                    2,
                    [
                        "skill-system-memory",
                        "skill-system-insight",
                        "skill-system-router",
                    ],
                ),
                (
                    "skill-system-insight",
                    1,
                    ["skill-system-memory", "skill-system-insight"],
                ),
            ]
        )

        result = mod.find_impact(conn, "skill-system-memory", max_depth=5)

        self.assertEqual(result["count"], 2)
        self.assertEqual(
            [item["impact_skill"] for item in result["impacts"]],
            ["skill-system-insight", "skill-system-router"],
        )

    def test_find_impact_returns_empty_payload_when_no_dependents(self):
        mod = load_module()
        conn = FakeConnection([])

        result = mod.find_impact(conn, "skill-system-memory", max_depth=4)

        self.assertEqual(result["skill"], "skill-system-memory")
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["impact_count"], 0)
        self.assertEqual(result["impacts"], [])
        self.assertEqual(result["impact"], [])

    def test_find_impact_clamps_non_positive_max_depth(self):
        mod = load_module()
        conn = FakeConnection([])

        result = mod.find_impact(conn, "skill-system-memory", max_depth=-5)

        self.assertEqual(result["max_depth"], 1)
        self.assertEqual(conn.executed[0][1], ("skill-system-memory", 1))


if __name__ == "__main__":
    unittest.main()
