#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("graph_core.py")


def load_module():
    spec = importlib.util.spec_from_file_location(
        "skill_system_graph_core", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeCursor:
    def __init__(self, conn: "FakeConn"):
        self.conn = conn
        self.rowcount = 0
        self._fetchone = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        sql = " ".join(query.lower().split())
        self.rowcount = 0
        self._fetchone = None

        if "from skill_system.skill_graph_nodes where skill_name = %s" in sql:
            if params is None:
                raise AssertionError("Expected params for node lookup")
            skill_name = params[0]
            node = self.conn.nodes.get(skill_name)
            if node is None:
                self._fetchone = None
            else:
                self._fetchone = (node["content_hash"], node["spec_path"])
            return

        if "select node_id, skipped from skill_system.refresh_graph_node(" in sql:
            if params is None:
                raise AssertionError("Expected params for refresh_graph_node")
            (
                skill_name,
                description,
                version,
                capabilities,
                effects,
                metadata,
                content_hash,
                spec_path,
                operations_count,
            ) = params
            existing = self.conn.nodes.get(skill_name)
            if (
                existing is not None
                and existing["content_hash"] is not None
                and content_hash is not None
                and existing["content_hash"] == content_hash
                and existing["spec_path"] == spec_path
            ):
                self._fetchone = (existing["id"], True)
                return

            if existing is None:
                node_id = self.conn.next_node_id
                self.conn.next_node_id += 1
            else:
                node_id = existing["id"]

            self.conn.nodes[skill_name] = {
                "id": node_id,
                "skill_name": skill_name,
                "description": description,
                "version": version,
                "capabilities": list(capabilities or []),
                "effects": list(effects or []),
                "metadata": metadata or {},
                "content_hash": content_hash,
                "spec_path": spec_path,
                "operations_count": int(operations_count or 0),
            }
            self._fetchone = (node_id, False)
            return

        if "delete from skill_system.skill_graph_edges" in sql:
            self.rowcount = len(self.conn.edges)
            self.conn.edges = []
            return

        if "insert into skill_system.skill_graph_edges" in sql:
            if params is None:
                raise AssertionError("Expected params for edge insert")
            from_skill, to_skill, edge_type, metadata = params
            self.conn.edges.append(
                {
                    "from_skill": from_skill,
                    "to_skill": to_skill,
                    "edge_type": edge_type,
                    "metadata": metadata or {},
                }
            )
            self.rowcount = 1
            return

        if (
            "delete from skill_system.skill_graph_nodes where skill_name <> all(%s)"
            in sql
        ):
            if params is None:
                raise AssertionError("Expected params for stale removal")
            keep = set(params[0])
            to_delete = [name for name in self.conn.nodes if name not in keep]
            self.rowcount = len(to_delete)
            for name in to_delete:
                del self.conn.nodes[name]
            self.conn.edges = [
                e
                for e in self.conn.edges
                if e["from_skill"] in self.conn.nodes
                and e["to_skill"] in self.conn.nodes
            ]
            return

        if "delete from skill_system.skill_graph_nodes" in sql:
            self.rowcount = len(self.conn.nodes)
            self.conn.nodes = {}
            self.conn.edges = []
            return

        raise AssertionError(f"Unhandled SQL in fake cursor: {query}")

    def fetchone(self):
        return self._fetchone


class FakeConn:
    def __init__(self):
        self.nodes = {}
        self.edges = []
        self.next_node_id = 1
        self.autocommit = True

    def cursor(self):
        return FakeCursor(self)


class GraphSyncTests(unittest.TestCase):
    def test_resolve_db_target_prefers_skill_pgdatabase(self):
        mod = load_module()
        with patch.dict(
            os.environ,
            {"SKILL_PGDATABASE": "workflow_db", "PGDATABASE": "ambient_db"},
            clear=True,
        ):
            db_target, source = mod.resolve_db_target()

        self.assertEqual(db_target, "workflow_db")
        self.assertEqual(source, "SKILL_PGDATABASE(overrides:ambient_db)")

    def test_resolve_db_target_uses_ambient_when_skill_unset(self):
        mod = load_module()
        with patch.dict(os.environ, {"PGDATABASE": "ambient_db"}, clear=True):
            db_target, source = mod.resolve_db_target()

        self.assertEqual(db_target, "ambient_db")
        self.assertEqual(source, "PGDATABASE")

    def test_connect_db_uses_local_resolver_pattern(self):
        mod = load_module()
        calls = {}

        class FakePsycopg2:
            @staticmethod
            def connect(**kwargs):
                calls["kwargs"] = kwargs
                return FakeConn()

        with patch.dict(
            os.environ,
            {
                "PGHOST": "db.local",
                "PGPORT": "5544",
                "PGUSER": "robot",
                "SKILL_PGDATABASE": "workflow_db",
            },
            clear=False,
        ):
            with patch.dict("sys.modules", {"psycopg2": FakePsycopg2}):
                conn = mod.connect_db()

        self.assertIsInstance(conn, FakeConn)
        self.assertEqual(calls["kwargs"]["host"], "db.local")
        self.assertEqual(calls["kwargs"]["port"], 5544)
        self.assertEqual(calls["kwargs"]["dbname"], "workflow_db")
        self.assertEqual(calls["kwargs"]["user"], "robot")
        self.assertIn("connect_timeout", calls["kwargs"])
        self.assertFalse(conn.autocommit)

    def _graph_v1(self):
        return {
            "nodes": [
                {
                    "skill_name": "skill-a",
                    "description": "A",
                    "version": "1.0.0",
                    "capabilities": ["cap.a"],
                    "effects": ["db.read"],
                    "metadata": {"stub": False},
                    "content_hash": "h-a-1",
                    "spec_path": "skills/skill-a/SKILL.spec.yaml",
                    "operations_count": 2,
                },
                {
                    "skill_name": "skill-b",
                    "description": "B",
                    "version": "1.0.0",
                    "capabilities": [],
                    "effects": [],
                    "metadata": {"stub": False},
                    "content_hash": "h-b-1",
                    "spec_path": "skills/skill-b/SKILL.spec.yaml",
                    "operations_count": 1,
                },
            ],
            "edges": [
                {
                    "from_skill": "skill-a",
                    "to_skill": "skill-b",
                    "edge_type": "depends_on",
                }
            ],
        }

    def _graph_v1_changed_a(self):
        return {
            "nodes": [
                {
                    "skill_name": "skill-a",
                    "description": "A changed",
                    "version": "1.1.0",
                    "capabilities": ["cap.a"],
                    "effects": ["db.read", "db.write"],
                    "metadata": {"stub": False},
                    "content_hash": "h-a-2",
                    "spec_path": "skills/skill-a/SKILL.spec.yaml",
                    "operations_count": 3,
                },
                {
                    "skill_name": "skill-b",
                    "description": "B",
                    "version": "1.0.0",
                    "capabilities": [],
                    "effects": [],
                    "metadata": {"stub": False},
                    "content_hash": "h-b-1",
                    "spec_path": "skills/skill-b/SKILL.spec.yaml",
                    "operations_count": 1,
                },
            ],
            "edges": [
                {
                    "from_skill": "skill-a",
                    "to_skill": "skill-b",
                    "edge_type": "depends_on",
                }
            ],
        }

    def _graph_v2(self):
        return {
            "nodes": [
                {
                    "skill_name": "skill-a",
                    "description": "A changed",
                    "version": "1.1.0",
                    "capabilities": ["cap.a"],
                    "effects": ["db.read", "db.write"],
                    "metadata": {"stub": False},
                    "content_hash": "h-a-2",
                    "spec_path": "skills/skill-a/SKILL.spec.yaml",
                    "operations_count": 3,
                },
                {
                    "skill_name": "skill-c",
                    "description": "C",
                    "version": "1.0.0",
                    "capabilities": [],
                    "effects": [],
                    "metadata": {"stub": False},
                    "content_hash": "h-c-1",
                    "spec_path": "skills/skill-c/SKILL.spec.yaml",
                    "operations_count": 1,
                },
            ],
            "edges": [
                {
                    "from_skill": "skill-a",
                    "to_skill": "skill-c",
                    "edge_type": "delegates_to",
                }
            ],
        }

    def test_sync_inserts_new_nodes_and_edges(self):
        mod = load_module()
        conn = FakeConn()

        result = mod.sync_graph_to_db(self._graph_v1(), conn)
        self.assertEqual(result["inserted"], 2)
        self.assertEqual(result["updated"], 0)
        self.assertEqual(result["skipped"], 0)
        self.assertEqual(result["removed"], 0)
        self.assertEqual(result["edges_removed"], 0)
        self.assertEqual(result["edges_inserted"], 1)

    def test_sync_skips_unchanged_hashes_on_repeat(self):
        mod = load_module()
        conn = FakeConn()
        mod.sync_graph_to_db(self._graph_v1(), conn)

        result = mod.sync_graph_to_db(self._graph_v1(), conn)
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["updated"], 0)
        self.assertEqual(result["skipped"], 2)
        self.assertEqual(result["removed"], 0)

    def test_sync_updates_changed_nodes_when_hash_changes(self):
        mod = load_module()
        conn = FakeConn()
        mod.sync_graph_to_db(self._graph_v1(), conn)

        result = mod.sync_graph_to_db(self._graph_v1_changed_a(), conn)
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["updated"], 1)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(result["removed"], 0)

    def test_sync_removes_stale_nodes_not_in_latest_graph(self):
        mod = load_module()
        conn = FakeConn()
        mod.sync_graph_to_db(self._graph_v1(), conn)

        result = mod.sync_graph_to_db(self._graph_v2(), conn)
        self.assertEqual(result["removed"], 1)
        self.assertIn("skill-a", conn.nodes)
        self.assertIn("skill-c", conn.nodes)
        self.assertNotIn("skill-b", conn.nodes)

    def test_sync_rebuilds_edges_each_run(self):
        mod = load_module()
        conn = FakeConn()
        mod.sync_graph_to_db(self._graph_v1(), conn)

        result = mod.sync_graph_to_db(self._graph_v2(), conn)
        self.assertEqual(result["edges_removed"], 1)
        self.assertEqual(result["edges_inserted"], 1)
        self.assertEqual(
            {(e["from_skill"], e["to_skill"], e["edge_type"]) for e in conn.edges},
            {("skill-a", "skill-c", "delegates_to")},
        )

    def test_sync_is_idempotent_across_repeated_runs(self):
        mod = load_module()
        conn = FakeConn()
        mod.sync_graph_to_db(self._graph_v1(), conn)

        second = mod.sync_graph_to_db(self._graph_v1(), conn)
        third = mod.sync_graph_to_db(self._graph_v1(), conn)

        self.assertEqual(second, third)
        self.assertEqual(second["skipped"], 2)
        self.assertEqual(second["edges_removed"], 1)
        self.assertEqual(second["edges_inserted"], 1)


if __name__ == "__main__":
    unittest.main()
