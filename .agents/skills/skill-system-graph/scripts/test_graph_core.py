#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import importlib.util
import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path


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


class GraphCoreTests(unittest.TestCase):
    def test_compute_content_hash_returns_md5(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            spec_path = Path(tmp) / "SKILL.spec.yaml"
            content = "skill_name: hash-test\n"
            spec_path.write_text(content, encoding="utf-8")

            got = mod.compute_content_hash(spec_path)
            expected = hashlib.md5(content.encode("utf-8")).hexdigest()
            self.assertEqual(got, expected)

    def test_parse_spec_extracts_single_node_and_direct_edges(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            spec_path = Path(tmp) / "SKILL.spec.yaml"
            spec_path.write_text(
                """
schema_version: 1
skill_name: skill-alpha
description: Alpha node
depends_on:
  - skill-beta
  - skill-gamma
delegates_to:
  - skill-delta
  - skill-gamma
operations:
  - name: op-a
    intent: test
""".lstrip(),
                encoding="utf-8",
            )

            parsed = mod.parse_spec(spec_path)
            self.assertEqual(parsed["node"]["skill_name"], "skill-alpha")
            self.assertEqual(parsed["node"]["description"], "Alpha node")
            self.assertEqual(parsed["node"]["operations_count"], 1)

            edge_keys = {
                (e["from_skill"], e["to_skill"], e["edge_type"])
                for e in parsed["edges"]
            }
            self.assertEqual(
                edge_keys,
                {
                    ("skill-alpha", "skill-beta", "depends_on"),
                    ("skill-alpha", "skill-gamma", "depends_on"),
                    ("skill-alpha", "skill-delta", "delegates_to"),
                    ("skill-alpha", "skill-gamma", "delegates_to"),
                },
            )

    def test_parse_spec_handles_empty_dependency_lists(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            spec_path = Path(tmp) / "SKILL.spec.yaml"
            spec_path.write_text(
                """
schema_version: 1
skill_name: skill-empty
description: no deps
depends_on: []
delegates_to: []
operations:
  - name: op
    intent: test
""".lstrip(),
                encoding="utf-8",
            )

            parsed = mod.parse_spec(spec_path)
            self.assertEqual(parsed["node"]["skill_name"], "skill-empty")
            self.assertEqual(parsed["node"]["operations_count"], 1)
            self.assertEqual(parsed["edges"], [])

    def test_parse_spec_v2_extracts_nodes_and_relationships(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            spec_path = Path(tmp) / "SKILL.spec.yaml"
            spec_path.write_text(
                """
schema_version: 2
spec_id: behavior-sample
description: Sample behavior spec
nodes:
  - id: runtime-doctor
    type: script
    description: Reports runtime alignment
    behaviors:
      - name: report-runtime-alignment
        intent: expose status
  - id: runtime-config
    type: config
    description: Runtime configuration source
relationships:
  - from: runtime-doctor
    to: runtime-config
    type: reads
  - from: runtime-doctor
    to: ext-policy-doc
    type: references
""".lstrip(),
                encoding="utf-8",
            )

            parsed = mod.parse_spec(spec_path)
            node_names = [node["skill_name"] for node in parsed["nodes"]]
            self.assertEqual(
                node_names,
                [
                    "behavior-sample::runtime-doctor",
                    "behavior-sample::runtime-config",
                ],
            )

            node_by_name = {node["skill_name"]: node for node in parsed["nodes"]}
            self.assertEqual(
                node_by_name["behavior-sample::runtime-doctor"]["operations_count"], 1
            )
            self.assertEqual(
                node_by_name["behavior-sample::runtime-config"]["operations_count"], 0
            )

            edge_keys = {
                (edge["from_skill"], edge["to_skill"], edge["edge_type"])
                for edge in parsed["edges"]
            }
            self.assertEqual(
                edge_keys,
                {
                    (
                        "behavior-sample::runtime-doctor",
                        "behavior-sample::runtime-config",
                        "depends_on",
                    ),
                    (
                        "behavior-sample::runtime-doctor",
                        "ext-policy-doc",
                        "depends_on",
                    ),
                },
            )

    def test_parse_spec_v2_without_nodes_uses_spec_id_fallback_node(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            spec_path = Path(tmp) / "SKILL.spec.yaml"
            spec_path.write_text(
                """
schema_version: 2
spec_id: behavior-empty
description: no nodes yet
""".lstrip(),
                encoding="utf-8",
            )

            parsed = mod.parse_spec(spec_path)
            self.assertEqual(
                [n["skill_name"] for n in parsed["nodes"]], ["behavior-empty"]
            )
            self.assertEqual(parsed["edges"], [])

    def test_scan_all_specs_collects_multiple_specs(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            skills_dir = Path(tmp) / "skills"
            (skills_dir / "skill-a").mkdir(parents=True)
            (skills_dir / "skill-b").mkdir(parents=True)

            (skills_dir / "skill-a" / "SKILL.spec.yaml").write_text(
                """
schema_version: 1
skill_name: skill-a
description: A
depends_on: [skill-b]
operations:
  - name: op
    intent: test
""".lstrip(),
                encoding="utf-8",
            )
            (skills_dir / "skill-b" / "SKILL.spec.yaml").write_text(
                """
schema_version: 1
skill_name: skill-b
description: B
operations:
  - name: op
    intent: test
""".lstrip(),
                encoding="utf-8",
            )

            specs = mod.scan_all_specs(skills_dir)
            names = sorted(item["node"]["skill_name"] for item in specs)
            self.assertEqual(names, ["skill-a", "skill-b"])

    def test_scan_all_specs_skips_malformed_yaml_with_warning(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            skills_dir = Path(tmp) / "skills"
            (skills_dir / "skill-good").mkdir(parents=True)
            (skills_dir / "skill-bad").mkdir(parents=True)

            (skills_dir / "skill-good" / "SKILL.spec.yaml").write_text(
                """
schema_version: 1
skill_name: skill-good
description: good
operations:
  - name: op
    intent: test
""".lstrip(),
                encoding="utf-8",
            )
            (skills_dir / "skill-bad" / "SKILL.spec.yaml").write_text(
                "skill_name: [broken\n",
                encoding="utf-8",
            )

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                specs = mod.scan_all_specs(skills_dir)

            self.assertEqual(
                [item["node"]["skill_name"] for item in specs], ["skill-good"]
            )
            self.assertIn("Warning:", stderr.getvalue())
            self.assertIn("skill-bad", stderr.getvalue())

    def test_build_graph_deduplicates_edges_and_creates_stub_nodes(self):
        mod = load_module()
        specs = [
            {
                "node": {
                    "skill_name": "skill-a",
                    "description": "A",
                    "spec_path": "a/SKILL.spec.yaml",
                    "content_hash": "aaa",
                    "operations_count": 2,
                },
                "edges": [
                    {
                        "from_skill": "skill-a",
                        "to_skill": "skill-b",
                        "edge_type": "depends_on",
                    },
                    {
                        "from_skill": "skill-a",
                        "to_skill": "skill-b",
                        "edge_type": "depends_on",
                    },
                    {
                        "from_skill": "skill-a",
                        "to_skill": "skill-c",
                        "edge_type": "delegates_to",
                    },
                ],
            },
            {
                "node": {
                    "skill_name": "skill-b",
                    "description": "B",
                    "spec_path": "b/SKILL.spec.yaml",
                    "content_hash": "bbb",
                    "operations_count": 1,
                },
                "edges": [],
            },
        ]

        graph = mod.build_graph(specs)
        self.assertEqual(set(graph.keys()), {"nodes", "edges"})

        node_by_name = {n["skill_name"]: n for n in graph["nodes"]}
        self.assertEqual(node_by_name["skill-a"]["operations_count"], 2)
        self.assertEqual(node_by_name["skill-b"]["operations_count"], 1)
        self.assertFalse(node_by_name["skill-a"].get("stub", False))
        self.assertFalse(node_by_name["skill-b"].get("stub", False))
        self.assertTrue(node_by_name["skill-c"]["stub"])
        self.assertEqual(node_by_name["skill-c"]["operations_count"], 0)

        edge_keys = {
            (e["from_skill"], e["to_skill"], e["edge_type"]) for e in graph["edges"]
        }
        self.assertEqual(
            edge_keys,
            {
                ("skill-a", "skill-b", "depends_on"),
                ("skill-a", "skill-c", "delegates_to"),
            },
        )

    def test_build_graph_includes_v1_and_v2_nodes_with_stub_targets(self):
        mod = load_module()
        specs = [
            {
                "node": {
                    "skill_name": "skill-a",
                    "description": "A",
                    "spec_path": "a/SKILL.spec.yaml",
                    "content_hash": "aaa",
                    "operations_count": 2,
                },
                "nodes": [
                    {
                        "skill_name": "skill-a",
                        "description": "A",
                        "spec_path": "a/SKILL.spec.yaml",
                        "content_hash": "aaa",
                        "operations_count": 2,
                    }
                ],
                "edges": [],
            },
            {
                "node": {
                    "skill_name": "behavior-demo::script-main",
                    "description": "Main behavior node",
                    "spec_path": "demo/SKILL.spec.yaml",
                    "content_hash": "ccc",
                    "operations_count": 3,
                },
                "nodes": [
                    {
                        "skill_name": "behavior-demo::script-main",
                        "description": "Main behavior node",
                        "spec_path": "demo/SKILL.spec.yaml",
                        "content_hash": "ccc",
                        "operations_count": 3,
                    },
                    {
                        "skill_name": "behavior-demo::config",
                        "description": "Config node",
                        "spec_path": "demo/SKILL.spec.yaml",
                        "content_hash": "ccc",
                        "operations_count": 0,
                    },
                ],
                "edges": [
                    {
                        "from_skill": "behavior-demo::script-main",
                        "to_skill": "behavior-demo::config",
                        "edge_type": "depends_on",
                    },
                    {
                        "from_skill": "behavior-demo::script-main",
                        "to_skill": "external-doc",
                        "edge_type": "depends_on",
                    },
                    {
                        "from_skill": "behavior-demo::script-main",
                        "to_skill": "skill-a",
                        "edge_type": "delegates_to",
                    },
                ],
            },
        ]

        graph = mod.build_graph(specs)
        node_by_name = {n["skill_name"]: n for n in graph["nodes"]}
        self.assertFalse(node_by_name["skill-a"]["stub"])
        self.assertEqual(node_by_name["skill-a"]["operations_count"], 2)
        self.assertFalse(node_by_name["behavior-demo::script-main"]["stub"])
        self.assertEqual(
            node_by_name["behavior-demo::script-main"]["operations_count"], 3
        )
        self.assertFalse(node_by_name["behavior-demo::config"]["stub"])
        self.assertTrue(node_by_name["external-doc"]["stub"])
        self.assertEqual(
            {
                (edge["from_skill"], edge["to_skill"], edge["edge_type"])
                for edge in graph["edges"]
            },
            {
                (
                    "behavior-demo::script-main",
                    "behavior-demo::config",
                    "depends_on",
                ),
                ("behavior-demo::script-main", "external-doc", "depends_on"),
                ("behavior-demo::script-main", "skill-a", "delegates_to"),
            },
        )


if __name__ == "__main__":
    unittest.main()
