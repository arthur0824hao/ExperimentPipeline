#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("architecture_graph.py")


def load_module():
    spec = importlib.util.spec_from_file_location(
        "skill_system_architecture_graph", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ArchitectureGraphTests(unittest.TestCase):
    def test_build_architecture_graph_collects_required_sections(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / "skills" / "skill-alpha").mkdir(parents=True)
            (repo_root / "skills" / "skill-system-workflow" / "scripts").mkdir(
                parents=True
            )
            (repo_root / "note").mkdir(parents=True)
            (repo_root / "review").mkdir(parents=True)
            (repo_root / "spec").mkdir(parents=True)
            (repo_root / "skills" / "skill-alpha" / "SKILL.md").write_text(
                "See note/note_tasks.md\n", encoding="utf-8"
            )
            (
                repo_root
                / "skills"
                / "skill-system-workflow"
                / "scripts"
                / "tickets.py"
            ).write_text("Reads note/note_tasks.md\n", encoding="utf-8")
            (repo_root / "note" / "note_tasks.md").write_text(
                "# batch\n", encoding="utf-8"
            )
            (repo_root / "review" / "REVIEW_BUNDLE.md").write_text(
                "# bundle\n", encoding="utf-8"
            )
            (repo_root / "spec" / "verify_example.py").write_text(
                "# verifier\n", encoding="utf-8"
            )

            graph = mod.build_architecture_graph(repo_root)

        self.assertTrue(any(node["node_type"] == "skill" for node in graph["nodes"]))
        self.assertTrue(any(node["node_type"] == "workflow" for node in graph["nodes"]))
        self.assertTrue(any(node["node_type"] == "doc" for node in graph["nodes"]))
        self.assertIn("L2 Skills", graph["layer_map"])

    def test_render_architecture_map_contains_required_sections(self):
        mod = load_module()
        graph = {
            "nodes": [
                {
                    "node_type": "runtime",
                    "name": "L0 Runtime",
                    "source_path": "runtime:L0",
                },
                {
                    "node_type": "skill",
                    "name": "skill-alpha",
                    "source_path": "skills/skill-alpha/SKILL.md",
                },
                {
                    "node_type": "doc",
                    "name": "note_tasks",
                    "source_path": "note/note_tasks.md",
                },
                {
                    "node_type": "workflow",
                    "name": "tickets",
                    "source_path": "skills/skill-system-workflow/scripts/tickets.py",
                },
            ],
            "edges": [
                {
                    "source_path": "runtime:L0",
                    "target_path": "runtime:L1",
                    "relation_type": "depends_on",
                }
            ],
            "layer_map": {
                "L0 Runtime": 1,
                "L1 Hooks": 1,
                "L2 Skills": 1,
                "L3 Workflow": 1,
                "L4 Human Interface": 1,
            },
        }

        rendered = mod.render_architecture_map(graph)
        self.assertIn("## Runtime Layers", rendered)
        self.assertIn("## Skill Graph", rendered)
        self.assertIn("## Document Graph", rendered)
        self.assertIn("## Workflow Graph", rendered)


if __name__ == "__main__":
    unittest.main()
