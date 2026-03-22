#!/usr/bin/env python3

from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from refresh_behavior_projections import build_projection_bundle


class TestRefreshBehaviorProjections(unittest.TestCase):
    def test_build_projection_bundle_creates_nodes_edges_and_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skills_dir = Path(tmp) / "skills"
            skill_dir = skills_dir / "skill-alpha"
            skill_dir.mkdir(parents=True)
            (skill_dir / "SKILL.spec.yaml").write_text(
                textwrap.dedent(
                    """
                    schema_version: 1
                    skill_name: skill-alpha
                    description: Test skill
                    depends_on:
                      - skill-beta
                    delegates_to: []
                    operations:
                      - name: sync-summary
                        intent: sync summaries
                        inputs: []
                        outputs: []
                        constraints: ["must sync"]
                        expected_effects: []
                    acceptance_tests:
                      structural: []
                      behavioral: []
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            bundle = build_projection_bundle(skills_dir)
            node_keys = {
                (n["skill_id"], n["node_type"], n["node_key"]) for n in bundle["nodes"]
            }
            edge_keys = {
                (e["edge_type"], e["from_node_key"], e["to_node_key"])
                for e in bundle["edges"]
            }

            self.assertIn(("skill-alpha", "skill", "skill-alpha"), node_keys)
            self.assertIn(("skill-alpha", "operation", "sync-summary"), node_keys)
            self.assertIn(("implements", "skill-alpha", "sync-summary"), edge_keys)
            self.assertIn(("depends_on", "skill-alpha", "skill-beta"), edge_keys)
            self.assertEqual(bundle["snapshots"][0]["skill_id"], "skill-alpha")


if __name__ == "__main__":
    unittest.main()
