#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import tempfile
import textwrap
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("rule_projection.py")


def load_module():
    spec = importlib.util.spec_from_file_location(
        "skill_system_rule_projection", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RuleProjectionTests(unittest.TestCase):
    def test_extract_rule_entries_skips_code_fences_and_deduplicates(self):
        mod = load_module()
        text = textwrap.dedent(
            """
            - first rule
            - first rule

            ```md
            - hidden rule
            ```

            1. numbered rule
            > quoted rule
            plain paragraph
            """
        )

        self.assertEqual(
            mod.extract_rule_entries(text),
            ["first rule", "numbered rule", "quoted rule"],
        )

    def test_build_rule_bundle_emits_three_scopes(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / "note").mkdir(parents=True)
            (repo_root / "review").mkdir(parents=True)
            (repo_root / "AGENTS.md").write_text("- system rule\n", encoding="utf-8")
            (repo_root / "note" / "note_rules.md").write_text(
                "- project rule\n", encoding="utf-8"
            )
            (repo_root / "review" / "REVIEW_AGENT_PROTOCOL.md").write_text(
                "- compat rule\n", encoding="utf-8"
            )

            configs = [
                {
                    **item,
                    "source_path": repo_root
                    / Path(item["source_path"]).relative_to(mod.ROOT_DIR),
                    "projection_path": repo_root
                    / "note"
                    / Path(item["projection_path"]).name,
                }
                for item in mod.RULE_SOURCE_CONFIG
            ]
            original = mod.RULE_SOURCE_CONFIG
            setattr(mod, "RULE_SOURCE_CONFIG", configs)
            try:
                bundle = mod.build_rule_bundle(repo_root)
            finally:
                setattr(mod, "RULE_SOURCE_CONFIG", original)

        self.assertEqual(
            bundle["merge_priority"], ["skill_system", "project", "compat"]
        )
        self.assertEqual(
            [item["rule_scope"] for item in bundle["rule_sets"]],
            ["skill_system", "project", "compat"],
        )

    def test_write_rule_projection_files_is_deterministic(self):
        mod = load_module()
        rule_model = {
            "merge_priority": ["skill_system", "project", "compat"],
            "rule_sets": [
                {
                    "rule_scope": "skill_system",
                    "source_path": "AGENTS.md",
                    "priority": 10,
                    "entries": [{"rule_text": "system rule", "enabled": True}],
                },
                {
                    "rule_scope": "project",
                    "source_path": "note/note_rules.md",
                    "priority": 20,
                    "entries": [{"rule_text": "project rule", "enabled": True}],
                },
                {
                    "rule_scope": "compat",
                    "source_path": "review/REVIEW_AGENT_PROTOCOL.md",
                    "priority": 30,
                    "entries": [{"rule_text": "compat rule", "enabled": True}],
                },
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            output_dir = repo_root / "note"
            first = mod.write_rule_projection_files(rule_model, repo_root, output_dir)
            first_content = {
                path.name: path.read_text(encoding="utf-8")
                for path in output_dir.glob("*.md")
            }
            second = mod.write_rule_projection_files(rule_model, repo_root, output_dir)
            second_content = {
                path.name: path.read_text(encoding="utf-8")
                for path in output_dir.glob("*.md")
            }

        self.assertEqual(first, second)
        self.assertEqual(first_content, second_content)


if __name__ == "__main__":
    unittest.main()
