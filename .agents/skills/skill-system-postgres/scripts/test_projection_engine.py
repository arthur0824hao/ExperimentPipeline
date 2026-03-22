#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("projection_engine.py")


def load_module():
    spec = importlib.util.spec_from_file_location(
        "skill_system_projection_engine", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ProjectionEngineTests(unittest.TestCase):
    def test_replace_generated_block_preserves_manual_text_outside_block(self):
        mod = load_module()
        existing = "manual top\n\nmanual bottom\n"
        updated = mod.replace_generated_block(
            existing, "generated", append_if_missing=True
        )
        self.assertIn("manual top", updated)
        self.assertIn(mod.GENERATED_START, updated)
        self.assertIn("manual bottom", updated)

    def test_replace_generated_block_is_idempotent(self):
        mod = load_module()
        first = mod.replace_generated_block("", "generated", append_if_missing=False)
        second = mod.replace_generated_block(
            first, "generated", append_if_missing=False
        )
        self.assertEqual(first, second)

    def test_write_projection_documents_preserves_note_feedback_outside_block(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            feedback = repo_root / "note" / "note_feedback.md"
            feedback.parent.mkdir(parents=True)
            feedback.write_text("manual feedback\n", encoding="utf-8")
            targets = {
                "note/skill_system_rules.md": repo_root
                / "note"
                / "skill_system_rules.md",
                "note/project_rules.md": repo_root / "note" / "project_rules.md",
                "note/compat_rules.md": repo_root / "note" / "compat_rules.md",
                "note/architecture_map.md": repo_root / "note" / "architecture_map.md",
                "note/note_feedback.md": feedback,
            }
            original = mod.build_projection_documents
            setattr(
                mod,
                "build_projection_documents",
                lambda repo_root=repo_root: {
                    key: f"content for {key}" for key in targets
                },
            )
            try:
                written = mod.write_projection_documents(repo_root, targets)
            finally:
                setattr(mod, "build_projection_documents", original)

            updated_feedback = feedback.read_text(encoding="utf-8")
            self.assertEqual(len(written), 5)
            self.assertIn("manual feedback", updated_feedback)
            self.assertIn(mod.GENERATED_START, updated_feedback)


if __name__ == "__main__":
    unittest.main()
