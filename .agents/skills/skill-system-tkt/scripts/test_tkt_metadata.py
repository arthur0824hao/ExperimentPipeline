#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]
SCRIPT_PATH = Path(__file__).with_name("tkt.sh")


class TktMetadataTests(unittest.TestCase):
    def _run(
        self, *args: str, env: dict[str, str], check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT_PATH), *args],
            cwd=ROOT_DIR,
            env=env,
            capture_output=True,
            text=True,
            check=check,
        )

    def test_add_persists_structured_ticket_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ.copy()
            env["TKT_ROOT"] = str(Path(tmp) / ".tkt")
            self._run("init-roadmap", "--project", "meta", env=env)
            self._run("create-bundle", "--goal", "meta bundle", env=env)
            self._run(
                "add",
                "--bundle",
                "B-001",
                "--title",
                "Worker with metadata",
                "--description",
                "desc",
                "--acceptance",
                "acc",
                "--category",
                "quick",
                "--effort-estimate",
                "30m",
                "--depends-on",
                "TKT-001,TKT-003",
                "--source-plan",
                "plan.md",
                "--source-ticket-index",
                "2",
                env=env,
            )
            text = (
                Path(env["TKT_ROOT"]) / "bundles" / "B-001" / "TKT-001.yaml"
            ).read_text(encoding="utf-8")
            self.assertIn('category: "quick"', text)
            self.assertIn('effort_estimate: "30m"', text)
            self.assertIn('source_plan: "plan.md"', text)
            self.assertIn("source_ticket_index: 2", text)
            self.assertIn('  - "TKT-001"', text)
            self.assertIn('  - "TKT-003"', text)

    def test_express_create_persists_category_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ.copy()
            env["TKT_ROOT"] = str(Path(tmp) / ".tkt")
            self._run(
                "express",
                "--title",
                "Express meta",
                "--acceptance",
                "acc",
                "--category",
                "quick",
                "--effort-estimate",
                "5m",
                "--source-plan",
                "plan.md",
                "--source-ticket-index",
                "1",
                env=env,
            )
            text = (Path(env["TKT_ROOT"]) / "express" / "EXP-001.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn('category: "quick"', text)
            self.assertIn('effort_estimate: "5m"', text)
            self.assertIn('source_plan: "plan.md"', text)
            self.assertIn("source_ticket_index: 1", text)


if __name__ == "__main__":
    unittest.main()
