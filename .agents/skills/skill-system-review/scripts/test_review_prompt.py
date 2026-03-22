#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]
SCRIPT_PATH = Path(__file__).with_name("review_prompt.py")


def run_plan_to_bundle(plan_text: str) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        plan_path = Path(tmp) / "plan.md"
        plan_path.write_text(plan_text, encoding="utf-8")
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "plan-to-bundle",
                "--plan-file",
                str(plan_path),
            ],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(result.stdout)


class ReviewPromptMetadataTests(unittest.TestCase):
    def test_bundle_route_preserves_structured_metadata(self) -> None:
        payload = run_plan_to_bundle(
            """
# Metadata Plan

- [ ] Update `src/a.py`
  - category: quick
  - effort: 30m
  - skills: skill-system-tkt, skill-system-review
  - qa_scenarios: run pytest, inspect output
  - acceptance: function works
- [ ] Update `src/b.py`
  - category: deep
  - depends_on: TKT-001
""".strip()
        )

        self.assertEqual(payload["route"], "bundle")
        self.assertEqual(payload["tickets"][0]["category"], "quick")
        self.assertEqual(payload["tickets"][0]["effort_estimate"], "30m")
        self.assertEqual(
            payload["tickets"][0]["skills"],
            ["skill-system-tkt", "skill-system-review"],
        )
        self.assertEqual(
            payload["tickets"][0]["qa_scenarios"],
            ["run pytest", "inspect output"],
        )
        self.assertEqual(
            payload["tickets"][0]["acceptance_criteria"], ["function works"]
        )
        self.assertEqual(payload["tickets"][1]["depends_on"], ["TKT-001"])
        self.assertIn('--category "quick"', payload["tkt_commands"][1])
        self.assertIn('--effort-estimate "30m"', payload["tkt_commands"][1])
        self.assertIn(
            '--skills "skill-system-tkt, skill-system-review"',
            payload["tkt_commands"][1],
        )
        self.assertIn(
            '--qa-scenarios "run pytest, inspect output"',
            payload["tkt_commands"][1],
        )
        self.assertIn('--source-plan "plan.md"', payload["tkt_commands"][1])
        self.assertIn("--source-ticket-index 1", payload["tkt_commands"][1])
        self.assertIn('--depends-on "TKT-001"', payload["tkt_commands"][2])

    def test_express_route_no_filename_still_uses_express(self) -> None:
        payload = run_plan_to_bundle(
            """
# Tiny Metadata Plan

- [ ] Fix login button label
  - category: quick
  - acceptance: fix a
- [ ] Tidy submit copy
  - category: deep
  - acceptance: fix b
""".strip()
        )

        self.assertEqual(payload["route"], "express")
        self.assertEqual(len(payload["tkt_commands"]), 2)
        self.assertIn('--category "quick"', payload["tkt_commands"][0])
        self.assertIn("--source-ticket-index 1", payload["tkt_commands"][0])
        self.assertIn('--category "deep"', payload["tkt_commands"][1])
        self.assertIn("--source-ticket-index 2", payload["tkt_commands"][1])
        self.assertTrue(
            all("create-bundle" not in cmd for cmd in payload["tkt_commands"])
        )

    def test_wave_extraction_emits_wave_commands(self) -> None:
        payload = run_plan_to_bundle(
            """
# Wave Plan

### WAVE 1
- [ ] Prepare metadata
  - category: quick
  - qa_scenarios: run pytest

### WAVE 2
- [ ] Finish bundle wiring
  - skills: skill-system-tkt
""".strip()
        )

        self.assertEqual(payload["route"], "bundle")
        self.assertEqual(payload["tickets"][0]["wave"], 1)
        self.assertEqual(payload["tickets"][1]["wave"], 2)
        self.assertIn("--wave 1", payload["tkt_commands"][1])
        self.assertIn("--wave 2", payload["tkt_commands"][2])
        self.assertIn('--skills "skill-system-tkt"', payload["tkt_commands"][2])


if __name__ == "__main__":
    unittest.main()
