from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("cockpit_proposals.py")


def load_module():
    spec = importlib.util.spec_from_file_location(
        "insight_cockpit_proposals", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sample_state() -> dict:
    return {
        "active_round": {"version": "cockpit-kernel-and-readonly-tui-round"},
        "frictions": [
            {
                "status": "OPEN",
                "summary": "current-state block must refresh without duplicating execution log content",
            }
        ],
        "active_tasks": [],
        "watcher_gaps": [
            {
                "watcher": "domain_profile_adapter",
                "recommended_next": "Attach one generic profile adapter.",
            }
        ],
        "source_refs": {
            "note_tasks": "note/note_tasks.md",
            "note_feedback": "note/note_feedback.md",
            "runtime_doctor": "skills/skill-system-memory/scripts/runtime_doctor.py",
        },
    }


class CockpitProposalTests(unittest.TestCase):
    def test_generate_humane_workflow_proposals_shape(self):
        mod = load_module()

        proposals = mod.generate_humane_workflow_proposals(sample_state(), limit=3)

        self.assertGreaterEqual(len(proposals), 2)
        for proposal in proposals:
            self.assertEqual(proposal["status"], "PROPOSED")
            self.assertIn(proposal["kind"], {"friction", "next_step", "decision"})
            self.assertIn("proposal_id", proposal)
            self.assertIn("summary", proposal)
            self.assertIn("rationale", proposal)
            self.assertIn("suggested_change", proposal)
            self.assertTrue(proposal["evidence_refs"])

    def test_generate_humane_workflow_proposals_is_stable_for_same_state(self):
        mod = load_module()
        state = sample_state()

        first = mod.generate_humane_workflow_proposals(state, limit=3)
        second = mod.generate_humane_workflow_proposals(state, limit=3)

        self.assertEqual(
            [item["proposal_id"] for item in first],
            [item["proposal_id"] for item in second],
        )
        self.assertEqual(
            [item["summary"] for item in first],
            [item["summary"] for item in second],
        )

    def test_cli_outputs_non_durable_pending_model(self):
        mod = load_module()
        payload = sample_state()

        with patch("builtins.print") as fake_print:
            exit_code = mod.main(
                [
                    "generate-proposals",
                    "--state-json",
                    json.dumps(payload),
                    "--limit",
                    "2",
                ]
            )

        printed = json.loads(fake_print.call_args[0][0])
        self.assertEqual(exit_code, 0)
        self.assertEqual(printed["status"], "ok")
        self.assertEqual(printed["pending_storage_model"], "non_durable_in_memory_only")
        self.assertEqual(
            printed["proposal_scope"], "humane_agent_workflow_improvements_only"
        )
        self.assertLessEqual(len(printed["proposals"]), 2)
        self.assertTrue(printed["summary"])


if __name__ == "__main__":
    unittest.main()
