#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


MODULE_PATH = Path(__file__).with_name("evolution_proposal.py")


def load_module():
    spec = importlib.util.spec_from_file_location("evolution_proposal", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {MODULE_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestEvolutionProposalFlow(unittest.TestCase):
    def _semantic_identity(
        self, mod, proposal, *, action, requested_parent_node_id=None
    ):
        return mod.build_semantic_identity(
            proposal,
            action=action,
            requested_parent_node_id=requested_parent_node_id,
        )

    def test_generate_proposal_produces_no_db_write(self):
        mod = load_module()
        proposal = mod.make_proposal(
            kind="unknown",
            summary="Need a proposal",
            rationale="Because evidence is partial",
            suggested_change="Wait for approval",
            evidence_refs=["note/note_tasks.md"],
        )

        self.assertEqual(proposal["status"], "PROPOSED")
        self.assertEqual(proposal["kind"], "unknown")
        self.assertIn("proposal_id", proposal)

    def test_ambient_pgdatabase_does_not_steer_workflow_target(self):
        mod = load_module()
        with patch.dict(os.environ, {"PGDATABASE": "skill_system"}, clear=True):
            db_target, source = mod.resolve_workflow_db_target()

        self.assertEqual(db_target, "agent_memory")
        self.assertEqual(source, "canonical:agent_memory(ambient_ignored:skill_system)")

    def test_explicit_noncanonical_target_fails(self):
        mod = load_module()
        with patch.dict(os.environ, {"SKILL_PGDATABASE": "skill_system"}, clear=True):
            with self.assertRaises(SystemExit):
                mod.resolve_workflow_db_target()

    def test_reject_produces_no_write(self):
        mod = load_module()
        proposal = mod.make_proposal(
            kind="decision",
            summary="Reject me",
            rationale="No approval",
            suggested_change="Do nothing",
            evidence_refs=[],
        )

        rejected = mod.reject_proposal(proposal, mode="reject")
        self.assertEqual(rejected["status"], "REJECTED")
        self.assertEqual(rejected["rejected_via"], "reject")

    def test_invalid_payload_is_rejected(self):
        mod = load_module()
        with self.assertRaises(SystemExit):
            mod.validate_proposal_payload({"proposal_id": "p1", "kind": "unknown"})

    def test_approve_creates_tree_node_via_store_memory(self):
        mod = load_module()
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        cur.fetchone.side_effect = [None, (123,), (901,)]
        proposal = mod.make_proposal(
            kind="friction",
            summary="Approved proposal",
            rationale="Repeated issue",
            suggested_change="Adopt explicit target",
            evidence_refs=["review/REVIEW_BUNDLE.md"],
        )
        semantic_identity = self._semantic_identity(mod, proposal, action="approve")

        memory_id, accepted = mod.persist_accepted_proposal(
            conn,
            proposal,
            target_source="SKILL_PGDATABASE",
            semantic_identity=semantic_identity,
            semantic_fingerprint=mod.fingerprint_semantic_identity(semantic_identity),
        )

        self.assertEqual(memory_id, 123)
        self.assertEqual(accepted["status"], "ACCEPTED")
        self.assertIn("node_id", accepted)
        self.assertEqual(accepted["task_id"], 901)

        statements = [call.args[0] for call in cur.execute.call_args_list]
        self.assertTrue(any("store_memory" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO evolution_nodes" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO evolution_tasks" in sql for sql in statements))

    def test_reject_is_retained_in_rejected_log(self):
        mod = load_module()
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        cur.fetchone.return_value = (321,)
        proposal = mod.make_proposal(
            kind="decision",
            summary="Rejected proposal",
            rationale="User said no",
            suggested_change="Do not apply",
            evidence_refs=[],
        )
        semantic_identity = self._semantic_identity(mod, proposal, action="reject")

        memory_id, rejected = mod.persist_rejected_proposal(
            conn,
            proposal,
            target_source="canonical",
            mode="reject",
            semantic_identity=semantic_identity,
            semantic_fingerprint=mod.fingerprint_semantic_identity(semantic_identity),
        )

        self.assertEqual(memory_id, 321)
        self.assertEqual(rejected["status"], "REJECTED")
        statements = [call.args[0] for call in cur.execute.call_args_list]
        self.assertTrue(any("store_memory" in sql for sql in statements))
        self.assertTrue(
            any("INSERT INTO evolution_rejections" in sql for sql in statements)
        )

    def test_list_read_and_lineage_use_accepted_nodes(self):
        mod = load_module()
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        cur.fetchall.return_value = [
            (
                "n1",
                "p1",
                "Evolution Node Accepted: Example",
                "unknown",
                "ACCEPTED",
                None,
                901,
                {"action": "approve"},
                "fp1",
                123,
                '{"summary": "Example"}',
                "Example",
            )
        ]
        rows = mod.list_accepted_records(conn, limit=5)
        self.assertEqual(rows[0]["proposal_id"], "p1")
        self.assertEqual(rows[0]["node_id"], "n1")
        self.assertEqual(rows[0]["task_id"], 901)
        self.assertIn("FROM evolution_nodes", cur.execute.call_args[0][0])
        self.assertIn("JOIN evolution_tasks et", cur.execute.call_args[0][0])
        self.assertIn("JOIN agent_tasks t", cur.execute.call_args[0][0])

        cur.fetchone.return_value = (
            "n1",
            "p1",
            "Evolution Node Accepted: Example",
            "payload",
            "unknown",
            "ACCEPTED",
            None,
            123,
            901,
            {"action": "approve"},
            "fp1",
            "Example",
        )
        record = mod.read_accepted_record(conn, "n1")
        self.assertEqual(record["proposal_id"], "p1")
        self.assertEqual(record["memory_id"], 123)
        self.assertEqual(record["task_id"], 901)

        with patch.object(
            mod,
            "read_accepted_record",
            side_effect=[
                {
                    "node_id": "n2",
                    "proposal_id": "p2",
                    "title": "child",
                    "kind": "next_step",
                    "status": "ACCEPTED",
                    "parent_node_id": "n1",
                },
                {
                    "node_id": "n1",
                    "proposal_id": "p1",
                    "title": "root",
                    "kind": "unknown",
                    "status": "ACCEPTED",
                    "parent_node_id": None,
                },
            ],
        ):
            lineage = mod.build_lineage(conn, "n2")

        self.assertEqual([item["node_id"] for item in lineage], ["n2", "n1"])

    def test_list_materialized_tasks_reads_canonical_task_table(self):
        mod = load_module()
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        cur.fetchall.return_value = [
            (901, "n1", "Approved proposal", "open", "2026-03-12T12:00:00+00:00")
        ]

        rows = mod.list_materialized_tasks(conn, limit=5)

        self.assertEqual(rows[0]["task_id"], 901)
        self.assertEqual(rows[0]["source_node_id"], "n1")
        self.assertEqual(rows[0]["status"], "open")
        self.assertIn("FROM evolution_tasks et", cur.execute.call_args[0][0])
        self.assertIn("JOIN agent_tasks t", cur.execute.call_args[0][0])

    def test_approve_main_rolls_back_on_mid_step_failure(self):
        mod = load_module()
        conn = MagicMock()
        proposal = mod.make_proposal(
            kind="friction",
            summary="Approve safely",
            rationale="Need atomicity",
            suggested_change="Rollback on failure",
            evidence_refs=[],
        )

        with (
            patch.object(
                sys,
                "argv",
                [
                    "evolution_proposal.py",
                    "approve",
                    "--proposal-json",
                    json.dumps(proposal),
                ],
            ),
            patch.object(mod, "connect_workflow_db", return_value=conn),
            patch.object(mod, "parse_proposal_payload", return_value=proposal),
            patch.object(
                mod, "decide_accept_proposal", side_effect=RuntimeError("boom")
            ),
            self.assertRaises(RuntimeError),
        ):
            mod.main()

        self.assertFalse(conn.autocommit)
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()

    def test_reject_main_rolls_back_on_mid_step_failure(self):
        mod = load_module()
        conn = MagicMock()
        proposal = mod.make_proposal(
            kind="decision",
            summary="Reject safely",
            rationale="Need atomicity",
            suggested_change="Rollback on failure",
            evidence_refs=[],
        )

        with (
            patch.object(
                sys,
                "argv",
                [
                    "evolution_proposal.py",
                    "reject",
                    "--proposal-json",
                    json.dumps(proposal),
                ],
            ),
            patch.object(mod, "connect_workflow_db", return_value=conn),
            patch.object(mod, "parse_proposal_payload", return_value=proposal),
            patch.object(
                mod, "decide_reject_proposal", side_effect=RuntimeError("boom")
            ),
            self.assertRaises(RuntimeError),
        ):
            mod.main()

        self.assertFalse(conn.autocommit)
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()

    def test_render_feedback_is_concise(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "list_accepted_records",
                return_value=[
                    {
                        "title": "Accepted item",
                        "node_id": "n1",
                        "parent_node_id": None,
                        "kind": "next_step",
                    }
                ],
            ),
            patch.object(
                mod,
                "list_rejected_records",
                return_value=[
                    {
                        "title": "Rejected item",
                        "rejected_via": "reject",
                        "kind": "unknown",
                    }
                ],
            ),
        ):
            rendered = mod.render_feedback_surface(
                conn,
                proposal={
                    "status": "PROPOSED",
                    "kind": "unknown",
                    "summary": "Need approval",
                    "proposal_id": "p1",
                },
            )

        self.assertIn("### Evolution Ledger", rendered)
        self.assertIn("proposal:", rendered)
        self.assertIn("accepted:", rendered)
        self.assertIn("rejected:", rendered)

    def test_approve_replay_same_payload_returns_existing_result(self):
        mod = load_module()
        conn = MagicMock()
        proposal = mod.make_proposal(
            proposal_id="evop-approve-replay",
            kind="decision",
            summary="Replay me",
            rationale="Same input",
            suggested_change="Return existing node",
            evidence_refs=["note/note_tasks.md"],
        )
        semantic_identity = self._semantic_identity(mod, proposal, action="approve")
        existing = {
            "proposal_id": proposal["proposal_id"],
            "node_id": "evo-node-existing",
            "kind": proposal["kind"],
            "status": "ACCEPTED",
            "parent_node_id": None,
            "task_id": 901,
            "memory_id": 123,
            "content": json.dumps(
                {
                    **proposal,
                    "status": "ACCEPTED",
                    "node_id": "evo-node-existing",
                    "task_id": 901,
                }
            ),
            "semantic_identity": semantic_identity,
            "semantic_fingerprint": mod.fingerprint_semantic_identity(
                semantic_identity
            ),
            "summary": proposal["summary"],
        }

        with (
            patch.object(mod, "lock_proposal_decision") as lock,
            patch.object(mod, "read_accepted_record", return_value=existing),
            patch.object(mod, "read_rejected_record", return_value=None),
            patch.object(mod, "persist_accepted_proposal") as persist,
        ):
            memory_id, accepted, replay_status = mod.decide_accept_proposal(
                conn, proposal, target_source="canonical:agent_memory"
            )

        lock.assert_called_once_with(conn, proposal["proposal_id"])
        persist.assert_not_called()
        self.assertEqual(memory_id, 123)
        self.assertEqual(accepted["task_id"], 901)
        self.assertEqual(replay_status, "REPLAYED_EXISTING")

    def test_reject_replay_same_payload_returns_existing_result(self):
        mod = load_module()
        conn = MagicMock()
        proposal = mod.make_proposal(
            proposal_id="evop-reject-replay",
            kind="decision",
            summary="Reject replay",
            rationale="Same input",
            suggested_change="Return existing rejection",
            evidence_refs=["note/note_tasks.md"],
        )
        semantic_identity = self._semantic_identity(mod, proposal, action="reject")
        existing = {
            "proposal_id": proposal["proposal_id"],
            "kind": proposal["kind"],
            "status": "REJECTED",
            "rejected_via": "reject",
            "memory_id": 321,
            "content": json.dumps(
                {
                    **proposal,
                    "status": "REJECTED",
                    "rejected_via": "reject",
                }
            ),
            "semantic_identity": semantic_identity,
            "semantic_fingerprint": mod.fingerprint_semantic_identity(
                semantic_identity
            ),
            "summary": proposal["summary"],
        }

        with (
            patch.object(mod, "lock_proposal_decision"),
            patch.object(mod, "read_accepted_record", return_value=None),
            patch.object(mod, "read_rejected_record", return_value=existing),
            patch.object(mod, "persist_rejected_proposal") as persist,
        ):
            memory_id, rejected, replay_status = mod.decide_reject_proposal(
                conn,
                proposal,
                target_source="canonical:agent_memory",
                mode="reject",
            )

        persist.assert_not_called()
        self.assertEqual(memory_id, 321)
        self.assertEqual(rejected["rejected_via"], "reject")
        self.assertEqual(replay_status, "REPLAYED_EXISTING")

    def test_dismiss_replay_same_payload_returns_existing_result(self):
        mod = load_module()
        conn = MagicMock()
        proposal = mod.make_proposal(
            proposal_id="evop-dismiss-replay",
            kind="unknown",
            summary="Dismiss replay",
            rationale="Same input",
            suggested_change="Return existing dismissal",
            evidence_refs=[],
        )
        semantic_identity = self._semantic_identity(mod, proposal, action="dismiss")
        existing = {
            "proposal_id": proposal["proposal_id"],
            "kind": proposal["kind"],
            "status": "REJECTED",
            "rejected_via": "dismiss",
            "memory_id": 654,
            "content": json.dumps(
                {
                    **proposal,
                    "status": "REJECTED",
                    "rejected_via": "dismiss",
                }
            ),
            "semantic_identity": semantic_identity,
            "semantic_fingerprint": mod.fingerprint_semantic_identity(
                semantic_identity
            ),
            "summary": proposal["summary"],
        }

        with (
            patch.object(mod, "lock_proposal_decision"),
            patch.object(mod, "read_accepted_record", return_value=None),
            patch.object(mod, "read_rejected_record", return_value=existing),
            patch.object(mod, "persist_rejected_proposal") as persist,
        ):
            memory_id, rejected, replay_status = mod.decide_reject_proposal(
                conn,
                proposal,
                target_source="canonical:agent_memory",
                mode="dismiss",
            )

        persist.assert_not_called()
        self.assertEqual(memory_id, 654)
        self.assertEqual(rejected["rejected_via"], "dismiss")
        self.assertEqual(replay_status, "REPLAYED_EXISTING")

    def test_approve_after_reject_conflict_is_blocked(self):
        mod = load_module()
        conn = MagicMock()
        proposal = mod.make_proposal(
            proposal_id="evop-approve-conflict",
            kind="decision",
            summary="Conflict",
            rationale="Opposite terminal state",
            suggested_change="Should fail",
            evidence_refs=[],
        )

        with (
            patch.object(mod, "lock_proposal_decision"),
            patch.object(mod, "read_accepted_record", return_value=None),
            patch.object(
                mod,
                "read_rejected_record",
                return_value={
                    "proposal_id": proposal["proposal_id"],
                    "rejected_via": "reject",
                },
            ),
            self.assertRaises(mod.DecisionReplayError) as ctx,
        ):
            mod.decide_accept_proposal(conn, proposal, target_source="canonical")

        self.assertEqual(ctx.exception.code, "TERMINAL_CONFLICT")

    def test_reject_after_approve_conflict_is_blocked(self):
        mod = load_module()
        conn = MagicMock()
        proposal = mod.make_proposal(
            proposal_id="evop-reject-conflict",
            kind="decision",
            summary="Conflict",
            rationale="Opposite terminal state",
            suggested_change="Should fail",
            evidence_refs=[],
        )

        with (
            patch.object(mod, "lock_proposal_decision"),
            patch.object(
                mod,
                "read_accepted_record",
                return_value={
                    "proposal_id": proposal["proposal_id"],
                    "status": "ACCEPTED",
                },
            ),
            self.assertRaises(mod.DecisionReplayError) as ctx,
        ):
            mod.decide_reject_proposal(
                conn, proposal, target_source="canonical", mode="reject"
            )

        self.assertEqual(ctx.exception.code, "TERMINAL_CONFLICT")

    def test_payload_mismatch_blocked(self):
        mod = load_module()
        conn = MagicMock()
        proposal = mod.make_proposal(
            proposal_id="evop-mismatch",
            kind="decision",
            summary="Mismatch",
            rationale="Payload changed",
            suggested_change="Should fail",
            evidence_refs=["note/note_tasks.md"],
        )
        different_identity = self._semantic_identity(
            mod,
            {**proposal, "summary": "Different summary"},
            action="approve",
        )

        with (
            patch.object(mod, "lock_proposal_decision"),
            patch.object(
                mod,
                "read_accepted_record",
                return_value={
                    "proposal_id": proposal["proposal_id"],
                    "node_id": "node-1",
                    "kind": proposal["kind"],
                    "status": "ACCEPTED",
                    "parent_node_id": None,
                    "task_id": 1,
                    "memory_id": 2,
                    "content": json.dumps(proposal),
                    "semantic_identity": different_identity,
                    "semantic_fingerprint": mod.fingerprint_semantic_identity(
                        different_identity
                    ),
                    "summary": proposal["summary"],
                },
            ),
            patch.object(mod, "read_rejected_record", return_value=None),
            self.assertRaises(mod.DecisionReplayError) as ctx,
        ):
            mod.decide_accept_proposal(conn, proposal, target_source="canonical")

        self.assertEqual(ctx.exception.code, "PAYLOAD_MISMATCH")


if __name__ == "__main__":
    unittest.main()
