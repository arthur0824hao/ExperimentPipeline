from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


MODULE_PATH = Path(__file__).with_name("tickets.py")


def load_module():
    spec = importlib.util.spec_from_file_location(
        "skill_system_workflow_tickets", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def ticket_row(*, status: str = "open", metadata: dict | None = None) -> tuple:
    return (
        1,
        "TKT-001",
        "Establish ticket workflow kernel and review handoff",
        "Create the first durable ticket-based top-level workflow.",
        status,
        "2026-03-13T08:00:00+00:00",
        None,
        None,
        metadata
        or {
            "kind": "workflow_ticket",
            "source": "note_tasks",
            "batch_id": None,
            "queue_order": None,
            "claimed_at": None,
            "claimed_by_session": None,
            "task_provenance": "workflow",
            "task_surface_visibility": "default",
        },
    )


class WorkflowTicketTests(unittest.TestCase):
    def test_resolve_note_tasks_path_single_path(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            note_path.write_text("# Prompt\n", encoding="utf-8")

            resolved = mod.resolve_note_tasks_path([note_path])

        self.assertEqual(resolved["canonical_path"], note_path)
        self.assertEqual(resolved["path_resolution_status"], "RESOLVED_SINGLE_PATH")

    def test_resolve_note_tasks_path_conflict(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            phase3_path = Path(tmp) / "phase3_note_tasks.md"
            note_path.write_text("# Prompt\n", encoding="utf-8")
            phase3_path.write_text("# Prompt\n", encoding="utf-8")

            with self.assertRaises(mod.TicketError) as ctx:
                mod.resolve_note_tasks_path([note_path, phase3_path])

        self.assertEqual(ctx.exception.code, "PATH_ALIAS_CONFLICT")

    def test_resolve_review_inbox_path_single_observed_source(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            note_path.write_text(
                "# Prompt\n\n## Review Agent Inbox\n\n# TICKET_BATCH\n"
                "batch_id: BATCH-007\nmode: review-inbox-mainline-integration\n"
                "status: READY\n",
                encoding="utf-8",
            )

            resolved = mod.resolve_review_inbox_path([note_path])

        self.assertEqual(resolved["canonical_path"], note_path)
        self.assertEqual(
            resolved["path_resolution_status"], "RESOLVED_SINGLE_INBOX_PATH"
        )

    def test_resolve_review_inbox_path_missing_fails_closed(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            note_path.write_text("# Prompt\n", encoding="utf-8")

            with self.assertRaises(mod.TicketError) as ctx:
                mod.resolve_review_inbox_path([note_path])

        self.assertEqual(ctx.exception.code, "REVIEW_INBOX_NOT_FOUND")

    def test_resolve_review_inbox_path_conflict_fails_closed(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            phase3_path = Path(tmp) / "phase3_note_tasks.md"
            content = (
                "# Prompt\n\n## Review Agent Inbox\n\n# TICKET_BATCH\n"
                "batch_id: BATCH-007\nmode: review-inbox-mainline-integration\n"
                "status: READY\n"
            )
            note_path.write_text(content, encoding="utf-8")
            phase3_path.write_text(content, encoding="utf-8")

            with self.assertRaises(mod.TicketError) as ctx:
                mod.resolve_review_inbox_path([note_path, phase3_path])

        self.assertEqual(ctx.exception.code, "REVIEW_INBOX_PATH_CONFLICT")

    def test_extract_review_agent_inbox_section_returns_current_shape(self):
        mod = load_module()
        text = (
            "# Prompt\n\n## User\n\n### New\n\n### Request\n\n"
            "## Review Agent Inbox\n\n# TICKET_BATCH\n"
            "batch_id: BATCH-007\nmode: review-inbox-mainline-integration\n"
            "status: READY\n\n## TICKETS\n\n### TKT-023\n"
            "title: Canonical review inbox path resolution and parsing\n"
            "type: WORKER\nstatus: OPEN\nobjective:\n- resolve it\n"
        )

        section = mod.extract_review_agent_inbox_section(text)

        self.assertTrue(section.startswith("# TICKET_BATCH"))
        self.assertIn("### TKT-023", section)

    def test_parse_review_agent_inbox_supports_embedded_batch_shape(self):
        mod = load_module()
        text = (
            "# Prompt\n\n## Review Agent Inbox\n\n# TICKET_BATCH\n"
            "batch_id: BATCH-007\n"
            "mode: review-inbox-mainline-integration\n"
            "status: READY\n\n"
            "## TICKETS\n\n"
            "### TKT-023\n"
            "title: Canonical review inbox path resolution and parsing\n"
            "type: WORKER\n"
            "status: OPEN\n"
            "objective:\n"
            "- resolve the canonical review inbox path\n"
        )

        parsed = mod.parse_review_agent_inbox(text)

        self.assertEqual(parsed["parser_shape"], "embedded_ticket_batch_in_note_tasks")
        self.assertEqual(parsed["batch"]["batch_id"], "BATCH-007")
        self.assertEqual(parsed["batch"]["tickets"][0]["ticket_id"], "TKT-023")

    def test_parse_review_agent_inbox_supports_request_bundle_shape(self):
        mod = load_module()
        text = (
            "# Prompt\n\n## Review Agent Inbox\n\nREQUEST_BUNDLE\n\n"
            "Round Charter (normalized)\n"
            "- round_goal: Keep the single entry.\n\n"
            "Requested ticket plan\n"
            "- requested_ticket_count: 2\n"
            "- required tickets:\n"
            "  1. mainline worker — typed intake contract\n"
            "  2. integrator — closeout\n\n"
            "Implementation requirements\n"
            "- none\n\n"
            "## System Managed Ticket Projection\n\n"
            "# TICKET_BATCH\n"
            "batch_id: BATCH-010\n"
            "mode: typed-intake\n"
            "status: READY\n"
        )

        parsed = mod.parse_review_agent_inbox(text)

        self.assertEqual(parsed["parser_shape"], "request_bundle_round_charter")
        self.assertEqual(parsed["charter"]["requested_ticket_count"], 2)
        self.assertEqual(len(parsed["charter"]["required_tickets"]), 2)

    def test_extract_system_managed_projection_section_returns_projection_block(self):
        mod = load_module()
        text = (
            "# Prompt\n\n## Review Agent Inbox\n\nREQUEST_BUNDLE\n\n"
            "## System Managed Ticket Projection\n\n"
            "# TICKET_BATCH\n"
            "batch_id: BATCH-010\n"
            "mode: typed-intake\n"
            "status: READY\n"
        )

        section = mod.extract_system_managed_ticket_projection_section(text)

        self.assertTrue(section.startswith("# TICKET_BATCH"))

    def test_extract_new_section_returns_current_shape(self):
        mod = load_module()
        text = (
            "# Prompt\n\n## User\n\n### New \n\n- follow-up task\n\n### Request\n\n"
            "### Asking Question(require 口頭回答 用戶確認前不要動手)\n\n<none>\n"
        )

        section = mod.extract_new_section(text)

        self.assertEqual(section, "- follow-up task")

    def test_parse_new_section_items_supports_simple_tasks(self):
        mod = load_module()
        text = (
            "# Prompt\n\n## User\n\n### New\n\n- follow-up task\n- second task\n\n"
            "### Request\n\n"
        )

        items = mod.parse_new_section_items(
            text, batch_id="BATCH-004", starting_queue_order=4
        )

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["item_type"], "simple_task")
        self.assertEqual(items[0]["queue_order"], 4)
        self.assertEqual(items[1]["queue_order"], 5)
        self.assertTrue(items[0]["ticket_id"].startswith("TKT-NEW-"))
        self.assertEqual(items[0]["resolution"], "materialize_ticket")

    def test_parse_new_section_items_supports_typed_resolution_labels(self):
        mod = load_module()
        text = (
            "# Prompt\n\n## User\n\n### New\n\n"
            "- [defer] wait until next round\n"
            "- [map_to_review_followup] ask reviewer for evidence\n"
            "- [absorb_into_current_work] fold into current ticket\n\n"
            "### Request\n\n"
        )

        items = mod.parse_new_section_items(
            text, batch_id="BATCH-004", starting_queue_order=4
        )

        self.assertEqual(
            [item["resolution"] for item in items],
            ["defer", "map_to_review_followup", "absorb_into_current_work"],
        )
        self.assertEqual(items[0]["summary"], "wait until next round")

    def test_parse_new_section_items_supports_explicit_ticket_blocks(self):
        mod = load_module()
        text = (
            "# Prompt\n\n## User\n\n### New\n\n"
            "### TKT-099\n"
            "title: Added from new\n"
            "type: WORKER\n"
            "status: OPEN\n"
            "objective:\n"
            "- do thing\n\n"
            "### Request\n\n"
        )

        items = mod.parse_new_section_items(
            text, batch_id="BATCH-004", starting_queue_order=7
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["ticket_id"], "TKT-099")
        self.assertEqual(items[0]["item_type"], "explicit_ticket")
        self.assertEqual(items[0]["queue_order"], 7)

    def test_refresh_new_tasks_reports_ingested_items(self):
        mod = load_module()
        conn = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            note_path.write_text(
                "# TICKET_BATCH\n"
                "batch_id: BATCH-004\n"
                "mode: new-section-refresh-law\n"
                "status: READY\n\n"
                "## TICKETS\n\n"
                "### TKT-013\n"
                "title: Refresh law\n"
                "type: WORKER\n"
                "status: OPEN\n"
                "objective:\n"
                "- refresh\n\n"
                "### New\n\n"
                "- newly added worker task\n\n"
                "### Request\n\n",
                encoding="utf-8",
            )
            with (
                patch.object(
                    mod,
                    "ensure_note_tasks_tickets",
                    return_value={
                        "batch_id": "BATCH-004",
                        "mode": "new-section-refresh-law",
                        "status": "READY",
                        "tickets": [{"ticket_id": "TKT-013"}],
                    },
                ),
                patch.object(mod, "fetch_ticket", return_value=None),
                patch.object(
                    mod,
                    "list_tickets",
                    return_value=[{"ticket_id": "TKT-013", "queue_order": 1}],
                ),
                patch.object(
                    mod,
                    "intake_ticket",
                    return_value={
                        "ticket_id": "TKT-NEW-AAAA1111",
                        "ticket_type": "WORKER",
                        "workflow_state": "OPEN",
                    },
                ) as intake,
                patch.object(
                    mod,
                    "summarize_claim_ownership",
                    return_value={
                        "claimable_worker_tickets": [{"ticket_id": "TKT-NEW-AAAA1111"}]
                    },
                ),
                patch.object(mod, "persist_refresh_result") as persist,
            ):
                refresh = mod.refresh_new_tasks(
                    conn,
                    trigger_point="manual",
                    batch_id="BATCH-004",
                    candidate_paths=[note_path],
                )

        intake.assert_called_once()
        persist.assert_called_once()
        self.assertEqual(refresh["path_resolution_status"], "RESOLVED_SINGLE_PATH")
        self.assertEqual(refresh["refresh_trigger_point"], "manual")
        self.assertEqual(len(refresh["new_items_detected"]), 1)
        self.assertEqual(refresh["new_tickets_ingested"], ["TKT-NEW-AAAA1111"])
        self.assertTrue(refresh["continue_due_to_new_policy"])
        self.assertEqual(
            refresh["new_items_detected"][0]["resolution"], "materialize_ticket"
        )

    def test_refresh_new_tasks_does_not_materialize_deferred_items(self):
        mod = load_module()
        conn = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            note_path.write_text(
                "# TICKET_BATCH\n"
                "batch_id: BATCH-004\n"
                "mode: new-section-refresh-law\n"
                "status: READY\n\n"
                "## TICKETS\n\n"
                "### TKT-013\n"
                "title: Refresh law\n"
                "type: WORKER\n"
                "status: OPEN\n"
                "objective:\n"
                "- refresh\n\n"
                "### New\n\n"
                "- [defer] wait for next round\n\n"
                "### Request\n\n",
                encoding="utf-8",
            )
            with (
                patch.object(
                    mod,
                    "ensure_note_tasks_tickets",
                    return_value={
                        "batch_id": "BATCH-004",
                        "mode": "new-section-refresh-law",
                        "status": "READY",
                        "tickets": [{"ticket_id": "TKT-013"}],
                    },
                ),
                patch.object(
                    mod,
                    "list_tickets",
                    return_value=[{"ticket_id": "TKT-013", "queue_order": 1}],
                ),
                patch.object(mod, "intake_ticket") as intake,
                patch.object(
                    mod,
                    "summarize_claim_ownership",
                    return_value={"claimable_worker_tickets": []},
                ),
                patch.object(mod, "persist_refresh_result"),
            ):
                refresh = mod.refresh_new_tasks(
                    conn,
                    trigger_point="manual",
                    batch_id="BATCH-004",
                    candidate_paths=[note_path],
                )

        intake.assert_not_called()
        self.assertEqual(refresh["new_tickets_ingested"], [])
        self.assertFalse(refresh["continue_due_to_new_policy"])
        self.assertEqual(refresh["new_items_detected"][0]["resolution"], "defer")

    def test_refresh_new_tasks_inherits_verification_provenance(self):
        mod = load_module()
        conn = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            note_path.write_text(
                "# TICKET_BATCH\n"
                "batch_id: BATCH-VERIFY-123456\n"
                "mode: verification\n"
                "status: READY\n\n"
                "## TICKETS\n\n"
                "### TKT-019-VERIFY\n"
                "title: Verification fixture\n"
                "type: WORKER\n"
                "status: OPEN\n"
                "objective:\n"
                "- fixture\n\n"
                "### New\n\n"
                "- verifier follow-up\n\n"
                "### Request\n\n",
                encoding="utf-8",
            )
            with (
                patch.object(
                    mod,
                    "ensure_note_tasks_tickets",
                    return_value={
                        "batch_id": "BATCH-VERIFY-123456",
                        "mode": "verification",
                        "status": "READY",
                        "tickets": [{"ticket_id": "TKT-019-VERIFY"}],
                    },
                ),
                patch.object(mod, "fetch_ticket", return_value=None),
                patch.object(
                    mod,
                    "list_tickets",
                    side_effect=[
                        [
                            {
                                "ticket_id": "TKT-019-VERIFY",
                                "queue_order": 1,
                                "task_provenance": "verification",
                            }
                        ],
                        [
                            {
                                "ticket_id": "TKT-019-VERIFY",
                                "queue_order": 1,
                                "task_provenance": "verification",
                            }
                        ],
                    ],
                ),
                patch.object(
                    mod,
                    "intake_ticket",
                    return_value={
                        "ticket_id": "TKT-NEW-AAAA1111",
                        "ticket_type": "WORKER",
                        "workflow_state": "OPEN",
                    },
                ) as intake,
                patch.object(
                    mod,
                    "summarize_claim_ownership",
                    return_value={
                        "claimable_worker_tickets": [{"ticket_id": "TKT-NEW-AAAA1111"}]
                    },
                ),
                patch.object(mod, "persist_refresh_result"),
            ):
                mod.refresh_new_tasks(
                    conn,
                    trigger_point="manual",
                    batch_id="BATCH-VERIFY-123456",
                    candidate_paths=[note_path],
                )

        self.assertEqual(intake.call_args.kwargs["task_provenance"], "verification")
        self.assertEqual(
            intake.call_args.kwargs["task_surface_visibility"], "hidden_by_default"
        )

    def test_refresh_review_inbox_reports_ingested_and_existing_items(self):
        mod = load_module()
        conn = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            note_path.write_text(
                "# Prompt\n\n"
                "## Review Agent Inbox\n\n"
                "# TICKET_BATCH\n"
                "batch_id: BATCH-007\n"
                "mode: review-inbox-mainline-integration\n"
                "status: READY\n\n"
                "## TICKETS\n\n"
                "### TKT-023\n"
                "title: First inbox item\n"
                "type: WORKER\n"
                "status: OPEN\n"
                "objective:\n"
                "- first\n\n"
                "### TKT-024\n"
                "title: Second inbox item\n"
                "type: WORKER\n"
                "status: OPEN\n"
                "objective:\n"
                "- second\n",
                encoding="utf-8",
            )
            with (
                patch.object(
                    mod,
                    "fetch_ticket",
                    side_effect=[
                        {"ticket_id": "TKT-023", "source": "note_tasks_batch"},
                        None,
                    ],
                ),
                patch.object(
                    mod,
                    "intake_ticket",
                    return_value={
                        "ticket_id": "TKT-024",
                        "ticket_type": "WORKER",
                        "workflow_state": "OPEN",
                    },
                ) as intake,
                patch.object(mod, "persist_review_inbox_refresh_result") as persist,
            ):
                refresh = mod.refresh_review_inbox(
                    conn,
                    trigger_point="manual",
                    batch_id="BATCH-007",
                    candidate_paths=[note_path],
                )

        intake.assert_called_once()
        persist.assert_called_once()
        self.assertEqual(
            refresh["path_resolution_status"], "RESOLVED_SINGLE_INBOX_PATH"
        )
        self.assertEqual(refresh["parser_shape"], "embedded_ticket_batch_in_note_tasks")
        self.assertEqual(refresh["new_tickets_ingested"], ["TKT-024"])
        self.assertEqual(
            refresh["consumed_or_pending_inbox_items"],
            [
                {
                    "inbox_item_id": "TKT-023",
                    "ticket_id": "TKT-023",
                    "mapped_ticket_id": "TKT-023",
                    "state": "already_present",
                },
                {
                    "inbox_item_id": "TKT-024",
                    "ticket_id": "TKT-024",
                    "mapped_ticket_id": "TKT-024",
                    "state": "ingested",
                },
            ],
        )

    def test_refresh_review_inbox_maps_request_bundle_to_projection(self):
        mod = load_module()
        conn = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            note_path.write_text(
                "# Prompt\n\n"
                "## Review Agent Inbox\n\n"
                "REQUEST_BUNDLE\n\n"
                "Round Charter (normalized)\n"
                "- round_goal: Keep the single entry.\n\n"
                "Requested ticket plan\n"
                "- requested_ticket_count: 2\n"
                "- required tickets:\n"
                "  1. mainline worker — typed intake contract\n"
                "  2. integrator — closeout\n\n"
                "Implementation requirements\n"
                "- none\n\n"
                "## System Managed Ticket Projection\n\n"
                "# TICKET_BATCH\n"
                "batch_id: BATCH-010\n"
                "mode: typed-intake\n"
                "status: READY\n\n"
                "## TICKETS\n\n"
                "### TKT-037\n"
                "title: Typed intake\n"
                "type: WORKER\n"
                "status: OPEN\n"
                "objective:\n"
                "- contract\n\n"
                "### TKT-041\n"
                "title: Closeout\n"
                "type: INTEGRATOR\n"
                "status: OPEN\n"
                "objective:\n"
                "- close\n",
                encoding="utf-8",
            )
            with patch.object(mod, "persist_review_inbox_refresh_result"):
                refresh = mod.refresh_review_inbox(
                    conn,
                    trigger_point="manual",
                    batch_id="BATCH-010",
                    candidate_paths=[note_path],
                )

        self.assertEqual(refresh["parser_shape"], "request_bundle_round_charter")
        self.assertEqual(refresh["new_tickets_ingested"], [])
        self.assertEqual(refresh["projection_batch_id"], "BATCH-010")
        self.assertEqual(
            refresh["consumed_or_pending_inbox_items"][0]["mapped_ticket_id"],
            "TKT-037",
        )

    def test_refresh_review_inbox_keeps_existing_source_when_ticket_exists(self):
        mod = load_module()
        conn = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            note_path = Path(tmp) / "note_tasks.md"
            note_path.write_text(
                "# Prompt\n\n"
                "## Review Agent Inbox\n\n"
                "# TICKET_BATCH\n"
                "batch_id: BATCH-007\n"
                "mode: review-inbox-mainline-integration\n"
                "status: READY\n\n"
                "## TICKETS\n\n"
                "### TKT-023\n"
                "title: First inbox item\n"
                "type: WORKER\n"
                "status: OPEN\n"
                "objective:\n"
                "- first\n",
                encoding="utf-8",
            )
            with (
                patch.object(
                    mod,
                    "fetch_ticket",
                    return_value={
                        "ticket_id": "TKT-023",
                        "source": "note_tasks_batch",
                    },
                ),
                patch.object(mod, "intake_ticket") as intake,
                patch.object(mod, "persist_review_inbox_refresh_result"),
            ):
                refresh = mod.refresh_review_inbox(
                    conn,
                    trigger_point="manual",
                    batch_id="BATCH-007",
                    candidate_paths=[note_path],
                )

        intake.assert_not_called()
        self.assertEqual(refresh["new_tickets_ingested"], [])

    def test_parse_note_tasks_ticket(self):
        mod = load_module()
        text = """# Prompt\n\n## Review Agent\n\n# TICKET\nticket_id: TKT-001\ntitle: Establish ticket workflow kernel and review handoff\nowner: OpenCode\nmode: execution\nstatus: READY\n\n## OBJECTIVE\nCreate the first durable ticket-based top-level workflow for skill-system.\n"""

        parsed = mod.parse_note_tasks_ticket(text)

        self.assertEqual(parsed["ticket_id"], "TKT-001")
        self.assertEqual(
            parsed["title"], "Establish ticket workflow kernel and review handoff"
        )
        self.assertIn("durable ticket-based", parsed["summary"])
        self.assertEqual(parsed["source"], "note_tasks")

    def test_parse_note_tasks_batch(self):
        mod = load_module()
        text = """# TICKET_BATCH
batch_id: BATCH-001
mode: multi-session-ticket-queue
status: READY

## TICKETS

### TKT-002
title: Multi-ticket intake and queue model
status: OPEN
objective:
- Extend the current workflow ticket kernel from single-ticket intake to batch-ticket intake.
- Keep the intake syntax human-editable and minimal.

### TKT-003
title: Multi-session-safe claiming semantics
status: OPEN
objective:
- Make ticket claiming safe when multiple OpenCode sessions may run concurrently.
"""

        batch = mod.parse_note_tasks_batch(text)

        self.assertEqual(batch["batch_id"], "BATCH-001")
        self.assertEqual(batch["mode"], "multi-session-ticket-queue")
        self.assertEqual(
            [item["ticket_id"] for item in batch["tickets"]], ["TKT-002", "TKT-003"]
        )
        self.assertEqual(batch["tickets"][0]["queue_order"], 1)
        self.assertEqual(batch["tickets"][1]["queue_order"], 2)

    def test_intake_ticket_returns_existing_when_present(self):
        mod = load_module()
        conn = MagicMock()
        with patch.object(
            mod, "fetch_ticket", return_value={"ticket_id": "TKT-001", "status": "OPEN"}
        ):
            ticket = mod.intake_ticket(
                conn,
                ticket_id="TKT-001",
                title="Title",
                summary="Summary",
                source="note_tasks",
            )
        self.assertEqual(ticket["ticket_id"], "TKT-001")

    def test_intake_ticket_updates_existing_batch_metadata(self):
        mod = load_module()
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ticket_row(
            metadata={
                "kind": "workflow_ticket",
                "source": "note_tasks_batch",
                "batch_id": "BATCH-001",
                "queue_order": 2,
                "claimed_at": None,
                "claimed_by_session": None,
            }
        )
        with patch.object(
            mod,
            "fetch_ticket",
            return_value={"ticket_id": "TKT-002", "status": "OPEN"},
        ):
            ticket = mod.intake_ticket(
                conn,
                ticket_id="TKT-002",
                title="Title",
                summary="Summary",
                source="note_tasks_batch",
                batch_id="BATCH-001",
                queue_order=2,
            )
        self.assertEqual(ticket["queue_order"], 2)

    def test_intake_ticket_persists_default_workflow_provenance(self):
        mod = load_module()
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ticket_row(
            metadata={
                "kind": "workflow_ticket",
                "source": "note_tasks_batch",
                "batch_id": "BATCH-006",
                "queue_order": 1,
                "claimed_at": None,
                "claimed_by_session": None,
                "task_provenance": "workflow",
                "task_surface_visibility": "default",
            }
        )
        with patch.object(mod, "fetch_ticket", return_value=None):
            mod.intake_ticket(
                conn,
                ticket_id="TKT-019",
                title="Task provenance model",
                summary="Make the default task surface trustworthy.",
                source="note_tasks_batch",
                batch_id="BATCH-006",
                queue_order=1,
            )

        inserted_metadata = json.loads(cur.execute.call_args[0][1][3])
        self.assertEqual(inserted_metadata["task_provenance"], "workflow")
        self.assertEqual(inserted_metadata["task_surface_visibility"], "default")

    def test_intake_ticket_accepts_explicit_verification_provenance(self):
        mod = load_module()
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ticket_row(
            metadata={
                "kind": "workflow_ticket",
                "source": "verify_batch_ticket_queue",
                "batch_id": "BATCH-VERIFY-123456",
                "queue_order": 1,
                "claimed_at": None,
                "claimed_by_session": None,
                "task_provenance": "verification",
                "task_surface_visibility": "hidden_by_default",
            }
        )
        with patch.object(mod, "fetch_ticket", return_value=None):
            mod.intake_ticket(
                conn,
                ticket_id="TKT-019-VERIFY",
                title="Verification fixture",
                summary="Used by batch verification only.",
                source="verify_batch_ticket_queue",
                batch_id="BATCH-VERIFY-123456",
                queue_order=1,
                task_provenance="verification",
                task_surface_visibility="hidden_by_default",
            )

        inserted_metadata = json.loads(cur.execute.call_args[0][1][3])
        self.assertEqual(inserted_metadata["task_provenance"], "verification")
        self.assertEqual(
            inserted_metadata["task_surface_visibility"], "hidden_by_default"
        )

    def test_claim_ticket_fails_when_session_already_has_claimed_ticket(self):
        mod = load_module()
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.side_effect = [("TKT-999",)]

        with patch.object(mod, "advisory_lock"):
            with self.assertRaises(mod.TicketError) as ctx:
                mod.claim_ticket(conn, ticket_id="TKT-001", session_id="ses-123")

        self.assertEqual(ctx.exception.code, "SESSION_ALREADY_HAS_CLAIMED_TICKET")

    def test_claim_ticket_fails_when_claimed_by_other_session(self):
        mod = load_module()
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.side_effect = [
            None,
            ticket_row(
                status="in_progress",
                metadata={
                    "kind": "workflow_ticket",
                    "source": "note_tasks_batch",
                    "batch_id": "BATCH-001",
                    "queue_order": 1,
                    "claimed_at": "2026-03-13T08:12:00+00:00",
                    "claimed_by_session": "other-session",
                },
            ),
        ]

        with patch.object(mod, "advisory_lock"):
            with self.assertRaises(mod.TicketError) as ctx:
                mod.claim_ticket(conn, ticket_id="TKT-002", session_id="ses-123")

        self.assertEqual(ctx.exception.code, "TICKET_CLAIMED_BY_OTHER_SESSION")

    def test_claim_ticket_updates_metadata(self):
        mod = load_module()
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.side_effect = [
            None,
            ticket_row(
                status="open",
                metadata={
                    "kind": "workflow_ticket",
                    "source": "note_tasks_batch",
                    "batch_id": "BATCH-001",
                    "queue_order": 1,
                    "claimed_at": None,
                    "claimed_by_session": None,
                },
            ),
            ticket_row(
                status="in_progress",
                metadata={
                    "kind": "workflow_ticket",
                    "source": "note_tasks",
                    "batch_id": "BATCH-001",
                    "queue_order": 1,
                    "claimed_at": "2026-03-13T08:12:00+00:00",
                    "claimed_by_session": "ses-123",
                },
            ),
        ]

        with patch.object(mod, "advisory_lock"):
            ticket = mod.claim_ticket(conn, ticket_id="TKT-001", session_id="ses-123")

        self.assertEqual(ticket["status"], "CLAIMED")
        self.assertEqual(ticket["claimed_by_session"], "ses-123")

    def test_close_ticket_resolves_current_claimed_ticket(self):
        mod = load_module()
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [("TKT-001",)]
        cur.fetchone.side_effect = [
            ticket_row(
                status="in_progress",
                metadata={
                    "kind": "workflow_ticket",
                    "source": "note_tasks",
                    "claimed_at": "2026-03-13T08:12:00+00:00",
                    "claimed_by_session": "ses-123",
                },
            ),
            ticket_row(
                status="closed",
                metadata={
                    "kind": "workflow_ticket",
                    "source": "note_tasks",
                    "claimed_at": "2026-03-13T08:12:00+00:00",
                    "claimed_by_session": "ses-123",
                },
            ),
        ]

        with patch.object(mod, "advisory_lock"):
            ticket = mod.close_ticket(conn, session_id="ses-123", resolution="done")

        self.assertEqual(ticket["status"], "CLOSED")

    def test_close_ticket_fails_for_non_owner(self):
        mod = load_module()
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ticket_row(
            status="in_progress",
            metadata={
                "kind": "workflow_ticket",
                "source": "note_tasks_batch",
                "claimed_at": "2026-03-13T08:12:00+00:00",
                "claimed_by_session": "other-session",
            },
        )

        with patch.object(mod, "advisory_lock"):
            with self.assertRaises(mod.TicketError) as ctx:
                mod.close_ticket(conn, session_id="ses-123", ticket_id="TKT-001")

        self.assertEqual(ctx.exception.code, "TICKET_NOT_OWNED_BY_SESSION")

    def test_block_ticket_fails_for_non_owner(self):
        mod = load_module()
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ticket_row(
            status="closed",
            metadata={
                "kind": "workflow_ticket",
                "source": "note_tasks_batch",
                "claimed_at": "2026-03-13T08:12:00+00:00",
                "claimed_by_session": "other-session",
            },
        )

        with patch.object(mod, "advisory_lock"):
            with self.assertRaises(mod.TicketError) as ctx:
                mod.block_ticket(
                    conn, ticket_id="TKT-001", session_id="ses-123", reason="blocked"
                )

        self.assertEqual(ctx.exception.code, "TICKET_NOT_OWNED_BY_SESSION")

    def test_check_open_tickets_reports_active_ticket(self):
        mod = load_module()
        conn = MagicMock()
        with patch.object(
            mod,
            "list_tickets",
            return_value=[
                {
                    "ticket_id": "TKT-001",
                    "status": "CLAIMED",
                    "claimed_by_session": "ses-123",
                },
                {
                    "ticket_id": "TKT-002",
                    "status": "OPEN",
                    "claimed_by_session": None,
                },
            ],
        ):
            report = mod.check_open_tickets(conn, session_id="ses-123")

        self.assertEqual(report["open_ticket_count"], 2)
        self.assertEqual(report["active_ticket"]["ticket_id"], "TKT-001")

    def test_close_ticket_with_refresh_blocks_integrator_when_new_items_detected(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "fetch_ticket",
                return_value={
                    "ticket_id": "TKT-015",
                    "ticket_type": "INTEGRATOR",
                    "batch_id": "BATCH-004",
                },
            ),
            patch.object(
                mod,
                "build_integrator_closure_report_with_refresh",
                side_effect=mod.TicketError(
                    "CONTINUE_DUE_TO_NEW_TASKS",
                    "blocked by fresh note_tasks refresh",
                ),
            ),
        ):
            with self.assertRaises(mod.TicketError) as ctx:
                mod.close_ticket_with_refresh(
                    conn, session_id="ses-123", ticket_id="TKT-015", resolution="done"
                )

        self.assertEqual(ctx.exception.code, "CONTINUE_DUE_TO_NEW_TASKS")

    def test_close_ticket_with_refresh_blocks_integrator_when_workers_remain(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "fetch_ticket",
                return_value={
                    "ticket_id": "TKT-015",
                    "ticket_type": "INTEGRATOR",
                    "batch_id": "BATCH-004",
                },
            ),
            patch.object(
                mod,
                "build_integrator_closure_report_with_refresh",
                return_value=(
                    {
                        "can_close_batch": False,
                        "final_batch_closure_status": "NOT_READY_UNRESOLVED_WORKERS",
                    },
                    {"latest_new_check_at": "2026-03-14T00:00:00+00:00"},
                ),
            ),
        ):
            with self.assertRaises(mod.TicketError) as ctx:
                mod.close_ticket_with_refresh(
                    conn, session_id="ses-123", ticket_id="TKT-015", resolution="done"
                )

        self.assertEqual(ctx.exception.code, "INTEGRATOR_CLOSE_NOT_READY")

    def test_close_ticket_with_refresh_persists_dual_refresh_snapshots_for_integrator(
        self,
    ):
        mod = load_module()
        conn = MagicMock()
        refresh_status = {
            "continue_due_to_new_policy": False,
            "latest_new_check_at": "2026-03-14T00:00:00+00:00",
        }
        review_inbox_refresh = {
            "latest_review_inbox_check_at": "2026-03-14T00:01:00+00:00",
            "path_resolution_status": "RESOLVED_SINGLE_INBOX_PATH",
        }
        with (
            patch.object(
                mod,
                "fetch_ticket",
                return_value={
                    "ticket_id": "TKT-015",
                    "ticket_type": "INTEGRATOR",
                    "batch_id": "BATCH-004",
                },
            ),
            patch.object(
                mod,
                "build_integrator_closure_report_with_refresh",
                return_value=(
                    {
                        "can_close_batch": True,
                        "latest_review_inbox_refresh": review_inbox_refresh,
                    },
                    refresh_status,
                ),
            ),
            patch.object(
                mod,
                "close_ticket",
                return_value={"ticket_id": "TKT-015", "status": "CLOSED"},
            ),
            patch.object(mod, "persist_ticket_refresh_snapshot") as persist_snapshot,
        ):
            closed, latest_refresh = mod.close_ticket_with_refresh(
                conn, session_id="ses-123", ticket_id="TKT-015", resolution="done"
            )

        self.assertEqual(closed["latest_refresh"], refresh_status)
        self.assertEqual(closed["latest_review_inbox_refresh"], review_inbox_refresh)
        self.assertEqual(latest_refresh, refresh_status)
        self.assertEqual(persist_snapshot.call_count, 3)

    def test_build_integrator_closure_report_with_refresh_includes_refresh_status(self):
        mod = load_module()
        conn = MagicMock()
        refresh_status = {
            "continue_due_to_new_policy": False,
            "latest_new_check_at": "2026-03-14T00:00:00+00:00",
        }
        review_inbox_refresh = {
            "latest_review_inbox_check_at": "2026-03-14T00:01:00+00:00",
            "new_inbox_items_detected": [],
            "consumed_or_pending_inbox_items": [],
        }
        with (
            patch.object(
                mod,
                "fetch_ticket",
                return_value={
                    "ticket_id": "TKT-015",
                    "ticket_type": "INTEGRATOR",
                    "batch_id": "BATCH-004",
                },
            ),
            patch.object(mod, "refresh_new_tasks", return_value=refresh_status),
            patch.object(
                mod, "refresh_review_inbox", return_value=review_inbox_refresh
            ),
            patch.object(
                mod,
                "summarize_claim_ownership",
                return_value={"claimable_worker_tickets": []},
            ),
            patch.object(
                mod,
                "build_integrator_closure_report",
                return_value={"ticket_id": "TKT-015", "closure_legality": "LEGAL"},
            ),
        ):
            report, latest_refresh = mod.build_integrator_closure_report_with_refresh(
                conn, session_id="ses-123", ticket_id="TKT-015"
            )

        self.assertEqual(report["latest_refresh"], refresh_status)
        self.assertEqual(report["latest_review_inbox_refresh"], review_inbox_refresh)
        self.assertEqual(latest_refresh, refresh_status)

    def test_build_integrator_closure_report_with_refresh_blocks_on_review_inbox_items(
        self,
    ):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "fetch_ticket",
                return_value={
                    "ticket_id": "TKT-027",
                    "ticket_type": "INTEGRATOR",
                    "batch_id": "BATCH-007",
                },
            ),
            patch.object(
                mod,
                "refresh_new_tasks",
                return_value={"continue_due_to_new_policy": False},
            ),
            patch.object(
                mod,
                "refresh_review_inbox",
                return_value={
                    "latest_review_inbox_check_at": "2026-03-14T10:41:00+00:00",
                    "new_inbox_items_detected": [
                        {"inbox_item_id": "TKT-025", "ticket_type": "WORKER"}
                    ],
                    "consumed_or_pending_inbox_items": [
                        {
                            "inbox_item_id": "TKT-025",
                            "ticket_id": "TKT-025",
                            "state": "already_present",
                        }
                    ],
                },
            ),
            patch.object(
                mod,
                "summarize_claim_ownership",
                return_value={
                    "claimable_worker_tickets": [
                        {"ticket_id": "TKT-025", "ticket_type": "WORKER"}
                    ]
                },
            ),
        ):
            with self.assertRaises(mod.TicketError) as ctx:
                mod.build_integrator_closure_report_with_refresh(
                    conn, session_id="ses-123", ticket_id="TKT-027"
                )

        self.assertEqual(ctx.exception.code, "CONTINUE_DUE_TO_REVIEW_INBOX_ITEMS")

    def test_build_session_loop_state_resumes_claimed_ticket(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "refresh_new_tasks",
                return_value={
                    "latest_new_check_at": "2026-03-14T00:00:00+00:00",
                    "new_items_detected": [],
                },
            ),
            patch.object(
                mod,
                "summarize_claim_ownership",
                return_value={
                    "batch_id": "BATCH-005",
                    "claimed_by_this_session": [
                        {"ticket_id": "TKT-016", "status": "CLAIMED"}
                    ],
                    "claimable_worker_tickets": [
                        {"ticket_id": "TKT-017", "status": "OPEN"}
                    ],
                    "claimed_by_other_sessions": [],
                    "integrator_ticket_status": "OPEN",
                    "scope_ticket_id": "TKT-016",
                },
            ),
            patch.object(
                mod, "session_ticket_sequences", return_value=(["TKT-016"], [])
            ),
            patch.object(mod, "claim_ticket") as claim,
        ):
            state = mod.build_session_loop_state(
                conn, session_id="ses-123", batch_id="BATCH-005"
            )

        claim.assert_not_called()
        self.assertEqual(state["stop_reason"], "resume_claimed_ticket")
        self.assertEqual(state["current_ticket"]["ticket_id"], "TKT-016")
        self.assertEqual(state["scope_ticket_id"], "TKT-016")

    def test_build_session_loop_state_claims_next_worker(self):
        mod = load_module()
        conn = MagicMock()
        summaries = [
            {
                "batch_id": "BATCH-005",
                "claimed_by_this_session": [],
                "claimable_worker_tickets": [
                    {"ticket_id": "TKT-016", "status": "OPEN"}
                ],
                "next_claimable_worker_ticket": {
                    "ticket_id": "TKT-016",
                    "status": "OPEN",
                },
                "claimed_by_other_sessions": [],
                "integrator_ticket_status": "OPEN",
                "scope_ticket_id": None,
            },
            {
                "batch_id": "BATCH-005",
                "claimed_by_this_session": [
                    {"ticket_id": "TKT-016", "status": "CLAIMED"}
                ],
                "claimable_worker_tickets": [
                    {"ticket_id": "TKT-017", "status": "OPEN"}
                ],
                "next_claimable_worker_ticket": {
                    "ticket_id": "TKT-017",
                    "status": "OPEN",
                },
                "claimed_by_other_sessions": [],
                "integrator_ticket_status": "OPEN",
                "scope_ticket_id": "TKT-016",
            },
        ]
        with (
            patch.object(
                mod,
                "refresh_new_tasks",
                return_value={
                    "latest_new_check_at": "2026-03-14T00:00:00+00:00",
                    "new_items_detected": [],
                },
            ),
            patch.object(mod, "summarize_claim_ownership", side_effect=summaries),
            patch.object(
                mod,
                "claim_ticket",
                return_value={"ticket_id": "TKT-016", "status": "CLAIMED"},
            ) as claim,
            patch.object(
                mod, "session_ticket_sequences", return_value=(["TKT-016"], [])
            ),
        ):
            state = mod.build_session_loop_state(
                conn, session_id="ses-123", batch_id="BATCH-005"
            )

        claim.assert_called_once()
        self.assertEqual(state["stop_reason"], "claimed_next_worker_ticket")
        self.assertEqual(state["claimed_ticket"]["ticket_id"], "TKT-016")
        self.assertEqual(state["next_action"], "work_claimed_ticket")

    def test_build_session_loop_state_reports_integrator_eligibility(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "refresh_new_tasks",
                return_value={
                    "latest_new_check_at": "2026-03-14T00:00:00+00:00",
                    "new_items_detected": [],
                },
            ),
            patch.object(
                mod,
                "summarize_claim_ownership",
                return_value={
                    "batch_id": "BATCH-005",
                    "claimed_by_this_session": [],
                    "claimable_worker_tickets": [],
                    "claimed_by_other_sessions": [],
                    "integrator_ticket_status": "OPEN",
                    "scope_ticket_id": None,
                },
            ),
            patch.object(
                mod,
                "session_ticket_sequences",
                return_value=(["TKT-016", "TKT-017"], ["TKT-016", "TKT-017"]),
            ),
        ):
            state = mod.build_session_loop_state(
                conn, session_id="ses-123", batch_id="BATCH-005"
            )

        self.assertEqual(state["stop_reason"], "ready_for_integrator_review")
        self.assertTrue(state["integrator_eligible"])
        self.assertEqual(state["open_or_stale_remaining"], 0)

    def test_summarize_claim_ownership_reports_batch_counts(self):
        mod = load_module()
        conn = MagicMock()
        with patch.object(
            mod,
            "list_tickets",
            return_value=[
                {
                    "ticket_id": "TKT-002",
                    "status": "CLAIMED",
                    "claimed_by_session": "ses-123",
                },
                {
                    "ticket_id": "TKT-003",
                    "status": "CLAIMED",
                    "claimed_by_session": "other-session",
                },
                {
                    "ticket_id": "TKT-004",
                    "status": "OPEN",
                    "claimed_by_session": None,
                },
                {
                    "ticket_id": "TKT-005",
                    "status": "BLOCKED",
                    "claimed_by_session": "other-session",
                },
                {
                    "ticket_id": "TKT-006",
                    "status": "CLOSED",
                    "claimed_by_session": None,
                },
            ],
        ):
            summary = mod.summarize_claim_ownership(
                conn, session_id="ses-123", batch_id="BATCH-001"
            )

        self.assertEqual(summary["total_tickets"], 5)
        self.assertEqual(len(summary["claimed_by_this_session"]), 1)
        self.assertEqual(len(summary["claimed_by_other_sessions"]), 2)
        self.assertEqual(len(summary["claimable_tickets"]), 1)
        self.assertEqual(summary["next_claimable_ticket"]["ticket_id"], "TKT-004")
        self.assertEqual(summary["scope_ticket_id"], "TKT-002")
        self.assertEqual(
            summary["collaboration_policy"]["question_tool_prompt_shape"],
            "2-4 options, one recommended option, single choice.",
        )
        self.assertEqual(
            summary["collaboration_policy"]["cross_ticket_loophole_policy"],
            "Subagents cannot be used as a loophole to cross ticket boundaries.",
        )
        self.assertEqual(summary["question_tool_used"], "no-branch-decision-required")
        self.assertEqual(summary["subagent_usage"], [])

    def test_build_startup_context_prefers_claimed_ticket(self):
        mod = load_module()
        conn = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "note_tasks.md"
            path.write_text(
                "# TICKET_BATCH\n"
                "batch_id: BATCH-002\n"
                "mode: batch-loop-and-integrator-semantics\n"
                "status: READY\n\n"
                "### TKT-005\n"
                "title: Startup flow integration for zero-to-review-agent workflow\n"
                "type: WORKER\n"
                "status: OPEN\n"
                "objective:\n"
                "- startup\n\n"
                "### TKT-006\n"
                "title: Question tool\n"
                "type: WORKER\n"
                "status: OPEN\n"
                "objective:\n"
                "- policy\n\n"
                "### TKT-008\n"
                "title: Integrator\n"
                "type: INTEGRATOR\n"
                "status: OPEN\n"
                "objective:\n"
                "- integrator\n",
                encoding="utf-8",
            )
            with (
                patch.object(mod, "NOTE_TASKS_PATH", path),
                patch.object(
                    mod,
                    "summarize_claim_ownership_with_refresh",
                    return_value=(
                        {
                            "claimed_by_this_session": [
                                {
                                    "ticket_id": "TKT-005",
                                    "title": "Startup",
                                    "status": "CLAIMED",
                                }
                            ],
                            "claimable_tickets": [
                                {
                                    "ticket_id": "TKT-006",
                                    "title": "Question",
                                    "status": "OPEN",
                                }
                            ],
                            "claimed_by_other_sessions": [],
                            "blocked_tickets": [],
                            "closed_tickets": [],
                            "latest_review_inbox_refresh": {},
                            "latest_review_inbox_check_at": None,
                            "latest_inbox_items_detected": [],
                            "continue_due_to_inbox_items": False,
                        },
                        {"latest_new_check_at": "2026-03-14T00:00:00+00:00"},
                    ),
                ),
                patch.object(
                    mod,
                    "list_tickets",
                    side_effect=[
                        [
                            {
                                "ticket_id": "TKT-005",
                                "title": "Startup",
                                "status": "CLAIMED",
                                "ticket_type": "WORKER",
                                "workflow_state": "CLAIMED",
                            },
                            {
                                "ticket_id": "TKT-006",
                                "title": "Question",
                                "status": "OPEN",
                                "ticket_type": "WORKER",
                                "workflow_state": "OPEN",
                            },
                            {
                                "ticket_id": "TKT-008",
                                "title": "Integrator",
                                "status": "OPEN",
                                "ticket_type": "INTEGRATOR",
                                "workflow_state": "OPEN",
                            },
                        ],
                        [
                            {
                                "ticket_id": "TKT-005",
                                "title": "Startup",
                                "status": "CLAIMED",
                                "ticket_type": "WORKER",
                                "workflow_state": "CLAIMED",
                            },
                            {
                                "ticket_id": "TKT-006",
                                "title": "Question",
                                "status": "OPEN",
                                "ticket_type": "WORKER",
                                "workflow_state": "OPEN",
                            },
                        ],
                    ],
                ),
            ):
                context = mod.build_startup_context(
                    conn, session_id="ses-123", batch_id="BATCH-002"
                )

        self.assertEqual(context["current_ticket_context"]["ticket_id"], "TKT-005")
        self.assertEqual(
            context["next_claimable_worker_ticket"]["ticket_id"], "TKT-006"
        )
        self.assertEqual(context["integrator_ticket_status"], "OPEN")
        self.assertEqual(context["workflow_owner"], "skill-system-workflow")
        self.assertEqual(context["scope_ticket_id"], "TKT-005")
        self.assertEqual(
            context["question_tool_prompt_shape"],
            "2-4 options, one recommended option, single choice.",
        )
        self.assertEqual(context["question_tool_used"], "no-branch-decision-required")
        self.assertEqual(context["subagent_usage"], [])
        self.assertIn(
            "generate-review-prompt --ticket-id TKT-005",
            context["review_prompt_command"],
        )

    def test_build_startup_context_exposes_review_inbox_refresh(self):
        mod = load_module()
        conn = MagicMock()
        note_tasks_text = (
            "# Prompt\n\n## Review Agent Inbox\n\n# TICKET_BATCH\n"
            "batch_id: BATCH-007\nmode: review-inbox-mainline-integration\n"
            "status: READY\n\n## TICKETS\n\n"
            "### TKT-025\n"
            "title: Session loop and integrator closure must honor review inbox\n"
            "type: WORKER\nstatus: OPEN\nobjective:\n- honor inbox\n"
        )
        summary = {
            "total_tickets": 3,
            "claimed_by_this_session": [],
            "claimable_worker_tickets": [
                {"ticket_id": "TKT-025", "status": "OPEN", "ticket_type": "WORKER"}
            ],
            "claimed_by_other_sessions": [],
            "blocked_tickets": [],
            "closed_tickets": [],
            "latest_review_inbox_refresh": {
                "latest_review_inbox_check_at": "2026-03-14T10:41:00+00:00",
                "new_inbox_items_detected": [
                    {"inbox_item_id": "TKT-025", "ticket_type": "WORKER"}
                ],
            },
            "latest_review_inbox_check_at": "2026-03-14T10:41:00+00:00",
            "latest_inbox_items_detected": [
                {"inbox_item_id": "TKT-025", "ticket_type": "WORKER"}
            ],
            "continue_due_to_inbox_items": True,
            "integrator_ticket_status": "OPEN",
            "question_tool_used": "no-branch-decision-required",
            "question_tool_prompt_shape": "2-4 options, one recommended option, single choice.",
            "subagent_usage": [],
            "scope_ticket_id": "TKT-025",
        }
        with (
            patch.object(
                mod,
                "read_note_tasks_document",
                return_value=(
                    {"canonical_path": "note/note_tasks.md"},
                    note_tasks_text,
                ),
            ),
            patch.object(mod, "ensure_note_tasks_tickets"),
            patch.object(
                mod,
                "parse_note_tasks_batch",
                return_value={
                    "batch_id": "BATCH-007",
                    "tickets": [{"ticket_id": "TKT-025", "ticket_type": "WORKER"}],
                },
            ),
            patch.object(
                mod,
                "list_tickets",
                side_effect=[
                    [
                        {
                            "ticket_id": "TKT-025",
                            "ticket_type": "WORKER",
                            "workflow_state": "OPEN",
                        }
                    ],
                    [
                        {
                            "ticket_id": "TKT-025",
                            "ticket_type": "WORKER",
                            "status": "OPEN",
                        }
                    ],
                ],
            ),
            patch.object(
                mod,
                "summarize_claim_ownership_with_refresh",
                return_value=(
                    summary,
                    {"latest_new_check_at": "2026-03-14T10:40:00+00:00"},
                ),
            ),
        ):
            context = mod.build_startup_context(
                conn, session_id="ses-123", batch_id="BATCH-007"
            )

        self.assertEqual(
            context["latest_review_inbox_check_at"], "2026-03-14T10:41:00+00:00"
        )
        self.assertTrue(context["continue_due_to_inbox_items"])

    def test_main_startup_flow_emits_json(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "connect_workflow_db",
                return_value=(conn, "agent_memory", "SKILL_PGDATABASE"),
            ),
            patch.object(
                mod,
                "build_startup_context",
                return_value={
                    "batch_id": "BATCH-002",
                    "current_ticket_context": {"ticket_id": "TKT-005"},
                    "collaboration_policy": {
                        "question_tool_policy": "Prefer the question tool for branch decisions when it is available.",
                        "fallback_policy": "Fallback to a structured single-choice prompt with one recommended option when the question tool is unavailable.",
                        "subagent_policy": "Subagents are encouraged for clean decomposition but must stay inside the currently claimed ticket scope.",
                    },
                },
            ),
            patch("builtins.print") as fake_print,
        ):
            exit_code = mod.main(
                ["startup-flow", "--session-id", "ses-123", "--batch-id", "BATCH-002"]
            )

        payload = json.loads(fake_print.call_args[0][0])
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["startup_context"]["batch_id"], "BATCH-002")
        self.assertEqual(
            payload["startup_context"]["collaboration_policy"]["question_tool_policy"],
            "Prefer the question tool for branch decisions when it is available.",
        )
        self.assertIn(
            "structured single-choice prompt",
            payload["startup_context"]["collaboration_policy"]["fallback_policy"],
        )
        self.assertIn(
            "claimed ticket scope",
            payload["startup_context"]["collaboration_policy"]["subagent_policy"],
        )

    def test_main_refresh_new_tasks_emits_json(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "connect_workflow_db",
                return_value=(conn, "agent_memory", "SKILL_PGDATABASE"),
            ),
            patch.object(
                mod,
                "refresh_new_tasks",
                return_value={
                    "canonical_note_tasks_path": "note/note_tasks.md",
                    "path_resolution_status": "RESOLVED_SINGLE_PATH",
                    "new_items_detected": [],
                    "new_tickets_ingested": [],
                    "continue_due_to_new_policy": False,
                },
            ),
            patch("builtins.print") as fake_print,
        ):
            exit_code = mod.main(["refresh-new-tasks", "--trigger-point", "manual"])

        payload = json.loads(fake_print.call_args[0][0])
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(
            payload["refresh_status"]["path_resolution_status"],
            "RESOLVED_SINGLE_PATH",
        )

    def test_summarize_claim_ownership_reports_worker_loop_fields(self):
        mod = load_module()
        conn = MagicMock()
        with patch.object(
            mod,
            "list_tickets",
            return_value=[
                {
                    "ticket_id": "TKT-006",
                    "status": "CLAIMED",
                    "claimed_by_session": "ses-123",
                    "ticket_type": "WORKER",
                },
                {
                    "ticket_id": "TKT-007",
                    "status": "STALE",
                    "claimed_by_session": None,
                    "ticket_type": "WORKER",
                },
                {
                    "ticket_id": "TKT-008",
                    "status": "OPEN",
                    "claimed_by_session": None,
                    "ticket_type": "INTEGRATOR",
                },
            ],
        ):
            summary = mod.summarize_claim_ownership(
                conn, session_id="ses-123", batch_id="BATCH-002"
            )

        self.assertEqual(len(summary["stale_worker_tickets"]), 1)
        self.assertEqual(summary["stale_worker_tickets"][0]["ticket_id"], "TKT-007")
        self.assertEqual(len(summary["claimable_worker_tickets"]), 1)
        self.assertEqual(
            summary["next_claimable_worker_ticket"]["ticket_id"], "TKT-007"
        )
        self.assertEqual(summary["integrator_ticket_status"], "OPEN")
        self.assertIn("cannot declare batch complete", summary["worker_closure_rule"])
        self.assertIn("only the integrator", summary["integrator_closure_rule"])
        self.assertEqual(summary["scope_ticket_id"], "TKT-006")
        self.assertIn(
            "single choice",
            summary["collaboration_policy"]["question_tool_prompt_shape"],
        )
        self.assertEqual(summary["question_tool_used"], "no-branch-decision-required")
        self.assertEqual(summary["subagent_usage"], [])
        self.assertIn("preferred_use_cases", summary["subagent_playbook"])
        self.assertIn(
            "No subagent was needed",
            summary["why_subagent_was_needed_or_not_needed"],
        )

    def test_get_ticket_scope_supports_tkt_009(self):
        mod = load_module()

        report = mod.evaluate_ticket_scope(
            "TKT-009",
            [
                "skills/skill-system-workflow/scripts/tickets.py",
                "skills/skill-system-review/scripts/review_prompt.py",
                "review/templates/REVIEW_BUNDLE.md",
                "review/REVIEW_BUNDLE.md",
                "spec/verify_batch_ticket_queue.py",
            ],
        )

        self.assertEqual(report["scope_breach_status"], "CLEAN")
        self.assertFalse(report["out_of_scope_files"])

    def test_get_ticket_scope_blocks_tkt_009_cross_ticket_loophole(self):
        mod = load_module()

        report = mod.evaluate_ticket_scope(
            "TKT-009",
            [
                "skills/skill-system-workflow/scripts/tickets.py",
                "skills/skill-system-cockpit/scripts/cockpit.py",
            ],
        )

        self.assertEqual(report["scope_breach_status"], "SCOPE_BREACH")
        self.assertEqual(
            report["out_of_scope_files"],
            ["skills/skill-system-cockpit/scripts/cockpit.py"],
        )

    def test_get_ticket_scope_supports_tkt_019(self):
        mod = load_module()

        report = mod.evaluate_ticket_scope(
            "TKT-019",
            [
                "skills/skill-system-workflow/scripts/tickets.py",
                "skills/skill-system-workflow/scripts/test_tickets.py",
                "skills/skill-system-cockpit/scripts/cockpit.py",
                "skills/skill-system-cockpit/scripts/test_cockpit.py",
                "skills/skill-system-cockpit/schema/cockpit-state.yaml",
                "spec/verify_batch_ticket_queue.py",
                "spec/verify_cockpit_round.py",
            ],
        )

        self.assertEqual(report["scope_breach_status"], "CLEAN")
        self.assertFalse(report["out_of_scope_files"])

    def test_get_ticket_scope_supports_tkt_027(self):
        mod = load_module()

        report = mod.evaluate_ticket_scope(
            "TKT-027",
            [
                "skills/skill-system-workflow/scripts/tickets.py",
                "skills/skill-system-cockpit/scripts/cockpit.py",
                "skills/skill-system-review/scripts/review_prompt.py",
                "review/artifacts/tkt-027/note-feedback-current-state.txt",
                "review/artifacts/tkt-027/cockpit-state.txt",
                "review/artifacts/tkt-027/tui-overview-panel.txt",
                "review/artifacts/tkt-027/tui-tasks-panel.txt",
                "review/artifacts/tkt-027/startup-review-prompt-summary.txt",
                "review/REVIEW_BUNDLE.md",
                "note/note_feedback.md",
            ],
        )

        self.assertEqual(report["scope_breach_status"], "CLEAN")
        self.assertFalse(report["out_of_scope_files"])

    def test_build_integrator_closure_report_requires_integrator_ticket(self):
        mod = load_module()
        conn = MagicMock()
        with patch.object(
            mod,
            "require_owned_active_ticket",
            return_value={
                "ticket_id": "TKT-007",
                "ticket_type": "WORKER",
                "batch_id": "BATCH-002",
                "status": "CLAIMED",
            },
        ):
            with self.assertRaises(mod.TicketError) as ctx:
                mod.build_integrator_closure_report(
                    conn, session_id="ses-123", ticket_id="TKT-007"
                )

        self.assertEqual(ctx.exception.code, "TICKET_NOT_INTEGRATOR")

    def test_build_integrator_closure_report_summarizes_batch_state(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "require_owned_active_ticket",
                return_value={
                    "ticket_id": "TKT-008",
                    "ticket_type": "INTEGRATOR",
                    "batch_id": "BATCH-002",
                    "status": "CLAIMED",
                },
            ),
            patch.object(
                mod,
                "list_tickets",
                return_value=[
                    {
                        "ticket_id": "TKT-005",
                        "status": "CLAIMED",
                        "ticket_type": "WORKER",
                        "workflow_state": "CLAIMED",
                    },
                    {
                        "ticket_id": "TKT-006",
                        "status": "CLOSED",
                        "ticket_type": "WORKER",
                        "workflow_state": "CLOSED",
                    },
                    {
                        "ticket_id": "TKT-007",
                        "status": "BLOCKED",
                        "ticket_type": "WORKER",
                        "workflow_state": "BLOCKED",
                    },
                    {
                        "ticket_id": "TKT-008",
                        "status": "CLAIMED",
                        "ticket_type": "INTEGRATOR",
                        "workflow_state": "CLAIMED",
                    },
                ],
            ),
        ):
            report = mod.build_integrator_closure_report(
                conn, session_id="ses-123", ticket_id="TKT-008"
            )

        self.assertEqual(report["ticket_id"], "TKT-008")
        self.assertEqual(report["closed_worker_tickets"], ["TKT-006"])
        self.assertEqual(report["unresolved_blocked_tickets"], ["TKT-007"])
        self.assertEqual(report["unresolved_claimed_worker_tickets"], ["TKT-005"])
        self.assertEqual(
            report["final_batch_closure_status"], "NOT_READY_UNRESOLVED_WORKERS"
        )

    def test_evaluate_ticket_scope_reports_clean(self):
        mod = load_module()

        report = mod.evaluate_ticket_scope(
            "TKT-003",
            [
                "skills/skill-system-workflow/scripts/tickets.py",
                "spec/verify_batch_ticket_queue.py",
                "review/REVIEW_BUNDLE.md",
            ],
        )

        self.assertEqual(report["scope_breach_status"], "CLEAN")
        self.assertFalse(report["out_of_scope_files"])

    def test_evaluate_ticket_scope_reports_scope_breach(self):
        mod = load_module()

        report = mod.evaluate_ticket_scope(
            "TKT-003",
            [
                "skills/skill-system-workflow/scripts/tickets.py",
                "skills/skill-system-cockpit/scripts/cockpit.py",
            ],
        )

        self.assertEqual(report["scope_breach_status"], "SCOPE_BREACH")
        self.assertEqual(
            report["out_of_scope_files"],
            ["skills/skill-system-cockpit/scripts/cockpit.py"],
        )

    def test_ensure_note_tasks_ticket_reads_from_file(self):
        mod = load_module()
        conn = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "note_tasks.md"
            path.write_text(
                "# TICKET\nticket_id: TKT-001\ntitle: Ticket title\nstatus: READY\n\n## OBJECTIVE\nTicket summary text.\n",
                encoding="utf-8",
            )
            with (
                patch.object(
                    mod,
                    "resolve_note_tasks_path",
                    return_value={
                        "canonical_path": path,
                        "path_resolution_status": "RESOLVED_SINGLE_PATH",
                        "candidate_paths": [str(path)],
                        "existing_paths": [str(path)],
                    },
                ),
                patch.object(
                    mod,
                    "intake_ticket",
                    return_value={"ticket_id": "TKT-001", "status": "OPEN"},
                ) as intake,
            ):
                ticket = mod.ensure_note_tasks_ticket(conn)

        intake.assert_called_once()
        self.assertEqual(ticket["ticket_id"], "TKT-001")

    def test_ensure_note_tasks_ticket_forwards_provenance_override(self):
        mod = load_module()
        conn = MagicMock()
        with patch.object(
            mod,
            "intake_ticket",
            return_value={"ticket_id": "TKT-001", "status": "OPEN"},
        ) as intake:
            mod.ensure_note_tasks_ticket(
                conn,
                note_tasks_text=(
                    "# TICKET\n"
                    "ticket_id: TKT-001\n"
                    "title: Ticket title\n"
                    "status: READY\n\n"
                    "## OBJECTIVE\n"
                    "Ticket summary text.\n"
                ),
                task_provenance="verification",
                task_surface_visibility="hidden_by_default",
            )

        self.assertEqual(intake.call_args.kwargs["task_provenance"], "verification")
        self.assertEqual(
            intake.call_args.kwargs["task_surface_visibility"], "hidden_by_default"
        )

    def test_ensure_note_tasks_tickets_reads_batch_from_file(self):
        mod = load_module()
        conn = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "note_tasks.md"
            path.write_text(
                "# TICKET_BATCH\n"
                "batch_id: BATCH-001\n"
                "mode: multi-session-ticket-queue\n"
                "status: READY\n\n"
                "### TKT-002\n"
                "title: First\n"
                "status: OPEN\n"
                "objective:\n"
                "- first summary\n\n"
                "### TKT-003\n"
                "title: Second\n"
                "status: OPEN\n"
                "objective:\n"
                "- second summary\n",
                encoding="utf-8",
            )
            with (
                patch.object(
                    mod,
                    "resolve_note_tasks_path",
                    return_value={
                        "canonical_path": path,
                        "path_resolution_status": "RESOLVED_SINGLE_PATH",
                        "candidate_paths": [str(path)],
                        "existing_paths": [str(path)],
                    },
                ),
                patch.object(
                    mod,
                    "intake_ticket",
                    side_effect=[
                        {"ticket_id": "TKT-002", "status": "OPEN"},
                        {"ticket_id": "TKT-003", "status": "OPEN"},
                    ],
                ) as intake,
            ):
                payload = mod.ensure_note_tasks_tickets(conn)

        self.assertEqual(payload["batch_id"], "BATCH-001")
        self.assertEqual(
            [item["ticket_id"] for item in payload["tickets"]], ["TKT-002", "TKT-003"]
        )
        self.assertEqual(intake.call_count, 2)

    def test_ensure_note_tasks_tickets_forwards_provenance_override(self):
        mod = load_module()
        conn = MagicMock()
        with patch.object(
            mod,
            "intake_ticket",
            side_effect=[
                {"ticket_id": "TKT-002", "status": "OPEN"},
                {"ticket_id": "TKT-003", "status": "OPEN"},
            ],
        ) as intake:
            mod.ensure_note_tasks_tickets(
                conn,
                note_tasks_text=(
                    "# TICKET_BATCH\n"
                    "batch_id: BATCH-VERIFY-123456\n"
                    "mode: multi-session-ticket-queue\n"
                    "status: READY\n\n"
                    "### TKT-002\n"
                    "title: First\n"
                    "status: OPEN\n"
                    "objective:\n"
                    "- first summary\n\n"
                    "### TKT-003\n"
                    "title: Second\n"
                    "status: OPEN\n"
                    "objective:\n"
                    "- second summary\n"
                ),
                task_provenance="verification",
                task_surface_visibility="hidden_by_default",
            )

        for call in intake.call_args_list:
            self.assertEqual(call.kwargs["task_provenance"], "verification")
            self.assertEqual(
                call.kwargs["task_surface_visibility"], "hidden_by_default"
            )

    def test_main_claim_ticket_emits_json(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "connect_workflow_db",
                return_value=(conn, "agent_memory", "SKILL_PGDATABASE"),
            ),
            patch.object(
                mod,
                "claim_ticket_with_refresh",
                return_value=(
                    {"ticket_id": "TKT-001", "status": "CLAIMED"},
                    {"path_resolution_status": "RESOLVED_SINGLE_PATH"},
                ),
            ),
            patch("builtins.print") as fake_print,
        ):
            exit_code = mod.main(
                ["claim-ticket", "--ticket-id", "TKT-001", "--session-id", "ses-123"]
            )

        payload = json.loads(fake_print.call_args[0][0])
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["ticket"]["ticket_id"], "TKT-001")
        self.assertEqual(
            payload["refresh_status"]["path_resolution_status"],
            "RESOLVED_SINGLE_PATH",
        )

    def test_record_subagent_usage_records_parent_binding(self):
        mod = load_module()
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (
            1,
            "TKT-039",
            "title",
            "desc",
            "open",
            None,
            None,
            None,
            {},
        )
        with (
            patch.object(
                mod,
                "fetch_ticket",
                return_value={"ticket_id": "TKT-039", "batch_id": "BATCH-010"},
            ),
            patch.object(
                mod,
                "normalize_ticket_row",
                return_value={"ticket_id": "TKT-039", "metadata": {}},
            ),
        ):
            ticket = mod.record_subagent_usage(
                conn,
                parent_ticket_id="TKT-039",
                subagent_name="general",
                purpose="bounded survey",
            )

        self.assertEqual(
            ticket["recorded_subagent_usage"]["parent_ticket_id"], "TKT-039"
        )
        self.assertEqual(ticket["recorded_subagent_usage"]["ticket_scope"], "TKT-039")

    def test_main_claim_summary_emits_json(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "connect_workflow_db",
                return_value=(conn, "agent_memory", "SKILL_PGDATABASE"),
            ),
            patch.object(
                mod,
                "summarize_claim_ownership_with_refresh",
                return_value=(
                    {
                        "total_tickets": 3,
                        "next_claimable_ticket": {"ticket_id": "TKT-004"},
                        "collaboration_policy": {
                            "question_tool_policy": "Prefer the question tool for branch decisions when it is available.",
                            "fallback_policy": "Fallback to a structured single-choice prompt with one recommended option when the question tool is unavailable.",
                            "subagent_policy": "Subagents are encouraged for clean decomposition but must stay inside the currently claimed ticket scope.",
                        },
                    },
                    {"path_resolution_status": "RESOLVED_SINGLE_PATH"},
                ),
            ),
            patch("builtins.print") as fake_print,
        ):
            exit_code = mod.main(
                ["claim-summary", "--session-id", "ses-123", "--batch-id", "BATCH-001"]
            )

        payload = json.loads(fake_print.call_args[0][0])
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["claim_summary"]["total_tickets"], 3)
        self.assertIn("collaboration_policy", payload["claim_summary"])
        self.assertEqual(
            payload["refresh_status"]["path_resolution_status"], "RESOLVED_SINGLE_PATH"
        )

    def test_main_refresh_review_inbox_emits_json(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "connect_workflow_db",
                return_value=(conn, "agent_memory", "SKILL_PGDATABASE"),
            ),
            patch.object(
                mod,
                "refresh_review_inbox",
                return_value={
                    "canonical_inbox_path": "note/note_tasks.md",
                    "path_resolution_status": "RESOLVED_SINGLE_INBOX_PATH",
                    "parser_shape": "embedded_ticket_batch_in_note_tasks",
                    "new_tickets_ingested": [],
                },
            ),
            patch("builtins.print") as fake_print,
        ):
            exit_code = mod.main(["refresh-review-inbox", "--trigger-point", "manual"])

        payload = json.loads(fake_print.call_args[0][0])
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(
            payload["refresh_status"]["path_resolution_status"],
            "RESOLVED_SINGLE_INBOX_PATH",
        )

    def test_summarize_claim_ownership_with_refresh_includes_review_inbox_fields(self):
        mod = load_module()
        conn = MagicMock()
        note_refresh = {
            "latest_new_check_at": "2026-03-14T10:40:00+00:00",
            "new_items_detected": [],
            "continue_due_to_new_policy": False,
        }
        review_inbox_refresh = {
            "latest_review_inbox_check_at": "2026-03-14T10:41:00+00:00",
            "new_inbox_items_detected": [
                {"inbox_item_id": "TKT-025", "ticket_type": "WORKER"}
            ],
            "consumed_or_pending_inbox_items": [
                {
                    "inbox_item_id": "TKT-025",
                    "ticket_id": "TKT-025",
                    "state": "already_present",
                }
            ],
        }
        with (
            patch.object(mod, "refresh_new_tasks", return_value=note_refresh),
            patch.object(
                mod, "refresh_review_inbox", return_value=review_inbox_refresh
            ),
            patch.object(
                mod,
                "summarize_claim_ownership",
                return_value={
                    "claimable_worker_tickets": [
                        {"ticket_id": "TKT-025", "ticket_type": "WORKER"}
                    ],
                    "claimed_by_this_session": [
                        {"ticket_id": "TKT-024", "claimed_by_session": "ses-123"}
                    ],
                },
            ),
        ):
            hydrated, refresh = mod.summarize_claim_ownership_with_refresh(
                conn, session_id="ses-123", batch_id="BATCH-007"
            )

        self.assertEqual(refresh, note_refresh)
        self.assertEqual(hydrated["latest_refresh"], note_refresh)
        self.assertEqual(hydrated["latest_review_inbox_refresh"], review_inbox_refresh)
        self.assertEqual(
            hydrated["latest_review_inbox_check_at"], "2026-03-14T10:41:00+00:00"
        )
        self.assertTrue(hydrated["continue_due_to_inbox_items"])
        self.assertEqual(hydrated["currently_claimed_by_this_session"], ["TKT-024"])

    def test_main_check_open_tickets_emits_json_and_commits(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "connect_workflow_db",
                return_value=(conn, "agent_memory", "SKILL_PGDATABASE"),
            ),
            patch.object(
                mod,
                "check_open_tickets_with_refresh",
                return_value=(
                    {"open_ticket_count": 0},
                    {"total_tickets": 3},
                    {"path_resolution_status": "RESOLVED_SINGLE_PATH"},
                ),
            ),
            patch("builtins.print") as fake_print,
        ):
            exit_code = mod.main(
                [
                    "check-open-tickets",
                    "--session-id",
                    "ses-123",
                    "--batch-id",
                    "BATCH-001",
                ]
            )

        payload = json.loads(fake_print.call_args[0][0])
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["report"]["open_ticket_count"], 0)
        self.assertEqual(
            payload["refresh_status"]["path_resolution_status"], "RESOLVED_SINGLE_PATH"
        )
        conn.commit.assert_called_once()

    def test_run_session_loop_resumes_claimed_ticket(self):
        mod = load_module()
        conn = MagicMock()
        summary = {
            "batch_id": "BATCH-005",
            "claimed_by_this_session": [
                {
                    "ticket_id": "TKT-016",
                    "title": "Workflow-owned session loop runner",
                    "status": "CLAIMED",
                    "claimed_by_session": "ses-123",
                    "ticket_type": "WORKER",
                }
            ],
            "claimable_worker_tickets": [
                {"ticket_id": "TKT-017", "status": "OPEN", "ticket_type": "WORKER"}
            ],
            "stale_worker_tickets": [],
            "next_claimable_worker_ticket": {
                "ticket_id": "TKT-017",
                "status": "OPEN",
                "ticket_type": "WORKER",
            },
            "integrator_ticket_status": "OPEN",
            "closed_tickets": [],
            "claimed_by_other_sessions": [],
            "blocked_tickets": [],
            "latest_review_inbox_refresh": {
                "latest_review_inbox_check_at": "2026-03-14T06:01:00+00:00"
            },
            "latest_review_inbox_check_at": "2026-03-14T06:01:00+00:00",
            "latest_inbox_items_detected": [
                {"inbox_item_id": "TKT-016", "ticket_type": "WORKER"}
            ],
            "continue_due_to_inbox_items": True,
            "currently_claimed_by_this_session": ["TKT-016"],
        }
        refresh = {
            "path_resolution_status": "RESOLVED_SINGLE_PATH",
            "latest_new_check_at": "2026-03-14T06:00:00+00:00",
            "new_items_detected": [],
            "continue_due_to_new_policy": False,
        }

        with (
            patch.object(
                mod,
                "summarize_claim_ownership_with_refresh",
                return_value=(summary, refresh),
            ),
            patch.object(mod, "claim_ticket") as claim,
        ):
            loop_state = mod.run_session_loop(
                conn, session_id="ses-123", batch_id="BATCH-005"
            )

        claim.assert_not_called()
        self.assertEqual(loop_state["stop_reason"], "resume_claimed_ticket")
        self.assertEqual(loop_state["active_ticket"]["ticket_id"], "TKT-016")
        self.assertFalse(loop_state["integrator_eligible"])
        self.assertEqual(loop_state["latest_refresh"], refresh)
        self.assertEqual(
            loop_state["latest_review_inbox_refresh_at"],
            "2026-03-14T06:01:00+00:00",
        )
        self.assertTrue(loop_state["continue_due_to_inbox_items"])
        self.assertEqual(loop_state["currently_claimed_by_this_session"], ["TKT-016"])

    def test_run_session_loop_claims_next_worker_when_idle(self):
        mod = load_module()
        conn = MagicMock()
        initial_summary = {
            "batch_id": "BATCH-005",
            "claimed_by_this_session": [],
            "claimable_worker_tickets": [
                {"ticket_id": "TKT-016", "status": "OPEN", "ticket_type": "WORKER"},
                {"ticket_id": "TKT-017", "status": "OPEN", "ticket_type": "WORKER"},
            ],
            "stale_worker_tickets": [],
            "next_claimable_worker_ticket": {
                "ticket_id": "TKT-016",
                "status": "OPEN",
                "ticket_type": "WORKER",
            },
            "integrator_ticket_status": "OPEN",
            "closed_tickets": [],
            "claimed_by_other_sessions": [],
            "blocked_tickets": [],
            "latest_review_inbox_refresh": {
                "latest_review_inbox_check_at": "2026-03-14T06:01:00+00:00"
            },
            "latest_review_inbox_check_at": "2026-03-14T06:01:00+00:00",
            "latest_inbox_items_detected": [
                {"inbox_item_id": "TKT-016", "ticket_type": "WORKER"}
            ],
            "continue_due_to_inbox_items": True,
            "currently_claimed_by_this_session": [],
        }
        refreshed_summary = {
            **initial_summary,
            "claimed_by_this_session": [
                {
                    "ticket_id": "TKT-016",
                    "title": "Workflow-owned session loop runner",
                    "status": "CLAIMED",
                    "claimed_by_session": "ses-123",
                    "ticket_type": "WORKER",
                }
            ],
            "claimable_worker_tickets": [
                {"ticket_id": "TKT-017", "status": "OPEN", "ticket_type": "WORKER"}
            ],
            "next_claimable_worker_ticket": {
                "ticket_id": "TKT-017",
                "status": "OPEN",
                "ticket_type": "WORKER",
            },
        }
        refresh = {
            "path_resolution_status": "RESOLVED_SINGLE_PATH",
            "latest_new_check_at": "2026-03-14T06:00:00+00:00",
            "new_items_detected": [],
            "continue_due_to_new_policy": False,
        }
        claimed_ticket = {
            "ticket_id": "TKT-016",
            "title": "Workflow-owned session loop runner",
            "status": "CLAIMED",
            "claimed_by_session": "ses-123",
            "ticket_type": "WORKER",
        }

        with (
            patch.object(
                mod,
                "summarize_claim_ownership_with_refresh",
                return_value=(initial_summary, refresh),
            ),
            patch.object(mod, "claim_ticket", return_value=claimed_ticket) as claim,
            patch.object(
                mod, "summarize_claim_ownership", return_value=refreshed_summary
            ),
        ):
            loop_state = mod.run_session_loop(
                conn, session_id="ses-123", batch_id="BATCH-005"
            )

        claim.assert_called_once_with(conn, ticket_id="TKT-016", session_id="ses-123")
        self.assertEqual(loop_state["stop_reason"], "claimed_next_worker_ticket")
        self.assertEqual(loop_state["claimed_ticket"]["ticket_id"], "TKT-016")
        self.assertEqual(loop_state["active_ticket"]["ticket_id"], "TKT-016")
        self.assertEqual(loop_state["open_or_stale_remaining"], 1)
        self.assertEqual(
            loop_state["latest_review_inbox_refresh_at"],
            "2026-03-14T06:01:00+00:00",
        )

    def test_run_session_loop_reports_integrator_eligibility(self):
        mod = load_module()
        conn = MagicMock()
        summary = {
            "batch_id": "BATCH-005",
            "claimed_by_this_session": [],
            "claimable_worker_tickets": [],
            "stale_worker_tickets": [],
            "next_claimable_worker_ticket": None,
            "integrator_ticket_status": "OPEN",
            "closed_tickets": [
                {"ticket_id": "TKT-016", "claimed_by_session": "ses-123"},
                {"ticket_id": "TKT-017", "claimed_by_session": "ses-123"},
            ],
            "claimed_by_other_sessions": [],
            "blocked_tickets": [],
            "latest_review_inbox_refresh": {
                "latest_review_inbox_check_at": "2026-03-14T06:01:00+00:00"
            },
            "latest_review_inbox_check_at": "2026-03-14T06:01:00+00:00",
            "latest_inbox_items_detected": [],
            "continue_due_to_inbox_items": False,
            "currently_claimed_by_this_session": [],
        }
        refresh = {
            "path_resolution_status": "RESOLVED_SINGLE_PATH",
            "latest_new_check_at": "2026-03-14T06:00:00+00:00",
            "new_items_detected": [],
            "continue_due_to_new_policy": False,
        }

        with (
            patch.object(
                mod,
                "summarize_claim_ownership_with_refresh",
                return_value=(summary, refresh),
            ),
            patch.object(mod, "claim_ticket") as claim,
        ):
            loop_state = mod.run_session_loop(
                conn, session_id="ses-123", batch_id="BATCH-005"
            )

        claim.assert_not_called()
        self.assertEqual(
            loop_state["stop_reason"], "await_integrator_closure_after_refresh"
        )
        self.assertTrue(loop_state["integrator_eligible"])
        self.assertEqual(loop_state["open_or_stale_remaining"], 0)
        self.assertEqual(
            loop_state["tickets_claimed_sequentially"], ["TKT-016", "TKT-017"]
        )
        self.assertEqual(
            loop_state["latest_review_inbox_refresh_at"],
            "2026-03-14T06:01:00+00:00",
        )

    def test_main_session_loop_emits_json_and_commits_via_builder(self):
        mod = load_module()
        conn = MagicMock()
        loop_state = {
            "batch_id": "BATCH-005",
            "stop_reason": "resume_claimed_ticket",
            "latest_refresh": {"path_resolution_status": "RESOLVED_SINGLE_PATH"},
        }
        with (
            patch.object(
                mod,
                "connect_workflow_db",
                return_value=(conn, "agent_memory", "SKILL_PGDATABASE"),
            ),
            patch.object(mod, "run_session_loop", return_value=loop_state),
            patch("builtins.print") as fake_print,
        ):
            exit_code = mod.main(
                ["session-loop", "--session-id", "ses-123", "--batch-id", "BATCH-005"]
            )

        payload = json.loads(fake_print.call_args[0][0])
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["loop_state"]["batch_id"], "BATCH-005")
        conn.commit.assert_called_once()

    def test_main_check_ticket_scope_emits_json(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "connect_workflow_db",
                return_value=(conn, "agent_memory", "SKILL_PGDATABASE"),
            ),
            patch.object(
                mod, "list_changed_files", return_value=["review/REVIEW_BUNDLE.md"]
            ),
            patch("builtins.print") as fake_print,
        ):
            exit_code = mod.main(["check-ticket-scope", "--ticket-id", "TKT-003"])

        payload = json.loads(fake_print.call_args[0][0])
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["scope_report"]["scope_breach_status"], "CLEAN")

    def test_main_check_ticket_scope_uses_explicit_changed_files(self):
        mod = load_module()
        conn = MagicMock()
        with (
            patch.object(
                mod,
                "connect_workflow_db",
                return_value=(conn, "agent_memory", "SKILL_PGDATABASE"),
            ),
            patch.object(mod, "list_changed_files") as list_changed,
            patch("builtins.print") as fake_print,
        ):
            exit_code = mod.main(
                [
                    "check-ticket-scope",
                    "--ticket-id",
                    "TKT-003",
                    "--changed-file",
                    "skills/skill-system-workflow/scripts/tickets.py",
                    "--changed-file",
                    "review/REVIEW_BUNDLE.md",
                ]
            )

        payload = json.loads(fake_print.call_args[0][0])
        list_changed.assert_not_called()
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["scope_report"]["scope_breach_status"], "CLEAN")


if __name__ == "__main__":
    unittest.main()
