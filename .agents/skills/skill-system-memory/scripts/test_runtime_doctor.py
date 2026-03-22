from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("runtime_doctor.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("runtime_doctor", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load runtime_doctor from {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RuntimeDoctorTests(unittest.TestCase):
    def test_build_capability_model_classifies_supported_gated_and_deferred(self):
        mod = _load_module()

        capability_model = mod.build_capability_model(
            agent_memory_tables={
                "tables": {
                    "agent_memories": True,
                    "evolution_snapshots": True,
                    "soul_states": True,
                    "insight_facets": True,
                    "user_preferences": True,
                    "session_summaries": False,
                    "project_summaries": False,
                    "context_rollups": False,
                    "behavior_sources": False,
                    "behavior_nodes": False,
                    "behavior_edges": False,
                    "behavior_snapshots": False,
                }
            },
            agent_memory_functions={
                "routines": {
                    "store_memory": True,
                    "search_memories": True,
                    "memory_health_check": True,
                    "insert_evolution_snapshot": True,
                    "get_evolution_history": True,
                    "get_agent_context": True,
                    "get_soul_state": True,
                    "get_recent_facets": True,
                    "get_user_preferences": True,
                }
            },
            skill_system_tables={
                "tables": {
                    "policy_profiles": True,
                    "runs": True,
                    "run_events": True,
                    "refresh_jobs": False,
                    "refresh_job_events": False,
                    "artifact_versions": False,
                }
            },
        )

        self.assertEqual(capability_model["core_memory"]["status"], "SUPPORTED_NOW")
        self.assertEqual(
            capability_model["evolution_ledger"]["status"], "SUPPORTED_NOW"
        )
        self.assertEqual(
            capability_model["typed_context_reads"]["status"], "SUPPORTED_NOW"
        )
        self.assertEqual(
            capability_model["runtime_sync_projections"]["status"],
            "GATED_OPTIONAL",
        )
        self.assertEqual(
            capability_model["behavior_refresh_graph"]["status"],
            "GATED_OPTIONAL",
        )
        self.assertEqual(
            capability_model["control_plane_refresh"]["status"],
            "DEFERRED_UNSUPPORTED",
        )

    def test_build_report_includes_capability_model_and_evolution_status(self):
        mod = _load_module()

        with (
            patch.object(
                mod,
                "build_plugin_report",
                return_value={
                    "live_plugin_path": "/tmp/live-plugin.js",
                    "live_runtime_sync_path": "/tmp/runtime_sync.js",
                    "sync_state_path": "/tmp/plugin-sync.json",
                    "current_drift_status": "in_sync",
                    "previous_drift_status": "in_sync",
                    "source_of_truth": "/tmp/repo/plugins",
                    "repo_plugin_path": "/tmp/repo/plugins/skill-system-memory.js",
                    "repo_runtime_sync_path": "/tmp/repo/plugins/runtime_sync.js",
                    "repo_hashes": {},
                    "live_hashes_before": {},
                    "live_hashes_after": {},
                    "sync_requested": False,
                    "sync_result": None,
                },
            ),
            patch.object(
                mod,
                "detect_omo_runtime",
                return_value={
                    "enabled_in_config": True,
                    "actual_resolution_path": "/tmp/omo.js",
                },
            ),
            patch.object(
                mod,
                "resolve_memory_target",
                return_value={
                    "canonical_target": "agent_memory",
                    "target": "agent_memory",
                    "source": "default:agent_memory",
                    "aligned": True,
                    "silent_redirection_status": "no-ambient-default",
                },
            ),
            patch.object(
                mod,
                "query_existing_tables",
                side_effect=[
                    {
                        "ok": True,
                        "db": "agent_memory",
                        "schema": "public",
                        "error": None,
                        "tables": {
                            "agent_memories": True,
                            "evolution_snapshots": True,
                            "soul_states": True,
                            "insight_facets": True,
                            "user_preferences": True,
                            "session_summaries": False,
                            "project_summaries": False,
                            "context_rollups": False,
                            "behavior_sources": False,
                            "behavior_nodes": False,
                            "behavior_edges": False,
                            "behavior_snapshots": False,
                        },
                    },
                    {
                        "ok": True,
                        "db": "skill_system",
                        "schema": "skill_system",
                        "error": None,
                        "tables": {
                            "policy_profiles": True,
                            "runs": True,
                            "run_events": True,
                            "refresh_jobs": False,
                            "refresh_job_events": False,
                            "artifact_versions": False,
                        },
                    },
                ],
            ),
            patch.object(
                mod,
                "query_existing_routines",
                return_value={
                    "ok": True,
                    "db": "agent_memory",
                    "schema": "public",
                    "error": None,
                    "routines": {
                        "store_memory": True,
                        "search_memories": True,
                        "memory_health_check": True,
                        "insert_evolution_snapshot": True,
                        "get_evolution_history": True,
                        "get_agent_context": True,
                        "get_soul_state": True,
                        "get_recent_facets": True,
                        "get_user_preferences": True,
                    },
                },
            ),
            patch.object(
                mod,
                "collect_missing_references",
                return_value=["missing projection tables surfaced honestly"],
            ),
        ):
            report = mod.build_report(
                home_dir=Path("/tmp/home"), repo_root=Path("/tmp/repo")
            )

        self.assertIn("capability_model", report)
        self.assertEqual(
            report["capability_model"]["runtime_sync_projections"]["status"],
            "GATED_OPTIONAL",
        )
        self.assertEqual(
            report["evolution_snapshots_status"]["classification"],
            "ACTIVE_CURRENT_SURFACE",
        )

    def test_build_report_includes_evolution_store_report(self):
        mod = _load_module()

        with (
            patch.object(
                mod,
                "build_plugin_report",
                return_value={
                    "live_plugin_path": "/tmp/live-plugin.js",
                    "live_runtime_sync_path": "/tmp/runtime_sync.js",
                    "sync_state_path": "/tmp/plugin-sync.json",
                    "current_drift_status": "in_sync",
                    "previous_drift_status": "in_sync",
                    "source_of_truth": "/tmp/repo/plugins",
                    "repo_plugin_path": "/tmp/repo/plugins/skill-system-memory.js",
                    "repo_runtime_sync_path": "/tmp/repo/plugins/runtime_sync.js",
                    "repo_hashes": {},
                    "live_hashes_before": {},
                    "live_hashes_after": {},
                    "sync_requested": False,
                    "sync_result": None,
                },
            ),
            patch.object(
                mod,
                "detect_omo_runtime",
                return_value={
                    "enabled_in_config": True,
                    "actual_resolution_path": "/tmp/omo.js",
                },
            ),
            patch.object(
                mod,
                "resolve_memory_target",
                return_value={
                    "canonical_target": "agent_memory",
                    "target": "agent_memory",
                    "source": "default:agent_memory",
                    "aligned": True,
                    "silent_redirection_status": "no-ambient-default",
                },
            ),
            patch.object(
                mod,
                "query_existing_tables",
                side_effect=[
                    {
                        "ok": True,
                        "db": "agent_memory",
                        "schema": "public",
                        "error": None,
                        "tables": {
                            "agent_memories": True,
                            "evolution_snapshots": True,
                            "soul_states": True,
                            "insight_facets": True,
                            "user_preferences": True,
                            "session_summaries": False,
                            "project_summaries": False,
                            "context_rollups": False,
                            "behavior_sources": False,
                            "behavior_nodes": False,
                            "behavior_edges": False,
                            "behavior_snapshots": False,
                            "evolution_nodes": True,
                            "evolution_rejections": True,
                            "evolution_tasks": True,
                        },
                    },
                    {
                        "ok": True,
                        "db": "skill_system",
                        "schema": "skill_system",
                        "error": None,
                        "tables": {
                            "policy_profiles": True,
                            "runs": True,
                            "run_events": True,
                            "refresh_jobs": False,
                            "refresh_job_events": False,
                            "artifact_versions": False,
                        },
                    },
                ],
            ),
            patch.object(
                mod,
                "query_existing_routines",
                return_value={
                    "ok": True,
                    "db": "agent_memory",
                    "schema": "public",
                    "error": None,
                    "routines": {
                        "store_memory": True,
                        "search_memories": True,
                        "memory_health_check": True,
                        "insert_evolution_snapshot": True,
                        "get_evolution_history": True,
                        "get_agent_context": True,
                        "get_soul_state": True,
                        "get_recent_facets": True,
                        "get_user_preferences": True,
                    },
                },
            ),
            patch.object(
                mod,
                "count_agent_memory_categories",
                return_value={
                    "evolution-node": 4,
                    "evolution-rejected": 2,
                    "evolution-snapshot": 3,
                },
            ),
            patch.object(
                mod,
                "collect_missing_references",
                return_value=[],
            ),
        ):
            report = mod.build_report(
                home_dir=Path("/tmp/home"), repo_root=Path("/tmp/repo")
            )

        self.assertIn("evolution_store_report", report)
        self.assertEqual(
            report["evolution_store_report"]["canonical_accepted_store"],
            "evolution_nodes",
        )
        self.assertEqual(
            report["evolution_store_report"]["canonical_task_store"],
            "evolution_tasks -> agent_tasks",
        )
        self.assertEqual(
            report["evolution_snapshots_status"]["classification"],
            "LEGACY_COMPAT_SURFACE",
        )

    def test_build_report_includes_write_integrity_report(self):
        mod = _load_module()

        with (
            patch.object(
                mod,
                "build_plugin_report",
                return_value={
                    "live_plugin_path": "/tmp/live-plugin.js",
                    "live_runtime_sync_path": "/tmp/runtime_sync.js",
                    "sync_state_path": "/tmp/plugin-sync.json",
                    "current_drift_status": "in_sync",
                    "previous_drift_status": "in_sync",
                    "source_of_truth": "/tmp/repo/plugins",
                    "repo_plugin_path": "/tmp/repo/plugins/skill-system-memory.js",
                    "repo_runtime_sync_path": "/tmp/repo/plugins/runtime_sync.js",
                    "repo_hashes": {},
                    "live_hashes_before": {},
                    "live_hashes_after": {},
                    "sync_requested": False,
                    "sync_result": None,
                },
            ),
            patch.object(
                mod,
                "detect_omo_runtime",
                return_value={
                    "enabled_in_config": True,
                    "actual_resolution_path": "/tmp/omo.js",
                },
            ),
            patch.object(
                mod,
                "resolve_memory_target",
                return_value={
                    "canonical_target": "agent_memory",
                    "target": "agent_memory",
                    "source": "default:agent_memory",
                    "aligned": True,
                    "silent_redirection_status": "no-ambient-default",
                },
            ),
            patch.object(
                mod,
                "query_existing_tables",
                side_effect=[
                    {
                        "ok": True,
                        "db": "agent_memory",
                        "schema": "public",
                        "error": None,
                        "tables": {
                            "agent_memories": True,
                            "evolution_snapshots": True,
                            "soul_states": True,
                            "insight_facets": True,
                            "user_preferences": True,
                            "session_summaries": False,
                            "project_summaries": False,
                            "context_rollups": False,
                            "behavior_sources": False,
                            "behavior_nodes": False,
                            "behavior_edges": False,
                            "behavior_snapshots": False,
                            "evolution_nodes": True,
                            "evolution_rejections": True,
                            "evolution_tasks": True,
                        },
                    },
                    {
                        "ok": True,
                        "db": "skill_system",
                        "schema": "skill_system",
                        "error": None,
                        "tables": {
                            "policy_profiles": True,
                            "runs": True,
                            "run_events": True,
                            "refresh_jobs": False,
                            "refresh_job_events": False,
                            "artifact_versions": False,
                        },
                    },
                ],
            ),
            patch.object(
                mod,
                "query_existing_routines",
                return_value={
                    "ok": True,
                    "db": "agent_memory",
                    "schema": "public",
                    "error": None,
                    "routines": {
                        "store_memory": True,
                        "search_memories": True,
                        "memory_health_check": True,
                        "insert_evolution_snapshot": True,
                        "get_evolution_history": True,
                        "get_agent_context": True,
                        "get_soul_state": True,
                        "get_recent_facets": True,
                        "get_user_preferences": True,
                    },
                },
            ),
            patch.object(
                mod,
                "count_agent_memory_categories",
                return_value={
                    "evolution-node": 2,
                    "evolution-rejected": 1,
                    "evolution-snapshot": 0,
                },
            ),
            patch.object(
                mod,
                "build_write_integrity_report",
                return_value={
                    "approve_path_status": "TRANSACTION_SAFE",
                    "reject_path_status": "TRANSACTION_SAFE",
                    "migration_backfill_idempotence": "RERUN_SAFE",
                    "decision_replay_status": "IDEMPOTENT_BY_PROPOSAL_ID",
                    "decision_idempotency_key": "proposal_id",
                    "semantic_identity_fields": [
                        "action",
                        "kind",
                        "summary",
                        "rationale",
                        "suggested_change",
                        "evidence_refs",
                        "requested_parent_node_id",
                    ],
                    "coordination_mechanism": "pg_advisory_xact_lock(proposal_id_hash) + semantic_fingerprint on canonical decision rows",
                    "conflicting_terminal_policy": "BLOCKED_NO_MUTATION",
                    "payload_mismatch_policy": "BLOCKED_NO_MUTATION",
                    "task_authority_model": "agent_tasks_is_lifecycle_authority",
                    "evolution_tasks_role": "mapping_only",
                    "sync_direction": "agent_tasks -> evolution_tasks",
                    "evidence": ["transaction_wrapper_present=True"],
                },
            ),
            patch.object(
                mod,
                "collect_missing_references",
                return_value=[],
            ),
        ):
            report = mod.build_report(
                home_dir=Path("/tmp/home"), repo_root=Path("/tmp/repo")
            )

        self.assertIn("write_integrity_report", report)
        self.assertEqual(
            report["write_integrity_report"]["approve_path_status"], "TRANSACTION_SAFE"
        )
        self.assertEqual(
            report["write_integrity_report"]["reject_path_status"], "TRANSACTION_SAFE"
        )
        self.assertEqual(
            report["write_integrity_report"]["decision_replay_status"],
            "IDEMPOTENT_BY_PROPOSAL_ID",
        )
        self.assertEqual(
            report["write_integrity_report"]["task_authority_model"],
            "agent_tasks_is_lifecycle_authority",
        )

    def test_resolve_memory_target_prefers_explicit_skill_pgdatabase(self):
        mod = _load_module()
        report = mod.resolve_memory_target(
            {
                "SKILL_PGDATABASE": "agent_memory",
                "PGDATABASE": "skill_system",
            }
        )

        self.assertEqual(report["target"], "agent_memory")
        self.assertEqual(report["source"], "SKILL_PGDATABASE(overrides:skill_system)")
        self.assertEqual(
            report["silent_redirection_status"], "blocked-by-explicit-target"
        )
        self.assertTrue(report["aligned"])

    def test_detect_omo_runtime_prefers_cache_resolution(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            config_dir = home / ".config" / "opencode"
            cache_dir = home / ".cache" / "opencode"
            (config_dir).mkdir(parents=True)
            (cache_dir / "node_modules" / "oh-my-opencode" / "dist").mkdir(parents=True)
            (home / ".bun" / "install" / "global" / "node_modules").mkdir(parents=True)

            (config_dir / "opencode.json").write_text(
                json.dumps({"plugin": ["oh-my-opencode"]}), encoding="utf-8"
            )
            (cache_dir / "package.json").write_text(
                json.dumps({"dependencies": {"oh-my-opencode": "3.10.0"}}),
                encoding="utf-8",
            )
            (cache_dir / "node_modules" / "oh-my-opencode" / "package.json").write_text(
                json.dumps({"name": "oh-my-opencode", "version": "3.10.0"}),
                encoding="utf-8",
            )
            entry = cache_dir / "node_modules" / "oh-my-opencode" / "dist" / "index.js"
            entry.write_text("export default {}\n", encoding="utf-8")

            report = mod.detect_omo_runtime(home)

        self.assertTrue(report["enabled_in_config"])
        self.assertEqual(report["actual_resolution_path"], str(entry))
        self.assertEqual(report["resolution_basis"], "project launcher cache")

    def test_build_plugin_report_detects_drift_and_syncs(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            home = root / "home"
            repo_plugin_dir = repo_root / "skills" / "skill-system-memory" / "plugins"
            repo_plugin_dir.mkdir(parents=True)
            repo_plugin = repo_plugin_dir / "skill-system-memory.js"
            repo_runtime = repo_plugin_dir / "runtime_sync.js"
            repo_plugin.write_text("repo plugin\n", encoding="utf-8")
            repo_runtime.write_text("repo runtime\n", encoding="utf-8")

            live_dir = home / ".config" / "opencode" / "plugins"
            live_dir.mkdir(parents=True)
            (live_dir / "skill-system-memory.js").write_text(
                "stale plugin\n", encoding="utf-8"
            )

            report = mod.build_plugin_report(home, repo_root, sync_requested=True)

            self.assertEqual(
                report["previous_drift_status"], "missing-live-runtime-sync"
            )
            self.assertEqual(report["current_drift_status"], "in_sync")
            self.assertTrue((live_dir / "runtime_sync.js").exists())
            state_path = (
                home
                / ".config"
                / "opencode"
                / "skill-system-memory"
                / "plugin-sync.json"
            )
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["source_of_truth"], str(repo_plugin_dir))
            self.assertRegex(
                state["synced_at_utc_minute"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}Z$"
            )


if __name__ == "__main__":
    unittest.main()
