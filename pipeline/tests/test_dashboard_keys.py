#!/usr/bin/env python3
import time
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

from rich.console import Console

# @behavior: experiments.behavior.yaml#heartbeat-and-ui-loop

from experiments import (
    UnifiedDashboard,
    _clean_experiment_artifacts,
    collect_gpu_status_with_error,
    format_terminal_reason_text,
    get_terminal_reason,
    get_completed_result_summary,
    normalize_initial_exp_page,
    reconcile_terminal_artifacts,
)


def make_dashboard(experiments=None, *, is_watch=False):
    cluster_mgr = MagicMock()
    cluster_mgr.stop_node.return_value = (True, "Stopped")
    cluster_mgr.start_node.return_value = (True, "Started")
    cluster_mgr.restart_node.return_value = (True, "Restarted")
    cluster_mgr.get_cluster_status.return_value = {}

    db = MagicMock()
    db.load.return_value = {"experiments": experiments or [], "archived": []}
    db.kill_experiments_on_worker.return_value = 0
    db.kill_experiment.return_value = True
    db.freeze_experiment.return_value = True
    db.rerun_experiment.return_value = True
    db.delete_experiment.return_value = True
    db.archive_experiment.return_value = True
    db.start_experiment_now.return_value = True
    db.move_experiment.return_value = True
    db.get_experiment.return_value = None

    return (
        UnifiedDashboard("worker-1", cluster_mgr, db, is_watch=is_watch),
        cluster_mgr,
        db,
    )


def _wait_for_action(dashboard, predicate, timeout: float = 3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        dashboard.drain_async_updates()
        if predicate():
            return
        time.sleep(0.01)
    dashboard.drain_async_updates()


class TestFocusModeToggle:
    def test_default_focus_mode_is_cluster_outside_watch(self):
        dashboard, _, _ = make_dashboard()
        assert dashboard.focus_mode == "cluster"

    def test_watch_mode_defaults_to_experiments(self):
        dashboard, _, _ = make_dashboard(is_watch=True)
        assert dashboard.focus_mode == "experiments"

    def test_tab_toggles_focus_mode(self):
        dashboard, _, _ = make_dashboard()

        dashboard.handle_key("\t", ["node1"])
        assert dashboard.focus_mode == "experiments"

        dashboard.handle_key("\t", ["node1"])
        assert dashboard.focus_mode == "cluster"

    def test_tab_sets_visible_focus_message(self):
        dashboard, _, _ = make_dashboard()

        dashboard.handle_key("\t", ["node1"])

        assert "Focus -> Experiments" in dashboard.message

    def test_experiment_only_key_autofocuses_from_cluster(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "cluster"
        dashboard.selected_exp_idx = 1
        dashboard.selected_exp_name = "exp-b"

        dashboard.handle_key("T", ["node1"])
        _wait_for_action(dashboard, lambda: db.start_experiment_now.called)

        assert dashboard.focus_mode == "experiments"
        db.start_experiment_now.assert_called_once_with("exp-b")


class TestClusterModeDisableAlsoKillsExperiments:
    def test_d_stops_worker_kills_experiments_and_disables(self):
        dashboard, cluster_mgr, db = make_dashboard()
        dashboard.focus_mode = "cluster"
        dashboard.selected_node_idx = 1
        workers = ["node1", "node2"]

        dashboard.handle_key("D", workers)
        _wait_for_action(dashboard, lambda: cluster_mgr.stop_node.called)

        cluster_mgr.stop_node.assert_called_once_with("node2")
        db.kill_experiments_on_worker.assert_called_once_with("node2")
        db.disable_worker.assert_called_once_with("node2")


class TestClusterModeHotkeys:
    def test_r_restarts_selected_worker(self):
        dashboard, cluster_mgr, _ = make_dashboard(is_watch=True)
        dashboard.focus_mode = "cluster"
        dashboard.selected_node_idx = 0

        dashboard.handle_key("R", ["plusle", "minun"])

        _wait_for_action(dashboard, lambda: cluster_mgr.restart_node.called)
        cluster_mgr.restart_node.assert_called_once_with("plusle", db=dashboard.db)
        assert (
            "Queued RESTART plusle" in dashboard.message
            or "Restarted" in dashboard.message
        )

    def test_s_starts_selected_worker(self):
        dashboard, cluster_mgr, _ = make_dashboard(is_watch=True)
        dashboard.focus_mode = "cluster"
        dashboard.selected_node_idx = 1

        dashboard.handle_key("S", ["plusle", "minun"])

        _wait_for_action(dashboard, lambda: cluster_mgr.start_node.called)
        cluster_mgr.start_node.assert_called_once_with("minun", db=dashboard.db)

    def test_y_cycles_to_next_strategy(self):
        dashboard, _, db = make_dashboard(is_watch=True)
        dashboard.focus_mode = "cluster"
        db.get_allocation_strategy.return_value = "distributed"

        dashboard.handle_key("Y", ["plusle", "minun"])

        _wait_for_action(dashboard, lambda: db.set_allocation_strategy.called)
        db.set_allocation_strategy.assert_called_once_with("centralized")

    def test_numeric_hotkey_sets_strategy_directly(self):
        dashboard, _, db = make_dashboard(is_watch=True)
        dashboard.focus_mode = "cluster"

        dashboard.handle_key("4", ["plusle", "minun"])

        _wait_for_action(dashboard, lambda: db.set_allocation_strategy.called)
        db.set_allocation_strategy.assert_called_once_with("fill-first")

    def test_cluster_panel_renders_visible_strategy_buttons(self):
        dashboard, cluster_mgr, db = make_dashboard(is_watch=True)
        dashboard.focus_mode = "cluster"
        db.get_allocation_strategy.return_value = "distributed"
        cluster_mgr.get_cluster_status.return_value = {
            "node-a": {"status": "ONLINE", "running_jobs": 0, "gpus": []}
        }

        panel = dashboard.build_cluster_panel(
            cluster_mgr.get_cluster_status.return_value, ["node-a"]
        )
        rendered = Console(record=True, width=320)
        rendered.print(panel)
        text = rendered.export_text()

        assert "Strategies:" in text
        assert "(1) distributed" in text
        assert "(4) fill-first" in text


class TestAssignWorkerSafety:
    def test_assign_worker_does_not_stop_old_worker(self):
        experiments = [
            {"name": "exp-a", "status": "RUNNING", "running_on": {"worker": "old-a"}}
        ]
        dashboard, cluster_mgr, db = make_dashboard(experiments=experiments)

        msg = dashboard._run_async_action(
            {
                "type": "assign_worker",
                "name": "exp-a",
                "old_worker": "old-a",
                "new_worker": "new-b",
            }
        )

        assert "Assign exp-a -> new-b" in msg
        db.assign_experiment_worker.assert_called_once_with("exp-a", "new-b")
        cluster_mgr.stop_node.assert_not_called()


class TestClusterActionDbMutationGuard:
    def test_disable_failure_does_not_mutate_db_state(self):
        dashboard, cluster_mgr, db = make_dashboard(is_watch=True)
        cluster_mgr.stop_node.return_value = (False, "ssh refused")

        msg = dashboard._do_action_sync("plusle", "disable")

        assert "✗ DISABLE plusle failed" in msg
        db.kill_experiments_on_worker.assert_not_called()
        db.disable_worker.assert_not_called()

    def test_enable_failure_does_not_mutate_db_state(self):
        dashboard, cluster_mgr, db = make_dashboard(is_watch=True)
        cluster_mgr.start_node.return_value = (False, "ssh timeout")

        msg = dashboard._do_action_sync("plusle", "enable")

        assert "✗ ENABLE plusle failed" in msg
        db.enable_worker.assert_not_called()

    def test_restart_failure_does_not_kill_experiments(self):
        dashboard, cluster_mgr, db = make_dashboard(is_watch=True)
        cluster_mgr.restart_node.return_value = (False, "ssh timeout")

        msg = dashboard._do_action_sync("plusle", "restart")

        assert "✗ RESTART plusle failed" in msg
        db.kill_experiments_on_worker.assert_not_called()


class TestExperimentModeNavigation:
    def test_w_s_navigate_selected_exp_idx_with_boundaries(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}, {"name": "exp-c"}]
        dashboard, _, _ = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 0

        dashboard.handle_key("w", ["node1"])
        assert dashboard.selected_exp_idx == 0

        dashboard.handle_key("s", ["node1"])
        assert dashboard.selected_exp_idx == 1

        dashboard.handle_key("s", ["node1"])
        dashboard.handle_key("s", ["node1"])
        assert dashboard.selected_exp_idx == 2

    def test_n_p_refresh_pagination_from_latest_data(self):
        experiments = [{"name": f"exp-{i}"} for i in range(25)]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.exp_page_size = 20
        dashboard.exp_total_pages = 1
        dashboard.exp_page = 0

        dashboard.handle_key("N", ["node1"])
        assert dashboard.exp_page == 1

        dashboard.handle_key("P", ["node1"])
        assert dashboard.exp_page == 0

    def test_n_p_uses_display_total_with_synthetic_rows(self):
        experiments = [{"name": f"exp-{i}"} for i in range(5)]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.exp_page_size = 20
        dashboard.exp_total_pages = 1
        dashboard.exp_page = 0
        cluster_mgr.get_cluster_status.return_value = {}

        synthetic_rows = [
            {
                "name": f"cond-{i}",
                "status": "NEEDS_RERUN",
                "role": "condition_node",
                "_non_actionable": True,
            }
            for i in range(20)
        ]

        with (
            patch(
                "experiments._build_condition_node_rows", return_value=synthetic_rows
            ),
            patch("experiments._build_staged_matrix_rows", return_value=[]),
        ):
            dashboard.build_experiments_panel({})

        assert dashboard.exp_total_pages == 2

        dashboard.handle_key("N", ["node1"])
        assert dashboard.exp_page == 1

        dashboard.handle_key("P", ["node1"])
        assert dashboard.exp_page == 0

    def test_normalize_initial_exp_page_clamps_into_range(self):
        assert normalize_initial_exp_page(1, 2) == 0
        assert normalize_initial_exp_page(2, 2) == 1
        assert normalize_initial_exp_page(7, 2) == 1
        assert normalize_initial_exp_page(0, 2) == 0


class TestExperimentModeKill:
    def test_k_kills_current_experiment(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 1

        dashboard.handle_key("K", ["node1"])

        _wait_for_action(dashboard, lambda: db.kill_experiment.called)
        db.kill_experiment.assert_called_once_with("exp-b")

    def test_k_alone_uses_selected_scope_by_default(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 1

        dashboard.handle_key("K", ["node1"])

        _wait_for_action(dashboard, lambda: db.kill_experiment.called)
        db.kill_experiment.assert_called_once_with("exp-b")

    def test_footer_shows_two_step_hint_contract(self):
        experiments = [{"name": "exp-a"}]
        dashboard, _, _ = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"

        layout = dashboard.build_layout(running_count=0)
        rendered = Console(record=True, width=520)
        rendered.print(layout["footer"].renderable)
        text = rendered.export_text()

        assert "All" in text
        assert "Selected" in text
        assert "Kill" in text
        assert "Freeze" in text


class TestExperimentModeFreeze:
    def test_f_freezes_current_experiment(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 1

        dashboard.handle_key("F", ["node1"])

        _wait_for_action(dashboard, lambda: db.freeze_experiment.called)
        db.freeze_experiment.assert_called_once_with("exp-b")


class TestExperimentModeTwoStepDeleteArchive:
    def test_d_deletes_current_experiment(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 1

        dashboard.handle_key("d", ["node1"])

        _wait_for_action(dashboard, lambda: db.delete_experiment.called)
        db.delete_experiment.assert_called_once_with("exp-b")

    def test_all_scope_then_d_deletes_all_experiments(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"

        dashboard.handle_key("a", ["node1"])
        dashboard.handle_key("d", ["node1"])

        _wait_for_action(dashboard, lambda: db.delete_experiment.call_count == 2)
        db.delete_experiment.assert_has_calls(
            [
                call("exp-a"),
                call("exp-b"),
            ]
        )

    def test_v_archives_current_experiment(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 0

        dashboard.handle_key("v", ["node1"])

        _wait_for_action(dashboard, lambda: db.archive_experiment.called)
        db.archive_experiment.assert_called_once_with("exp-a")

    def test_all_scope_then_v_archives_all_experiments(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"

        dashboard.handle_key("a", ["node1"])
        dashboard.handle_key("v", ["node1"])

        _wait_for_action(dashboard, lambda: db.archive_experiment.call_count == 2)
        db.archive_experiment.assert_has_calls(
            [
                call("exp-a"),
                call("exp-b"),
            ]
        )


class TestExperimentModeStartNow:
    def test_t_starts_current_experiment_now(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 0

        dashboard.handle_key("T", ["node1"])

        _wait_for_action(dashboard, lambda: db.start_experiment_now.called)
        db.start_experiment_now.assert_called_once_with("exp-a")


class TestExperimentModeRerun:
    def test_r_reruns_current_experiment_clean(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 0

        dashboard.handle_key("R", ["node1"])

        _wait_for_action(dashboard, lambda: db.rerun_experiment.called)
        db.rerun_experiment.assert_called_once_with("exp-a")

    def test_r_reruns_current_experiment_clean(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        with patch("experiments._clean_experiment_artifacts", return_value=["a", "b"]):
            dashboard._run_async_action({"type": "exp_rerun", "name": "exp-a"})

        db.rerun_experiment.assert_called_once_with("exp-a")


class TestCleanExperimentArtifacts:
    def test_clean_experiment_artifacts_removes_resume_and_result_files(self, tmp_path):
        base_dir = tmp_path
        exp_name = "exp-a"
        exp_dir = base_dir / "experiments" / exp_name
        (exp_dir / "checkpoints").mkdir(parents=True)
        (exp_dir / "outputs").mkdir(parents=True)
        (exp_dir / "results_db").mkdir(parents=True)
        (base_dir / "results_db").mkdir(parents=True)
        (base_dir / "logs").mkdir(parents=True)

        files = [
            exp_dir / ".progress",
            exp_dir / "resource_usage.json",
            exp_dir / "outputs" / "results.json",
            exp_dir / "results_db" / f"{exp_name}.json",
            exp_dir / "checkpoints" / "best_model.pt",
            base_dir / "results_db" / f"{exp_name}.json",
            base_dir / "logs" / f"{exp_name}.out",
            base_dir / "logs" / f"{exp_name}.err",
        ]
        for path in files:
            path.write_text("stale", encoding="utf-8")

        with (
            patch("worker.BASE_DIR", base_dir),
            patch("worker.RESULTS_DB_DIR", base_dir / "results_db"),
            patch("worker.LOGS_DIR", base_dir / "logs"),
        ):
            removed = _clean_experiment_artifacts(exp_name)

        assert set(removed) == {
            f"experiments/{exp_name}/.progress",
            f"experiments/{exp_name}/resource_usage.json",
            f"experiments/{exp_name}/outputs",
            f"experiments/{exp_name}/results_db",
            f"experiments/{exp_name}/checkpoints",
            f"results_db/{exp_name}.json",
            f"logs/{exp_name}.out",
            f"logs/{exp_name}.err",
        }
        for path in files:
            assert not path.exists()

    def test_clean_experiment_artifacts_retries_directory_not_empty(self, tmp_path):
        base_dir = tmp_path
        exp_name = "exp-race"
        exp_dir = base_dir / "experiments" / exp_name
        checkpoints = exp_dir / "checkpoints"
        checkpoints.mkdir(parents=True)
        (checkpoints / "best_model.pt").write_text("stale", encoding="utf-8")

        calls = {"count": 0}

        def fake_rmtree(path):
            calls["count"] += 1
            if calls["count"] == 1:
                raise OSError(39, "Directory not empty")
            for child in Path(path).iterdir():
                child.unlink()
            Path(path).rmdir()

        with (
            patch("worker.BASE_DIR", base_dir),
            patch("worker.RESULTS_DB_DIR", base_dir / "results_db"),
            patch("worker.LOGS_DIR", base_dir / "logs"),
            patch("worker.shutil.rmtree", side_effect=fake_rmtree),
            patch("worker.time.sleep"),
        ):
            removed = _clean_experiment_artifacts(exp_name)

        assert f"experiments/{exp_name}/checkpoints" in removed
        assert calls["count"] == 2
        assert not checkpoints.exists()


class TestReconcileTerminalArtifacts:
    def test_reclassifies_script_error_as_oom_from_resource_usage(self, tmp_path):
        base_dir = tmp_path
        exp_name = "SubExp_XL_128K"
        exp_dir = base_dir / "experiments" / exp_name
        exp_dir.mkdir(parents=True)
        (base_dir / "results_db").mkdir(parents=True)
        (base_dir / "logs").mkdir(parents=True)
        failed_at = "2026-03-10T12:04:58.922275+08:00"
        (exp_dir / "resource_usage.json").write_text(
            '{"status": "OOM", "error_type": "OOM", "error_message": "CUDA out of memory", "peak_memory_mb": 6131.12, "is_oom": true}',
            encoding="utf-8",
        )
        db = MagicMock()
        db.load.return_value = {
            "experiments": [
                {
                    "name": exp_name,
                    "status": "NEEDS_RERUN",
                    "error_info": {
                        "type": "SCRIPT_ERROR",
                        "message": "",
                        "failed_at": failed_at,
                    },
                }
            ]
        }
        db.update_experiment.return_value = True

        with (
            patch("artifact.BASE_DIR", base_dir),
            patch("artifact.RESULTS_DB_DIR", base_dir / "results_db"),
            patch("artifact.LOGS_DIR", base_dir / "logs"),
        ):
            repaired = reconcile_terminal_artifacts(db)

        assert repaired == [f"{exp_name}:oom"]
        db.update_experiment.assert_called_once()
        _, payload = db.update_experiment.call_args.args
        assert payload["error_info"]["type"] == "OOM"
        assert payload["error_info"]["message"] == "CUDA out of memory"


class TestExperimentPanelInlineError:
    def test_error_message_is_appended_to_progress(self):
        experiments = [
            {
                "name": "exp-a",
                "status": "NEEDS_RERUN",
                "error_info": {
                    "type": "SCRIPT_ERROR",
                    "message": "Traceback: boom happened in loader",
                },
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}
        dashboard.focus_mode = "experiments"

        panel = dashboard.build_experiments_panel({})
        table = panel.renderable
        terminal_cells = table.columns[8]._cells
        progress_cells = table.columns[9]._cells

        assert any(
            "FAILED_SCRIPT" in cell or "FAILED_SCRIPT_" in cell
            for cell in terminal_cells
        )
        assert any("Traceback:" in cell for cell in progress_cells)
        assert any("loader" in cell for cell in progress_cells)

    def test_recovers_completed_state_from_result_artifact(self, tmp_path):
        base_dir = tmp_path
        exp_name = "SUB_EnsFHr1_H90GCN"
        exp_dir = base_dir / "experiments" / exp_name / "outputs"
        exp_dir.mkdir(parents=True)
        (base_dir / "results_db").mkdir(parents=True)
        (base_dir / "logs").mkdir(parents=True)
        failed_at = "2026-03-10T12:31:56.225592+08:00"
        (exp_dir / "results.json").write_text(
            '{"test_f1": 0.0284, "test_auc": 0.5854, "peak_memory_mb": 0.0}',
            encoding="utf-8",
        )
        db = MagicMock()
        db.load.return_value = {
            "experiments": [
                {
                    "name": exp_name,
                    "status": "NEEDS_RERUN",
                    "error_info": {
                        "type": "ZOMBIE",
                        "message": "Process died unexpectedly",
                        "failed_at": failed_at,
                    },
                }
            ]
        }
        db.update_experiment.return_value = True

        with (
            patch("artifact.BASE_DIR", base_dir),
            patch("artifact.RESULTS_DB_DIR", base_dir / "results_db"),
            patch("artifact.LOGS_DIR", base_dir / "logs"),
        ):
            repaired = reconcile_terminal_artifacts(db)

        assert repaired == [f"{exp_name}:completed"]
        db.update_experiment.assert_called_once()
        _, payload = db.update_experiment.call_args.args
        assert payload["status"] == "COMPLETED"
        assert payload["result"]["f1_score"] == 0.0284
        assert payload["result"]["auc_score"] == 0.5854


class TestTerminalReasonSemantics:
    def test_terminal_reason_text_uses_semantic_colors(self):
        assert format_terminal_reason_text("FAILED_OOM").style == "bold red"
        assert (
            format_terminal_reason_text("FAILED_SCRIPT_ERROR").style == "bold magenta"
        )
        assert format_terminal_reason_text("COMPLETED").style == "bold green"
        assert format_terminal_reason_text("QUEUED_RETRY").style == "bold cyan"

    def test_completed_row_with_failed_artifact_marks_stale_db_truth(self, tmp_path):
        exp_name = "EX_SENIOR_ZEBRA_M1Diag_H20"
        results_dir = tmp_path / "results_db"
        results_dir.mkdir(parents=True)
        (results_dir / f"{exp_name}.json").write_text(
            '{"status":"FAILED","test_f1":0.0,"child_returncode":1,"child_failure_type":"likely_oom","ownership_verdict":"likely_cuda_oom"}',
            encoding="utf-8",
        )
        experiments = [{"name": exp_name, "status": "COMPLETED"}]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}

        with patch("artifact.RESULTS_DB_DIR", results_dir):
            panel = dashboard.build_experiments_panel({})
            rendered = Console(record=True, width=520)
            rendered.print(panel)
            text = rendered.export_text()

        assert "FAILED_WITHOUT" in text
        assert "artifact_failed_" in text

    def test_needs_rerun_script_error_shows_failed_script_error(self):
        experiments = [
            {
                "name": "exp-a",
                "status": "NEEDS_RERUN",
                "error_info": {"type": "SCRIPT_ERROR", "message": "boom"},
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}
        panel = dashboard.build_experiments_panel({})
        rendered = Console(record=True, width=180)
        rendered.print(panel)
        text = rendered.export_text()

        assert "FAILED_SCRIPT" in text or "SCR" in text or "boom" in text

    def test_waiting_row_shows_queued_retry(self):
        experiments = [{"name": "exp-a", "status": "NEEDS_RERUN"}]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}
        panel = dashboard.build_experiments_panel({})
        rendered = Console(record=True, width=180)
        rendered.print(panel)
        text = rendered.export_text()

        assert "QUEUED_RETRY" in text

    def test_soft_oom_needs_rerun_keeps_queued_retry_terminal_reason(self):
        assert (
            get_terminal_reason(
                "exp-a",
                "NEEDS_RERUN",
                None,
                {"type": "OOM", "is_true_oom": False, "peak_memory_mb": 12000},
            )
            == "QUEUED_RETRY"
        )

    def test_true_oom_needs_rerun_reports_failed_oom(self):
        assert (
            get_terminal_reason(
                "exp-a",
                "NEEDS_RERUN",
                None,
                {"type": "OOM", "is_true_oom": True, "peak_memory_mb": 24000},
            )
            == "FAILED_OOM"
        )

    def test_condition_parent_unmet_row_shows_blocked_condition(self):
        experiments = [
            {
                "name": "exp-child",
                "status": "NEEDS_RERUN",
                "condition_parent": "exp-parent",
                "progression_status": "BLOCKED_CONDITION",
                "block_reason": "condition_parent_unmet:exp-parent",
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}

        panel = dashboard.build_experiments_panel({})
        rendered = Console(record=True, width=520)
        rendered.print(panel)
        text = rendered.export_text()

        assert "Blocked by" in text
        assert "condition" in text
        assert "blocked" in text.lower()


class TestConditionNodeSurface:
    def test_runtime_condition_nodes_are_materialized_in_panel_rows(self):
        dashboard, cluster_mgr, _ = make_dashboard(experiments=[])
        cluster_mgr.get_cluster_status.return_value = {}

        condition_rows_mock = [
            {
                "name": "G0_SENIOR_CONTRACT",
                "role": "condition_node",
                "status": "COMPLETED",
                "depends_on": [],
                "description": "senior contract",
            },
            {
                "name": "G2_PHASE1_SMOKE_A",
                "role": "condition_node",
                "status": "COMPLETED",
                "depends_on": [],
                "description": "smoke A",
            },
            {
                "name": "G3_PHASE1_SMOKE_B",
                "role": "condition_node",
                "status": "COMPLETED",
                "depends_on": [],
                "description": "smoke B",
            },
            {
                "name": "D1_PHASE1_ROOT_CAUSE",
                "role": "condition_node",
                "status": "NEEDS_RERUN",
                "condition_parent": "G2_PHASE1_SMOKE_A",
                "depends_on": ["G2_PHASE1_SMOKE_A", "G3_PHASE1_SMOKE_B"],
                "description": "root cause",
                "_non_actionable": True,
            },
        ]
        with patch("experiments._build_condition_node_rows", return_value=condition_rows_mock):
            with patch("experiments._build_staged_matrix_rows", return_value=[]):
                dashboard.build_experiments_panel({})

        condition_rows = [
            row for row in dashboard._panel_exp_rows if row.get("role") == "condition_node"
        ]
        condition_names = {str(row.get("name")) for row in condition_rows}
        assert {"G0_SENIOR_CONTRACT", "D1_PHASE1_ROOT_CAUSE"}.issubset(condition_names)

        d1 = next(row for row in condition_rows if row.get("name") == "D1_PHASE1_ROOT_CAUSE")
        assert d1.get("condition_parent") == "G2_PHASE1_SMOKE_A"
        assert d1.get("depends_on") == ["G2_PHASE1_SMOKE_A", "G3_PHASE1_SMOKE_B"]
        assert d1.get("_non_actionable") is True

    def test_bulk_delete_skips_display_only_condition_nodes(self):
        experiments = [{"name": "exp-a"}]
        dashboard, cluster_mgr, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        cluster_mgr.get_cluster_status.return_value = {}
        dashboard.build_experiments_panel({})

        dashboard.handle_key("a", ["node1"])
        dashboard.handle_key("d", ["node1"])

        _wait_for_action(dashboard, lambda: db.delete_experiment.called)
        db.delete_experiment.assert_called_once_with("exp-a")

    def test_staged_matrix_leaves_are_materialized_as_blocked_non_actionable_rows(self):
        dashboard, cluster_mgr, _ = make_dashboard(
            experiments=[
                {
                    "name": "D1_PHASE1_ROOT_CAUSE",
                    "status": "COMPLETED",
                    "role": "condition_node",
                }
            ]
        )
        dashboard.exp_page_size = 64
        cluster_mgr.get_cluster_status.return_value = {}

        staged_rows_mock = [
            {
                "name": "EXP_P1_GS_H64_ORIGIN_V1",
                "role": "staged_matrix_leaf",
                "condition_parent": "D1_PHASE1_ROOT_CAUSE",
                "progression_status": "READY",
                "_non_actionable": True,
                "batch_id": "matrix-staged",
            },
            {
                "name": "EXP_P3_GS_H64_SENIOR10_V1",
                "role": "staged_matrix_leaf",
                "condition_parent": "D1_PHASE1_ROOT_CAUSE",
                "progression_status": "READY",
                "_non_actionable": True,
                "batch_id": "matrix-staged",
            },
            {
                "name": "EXP_P1_ZB_H30_COMBINED_V1",
                "role": "staged_matrix_leaf",
                "condition_parent": "D1_PHASE1_ROOT_CAUSE",
                "progression_status": "READY",
                "_non_actionable": True,
                "batch_id": "matrix-staged",
            },
            {
                "name": "EXP_P3_ZB_H30_ORIGIN_V1",
                "role": "staged_matrix_leaf",
                "condition_parent": "D1_PHASE1_ROOT_CAUSE",
                "progression_status": "READY",
                "_non_actionable": True,
                "batch_id": "matrix-staged",
            },
        ]
        with patch("experiments._build_staged_matrix_rows", return_value=staged_rows_mock):
            with patch("experiments._build_condition_node_rows", return_value=[]):
                dashboard.build_experiments_panel({})

        staged_rows = [
            row
            for row in dashboard._panel_exp_rows
            if row.get("role") == "staged_matrix_leaf"
        ]

        assert len(staged_rows) == 4
        names = {str(row.get("name")) for row in staged_rows}
        assert "EXP_P1_GS_H64_ORIGIN_V1" in names
        assert "EXP_P1_ZB_H30_COMBINED_V1" in names
        assert "EXP_P3_GS_H64_SENIOR10_V1" in names
        assert "EXP_P3_ZB_H30_ORIGIN_V1" in names

        sample = next(
            row for row in staged_rows if row.get("name") == "EXP_P1_GS_H64_ORIGIN_V1"
        )
        assert sample.get("condition_parent") == "D1_PHASE1_ROOT_CAUSE"
        assert sample.get("progression_status") == "READY"
        assert sample.get("_non_actionable") is True
        assert sample.get("batch_id") == "matrix-staged"

    def test_bulk_archive_skips_staged_matrix_leaf_rows(self):
        dashboard, cluster_mgr, db = make_dashboard(experiments=[])
        dashboard.focus_mode = "experiments"
        cluster_mgr.get_cluster_status.return_value = {}
        dashboard.build_experiments_panel({})

        dashboard.handle_key("a", ["node1"])
        dashboard.handle_key("v", ["node1"])

        assert not db.archive_experiment.called

    def test_running_progress_shows_valf1_label(self):
        experiments = [
            {
                "name": "exp-running-progress",
                "status": "RUNNING",
                "preferred_worker": "worker-a",
                "running_on": {"worker": "worker-a", "gpu": 0, "pid": 123},
                "batch_id": "phase3-baseline",
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}

        with patch(
            "experiments.get_experiment_progress",
            return_value={
                "percent": 75,
                "epoch": 4,
                "total_epochs": 10,
                "val_f1": 0.8123,
            },
        ):
            panel = dashboard.build_experiments_panel({})

        rendered = Console(record=True, width=520)
        rendered.print(panel)
        text = rendered.export_text()

        assert "valF1=0.812" in text
        assert " testF1=" not in text

    def test_running_progress_shows_warmed_when_epoch_passes_threshold(self):
        experiments = [
            {
                "name": "exp-running-warmed",
                "status": "RUNNING",
                "batch_id": "phase3-baseline",
                "running_on": {"worker": "worker-a", "gpu": 0, "pid": 321},
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}

        with (
            patch("experiments.WARMUP_COMPLETION_EPOCH", 1),
            patch(
                "experiments.get_experiment_progress",
                return_value={
                    "percent": 0.0,
                    "epoch": 1,
                    "total_epochs": 10,
                    "val_f1": 0.101,
                },
            ),
        ):
            panel = dashboard.build_experiments_panel({})

        rendered = Console(record=True, width=520)
        rendered.print(panel)
        text = rendered.export_text()

        assert "warmed" in text
        assert "full-run" not in text


class TestCompletedMetricSummary:
    def test_ignores_historical_targeted_test_f1_residue(self, tmp_path):
        exp_name = "EX_PHASE3_GraphSAGE_Baseline_LeakSafe"
        results_dir = tmp_path / "results_db"
        results_dir.mkdir(parents=True)
        (results_dir / f"{exp_name}.json").write_text(
            '{"test_f1": 0.0192, "targeted_test_f1": 0.1089, "epochs_ran": 7}',
            encoding="utf-8",
        )

        with patch("artifact.RESULTS_DB_DIR", results_dir):
            epochs, f1 = get_completed_result_summary(exp_name, {"f1_score": 0.0192})

        assert epochs == 7
        assert f1 == 0.0192

    def test_prefers_db_canonical_result_when_present(self):
        epochs, f1 = get_completed_result_summary(
            "exp-a",
            {"f1_score": 0.0192},
            {"test_f1": 0.2222, "epochs_ran": 7},
        )

        assert epochs == 7
        assert f1 == 0.2222


class TestGpuProbeParsing:
    def test_collect_gpu_status_with_error_tolerates_na_util(self):
        output = "0, 10240, 1024, 11264, N/A\n"
        with patch("experiments.subprocess.check_output", return_value=output):
            gpus, err = collect_gpu_status_with_error()

        assert err == ""
        assert gpus == [
            {"index": 0, "free": 10240, "used": 1024, "total": 11264, "util": 0}
        ]


class TestExperimentPanelWatchSemantics:
    def test_shows_lifecycle_preferred_actual_pid_and_wait_reason(self):
        experiments = [
            {
                "name": "exp-running",
                "status": "RUNNING",
                "preferred_worker": "plusle",
                "running_on": {"worker": "minun", "gpu": 1, "pid": 12345},
                "batch_id": "phase3-baseline-pair-mainline",
            },
            {
                "name": "exp-frozen",
                "status": "NEEDS_RERUN",
                "preferred_worker": "plusle",
                "error_info": {"type": "MANUAL_FREEZE", "message": "manual stop"},
            },
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {
            "minun": {
                "status": "ONLINE",
                "running_jobs": 1,
                "running_experiments": ["exp-running"],
                "gpus": [],
            },
            "plusle": {
                "status": "ONLINE",
                "running_jobs": 0,
                "running_experiments": [],
                "gpus": [],
            },
        }

        panel = dashboard.build_experiments_panel(
            cluster_mgr.get_cluster_status.return_value
        )
        rendered = Console(record=True, width=520)
        rendered.print(panel)
        text = rendered.export_text()

        assert "Lifecycle" in text
        assert "Preferred" in text
        assert "Actual" in text
        assert "PID" in text
        assert "Wait" in text
        assert "full-run" in text
        assert "plusle" in text
        assert "minun:1" in text
        assert "12345" in text
        assert "manual_fre" in text

    def test_shows_memory_contract_fields(self):
        experiments = [
            {
                "name": "exp-memory",
                "status": "NEEDS_RERUN",
                "memory_contract": {
                    "memory_family": "fullbatch",
                    "est_mem_decision_mb": 7732,
                    "execution_mode": "fullbatch",
                    "neighborloader_recommended": True,
                },
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}

        panel = dashboard.build_experiments_panel({})
        rendered = Console(record=True, width=520)
        rendered.print(panel)
        text = rendered.export_text()

        assert "MemFam" in text
        assert "EstMB" in text
        assert "Mode" in text
        assert "NBLdr" in text
        assert "7732" in text
        assert "fullbatch" in text
        assert "reco" in text

    def test_vgate_prefers_assigned_worker_gpu_free_memory(self):
        experiments = [
            {
                "name": "exp-assigned",
                "status": "NEEDS_RERUN",
                "preferred_worker": "minun",
                "memory_contract": {
                    "memory_family": "fullbatch",
                    "est_mem_decision_mb": 9000,
                    "execution_mode": "fullbatch",
                    "neighborloader_recommended": False,
                },
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(
            experiments=experiments, is_watch=True
        )
        cluster_mgr.get_cluster_status.return_value = {
            "plusle": {"status": "ONLINE", "gpus": [{"index": 0, "free": 20000}]},
            "minun": {"status": "ONLINE", "gpus": [{"index": 0, "free": 8000}]},
        }

        panel = dashboard.build_experiments_panel(
            cluster_mgr.get_cluster_status.return_value
        )
        rendered = Console(record=True, width=520)
        rendered.print(panel)
        text = rendered.export_text()

        assert "7.8/8.8G !!" in text

    def test_backfills_memory_contract_when_row_is_missing_it(self, monkeypatch):
        experiments = [
            {
                "name": "EX_PHASE3_GraphSAGE_Baseline_LeakSafe",
                "status": "COMPLETED",
                "script": "experiments/EX_PHASE3_GraphSAGE_Baseline_LeakSafe/scripts/train.py",
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}
        monkeypatch.setattr(
            "memory_contract.infer_memory_contract_for_exp",
            lambda exp, *_args, **_kwargs: {
                "memory_family": "fullbatch_sparse_gnn",
                "execution_mode": "fullbatch",
                "est_mem_decision_mb": 7748,
                "neighborloader_applicable": True,
            },
        )

        panel = dashboard.build_experiments_panel({})
        rendered = Console(record=True, width=520)
        rendered.print(panel)
        text = rendered.export_text()

        assert "7748" in text
        assert "fullbatch" in text

    def test_reestimates_stale_memory_contract_when_artifact_hidden_dim_conflicts(
        self, monkeypatch, tmp_path
    ):
        experiments = [
            {
                "name": "EXP_P3_ZB_H30_COMBINED_V1",
                "status": "COMPLETED",
                "script": "experiments/EXP_P3_ZB_H30_COMBINED_V1/scripts/train.py",
                "memory_contract": {
                    "hidden_dim": 25,
                    "memory_family": "no_batch_path_child",
                    "est_mem_decision_mb": 22919,
                    "execution_mode": "fullgraph_no_batch_path",
                    "neighborloader_applicable": False,
                },
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}
        monkeypatch.setattr("artifact.RESULTS_DB_DIR", tmp_path)
        (tmp_path / "EXP_P3_ZB_H30_COMBINED_V1.json").write_text(
            '{"hidden_dim": 30, "test_f1": 0.0953}', encoding="utf-8"
        )
        monkeypatch.setattr(
            "memory_contract.infer_memory_contract_for_exp",
            lambda exp, *_args, **_kwargs: {
                "hidden_dim": 30,
                "memory_family": "no_batch_path_child",
                "execution_mode": "fullgraph_no_batch_path",
                "est_mem_decision_mb": 6103,
                "neighborloader_applicable": False,
            },
        )

        panel = dashboard.build_experiments_panel({})
        rendered = Console(record=True, width=220)
        rendered.print(panel)
        text = rendered.export_text()

        assert "6103" in text
        assert "22919" not in text

    def test_probe_rows_do_not_display_metric_f1(self):
        experiments = [
            {
                "name": "EX_SENIOR_ZEBRA_FG_Probe_H10",
                "status": "COMPLETED",
                "role": "diagnostic_compatibility_probe",
                "result": {"f1_score": 0.3577, "peak_memory_mb": 327},
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}

        panel = dashboard.build_experiments_panel({})
        table = panel.renderable
        name_cells = table.columns[1]._cells
        progress_cells = table.columns[9]._cells
        f1_cells = table.columns[14]._cells

        assert any("EX_SENIOR_ZEBRA_FG_Probe_H10" in cell for cell in name_cells)
        assert all("testF1=" not in cell for cell in progress_cells)
        assert all("0.3577" not in cell for cell in f1_cells)

    def test_reclassifies_zombie_as_oom_when_stderr_is_empty(self, tmp_path):
        base_dir = tmp_path
        exp_name = "SubExp_XXL_256K"
        exp_dir = base_dir / "experiments" / exp_name
        exp_dir.mkdir(parents=True)
        (base_dir / "results_db").mkdir(parents=True)
        (base_dir / "logs").mkdir(parents=True)
        (base_dir / "logs" / f"{exp_name}.err").write_text("", encoding="utf-8")
        (exp_dir / "resource_usage.json").write_text(
            '{"status": "OOM", "error_type": "OOM", "error_message": "CUDA out of memory", "peak_memory_mb": 6131.12, "is_oom": true}',
            encoding="utf-8",
        )
        db = MagicMock()
        db.load.return_value = {
            "experiments": [
                {
                    "name": exp_name,
                    "status": "NEEDS_RERUN",
                    "error_info": {
                        "type": "ZOMBIE",
                        "message": "Process 123 died unexpectedly",
                        "failed_at": "2026-03-10T15:34:04.921273+08:00",
                    },
                }
            ]
        }
        db.update_experiment.return_value = True

        with (
            patch("artifact.BASE_DIR", base_dir),
            patch("artifact.RESULTS_DB_DIR", base_dir / "results_db"),
            patch("artifact.LOGS_DIR", base_dir / "logs"),
        ):
            repaired = reconcile_terminal_artifacts(db)

        assert repaired == [f"{exp_name}:oom"]
        _, payload = db.update_experiment.call_args.args
        assert payload["error_info"]["type"] == "OOM"

    def test_running_row_reports_heartbeat_source_unavailable(self):
        experiments = [
            {
                "name": "exp-running",
                "status": "RUNNING",
                "running_on": {"worker": "worker-a", "gpu": 0, "pid": 123},
                "batch_id": "phase3-baseline",
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}

        panel = dashboard.build_experiments_panel({})
        table = panel.renderable
        progress_cells = table.columns[9]._cells

        assert any("Heartbeat source unavailable" in cell for cell in progress_cells)

    def test_running_row_reports_waiting_specific_worker_heartbeat(self):
        experiments = [
            {
                "name": "exp-running",
                "status": "RUNNING",
                "running_on": {"worker": "worker-a", "gpu": 0, "pid": 123},
                "batch_id": "phase3-baseline",
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {
            "worker-b": {
                "status": "ONLINE",
                "running_jobs": 1,
                "running_experiments": ["other-exp"],
                "gpus": [],
            }
        }

        panel = dashboard.build_experiments_panel(
            cluster_mgr.get_cluster_status.return_value
        )
        table = panel.renderable
        progress_cells = table.columns[9]._cells

        assert any("Awaiting heartbeat (worker-a)" in cell for cell in progress_cells)

    def test_running_row_shows_warmup_wait_bar_for_epoch_zero_progress(self):
        experiments = [
            {
                "name": "exp-running-progress",
                "status": "RUNNING",
                "running_on": {
                    "worker": "worker-a",
                    "gpu": 0,
                    "pid": 123,
                    "started_at": "2026-03-18T15:00:00+08:00",
                },
                "batch_id": "phase3-baseline",
            }
        ]
        dashboard, cluster_mgr, _ = make_dashboard(experiments=experiments)
        cluster_mgr.get_cluster_status.return_value = {}

        with (
            patch(
                "experiments.get_experiment_progress",
                return_value={
                    "percent": 0,
                    "epoch": 0,
                    "total_epochs": 50,
                    "val_f1": 0.0,
                    "timestamp": "2026-03-18T15:00:30+08:00",
                },
            ),
            patch("experiments.time.time", return_value=1742281290.0),
        ):
            panel = dashboard.build_experiments_panel({})

        table = panel.renderable
        lifecycle_cells = table.columns[3]._cells
        progress_cells = table.columns[9]._cells

        assert any(str(cell).strip().lower() == "warm" for cell in lifecycle_cells)
        assert any("E0/50" in cell for cell in progress_cells)


class TestLocalPidStopSafety:
    def test_try_stop_skips_remote_worker_pid(self):
        dashboard, _, db = make_dashboard()
        db.get_experiment.return_value = {
            "name": "exp-a",
            "running_on": {"worker": "other-worker", "pid": 12345},
        }

        with patch("experiments._kill_local_pid_tree") as mock_kill:
            stopped, pid = dashboard._try_stop_local_experiment_pid("exp-a")

        assert stopped is False
        assert pid is None
        mock_kill.assert_not_called()


class TestCascadeRerunKill:
    def test_main_rerun_cascades_to_children(self):
        experiments = [
            {"name": "main", "role": "main"},
            {"name": "child-a", "parent_experiment": "main", "role": "child"},
            {"name": "child-b", "parent_experiment": "main", "role": "child"},
        ]
        dashboard, _, db = make_dashboard(experiments=experiments)

        dashboard._run_async_action({"type": "exp_rerun", "name": "main"})

        assert db.rerun_experiment.call_count == 3
        called_names = [call.args[0] for call in db.rerun_experiment.call_args_list]
        assert called_names[0] == "main"
        assert set(called_names[1:]) == {"child-a", "child-b"}

    def test_rerun_cascade_does_not_include_completed_children(self):
        experiments = [
            {"name": "main", "role": "main"},
            {"name": "child-a", "parent_experiment": "main", "role": "child"},
        ]
        dashboard, _, db = make_dashboard(experiments=experiments)
        db.load.return_value = {
            "experiments": experiments,
            "completed": [
                {"name": "child-done", "parent_experiment": "main", "status": "COMPLETED"}
            ],
            "archived": [],
        }

        dashboard._run_async_action({"type": "exp_rerun", "name": "main"})

        called_names = [call.args[0] for call in db.rerun_experiment.call_args_list]
        assert "child-done" not in called_names
        assert set(called_names) == {"main", "child-a"}


# @behavior: experiments.behavior.yaml#cluster-f-retry-failed-only
class TestClusterFRetryFailedOnly:
    """F in cluster mode calls reset_failed_experiments which must only
    target terminal-failed rows (true OOM, SCRIPT_ERROR, ZOMBIE, PID_MISSING).
    SQL-level filtering is in db_registry; here we verify key wiring."""

    def test_f_in_cluster_mode_calls_reset_failed(self):
        dashboard, _, db = make_dashboard(is_watch=True)
        dashboard.focus_mode = "cluster"
        db.reset_failed_experiments.return_value = 3

        dashboard.handle_key("F", ["plusle", "minun"])

        _wait_for_action(dashboard, lambda: db.reset_failed_experiments.called)
        db.reset_failed_experiments.assert_called_once()

    def test_f_in_cluster_mode_reports_count(self):
        dashboard, _, db = make_dashboard(is_watch=True)
        dashboard.focus_mode = "cluster"
        db.reset_failed_experiments.return_value = 2

        dashboard.handle_key("F", ["plusle", "minun"])

        _wait_for_action(dashboard, lambda: db.reset_failed_experiments.called)
        dashboard.drain_async_updates()
        assert "2" in dashboard.message or "Reset" in dashboard.message

    def test_f_in_cluster_mode_no_failed_reports_zero(self):
        dashboard, _, db = make_dashboard(is_watch=True)
        dashboard.focus_mode = "cluster"
        db.reset_failed_experiments.return_value = 0

        dashboard.handle_key("F", ["plusle", "minun"])

        _wait_for_action(dashboard, lambda: db.reset_failed_experiments.called)
        dashboard.drain_async_updates()
        assert "No failed" in dashboard.message or "0" in dashboard.message

    def test_f_in_experiment_mode_does_not_call_reset_failed(self):
        """F in experiments mode is 'freeze', not 'retry failed'."""
        experiments = [{"name": "exp-a"}]
        dashboard, _, db = make_dashboard(experiments=experiments, is_watch=True)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 0

        dashboard.handle_key("F", ["plusle"])

        db.reset_failed_experiments.assert_not_called()


class TestExperimentModePriorityMove:
    def test_u_moves_current_experiment_up(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 1

        dashboard.handle_key("U", ["node1"])
        _wait_for_action(dashboard, lambda: db.move_experiment.called)

        db.move_experiment.assert_called_once_with("exp-b", "up")

    def test_j_moves_current_experiment_down(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 0

        dashboard.handle_key("J", ["node1"])
        _wait_for_action(dashboard, lambda: db.move_experiment.called)


class TestRePipeSelectionCorrectness:
    def test_p_repipes_selected_experiment_by_name(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}, {"name": "exp-c"}]
        dashboard, _, db = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 1
        dashboard.selected_exp_name = "exp-b"

        with patch(
            "experiments._enqueue_repipeline_ready", return_value=True
        ) as mock_enq:
            with patch(
                "experiments._delete_experiment_from_registry", return_value=True
            ):
                dashboard.handle_key("p", ["node1"])
                _wait_for_action(dashboard, lambda: mock_enq.called)

        mock_enq.assert_called_once_with("exp-b", {"name": "exp-b"})

    def test_p_on_non_actionable_row_shows_message(self):
        experiments = [
            {"name": "cond-1", "_non_actionable": True},
        ]
        dashboard, _, _ = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 0
        dashboard.selected_exp_name = "cond-1"

        result = dashboard.handle_key("p", ["node1"])
        assert result is True
        assert dashboard.message and "display-only" in dashboard.message

    def test_p_with_stale_index_still_targets_correct_name(self):
        experiments = [{"name": "exp-a"}, {"name": "exp-b"}, {"name": "exp-c"}]
        dashboard, _, _ = make_dashboard(experiments=experiments)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 0
        dashboard.selected_exp_name = "exp-b"

        with patch(
            "experiments._enqueue_repipeline_ready", return_value=True
        ) as mock_enq:
            with patch(
                "experiments._delete_experiment_from_registry", return_value=True
            ):
                dashboard.handle_key("p", ["node1"])
                _wait_for_action(dashboard, lambda: mock_enq.called)

        mock_enq.assert_called_once_with("exp-b", {"name": "exp-b"})
