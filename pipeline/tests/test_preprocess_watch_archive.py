#!/usr/bin/env python3

import os
from types import SimpleNamespace

import preprocess


def test_render_watch_panel_shows_archive_controls_and_selection(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_rows",
        lambda _computing=None: [
            {
                "name": "base_34dim",
                "artifact_id": "artifact_base_34dim_cut_d152",
                "status": "GENERATED",
                "total_dim": "34",
                "depends_on": "compute_base_cutoff_features.py",
            }
        ],
    )
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_snapshot_rows",
        lambda: [
            {
                "name": "exp-a",
                "status": "COMPLETED",
                "parent": "-",
                "batch": "b1",
                "bucket": "completed",
                "mem_family": "fullbatch",
                "est_mb": "7732",
                "mem_mode": "fullbatch",
                "nbldr": "reco",
            }
        ],
    )
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {"ready_to_process": 0, "batch_id": "-", "experiments": []},
    )

    panel, _, _ = preprocess._render_watch_panel(0, 20, "exp-a", "hello")

    assert "All" in panel
    assert "Selected" in panel
    assert "Archive" in panel
    assert "Clear" in panel
    assert "hello" in panel
    assert "exp-a" in panel
    assert "Feature Bank Overview" in panel
    assert "artifact_base_34dim_cut_d152" in panel
    assert "depends_on" in panel
    assert "Current computing" in panel
    assert "MemFam" in panel
    assert "EstMB" in panel
    assert "Mode" in panel
    assert "NBLdr" in panel
    assert "7732" in panel
    assert "fullbatch" in panel


def test_archive_selected_requires_completed(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_snapshot_rows",
        lambda: [
            {
                "name": "exp-a",
                "status": "RUNNING",
                "parent": "-",
                "batch": "b1",
                "bucket": "active",
            }
        ],
    )

    archive_mod = SimpleNamespace(
        archive_selected_experiments=lambda names=None: {
            "count": len(names or []),
            "names": names or [],
            "batch_report": "/tmp/force_BATCH_REPORT.md",
        }
    )
    monkeypatch.setattr(preprocess, "_load_archive_module", lambda: archive_mod)

    msg = preprocess._archive_selected_from_watch("exp-a")

    assert "Archived exp-a" in msg
    assert "force_BATCH_REPORT.md" in msg


def test_archive_selected_completed_calls_archive_module(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_snapshot_rows",
        lambda: [
            {
                "name": "exp-a",
                "status": "COMPLETED",
                "parent": "-",
                "batch": "b1",
                "bucket": "completed",
            }
        ],
    )
    archive_mod = SimpleNamespace(
        archive_completed_experiments=lambda names=None: {
            "count": 1,
            "names": names or [],
            "batch_report": "/tmp/demo_BATCH_REPORT.md",
        }
    )
    monkeypatch.setattr(preprocess, "_load_archive_module", lambda: archive_mod)

    msg = preprocess._archive_selected_from_watch("exp-a")

    assert "Archived exp-a" in msg
    assert "demo_BATCH_REPORT.md" in msg


def test_archive_selected_returns_missing_when_archive_module_reports_zero(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_snapshot_rows",
        lambda: [
            {
                "name": "exp-a",
                "status": "COMPLETED",
                "parent": "-",
                "batch": "b1",
                "bucket": "completed",
            }
        ],
    )
    archive_mod = SimpleNamespace(
        archive_completed_experiments=lambda names=None: {
            "count": 0,
            "names": names or [],
            "batch_report": "/tmp/demo_BATCH_REPORT.md",
        }
    )
    monkeypatch.setattr(preprocess, "_load_archive_module", lambda: archive_mod)

    msg = preprocess._archive_selected_from_watch("exp-a")

    assert msg == "No archive target for exp-a"


def test_archive_all_completed_calls_archive_module(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_snapshot_rows",
        lambda: [
            {
                "name": "exp-a",
                "status": "COMPLETED",
                "parent": "-",
                "batch": "b1",
                "bucket": "completed",
            },
            {
                "name": "exp-b",
                "status": "RUNNING",
                "parent": "-",
                "batch": "b1",
                "bucket": "active",
            },
        ],
    )
    archive_mod = SimpleNamespace(
        archive_completed_experiments=lambda names=None: {
            "count": len(names or []),
            "names": names or [],
            "batch_report": "/tmp/all_BATCH_REPORT.md",
        }
    )
    monkeypatch.setattr(preprocess, "_load_archive_module", lambda: archive_mod)

    msg = preprocess._archive_all_completed_from_watch()

    assert "Archived 1 completed" in msg
    assert "all_BATCH_REPORT.md" in msg


def test_clear_latest_archive_calls_archive_module(monkeypatch):
    archive_mod = SimpleNamespace(
        clear_latest_archive_artifacts=lambda: {"count": 3, "removed": ["a", "b", "c"]}
    )
    monkeypatch.setattr(preprocess, "_load_archive_module", lambda: archive_mod)

    msg = preprocess._clear_latest_archive_from_watch()

    assert msg == "Cleared latest archive artifacts (3 files)"


def test_run_watch_tick_calls_run_once(monkeypatch):
    state = {"count": 0}

    def _fake_run_once():
        state["count"] += 1

    monkeypatch.setattr(preprocess, "run_once", _fake_run_once)

    msg = preprocess._run_watch_tick("")

    assert state["count"] == 1
    assert msg.startswith("tick ok @")


def test_run_watch_nontty_executes_tick_before_render(monkeypatch, capsys):
    monkeypatch.setattr(preprocess.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(preprocess, "run_once", lambda: None)
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_rows",
        lambda _computing=None: [
            {
                "name": "base_34dim",
                "artifact_id": "artifact_base_34dim_cut_d152",
                "status": "GENERATED",
                "total_dim": "34",
                "depends_on": "compute_base_cutoff_features.py",
            }
        ],
    )
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_snapshot_rows",
        lambda: [
            {
                "name": "exp-a",
                "status": "COMPLETED",
                "parent": "-",
                "batch": "b1",
                "bucket": "completed",
            }
        ],
    )
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {"ready_to_process": 0, "batch_id": "-", "experiments": []},
    )

    preprocess.run_watch(10, 20)
    output = capsys.readouterr().out

    assert "tick ok @" in output
    assert "Phase3 preprocess.py --watch" in output


def test_run_watch_uses_two_step_archive_actions_with_direct_navigation(monkeypatch):
    monkeypatch.setattr(preprocess.sys.stdin, "isatty", lambda: True)

    class _NoopRaw:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    keys = iter(["s", "S", "v", "A", "v", "S", "c", "q"])
    calls = {"move": 0, "selected": 0, "all": 0, "clear": 0}

    monkeypatch.setattr(preprocess, "_RawInputMode", _NoopRaw)
    monkeypatch.setattr(preprocess, "time", SimpleNamespace(time=lambda: 0.0))
    monkeypatch.setattr(
        preprocess, "_run_watch_tick", lambda _msg: "tick ok @ 12:34:56"
    )
    monkeypatch.setattr(preprocess, "_collect_watch_rows", lambda: [])
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_snapshot_rows",
        lambda: [{"name": "exp-a", "status": "COMPLETED", "bucket": "completed"}],
    )
    monkeypatch.setattr(
        preprocess,
        "_resolve_watch_selection",
        lambda _rows, selected: selected or "exp-a",
    )
    monkeypatch.setattr(
        preprocess,
        "_move_watch_selection",
        lambda _rows, _selected, _delta: calls.__setitem__("move", calls["move"] + 1)
        or "exp-a",
    )
    monkeypatch.setattr(
        preprocess,
        "_archive_selected_from_watch",
        lambda _name: calls.__setitem__("selected", calls["selected"] + 1)
        or "selected archived",
    )
    monkeypatch.setattr(
        preprocess,
        "_archive_all_completed_from_watch",
        lambda: calls.__setitem__("all", calls["all"] + 1) or "all archived",
    )
    monkeypatch.setattr(
        preprocess,
        "_clear_latest_archive_from_watch",
        lambda: calls.__setitem__("clear", calls["clear"] + 1) or "latest cleared",
    )
    monkeypatch.setattr(
        preprocess,
        "_render_watch_panel",
        lambda page, _page_size, _selected, _status: (f"panel-{page}", 1, page),
    )
    monkeypatch.setattr(
        preprocess,
        "_read_key_nonblocking",
        lambda _timeout: next(keys),
    )

    preprocess.run_watch(interval=10, page_size=20)

    assert calls["move"] == 1
    assert calls["selected"] == 1
    assert calls["all"] == 1
    assert calls["clear"] == 1


def test_collect_watch_rows_backfills_memory_contract(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "completed": [
                {
                    "name": "EX_PHASE3_GraphSAGE_Baseline_LeakSafe",
                    "status": "COMPLETED",
                    "script": "experiments/EX_PHASE3_GraphSAGE_Baseline_LeakSafe/scripts/train.py",
                }
            ]
        },
    )
    monkeypatch.setattr(
        preprocess,
        "infer_memory_contract_for_exp",
        lambda exp, *_args, **_kwargs: {
            "memory_family": "fullbatch_sparse_gnn",
            "execution_mode": "fullbatch",
            "est_mem_decision_mb": 7748,
            "neighborloader_applicable": True,
        },
    )

    rows = preprocess._collect_watch_snapshot_rows()

    assert rows[0]["mem_family"] == "fullbatch"
    assert rows[0]["est_mb"] == "7748"
    assert rows[0]["mem_mode"] == "fullbatch"
    assert rows[0]["nbldr"] == "yes"


def test_collect_watch_rows_prefers_completed_duplicate_name(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "completed": [{"name": "exp-a", "status": "COMPLETED"}],
            "experiments": [{"name": "exp-a", "status": "RUNNING"}],
        },
    )

    rows = preprocess._collect_watch_snapshot_rows()

    assert len(rows) == 1
    assert rows[0]["name"] == "exp-a"
    assert rows[0]["status"] == "COMPLETED"


def test_collect_watch_rows_promotes_active_row_to_completed_when_result_exists(
    monkeypatch,
):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "experiments": [{"name": "exp-a", "status": "RUNNING"}],
            "completed": [],
        },
    )
    monkeypatch.setattr(
        preprocess,
        "_read_result_payload",
        lambda _name: (None, {"test_f1": 0.1234}),
    )

    rows = preprocess._collect_watch_snapshot_rows()

    assert len(rows) == 1
    assert rows[0]["status"] == "COMPLETED"


def test_collect_watch_rows_reestimates_stale_contract_from_result_hidden_dim(
    monkeypatch,
):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "completed": [
                {
                    "name": "exp-zb",
                    "status": "COMPLETED",
                    "script": "experiments/exp-zb/scripts/train.py",
                    "memory_contract": {
                        "hidden_dim": 25,
                        "memory_family": "no_batch_path_child",
                        "est_mem_decision_mb": 22919,
                        "execution_mode": "fullgraph_no_batch_path",
                        "neighborloader_applicable": False,
                    },
                }
            ]
        },
    )
    monkeypatch.setattr(
        preprocess,
        "_read_result_payload",
        lambda _name: (None, {"hidden_dim": 20}),
    )
    monkeypatch.setattr(
        preprocess,
        "infer_memory_contract_for_exp",
        lambda exp, *_args, **_kwargs: {
            "hidden_dim": 20,
            "memory_family": "no_batch_path_child",
            "execution_mode": "fullgraph_no_batch_path",
            "est_mem_decision_mb": 6103,
            "neighborloader_applicable": False,
        },
    )

    rows = preprocess._collect_watch_snapshot_rows()

    assert rows[0]["est_mb"] == "6103"
    assert rows[0]["mem_family"] == "no-batch"


def test_collect_watch_rows_derives_ready_and_blocked_condition(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "experiments": [
                {
                    "name": "exp-parent",
                    "status": "NEEDS_RERUN",
                    "batch_id": "b1",
                },
                {
                    "name": "exp-child",
                    "status": "NEEDS_RERUN",
                    "batch_id": "b1",
                    "condition_parent": "exp-parent",
                },
                {
                    "name": "exp-ready",
                    "status": "NEEDS_RERUN",
                    "batch_id": "b1",
                },
            ],
            "completed": [],
        },
    )

    rows = preprocess._collect_watch_snapshot_rows()
    by_name = {row["name"]: row for row in rows}

    assert by_name["exp-parent"]["status"] == "READY"
    assert by_name["exp-ready"]["status"] == "READY"
    assert by_name["exp-child"]["status"] == "BLOCKED_CONDITION"


def test_render_watch_panel_filters_ready_queue_when_name_already_registered(
    monkeypatch,
):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda path: (
            {
                "ready_to_process": 0,
                "batch_id": "b1",
                "experiments": [{"name": "exp-a"}],
                "feature_jobs": [],
            }
            if str(path).endswith("ready.json")
            else {
                "completed": [{"name": "exp-a", "status": "COMPLETED"}],
                "experiments": [],
            }
        ),
    )

    panel, _, _ = preprocess._render_watch_panel(0, 20)

    assert "Queue: 0" in panel
    assert "(empty)" in panel


def test_render_watch_panel_shows_snapshot_rows(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "completed": [
                {
                    "name": "EX_PHASE3_GraphSAGE_BaselinePlus_Regenerated32",
                    "status": "COMPLETED",
                    "batch_id": "phase3-graphsage-baseline-pair",
                    "memory_contract": {
                        "memory_family": "fullbatch_sparse_gnn",
                        "execution_mode": "fullbatch",
                        "est_mem_decision_mb": 8024,
                        "neighborloader_applicable": True,
                    },
                }
            ],
            "experiments": [],
        },
    )

    panel, _, _ = preprocess._render_watch_panel(0, 20)

    assert "GraphSAGE" in panel
    assert "8024" in panel


def test_collect_watch_rows_builds_feature_first_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(preprocess, "FEATURE_BANK_DIR", tmp_path)
    (tmp_path / "base.pt").write_bytes(b"x")

    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "artifacts": {
                "artifact_base": {
                    "path": "base.pt",
                    "total_dim": 34,
                    "producer_script": "scripts/build_base.py",
                },
                "artifact_slice": {
                    "path": "missing.pt",
                    "total_dim": 34,
                    "producer_script": "scripts/build_slice.py",
                },
            },
            "features": {
                "base_34dim": {
                    "artifact_id": "artifact_base",
                    "start_idx": 0,
                    "end_idx": 34,
                    "dims": 34,
                },
                "base_basic12": {
                    "artifact_id": "artifact_slice",
                    "start_idx": 0,
                    "end_idx": 12,
                    "dims": 12,
                },
            },
        },
    )

    rows = preprocess._collect_watch_rows({"base_basic12"})

    assert rows[0]["name"] == "base_basic12"
    assert rows[0]["status"] == "COMPUTING"
    assert rows[0]["depends_on"] == "slice[0:12]"
    assert rows[0]["total_dim"] == "34"
    assert rows[1]["name"] == "base_34dim"
    assert rows[1]["status"] == "GENERATED"
    assert rows[1]["artifact_id"] == "artifact_base"


def test_render_watch_panel_shows_feature_bank_overview(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "_collect_watch_rows",
        lambda _computing=None: [
            {
                "name": "base_basic12_cut_d152",
                "artifact_id": "artifact_base_34dim_cut_d152",
                "status": "COMPUTING",
                "total_dim": "34",
                "depends_on": "slice[0:12]",
            },
            {
                "name": "base_34dim_cut_d152",
                "artifact_id": "artifact_base_34dim_cut_d152",
                "status": "GENERATED",
                "total_dim": "34",
                "depends_on": "compute_base_cutoff_features.py",
            },
        ],
    )
    monkeypatch.setattr(preprocess, "_collect_watch_snapshot_rows", lambda: [])
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "ready_to_process": 1,
            "batch_id": "b1",
            "experiments": [],
            "feature_jobs": [{"name": "base_basic12_cut_d152"}],
        },
    )

    panel, _, _ = preprocess._render_watch_panel(0, 20, status_msg="tick ok @ 12:34:56")

    assert "Feature Bank Overview" in panel
    assert "total=2" in panel
    assert "generated=1" in panel
    assert "missing=1" in panel
    assert "Current computing" in panel
    assert "base_basic12_cut_d152" in panel
    assert "artifact_base_34dim_cut_d152" in panel
    assert "slice[0:12]" in panel


def test_render_watch_panel_header_includes_mode_and_tick(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "ready_to_process": 1,
            "batch_id": "b1",
            "experiments": [],
            "feature_jobs": [],
        },
    )

    panel, _, _ = preprocess._render_watch_panel(0, 20, status_msg="tick ok @ 12:34:56")

    assert "Mode: WATCH" in panel
    assert "Tick: 12:34:56" in panel


def test_render_watch_panel_shows_stage_labels(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda path: (
            {
                "ready_to_process": 1,
                "batch_id": "b1",
                "experiments": [{"name": "exp-q"}],
                "feature_jobs": [{"name": "feat-a"}],
            }
            if str(path).endswith("ready.json")
            else {
                "completed": [
                    {
                        "name": "exp-done",
                        "status": "COMPLETED",
                        "batch_id": "b1",
                        "memory_contract": {
                            "memory_family": "fullbatch_sparse_gnn",
                            "execution_mode": "fullbatch",
                            "est_mem_decision_mb": 8024,
                            "neighborloader_applicable": True,
                        },
                    }
                ],
                "experiments": [
                    {"name": "exp-hand", "status": "NEEDS_RERUN", "batch_id": "b1"}
                ],
            }
        ),
    )

    panel, _, _ = preprocess._render_watch_panel(0, 20, status_msg="tick ok @ 12:34:56")

    assert "READY_QUEUE" in panel
    assert "GENERATING_FEATURES" in panel
    assert "HANDOFF_TO_RUNNER" in panel or "REGISTERING_TO_DB" in panel


def test_render_watch_panel_stacks_on_narrow_terminal(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "ready_to_process": 0,
            "batch_id": "-",
            "experiments": [],
            "feature_jobs": [],
        },
    )
    monkeypatch.setattr(
        preprocess.shutil,
        "get_terminal_size",
        lambda fallback=(120, 40): os.terminal_size((120, 40)),
    )

    panel, _, _ = preprocess._render_watch_panel(0, 20, status_msg="tick ok @ 12:34:56")

    assert "Ready Queue" in panel
    assert "Experiments Snapshot" in panel


def test_render_watch_panel_fills_terminal_height(monkeypatch):
    monkeypatch.setattr(
        preprocess,
        "load_json",
        lambda _path: {
            "ready_to_process": 0,
            "batch_id": "-",
            "experiments": [],
            "feature_jobs": [],
        },
    )
    monkeypatch.setattr(
        preprocess.shutil,
        "get_terminal_size",
        lambda fallback=(180, 40): os.terminal_size((180, 32)),
    )

    panel, _, _ = preprocess._render_watch_panel(0, 20, status_msg="tick ok @ 12:34:56")

    assert len(panel.splitlines()) == 32


def test_watch_panel_sizes_fill_wide_layout_height():
    sizes = preprocess._compute_watch_panel_sizes(
        terminal_width=240,
        main_height=23,
        ready_row_count=1,
    )

    assert sizes["stacked"] is False
    assert sizes["feature_height"] == 23
    assert sizes["ready_height"] == 8
    assert sizes["snapshot_height"] == 15


def test_normalize_initial_watch_page_clamps_into_range():
    assert preprocess.normalize_initial_watch_page(1, 3) == 0
    assert preprocess.normalize_initial_watch_page(3, 3) == 2
    assert preprocess.normalize_initial_watch_page(9, 3) == 2
    assert preprocess.normalize_initial_watch_page(0, 3) == 0


def test_watch_status_and_stage_text_use_semantic_styles():
    assert preprocess._format_watch_status_text("COMPLETED").style == "bold green"
    assert preprocess._format_watch_status_text("NEEDS_RERUN").style == "bold cyan"
    assert preprocess._format_watch_status_text("BLOCKED_CONDITION").style == "bold red"
    assert (
        preprocess._format_watch_feature_status_text("COMPUTING").style == "bold yellow"
    )
    assert (
        preprocess._format_watch_stage_text("HANDOFF_TO_RUNNER").style == "bold magenta"
    )
    assert preprocess._format_watch_stage_text("IDLE").style == "dim"
