#!/usr/bin/env python3

from unittest.mock import MagicMock

from experiments import UnifiedDashboard
from key_handler import (
    CLUSTER_PANEL_KEYMAP,
    EXPERIMENT_PANEL_KEYMAP,
)


def _mapping_dict(maps):
    result = {}
    for item in maps:
        for key in item.keys:
            result[key] = item.action
    return result


def _make_dashboard(experiments=None):
    cluster_mgr = MagicMock()
    cluster_mgr.load_machines.return_value = {}

    db = MagicMock()
    db.load.return_value = {
        "experiments": experiments or [],
        "completed": [],
        "archived": [],
    }

    return UnifiedDashboard("worker-1", cluster_mgr, db, is_watch=True)


def test_experiment_panel_keymap_has_expected_bindings():
    mapping = _mapping_dict(EXPERIMENT_PANEL_KEYMAP)

    assert mapping["A"] == "assign_open"
    assert mapping["w"] == "exp_nav_up"
    assert mapping["W"] == "exp_nav_up"
    assert mapping["\x1b[A"] == "exp_nav_up"
    assert mapping["s"] == "exp_nav_down"
    assert mapping["S"] == "exp_nav_down"
    assert mapping["\x1b[B"] == "exp_nav_down"
    assert mapping["N"] == "exp_page_next"
    assert mapping["P"] == "exp_page_prev"
    assert mapping["p"] == "exp_repipeline"
    assert mapping["T"] == "exp_start_now"
    assert mapping["U"] == "exp_move_up"
    assert mapping["J"] == "exp_move_down"


def test_cluster_panel_keymap_has_expected_bindings():
    mapping = _mapping_dict(CLUSTER_PANEL_KEYMAP)

    assert mapping["w"] == "cluster_nav_up"
    assert mapping["W"] == "cluster_nav_up"
    assert mapping["\x1b[A"] == "cluster_nav_up"
    assert mapping["s"] == "cluster_nav_down"
    assert mapping["\x1b[B"] == "cluster_nav_down"
    assert mapping["a"] == "cluster_nav_left"
    assert mapping["A"] == "cluster_nav_left"
    assert mapping["\x1b[D"] == "cluster_nav_left"
    assert mapping["d"] == "cluster_nav_right"
    assert mapping["\x1b[C"] == "cluster_nav_right"
    assert mapping["\r"] == "cluster_enter"
    assert mapping["\n"] == "cluster_enter"
    assert mapping["D"] == "node_disable"
    assert mapping["E"] == "node_enable"
    assert mapping["R"] == "node_restart"
    assert mapping["S"] == "node_start"
    assert mapping["K"] == "node_stop"
    assert mapping["F"] == "reset_failed"
    assert mapping["Y"] == "strategy_cycle"
    assert mapping["1"] == "strategy_1"
    assert mapping["2"] == "strategy_2"
    assert mapping["3"] == "strategy_3"
    assert mapping["4"] == "strategy_4"
    assert mapping["5"] == "strategy_5"


def test_experiment_panel_up_down_navigation_regression():
    dashboard = _make_dashboard(
        experiments=[{"name": "exp-a"}, {"name": "exp-b"}, {"name": "exp-c"}]
    )
    dashboard.focus_mode = "experiments"
    dashboard.selected_exp_idx = 1

    dashboard.handle_key("\x1b[A", ["node-1"])
    assert dashboard.selected_exp_idx == 0

    dashboard.handle_key("\x1b[B", ["node-1"])
    assert dashboard.selected_exp_idx == 1
