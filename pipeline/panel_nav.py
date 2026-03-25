#!/usr/bin/env python3
"""
Pure panel navigation — no DB, no enqueue, no side effects.

Every function here only mutates dashboard selection/page state
using the already-rendered _panel_exp_rows. Nothing here should
import db_registry or call db.load().
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def exp_nav_up(dashboard: Any, panel_experiments: List[Dict[str, Any]]) -> bool:
    """Move experiment selection cursor up by one row."""
    dashboard.selected_exp_idx = max(0, dashboard.selected_exp_idx - 1)
    if panel_experiments:
        dashboard.selected_exp_name = panel_experiments[dashboard.selected_exp_idx].get("name")
    return True


def exp_nav_down(dashboard: Any, panel_experiments: List[Dict[str, Any]]) -> bool:
    """Move experiment selection cursor down by one row."""
    if panel_experiments:
        dashboard.selected_exp_idx = min(
            len(panel_experiments) - 1,
            dashboard.selected_exp_idx + 1,
        )
        dashboard.selected_exp_name = panel_experiments[dashboard.selected_exp_idx].get("name")
    return True


def exp_page_next(dashboard: Any) -> bool:
    """Advance to the next experiment page."""
    total = dashboard._panel_exp_total or 0
    dashboard._refresh_experiment_pagination(total)
    dashboard._change_experiment_page(1)
    return True


def exp_page_prev(dashboard: Any) -> bool:
    """Go back to the previous experiment page."""
    total = dashboard._panel_exp_total or 0
    dashboard._refresh_experiment_pagination(total)
    dashboard._change_experiment_page(-1)
    return True


def clamp_exp_selection(dashboard: Any, panel_experiments: List[Dict[str, Any]]) -> None:
    """Clamp selected_exp_idx to valid bounds after a panel refresh.

    This is meant to be called by the render path, NOT by the key handler.
    """
    if not panel_experiments:
        dashboard.selected_exp_idx = 0
        dashboard.selected_exp_name = None
        return

    if dashboard.selected_exp_name is not None:
        for i, exp in enumerate(panel_experiments):
            if exp.get("name") == dashboard.selected_exp_name:
                dashboard.selected_exp_idx = i
                return

    dashboard.selected_exp_idx = max(0, min(dashboard.selected_exp_idx, len(panel_experiments) - 1))
    dashboard.selected_exp_name = panel_experiments[dashboard.selected_exp_idx].get("name")


def cluster_nav_up(dashboard: Any, worker_count: int) -> bool:
    """Move cluster selection cursor up."""
    dashboard._move_cluster_selection(-1, 0, worker_count)
    return True


def cluster_nav_down(dashboard: Any, worker_count: int) -> bool:
    """Move cluster selection cursor down."""
    dashboard._move_cluster_selection(1, 0, worker_count)
    return True


def cluster_nav_left(dashboard: Any, worker_count: int) -> bool:
    """Move cluster selection left (or action index if in action mode)."""
    if dashboard.action_mode:
        dashboard.action_idx = max(0, dashboard.action_idx - 1)
    else:
        dashboard._move_cluster_selection(0, -1, worker_count)
    return True


def cluster_nav_right(dashboard: Any, worker_count: int) -> bool:
    """Move cluster selection right (or action index if in action mode)."""
    if dashboard.action_mode:
        dashboard.action_idx = min(len(dashboard.actions) - 1, dashboard.action_idx + 1)
    else:
        dashboard._move_cluster_selection(0, 1, worker_count)
    return True
