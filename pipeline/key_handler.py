#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from panel_nav import (
    clamp_exp_selection,
    cluster_nav_down as _cluster_nav_down,
    cluster_nav_left as _cluster_nav_left,
    cluster_nav_right as _cluster_nav_right,
    cluster_nav_up as _cluster_nav_up,
    exp_nav_down as _exp_nav_down,
    exp_nav_up as _exp_nav_up,
    exp_page_next as _exp_page_next,
    exp_page_prev as _exp_page_prev,
)
from tui_keys import Action, TwoStepKeyHandler


@dataclass(frozen=True)
class KeyMap:
    action: str
    keys: Tuple[str, ...]


EXPERIMENT_PANEL_KEYMAP: Tuple[KeyMap, ...] = (
    KeyMap(action="assign_open", keys=("A",)),
    KeyMap(action="exp_nav_up", keys=("w", "W", "\x1b[A")),
    KeyMap(action="exp_nav_down", keys=("s", "\x1b[B")),
    KeyMap(action="exp_page_next", keys=("N",)),
    KeyMap(action="exp_page_prev", keys=("P",)),
    KeyMap(action="exp_repipeline", keys=("p",)),
    KeyMap(action="exp_start_now", keys=("T", "t")),
    KeyMap(action="exp_move_up", keys=("U", "u")),
    KeyMap(action="exp_move_down", keys=("J", "j")),
)


CLUSTER_PANEL_KEYMAP: Tuple[KeyMap, ...] = (
    KeyMap(action="cluster_nav_up", keys=("w", "W", "\x1b[A")),
    KeyMap(action="cluster_nav_down", keys=("s", "\x1b[B")),
    KeyMap(action="cluster_nav_left", keys=("a", "A", "\x1b[D")),
    KeyMap(action="cluster_nav_right", keys=("d", "\x1b[C")),
    KeyMap(action="cluster_enter", keys=("\r", "\n")),
    KeyMap(action="node_disable", keys=("D",)),
    KeyMap(action="node_enable", keys=("E",)),
    KeyMap(action="node_restart", keys=("R",)),
    KeyMap(action="node_start", keys=("S",)),
    KeyMap(action="node_stop", keys=("K",)),
    KeyMap(action="reset_failed", keys=("F",)),
    KeyMap(action="strategy_cycle", keys=("Y",)),
    KeyMap(action="strategy_1", keys=("1",)),
    KeyMap(action="strategy_2", keys=("2",)),
    KeyMap(action="strategy_3", keys=("3",)),
    KeyMap(action="strategy_4", keys=("4",)),
    KeyMap(action="strategy_5", keys=("5",)),
    KeyMap(action="escape", keys=("\x1b",)),
)


_EXPERIMENT_AUTOFOCUS_ACTIONS = {
    "exp_page_next",
    "exp_page_prev",
    "exp_repipeline",
    "exp_start_now",
    "exp_move_up",
    "exp_move_down",
}


def _resolve_mapped_action(key: str, mappings: Sequence[KeyMap]) -> Optional[str]:
    for mapping in mappings:
        if key in mapping.keys:
            return mapping.action
    return None


def _reset_experiment_two_step(dashboard: Any) -> None:
    dashboard.exp_two_step = TwoStepKeyHandler()


def _queue_selected_experiment_action(
    dashboard: Any,
    panel_experiments: List[Dict[str, Any]],
    request_type: str,
) -> bool:
    if not panel_experiments:
        return True
    selected_exp = panel_experiments[dashboard.selected_exp_idx]
    if dashboard._is_non_actionable_row(selected_exp):
        dashboard.set_message("Condition node is display-only")
        return True
    name = str(selected_exp.get("name", ""))
    if name:
        verb = request_type.replace("exp_", "").replace("_", " ")
        dashboard._enqueue_action({"type": request_type, "name": name}, f"{verb.title()} {name}")
    return True


def _queue_two_step_action(
    dashboard: Any,
    panel_experiments: List[Dict[str, Any]],
    action: Action,
) -> bool:
    action_map = {
        "kill": "exp_kill",
        "delete": "exp_delete",
        "archive": "exp_archive",
        "rerun": "exp_rerun",
        "freeze": "exp_freeze",
    }
    request_type = action_map.get(action.action)
    if request_type is None:
        dashboard.set_message(f"Unsupported experiment action: {action.action}")
        return True

    if action.scope == "all":
        names = [
            str(exp.get("name", ""))
            for exp in panel_experiments
            if str(exp.get("name", "")) and not dashboard._is_non_actionable_row(exp)
        ]
        dedup_names = sorted(dict.fromkeys(names))
        for name in dedup_names:
            verb = request_type.replace("exp_", "").replace("_", " ")
            dashboard._enqueue_action(
                {"type": request_type, "name": name},
                f"{verb.title()} {name}",
            )
        return True

    return _queue_selected_experiment_action(dashboard, panel_experiments, request_type)


def _handle_assign_mode(
    dashboard: Any,
    key: str,
    panel_experiments: List[Dict[str, Any]],
) -> bool:
    if key == "\x1b":
        dashboard.assign_mode = False
        dashboard.set_message("Assign cancelled")
        _reset_experiment_two_step(dashboard)
        return True

    if key.upper() == "C":
        selected_exp = panel_experiments[dashboard.selected_exp_idx]
        if dashboard._is_non_actionable_row(selected_exp):
            dashboard.assign_mode = False
            dashboard.set_message("Condition node is display-only")
            return True
        name = str(selected_exp.get("name", ""))
        if name:
            dashboard._enqueue_action(
                {
                    "type": "assign_worker",
                    "name": name,
                    "old_worker": "",
                    "new_worker": None,
                },
                f"Clear machine assignment for {name}",
            )
        dashboard.assign_mode = False
        return True

    if key.isdigit():
        choice = int(key)
        if 1 <= choice <= len(dashboard.assign_workers):
            selected_worker = dashboard.assign_workers[choice - 1]
            selected_exp = panel_experiments[dashboard.selected_exp_idx]
            if dashboard._is_non_actionable_row(selected_exp):
                dashboard.assign_mode = False
                dashboard.set_message("Condition node is display-only")
                return True
            name = str(selected_exp.get("name", ""))
            running_on = selected_exp.get("running_on") or {}
            old_worker = str(running_on.get("worker", "")) if running_on else ""
            if name:
                dashboard._enqueue_action(
                    {
                        "type": "assign_worker",
                        "name": name,
                        "old_worker": old_worker,
                        "new_worker": selected_worker,
                    },
                    f"Assign {name} -> {selected_worker}",
                )
            dashboard.assign_mode = False
    return True


def _handle_experiment_action(
    dashboard: Any,
    action_name: str,
    key: str,
    workers: List[str],
    panel_experiments: List[Dict[str, Any]],
) -> bool:
    if action_name == "assign_open" and panel_experiments:
        _reset_experiment_two_step(dashboard)
        dashboard.assign_mode = True
        machine_keys = sorted(dashboard.cluster_mgr.load_machines().keys())
        candidate_workers = workers or machine_keys
        dashboard.assign_workers = list(dict.fromkeys(candidate_workers))
        options = " ".join(
            f"[{i + 1}]{worker}" for i, worker in enumerate(dashboard.assign_workers)
        )
        dashboard.set_message(
            f"Assign to: {options} [C]clear" if options else "Assign to: (no workers) [C]clear"
        )
        return True

    if action_name == "exp_nav_up":
        _reset_experiment_two_step(dashboard)
        return _exp_nav_up(dashboard, panel_experiments)

    if action_name == "exp_nav_down":
        _reset_experiment_two_step(dashboard)
        return _exp_nav_down(dashboard, panel_experiments)

    if action_name == "exp_page_next":
        _reset_experiment_two_step(dashboard)
        return _exp_page_next(dashboard)

    if action_name == "exp_page_prev":
        _reset_experiment_two_step(dashboard)
        return _exp_page_prev(dashboard)

    if action_name == "exp_repipeline" and panel_experiments:
        _reset_experiment_two_step(dashboard)
        name = dashboard.selected_exp_name
        if not name:
            dashboard.set_message("No experiment selected")
            return True
        exp_payload = next((e for e in panel_experiments if e.get("name") == name), None)
        if exp_payload is None:
            dashboard.set_message(f"Experiment {name} not found")
            return True
        if dashboard._is_non_actionable_row(exp_payload):
            dashboard.set_message("Condition node is display-only")
            return True
        dashboard._enqueue_action(
            {
                "type": "exp_repipeline",
                "name": name,
                "exp_payload": dict(exp_payload),
            },
            f"Re-pipeline {name}",
        )
        return True

    if action_name in {"exp_start_now", "exp_move_up", "exp_move_down"}:
        _reset_experiment_two_step(dashboard)
        request_type_map = {
            "exp_start_now": "exp_start_now",
            "exp_move_up": "exp_move",
            "exp_move_down": "exp_move",
        }
        if action_name == "exp_start_now":
            return _queue_selected_experiment_action(
                dashboard,
                panel_experiments,
                request_type_map[action_name],
            )
        selected_exp = panel_experiments[dashboard.selected_exp_idx] if panel_experiments else {}
        if dashboard._is_non_actionable_row(selected_exp):
            dashboard.set_message("Condition node is display-only")
            return True
        name = str(selected_exp.get("name", ""))
        if name:
            direction = "up" if action_name == "exp_move_up" else "down"
            dashboard._enqueue_action(
                {"type": "exp_move", "name": name, "direction": direction},
                f"Move {direction} {name}",
            )
        return True

    key_lower = "d" if key.lower() == "x" else key.lower()
    action = dashboard.exp_two_step.handle_key(key_lower)
    if (
        action is None
        and dashboard.exp_two_step.state == "idle"
        and key.lower() in {"k", "r", "d", "x", "v", "f"}
    ):
        selected_action_map = {
            "k": "kill",
            "r": "rerun",
            "d": "delete",
            "x": "delete",
            "v": "archive",
            "f": "freeze",
        }
        mapped_action = selected_action_map.get(key_lower)
        if mapped_action:
            action = Action(scope="selected", action=mapped_action)

    if key == "\x1b" and dashboard.exp_two_step.state == "idle":
        dashboard.set_message("Scope cancelled")
    if action and panel_experiments:
        return _queue_two_step_action(dashboard, panel_experiments, action)
    return True


def _current_cluster_worker(dashboard: Any, workers: List[str]) -> Optional[str]:
    if not workers:
        return None
    if dashboard.selected_node_idx >= len(workers):
        dashboard.selected_node_idx = max(0, len(workers) - 1)
    return workers[dashboard.selected_node_idx]


def _queue_node_action(dashboard: Any, workers: List[str], action: str, label: str) -> bool:
    node_id = _current_cluster_worker(dashboard, workers)
    if not node_id:
        return True
    dashboard._enqueue_action(
        {"type": "node_action", "node_id": node_id, "action": action},
        f"{label} {node_id}",
    )
    return True


def _handle_cluster_action(
    dashboard: Any,
    action_name: str,
    key: str,
    workers: List[str],
) -> bool:
    if not workers:
        return True

    if action_name == "cluster_nav_up" and not dashboard.action_mode:
        return _cluster_nav_up(dashboard, len(workers))
    if action_name == "cluster_nav_down" and not dashboard.action_mode:
        return _cluster_nav_down(dashboard, len(workers))
    if action_name == "cluster_nav_left":
        return _cluster_nav_left(dashboard, len(workers))
    if action_name == "cluster_nav_right":
        return _cluster_nav_right(dashboard, len(workers))
    if action_name == "cluster_enter":
        if not dashboard.action_mode:
            dashboard.action_mode = True
            dashboard.action_idx = 0
        else:
            dashboard._execute_action(workers)
            dashboard.action_mode = False
        return True
    if action_name == "node_disable":
        return _queue_node_action(dashboard, workers, "disable", "DISABLE")
    if action_name == "node_enable":
        return _queue_node_action(dashboard, workers, "enable", "ENABLE")
    if action_name == "node_restart":
        return _queue_node_action(dashboard, workers, "restart", "RESTART")
    if action_name == "node_start":
        return _queue_node_action(dashboard, workers, "start", "START")
    if action_name == "node_stop":
        return _queue_node_action(dashboard, workers, "stop", "STOP")
    if action_name == "reset_failed":
        dashboard._enqueue_action({"type": "reset_failed"}, "Reset failed experiments")
        return True
    if action_name == "strategy_cycle":
        current = str(dashboard._current_allocation_strategy() or "").strip().lower()
        options = list(dashboard.strategy_hotkeys.values())
        if options:
            if current in options:
                idx = options.index(current)
                target = options[(idx + 1) % len(options)]
            else:
                target = options[0]
        else:
            target = current
        dashboard._enqueue_action(
            {"type": "scheduler_strategy", "strategy": target},
            f"STRATEGY {target}",
        )
        return True
    if action_name.startswith("strategy_"):
        hotkey = action_name.split("_", 1)[1]
        target = dashboard.strategy_hotkeys.get(hotkey)
        if target:
            dashboard._enqueue_action(
                {"type": "scheduler_strategy", "strategy": target},
                f"STRATEGY {target}",
            )
        return True
    if action_name == "escape":
        dashboard.action_mode = False
        return True

    if key in dashboard.strategy_hotkeys:
        target = dashboard.strategy_hotkeys[key]
        dashboard._enqueue_action(
            {"type": "scheduler_strategy", "strategy": target},
            f"STRATEGY {target}",
        )
    return True


def _toggle_focus(dashboard: Any) -> bool:
    target = "experiments" if dashboard.focus_mode == "cluster" else "cluster"
    dashboard.set_focus_mode(target, announce=True)
    return True


def _get_panel_experiments(dashboard: Any) -> List[Dict[str, Any]]:
    panel_experiments = list(getattr(dashboard, "_panel_exp_rows", []) or [])
    if not panel_experiments:
        try:
            snapshot = dashboard.db.load()
        except Exception:
            snapshot = {}
        experiments = snapshot.get("experiments", []) if isinstance(snapshot, dict) else []
        if isinstance(experiments, list):
            panel_experiments = [exp for exp in experiments if isinstance(exp, dict)]
    dashboard._panel_exp_total = len(panel_experiments)
    clamp_exp_selection(dashboard, panel_experiments)
    return panel_experiments


def dispatch_dashboard_key(dashboard: Any, key: Optional[str], workers: List[str]) -> bool:
    if not key:
        return True

    if key.lower() == "q":
        return False

    if key == "\t":
        return _toggle_focus(dashboard)

    experiment_action = _resolve_mapped_action(key, EXPERIMENT_PANEL_KEYMAP)
    cluster_action = _resolve_mapped_action(key, CLUSTER_PANEL_KEYMAP)

    if (
        dashboard.focus_mode == "cluster"
        and experiment_action in _EXPERIMENT_AUTOFOCUS_ACTIONS
        and cluster_action is None
    ):
        dashboard.set_focus_mode("experiments", announce=True)

    if dashboard.focus_mode == "experiments":
        panel_experiments = _get_panel_experiments(dashboard)

        if dashboard.assign_mode:
            return _handle_assign_mode(dashboard, key, panel_experiments)

        return _handle_experiment_action(
            dashboard,
            experiment_action or "exp_two_step",
            key,
            workers,
            panel_experiments,
        )

    return _handle_cluster_action(dashboard, cluster_action or "noop", key, workers)
