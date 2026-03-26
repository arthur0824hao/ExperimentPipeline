#!/usr/bin/env python3

from terminal_state import get_terminal_reason


def test_get_terminal_reason_completed_when_metrics_present():
    reason = get_terminal_reason(
        "exp-1",
        "COMPLETED",
        {"f1_score": 0.91, "auc_score": 0.95},
        None,
        {"child_returncode": 0},
    )
    assert reason == "COMPLETED"


def test_get_terminal_reason_oom_for_true_oom_rerun():
    reason = get_terminal_reason(
        "exp-2",
        "NEEDS_RERUN",
        {},
        {"type": "OOM", "is_true_oom": True},
        None,
    )
    assert reason == "FAILED_OOM"
