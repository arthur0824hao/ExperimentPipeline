#!/usr/bin/env python3
"""Tests for tui_keys.py two-step key state machine (T14)."""

import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from tui_keys import TwoStepKeyHandler, Action, SCOPE_KEYS, ACTION_KEYS, ESC


def test_initial_state_is_idle():
    h = TwoStepKeyHandler()
    assert h.state == "idle"


def test_scope_key_transitions_to_scope_selected():
    h = TwoStepKeyHandler()
    result = h.handle_key("a")
    assert result is None
    assert h.state == "scope_selected"


def test_complete_two_step_flow():
    h = TwoStepKeyHandler()
    h.handle_key("a")
    assert h.state == "scope_selected"
    result = h.handle_key("k")
    assert result == Action(scope="all", action="kill")
    assert h.state == "idle"


def test_selected_scope_then_rerun():
    h = TwoStepKeyHandler()
    h.handle_key("s")
    result = h.handle_key("r")
    assert result == Action(scope="selected", action="rerun")
    assert h.state == "idle"


def test_esc_cancels_scope_selection():
    h = TwoStepKeyHandler()
    h.handle_key("a")
    assert h.state == "scope_selected"
    result = h.handle_key(ESC)
    assert result is None
    assert h.state == "idle"


def test_all_action_keys_work():
    for scope_ch, scope_name in SCOPE_KEYS.items():
        for action_ch, action_name in ACTION_KEYS.items():
            h = TwoStepKeyHandler()
            h.handle_key(scope_ch)
            result = h.handle_key(action_ch)
            assert result == Action(scope=scope_name, action=action_name)
            assert h.state == "idle"


def test_invalid_key_in_idle_returns_none():
    h = TwoStepKeyHandler()
    assert h.handle_key("x") is None
    assert h.state == "idle"


def test_invalid_key_in_scope_selected_returns_none():
    h = TwoStepKeyHandler()
    h.handle_key("a")
    result = h.handle_key("x")
    assert result is None
    assert h.state == "scope_selected"


def test_timeout_resets_to_idle():
    h = TwoStepKeyHandler(timeout=0.01)
    h.handle_key("a")
    assert h.state == "scope_selected"
    time.sleep(0.02)
    result = h.handle_key("k")
    assert result is None
    assert h.state == "idle"


def test_scope_key_changes_pending_scope():
    h = TwoStepKeyHandler()
    h.handle_key("a")
    assert h._pending_scope == "all"
    h.handle_key("s")
    assert h._pending_scope == "selected"
    result = h.handle_key("k")
    assert result == Action(scope="selected", action="kill")


def test_prompt_property():
    h = TwoStepKeyHandler()
    assert "All" in h.prompt
    assert "Selected" in h.prompt
    h.handle_key("a")
    assert "Kill" in h.prompt
    assert "Cancel" in h.prompt
