#!/usr/bin/env python3
"""Two-step key state machine for Phase3 TUI dashboards (T14)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Action:
    scope: str
    action: str


SCOPE_KEYS = {
    "a": "all",
    "s": "selected",
}

ACTION_KEYS = {
    "k": "kill",
    "r": "rerun",
    "d": "delete",
    "v": "archive",
    "f": "freeze",
    "c": "clear",
}

ESC = "\x1b"
TIMEOUT_SEC = 3.0


class TwoStepKeyHandler:
    def __init__(self, timeout: float = TIMEOUT_SEC) -> None:
        self.state: str = "idle"
        self._pending_scope: Optional[str] = None
        self._scope_time: float = 0.0
        self._timeout = timeout

    def _reset(self) -> None:
        self.state = "idle"
        self._pending_scope = None
        self._scope_time = 0.0

    def handle_key(self, ch: str) -> Optional[Action]:
        now = time.monotonic()

        if self.state == "scope_selected":
            if now - self._scope_time > self._timeout:
                self._reset()

        if self.state == "idle":
            if ch in SCOPE_KEYS:
                self._pending_scope = SCOPE_KEYS[ch]
                self._scope_time = now
                self.state = "scope_selected"
                return None
            return None

        if self.state == "scope_selected":
            if ch == ESC:
                self._reset()
                return None
            if ch in ACTION_KEYS:
                assert self._pending_scope is not None
                action = Action(scope=self._pending_scope, action=ACTION_KEYS[ch])
                self._reset()
                return action
            if ch in SCOPE_KEYS:
                self._pending_scope = SCOPE_KEYS[ch]
                self._scope_time = now
                return None
            return None

        return None

    @property
    def prompt(self) -> str:
        if self.state == "idle":
            return "[a]All [s]Selected"
        scope_label = self._pending_scope or "?"
        return f"Scope={scope_label} → [k]Kill [r]Rerun [d]Delete [v]Archive [f]Freeze [c]Clear | [Esc]Cancel"
