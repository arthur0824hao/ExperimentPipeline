#!/usr/bin/env python3

from __future__ import annotations

import select
import sys
import termios
import tty
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Iterator, Optional, TextIO


def read_dashboard_key(input_stream: Optional[TextIO] = None) -> Optional[str]:
    try:
        stream = input_stream or sys.stdin
        ch = stream.read(1)
        if ch == "\x1b":
            if select.select([stream], [], [], 0.1)[0]:
                ch += stream.read(1)
                if select.select([stream], [], [], 0.1)[0]:
                    ch += stream.read(1)
        return ch
    except Exception:
        return None


@dataclass
class DashboardInputSession:
    stream: TextIO
    enabled: bool
    exit_stack: ExitStack
    fd: Optional[int]
    old_settings: object

    @classmethod
    def open(cls) -> "DashboardInputSession":
        input_stream: TextIO = sys.stdin
        exit_stack = ExitStack()
        enabled = True

        if not sys.stdin.isatty():
            try:
                input_stream = exit_stack.enter_context(open("/dev/tty", "r"))
            except Exception:
                enabled = False

        fd: Optional[int] = None
        old_settings: object = None
        if enabled:
            try:
                fd = int(input_stream.fileno())
                old_settings = termios.tcgetattr(fd)
            except Exception:
                enabled = False
                fd = None
                old_settings = None

        return cls(
            stream=input_stream,
            enabled=enabled,
            exit_stack=exit_stack,
            fd=fd,
            old_settings=old_settings,
        )

    def __enter__(self) -> "DashboardInputSession":
        if self.enabled and self.fd is not None:
            tty.setcbreak(self.fd)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.enabled and self.fd is not None and self.old_settings is not None:
            try:
                termios.tcsetattr(self.fd, termios.TCSADRAIN, self.old_settings)
            except Exception:
                pass
        self.exit_stack.close()

    def ready_keys(self) -> Iterator[str]:
        if not self.enabled:
            return
        while True:
            rlist, _, _ = select.select([self.stream], [], [], 0)
            if not rlist:
                break
            key = read_dashboard_key(self.stream)
            if key:
                yield key
