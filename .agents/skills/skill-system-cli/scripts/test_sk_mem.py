#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("sk.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("sk_mem_cli", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load sk.py from {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SkMemCliTests(unittest.TestCase):
    def test_mem_store_routes_scope(self) -> None:
        mod = _load_module()
        captured: list[list[str]] = []
        with patch.object(
            mod, "_run_mem_py", side_effect=lambda args: captured.append(args)
        ):
            mod.mem_store.callback(
                "semantic", "fraud", "title", "hello", "a,b", "project", 5.0
            )
        self.assertEqual(
            captured[0],
            [
                "store",
                "--type",
                "semantic",
                "--category",
                "fraud",
                "--title",
                "title",
                "--content",
                "hello",
                "--tags",
                "a,b",
                "--scope",
                "project",
                "--importance",
                "5.0",
            ],
        )

    def test_mem_search_and_list_route(self) -> None:
        mod = _load_module()
        captured: list[list[str]] = []
        with patch.object(
            mod, "_run_mem_py", side_effect=lambda args: captured.append(args)
        ):
            mod.mem_search.callback("fraud", 5, "project")
            mod.mem_list.callback("session", None, 3)
        self.assertEqual(
            captured[0], ["search", "fraud", "--limit", "5", "--scope", "project"]
        )
        self.assertEqual(captured[1], ["list", "--limit", "3", "--scope", "session"])


if __name__ == "__main__":
    unittest.main()
