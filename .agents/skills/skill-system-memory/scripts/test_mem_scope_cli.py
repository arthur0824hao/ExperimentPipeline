#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("mem.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("mem_scope_cli", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load mem.py from {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MemScopeCliTests(unittest.TestCase):
    def test_project_scope_store_and_search_filter(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_args = type(
                "Args",
                (),
                {
                    "cmd": "store",
                    "memory_type": "semantic",
                    "category": "fraud",
                    "title": "project note",
                    "tags_csv": "fraud,test",
                    "importance": 6,
                    "content": "important content",
                    "scope": "project",
                },
            )()
            with patch.object(mod, "ROOT_DIR", root):
                mod._handle_no_db(store_args, "db unavailable")
                out = io.StringIO()
                with patch("sys.stdout", out):
                    mod._handle_no_db(
                        type(
                            "Args",
                            (),
                            {
                                "cmd": "search",
                                "query": "project",
                                "limit": 10,
                                "scope": "project",
                            },
                        )(),
                        "db unavailable",
                    )
                self.assertIn("project note", out.getvalue())
                out = io.StringIO()
                with patch("sys.stdout", out):
                    mod._handle_no_db(
                        type(
                            "Args",
                            (),
                            {
                                "cmd": "search",
                                "query": "project",
                                "limit": 10,
                                "scope": "session",
                            },
                        )(),
                        "db unavailable",
                    )
                self.assertIn("(no results)", out.getvalue())

    def test_list_scope_uses_pending_records(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(mod, "ROOT_DIR", root):
                mod._write_pending_memory(
                    "semantic",
                    "fraud",
                    "global note",
                    "fraud",
                    5,
                    "body",
                    {"scope": "global"},
                )
                out = io.StringIO()
                with patch("sys.stdout", out):
                    mod._handle_no_db(
                        type(
                            "Args",
                            (),
                            {
                                "cmd": "list",
                                "scope": "global",
                                "category": None,
                                "limit": 20,
                            },
                        )(),
                        "db unavailable",
                    )
                self.assertIn("global note", out.getvalue())


if __name__ == "__main__":
    unittest.main()
