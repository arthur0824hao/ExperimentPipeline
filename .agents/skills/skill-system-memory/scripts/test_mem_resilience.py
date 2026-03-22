#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


_fake_psycopg2 = MagicMock()
_fake_psycopg2.OperationalError = type("OperationalError", (Exception,), {})
sys.modules.setdefault("psycopg2", _fake_psycopg2)

_MEM_PATH = Path(__file__).with_name("mem.py")


def _load_mem():
    spec = importlib.util.spec_from_file_location("mem_resilience", _MEM_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec for {_MEM_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cur


class MemResilienceTests(unittest.TestCase):
    def test_no_schema_graceful(self) -> None:
        mod = _load_mem()
        args = MagicMock(cmd="store", content="payload")
        out = io.StringIO()
        err = io.StringIO()
        with patch("sys.stdout", out), patch("sys.stderr", err):
            rc = mod._handle_missing_schema(args)
        self.assertEqual(rc, 0)
        self.assertIn("no-op", out.getvalue())
        self.assertIn("schema", err.getvalue())

        search_args = MagicMock(cmd="search")
        out = io.StringIO()
        err = io.StringIO()
        with patch("sys.stdout", out), patch("sys.stderr", err):
            rc = mod._handle_missing_schema(search_args)
        self.assertEqual(rc, 0)
        self.assertIn("(no results)", out.getvalue())

    def test_no_db_file_fallback(self) -> None:
        mod = _load_mem()
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(mod, "ROOT_DIR", Path(tmp)):
                args = MagicMock(
                    cmd="store",
                    memory_type="semantic",
                    category="resilience",
                    title="pending",
                    tags_csv="tag1,tag2",
                    importance=7,
                    content="fallback content",
                )
                out = io.StringIO()
                err = io.StringIO()
                with patch("sys.stdout", out), patch("sys.stderr", err):
                    rc = mod._handle_no_db(args, "db unavailable")
                self.assertEqual(rc, 0)
                pending = list((Path(tmp) / ".memory" / "pending").glob("*.json"))
                self.assertEqual(len(pending), 1)
                self.assertIn("fallback", out.getvalue())

    def test_flush_pending(self) -> None:
        mod = _load_mem()
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(mod, "ROOT_DIR", Path(tmp)):
                pending_dir = Path(tmp) / ".memory" / "pending"
                pending_dir.mkdir(parents=True)
                payload = pending_dir / "pending.json"
                payload.write_text(
                    '{"memory_type":"semantic","category":"cat","title":"Title","tags_csv":"tag","importance":5,"content":"Body","metadata":{"source":"test"}}',
                    encoding="utf-8",
                )
                conn, _cur = _mock_conn()
                out = io.StringIO()
                with (
                    patch.object(mod, "_store_to_db", return_value=42),
                    patch("sys.stdout", out),
                ):
                    mod.cmd_flush(conn)
                self.assertFalse(payload.exists())
                self.assertIn("flushed id=42", out.getvalue())

    def test_normal_path(self) -> None:
        mod = _load_mem()
        conn, cur = _mock_conn()
        cur.fetchone.return_value = (99,)
        out = io.StringIO()
        with patch("sys.stdout", out):
            mod.cmd_store(
                conn,
                memory_type="semantic",
                category="cat",
                title="Title",
                tags_csv="tag1",
                importance=5,
                content="Body",
            )
        self.assertIn("id=99", out.getvalue())


if __name__ == "__main__":
    unittest.main()
