#!/usr/bin/env python3
"""
TDD tests for mem.py
Uses unittest.mock — no real PostgreSQL connection required.
Pre-installs a fake 'psycopg2' module so tests work even if psycopg2 is not installed.

Run: python3 -m unittest test_mem -v
     python3 -m pytest test_mem.py -v   (if pytest installed)
"""

import importlib.util
import io
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Step 1: Inject a fake psycopg2 into sys.modules BEFORE anything else.
# This lets patch("psycopg2.connect", ...) work without the real package.
# ---------------------------------------------------------------------------
_fake_psycopg2 = MagicMock()
_fake_psycopg2.OperationalError = type("OperationalError", (Exception,), {})
sys.modules.setdefault("psycopg2", _fake_psycopg2)


# ---------------------------------------------------------------------------
# Step 2: Load mem.py as a module without running __main__ block.
# We reload each test to get a fresh module reference with our mock in place.
# ---------------------------------------------------------------------------
_MEM_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mem.py")


def _load_mem():
    spec = importlib.util.spec_from_file_location("mem", _MEM_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec for {_MEM_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Helper: capture stdout/stderr
# ---------------------------------------------------------------------------


def _capture(fn):
    out, err = io.StringIO(), io.StringIO()
    with patch("sys.stdout", out), patch("sys.stderr", err):
        fn()
    return out.getvalue(), err.getvalue()


# ---------------------------------------------------------------------------
# Helper: make a mock psycopg2 connection with cursor context manager
# ---------------------------------------------------------------------------


def _mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cur


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMemStatus(unittest.TestCase):
    """B3: status subcommand."""

    def test_status_ok(self):
        """status prints DB ok + memory count + last updated_at."""
        mod = _load_mem()
        conn, cur = _mock_conn()
        cur.fetchone.side_effect = [(42,), ("2026-02-24 12:00:00+00",)]

        out, err = _capture(
            lambda: mod.cmd_status(conn, "agent_memory", "SKILL_PGDATABASE")
        )

        self.assertIn("42", out)
        self.assertIn("2026-02-24", out)
        self.assertIn("記憶資料庫：agent_memory", out)
        self.assertEqual(err, "")

    def test_status_db_fail_friendly_message(self):
        """_get_conn() prints friendly error on OperationalError (A3)."""
        mod = _load_mem()
        psycopg2 = sys.modules["psycopg2"]
        psycopg2.connect.side_effect = psycopg2.OperationalError("auth fail")

        with patch.dict(os.environ, {"SKILL_PGDATABASE": "agent_memory"}, clear=False):
            with self.assertRaises(SystemExit):
                out, err = _capture(lambda: mod._get_conn())

        # We just verify SystemExit is raised (friendly message printed before exit)
        # The actual message assertion is in test_status_friendly_text below.

    def test_status_friendly_text(self):
        """_get_conn failure prints '連線失敗' not raw traceback."""
        mod = _load_mem()
        psycopg2 = sys.modules["psycopg2"]
        psycopg2.connect.side_effect = psycopg2.OperationalError("auth fail")

        captured = []
        original_stderr = sys.stderr
        buf = io.StringIO()
        sys.stderr = buf
        try:
            try:
                with patch.dict(
                    os.environ, {"SKILL_PGDATABASE": "agent_memory"}, clear=False
                ):
                    mod._get_conn()
            except SystemExit:
                pass
        finally:
            sys.stderr = original_stderr

        self.assertIn("連線失敗", buf.getvalue())

        # Reset so other tests can use a working connect
        psycopg2.connect.side_effect = None


class TestMemSearch(unittest.TestCase):
    """A1 + SQL injection safety."""

    def _collect_execute_calls(self, query, limit=5):
        mod = _load_mem()
        conn, cur = _mock_conn()
        calls_made = []

        def capture_execute(sql, params=None):
            calls_made.append((sql, params))

        cur.execute = capture_execute
        cur.fetchall.return_value = []

        mod.cmd_search(conn, query, limit)
        return calls_made

    def test_search_uses_parameterized_query(self):
        """search must pass query via params tuple, NOT via string formatting."""
        calls = self._collect_execute_calls("pgvector install")
        self.assertTrue(len(calls) >= 1, "execute() was never called")
        sql, params = calls[0]
        self.assertIsNotNone(
            params, "params should not be None — use parameterized query!"
        )
        self.assertIn("pgvector install", params)
        # The literal query must NOT appear baked into the SQL template
        self.assertNotIn("pgvector install", sql)

    def test_sql_injection_safe(self):
        """Dangerous input stays in params, never in SQL string."""
        evil = "it's a test; DROP TABLE agent_memories; --"
        calls = self._collect_execute_calls(evil)
        self.assertTrue(len(calls) >= 1)
        sql, params = calls[0]
        self.assertIsNotNone(params)
        self.assertIn(evil, params)
        self.assertNotIn("DROP TABLE", sql)


class TestMemStore(unittest.TestCase):
    """B1: --content flag; stdin fallback."""

    def _run_store(self, content_flag=None, stdin_data=None):
        mod = _load_mem()
        conn, cur = _mock_conn()
        calls_made = []

        def capture_execute(sql, params=None):
            calls_made.append((sql, params))

        cur.execute = capture_execute
        cur.fetchone.return_value = (99,)

        fake_stdin = io.StringIO(stdin_data or "")

        with patch("sys.stdin", fake_stdin):
            mod.cmd_store(
                conn=conn,
                memory_type="semantic",
                category="test",
                title="Test Title",
                tags_csv="t1,t2",
                importance=7,
                content=content_flag,
            )

        return calls_made

    def test_store_with_content_flag(self):
        """--content flag is passed into the parameterized query."""
        calls = self._run_store(content_flag="Hello from flag")
        self.assertTrue(len(calls) >= 1)
        sql, params = calls[0]
        self.assertIsNotNone(params)
        self.assertIn("Hello from flag", params)

    def test_store_with_stdin(self):
        """stdin content is passed into the parameterized query."""
        calls = self._run_store(stdin_data="Hello from stdin")
        self.assertTrue(len(calls) >= 1)
        sql, params = calls[0]
        self.assertIsNotNone(params)
        self.assertIn("Hello from stdin", params)


class TestMemTags(unittest.TestCase):
    """C2: tags subcommand."""

    def test_tags_queries_tags_column(self):
        mod = _load_mem()
        conn, cur = _mock_conn()
        calls_made = []

        def capture_execute(sql, params=None):
            calls_made.append((sql, params))

        cur.execute = capture_execute
        cur.fetchall.return_value = [("postgres", 3), ("ssh", 1)]
        cur.description = [("tag",), ("count",)]

        with patch("sys.stdout", io.StringIO()):
            mod.cmd_tags(conn)

        self.assertTrue(len(calls_made) >= 1)
        sql = calls_made[0][0].lower()
        self.assertIn("tags", sql)


class TestMemCategories(unittest.TestCase):
    """C2: categories subcommand."""

    def test_categories_queries_category_column(self):
        mod = _load_mem()
        conn, cur = _mock_conn()
        calls_made = []

        def capture_execute(sql, params=None):
            calls_made.append((sql, params))

        cur.execute = capture_execute
        cur.fetchall.return_value = [("friction", 3), ("soul-state", 2)]
        cur.description = [("category",), ("count",)]

        with patch("sys.stdout", io.StringIO()):
            mod.cmd_categories(conn)

        self.assertTrue(len(calls_made) >= 1)
        sql = calls_made[0][0].lower()
        self.assertIn("category", sql)


class TestMemContext(unittest.TestCase):
    """C1: context — auto-search and print summary."""

    def test_context_prints_title_in_summary(self):
        mod = _load_mem()
        conn, cur = _mock_conn()

        fake_row = (
            1,
            "semantic",
            "cat",
            "Target Memory Title",
            "Content here",
            8.0,
            0.9,
            "fulltext",
        )
        cur.fetchall.return_value = [fake_row]
        cur.description = [
            ("id",),
            ("memory_type",),
            ("category",),
            ("title",),
            ("content",),
            ("importance_score",),
            ("relevance_score",),
            ("match_type",),
        ]

        calls_made = []

        def capture_execute(sql, params=None):
            calls_made.append((sql, params))

        cur.execute = capture_execute

        out, _ = _capture(lambda: mod.cmd_context(conn, "pgvector install"))

        self.assertIn("Target Memory Title", out)
        self.assertTrue(len(calls_made) >= 1)
        # Must be parameterized
        self.assertIsNotNone(calls_made[0][1])
        self.assertIn("pgvector install", calls_made[0][1])


class TestDbTargetResolution(unittest.TestCase):
    def test_prefers_explicit_skill_pgdatabase(self):
        mod = _load_mem()
        with patch.dict(
            os.environ,
            {"SKILL_PGDATABASE": "agent_memory", "PGDATABASE": "agent_memory"},
            clear=False,
        ):
            db_target, source = mod.resolve_db_target()

        self.assertEqual(db_target, "agent_memory")
        self.assertEqual(source, "SKILL_PGDATABASE")

    def test_fails_closed_on_ambient_pgdatabase_only(self):
        mod = _load_mem()
        with patch.dict(
            os.environ,
            {"PGDATABASE": "skill_system"},
            clear=True,
        ):
            with self.assertRaises(SystemExit):
                mod.resolve_db_target()

    def test_explicit_target_overrides_ambient_pgdatabase(self):
        mod = _load_mem()
        with patch.dict(
            os.environ,
            {"SKILL_PGDATABASE": "agent_memory", "PGDATABASE": "skill_system"},
            clear=False,
        ):
            db_target, source = mod.resolve_db_target()

        self.assertEqual(db_target, "agent_memory")
        self.assertEqual(source, "SKILL_PGDATABASE(overrides:skill_system)")


if __name__ == "__main__":
    unittest.main()
