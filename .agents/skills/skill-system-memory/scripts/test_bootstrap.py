#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


BOOTSTRAP_PATH = Path(__file__).with_name("bootstrap.sh")


class TestBootstrap(unittest.TestCase):
    def _run_bootstrap(
        self, extra_env: dict[str, str] | None = None
    ) -> dict[str, str | bool]:
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ.copy()
            env["HOME"] = tmp
            env["PGUSER"] = "tester"
            env["PGDATABASE"] = "agent_memory"
            if extra_env:
                env.update(extra_env)
            result = subprocess.run(
                ["bash", str(BOOTSTRAP_PATH), "--install-all"],
                cwd=BOOTSTRAP_PATH.parent,
                env=env,
                text=True,
                input="",
                capture_output=True,
                check=True,
            )
            pgpass_path = Path(tmp) / ".pgpass"
            return {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "pgpass_exists": pgpass_path.exists(),
                "pgpass_content": pgpass_path.read_text(encoding="utf-8").strip()
                if pgpass_path.exists()
                else "",
            }

    def test_install_all_noninteractive_without_password_skips_pgpass_cleanly(
        self,
    ) -> None:
        result = self._run_bootstrap()
        self.assertNotIn("stty", str(result["stderr"]))
        self.assertNotIn("Password cannot be empty", str(result["stderr"]))
        self.assertFalse(bool(result["pgpass_exists"]))

    def test_install_all_uses_pgpassword_when_provided(self) -> None:
        result = self._run_bootstrap({"PGPASSWORD": "secret-pass"})
        self.assertTrue(bool(result["pgpass_exists"]))
        self.assertEqual(
            str(result["pgpass_content"]),
            "localhost:5432:agent_memory:tester:secret-pass",
        )


if __name__ == "__main__":
    unittest.main()
