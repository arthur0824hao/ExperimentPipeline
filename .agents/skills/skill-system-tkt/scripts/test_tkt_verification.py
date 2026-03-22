#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

import yaml


ROOT_DIR = Path(__file__).resolve().parents[3]
SCRIPT_PATH = Path(__file__).with_name("tkt.sh")


class TktVerificationTests(unittest.TestCase):
    def _run(
        self, *args: str, env: dict[str, str], check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT_PATH), *args],
            cwd=ROOT_DIR,
            env=env,
            capture_output=True,
            text=True,
            check=check,
        )

    def _make_env(
        self,
        tmp: str,
        *,
        structural_exit: int = 0,
        close_gate_command: str | None = None,
    ) -> dict[str, str]:
        root = Path(tmp) / "project"
        spec_dir = root / "spec"
        config_dir = root / "config"
        spec_dir.mkdir(parents=True)
        config_dir.mkdir(parents=True)
        (spec_dir / "validate_repo_structural.py").write_text(
            textwrap.dedent(
                f"""
                #!/usr/bin/env python3
                import sys

                print("structural check")
                sys.exit({structural_exit})
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        config = {"close_gate": {"command": close_gate_command}}
        (config_dir / "tkt.yaml").write_text(
            yaml.safe_dump(config, sort_keys=False),
            encoding="utf-8",
        )
        env = os.environ.copy()
        env["TKT_ROOT"] = str(Path(tmp) / ".tkt")
        env["PROJECT_ROOT"] = str(root)
        return env

    def _ticket_path(self, env: dict[str, str], ticket_id: str) -> Path:
        return Path(env["TKT_ROOT"]) / "bundles" / "B-001" / f"{ticket_id}.yaml"

    def _bundle_path(self, env: dict[str, str]) -> Path:
        return Path(env["TKT_ROOT"]) / "bundles" / "B-001"

    def test_evidence_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "verify", env=env)
            self._run("create-bundle", "--goal", "verify bundle", env=env)
            self._run("add", "--bundle", "B-001", "--title", "worker", env=env)
            result = self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--status",
                "done",
                env=env,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("SK-TKT-030", result.stdout)

    def test_evidence_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "verify", env=env)
            self._run("create-bundle", "--goal", "verify bundle", env=env)
            self._run("add", "--bundle", "B-001", "--title", "worker", env=env)
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--status",
                "done",
                "--evidence",
                "proof",
                env=env,
            )
            data = yaml.safe_load(self._ticket_path(env, "TKT-001").read_text())
            self.assertEqual(data["result"]["evidence"], "proof")

    def test_self_audit_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "verify", env=env)
            self._run("create-bundle", "--goal", "verify bundle", env=env)
            self._run("add", "--bundle", "B-001", "--title", "worker", env=env)
            self._run(
                "claim",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--agent",
                "same-agent",
                env=env,
            )
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--status",
                "done",
                "--evidence",
                "proof",
                env=env,
            )
            self._run(
                "claim",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-A00",
                "--agent",
                "same-agent",
                env=env,
            )
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-A00",
                "--status",
                "done",
                "--evidence",
                "audit-proof",
                env=env,
            )
            result = self._run("close", "--bundle", "B-001", env=env, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("SK-TKT-031", result.stdout)

    def test_audit_must_be_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "verify", env=env)
            self._run("create-bundle", "--goal", "verify bundle", env=env)
            self._run("add", "--bundle", "B-001", "--title", "worker", env=env)
            self._run(
                "claim",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--agent",
                "worker-agent",
                env=env,
            )
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--status",
                "done",
                "--evidence",
                "proof",
                env=env,
            )
            result = self._run("close", "--bundle", "B-001", env=env, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("SK-TKT-012", result.stdout)

    def test_structural_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp, structural_exit=1)
            self._run("init-roadmap", "--project", "verify", env=env)
            self._run("create-bundle", "--goal", "verify bundle", env=env)
            self._run("add", "--bundle", "B-001", "--title", "worker", env=env)
            self._run(
                "claim",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-A00",
                "--agent",
                "audit-agent",
                env=env,
            )
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-A00",
                "--status",
                "done",
                "--evidence",
                "audit-proof",
                env=env,
            )
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--status",
                "done",
                "--evidence",
                "proof",
                env=env,
            )
            result = self._run("close", "--bundle", "B-001", env=env, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("SK-TKT-032", result.stdout)

    def test_close_gate_command_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(
                tmp,
                close_gate_command="python3 -c 'import sys; sys.exit(1)'",
            )
            self._run("init-roadmap", "--project", "verify", env=env)
            self._run("create-bundle", "--goal", "verify bundle", env=env)
            self._run("add", "--bundle", "B-001", "--title", "worker", env=env)
            self._run(
                "claim",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-A00",
                "--agent",
                "audit-agent",
                env=env,
            )
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-A00",
                "--status",
                "done",
                "--evidence",
                "audit-proof",
                env=env,
            )
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--status",
                "done",
                "--evidence",
                "proof",
                env=env,
            )
            result = self._run("close", "--bundle", "B-001", env=env, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("SK-TKT-033", result.stdout)

    def test_executable_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "verify", env=env)
            self._run("create-bundle", "--goal", "verify bundle", env=env)
            self._run("add", "--bundle", "B-001", "--title", "worker", env=env)
            self._run(
                "claim",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-A00",
                "--agent",
                "audit-agent",
                env=env,
            )
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-A00",
                "--status",
                "done",
                "--evidence",
                "audit-proof",
                env=env,
            )
            ticket_path = self._ticket_path(env, "TKT-001")
            data = yaml.safe_load(ticket_path.read_text())
            data["acceptance_criteria"] = [
                {
                    "type": "command",
                    "run": "python3 -c 'import sys; sys.exit(1)'",
                    "expect_exit_code": 0,
                }
            ]
            ticket_path.write_text(
                yaml.safe_dump(data, sort_keys=False), encoding="utf-8"
            )
            self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--status",
                "done",
                "--evidence",
                "proof",
                env=env,
            )
            result = self._run("close", "--bundle", "B-001", env=env, check=False)
            self.assertEqual(result.returncode, 0)
            self.assertIn('"carryover_count":1', result.stdout)
            carryover = yaml.safe_load(
                (self._bundle_path(env) / "carryover.yaml").read_text()
            )
            self.assertEqual(len(carryover["carryover"]), 1)
            self.assertEqual(carryover["carryover"][0]["ticket_id"], "TKT-001")

    def test_skills_wave_qa_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "verify", env=env)
            self._run("create-bundle", "--goal", "verify bundle", env=env)
            self._run(
                "add",
                "--bundle",
                "B-001",
                "--title",
                "worker",
                "--skills",
                "skill-a,skill-b",
                "--wave",
                "2",
                "--qa-scenarios",
                "run pytest,run lint",
                env=env,
            )
            self._run(
                "express",
                "--title",
                "express worker",
                "--acceptance",
                "done",
                "--wave",
                "3",
                env=env,
            )
            worker = yaml.safe_load(self._ticket_path(env, "TKT-001").read_text())
            express = yaml.safe_load(
                (Path(env["TKT_ROOT"]) / "express" / "EXP-001.yaml").read_text()
            )
            self.assertEqual(worker["skills"], ["skill-a", "skill-b"])
            self.assertEqual(worker["wave"], 2)
            self.assertEqual(worker["qa_scenarios"], ["run pytest", "run lint"])
            self.assertEqual(express["wave"], 3)


if __name__ == "__main__":
    unittest.main()
