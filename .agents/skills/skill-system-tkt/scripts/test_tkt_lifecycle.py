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


class TktLifecycleTests(unittest.TestCase):
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

    def _run_git(self, root: Path, *args: str) -> None:
        subprocess.run(
            ["git", *args],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        )

    def _make_env(self, tmp: str, *, git_repo: bool = False) -> dict[str, str]:
        root = Path(tmp) / "project"
        spec_dir = root / "spec"
        config_dir = root / "config"
        spec_dir.mkdir(parents=True)
        config_dir.mkdir(parents=True)
        (spec_dir / "validate_repo_structural.py").write_text(
            textwrap.dedent(
                """
                #!/usr/bin/env python3
                import sys

                print("ok")
                sys.exit(0)
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        (config_dir / "tkt.yaml").write_text(
            yaml.safe_dump({"close_gate": {"command": None}}, sort_keys=False),
            encoding="utf-8",
        )
        if git_repo:
            self._run_git(root, "init", "-b", "main")
            self._run_git(root, "config", "user.email", "test@example.com")
            self._run_git(root, "config", "user.name", "Test User")
            (root / "README.md").write_text("base\n", encoding="utf-8")
            self._run_git(root, "add", "README.md")
            self._run_git(root, "commit", "-m", "init")
        env = os.environ.copy()
        env["PROJECT_ROOT"] = str(root)
        env["TKT_ROOT"] = str(root / ".tkt")
        return env

    def _bundle_dir(self, env: dict[str, str], bundle: str = "B-001") -> Path:
        return Path(env["TKT_ROOT"]) / "bundles" / bundle

    def test_state_machine(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "lifecycle", env=env)
            self._run("create-bundle", "--goal", "state machine", env=env)
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
            invalid = self._run(
                "update",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--status",
                "claimed",
                env=env,
                check=False,
            )
            self.assertNotEqual(invalid.returncode, 0)
            self.assertIn("SK-TKT-035", invalid.stdout)
            claim_done = self._run(
                "claim",
                "--bundle",
                "B-001",
                "--ticket",
                "TKT-001",
                "--agent",
                "worker-agent",
                env=env,
                check=False,
            )
            self.assertNotEqual(claim_done.returncode, 0)
            self.assertIn("SK-TKT-035", claim_done.stdout)

    def test_worktree_create(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp, git_repo=True)
            self._run("init-roadmap", "--project", "lifecycle", env=env)
            self._run(
                "create-bundle",
                "--goal",
                "worktree bundle",
                "--worktree",
                env=env,
            )
            bundle = yaml.safe_load((self._bundle_dir(env) / "bundle.yaml").read_text())
            self.assertEqual(bundle["worktree_path"], ".worktrees/B-001")
            self.assertEqual(bundle["worktree_branch"], "bundle/B-001")
            self.assertTrue(
                (Path(env["PROJECT_ROOT"]) / ".worktrees" / "B-001").exists()
            )

    def test_worktree_close_dirty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp, git_repo=True)
            self._run("init-roadmap", "--project", "lifecycle", env=env)
            self._run(
                "create-bundle",
                "--goal",
                "worktree bundle",
                "--worktree",
                env=env,
            )
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
            (
                Path(env["PROJECT_ROOT"]) / ".worktrees" / "B-001" / "dirty.txt"
            ).write_text("dirty\n", encoding="utf-8")
            result = self._run("close", "--bundle", "B-001", env=env, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("SK-TKT-037", result.stdout)

    def test_worktree_close_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp, git_repo=True)
            self._run("init-roadmap", "--project", "lifecycle", env=env)
            self._run(
                "create-bundle",
                "--goal",
                "worktree bundle",
                "--worktree",
                env=env,
            )
            self._run("add", "--bundle", "B-001", "--title", "worker", env=env)
            worktree_root = Path(env["PROJECT_ROOT"]) / ".worktrees" / "B-001"
            feature_file = worktree_root / "feature.txt"
            feature_file.write_text("merged\n", encoding="utf-8")
            self._run_git(worktree_root, "add", "feature.txt")
            self._run_git(worktree_root, "commit", "-m", "worktree change")
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
            self._run("close", "--bundle", "B-001", "--merge", env=env)
            self.assertFalse(worktree_root.exists())
            self.assertTrue((Path(env["PROJECT_ROOT"]) / "feature.txt").exists())

    def test_review_auto_fill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "lifecycle", env=env)
            self._run("create-bundle", "--goal", "review bundle", env=env)
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
                "--summary",
                "worker done",
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
            self._run("close", "--bundle", "B-001", env=env)
            review = yaml.safe_load((self._bundle_dir(env) / "review.yaml").read_text())
            self.assertEqual(review["tickets_completed"], 3)
            self.assertIn("evidence_summary", review)
            self.assertIn("acceptance_results", review)
            self.assertEqual(len(review["audit_result"]["checked_items"]), 2)
            self.assertEqual(review["audit_result"]["quality_score"], 1.0)
            evidence = {
                item["ticket_id"]: item["evidence"]
                for item in review["evidence_summary"]
            }
            self.assertEqual(evidence["TKT-001"], "proof")

    def test_carryover_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "lifecycle", env=env)
            self._run("create-bundle", "--goal", "carryover bundle", env=env)
            self._run("add", "--bundle", "B-001", "--title", "worker", env=env)
            ticket_path = self._bundle_dir(env) / "TKT-001.yaml"
            data = yaml.safe_load(ticket_path.read_text())
            data["acceptance_criteria"] = [
                {"type": "command", "run": "python3 -c 'import sys; sys.exit(1)'"}
            ]
            ticket_path.write_text(
                yaml.safe_dump(data, sort_keys=False), encoding="utf-8"
            )
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
            result = self._run("close", "--bundle", "B-001", env=env)
            self.assertIn('"carryover_count":1', result.stdout)
            carryover = yaml.safe_load(
                (self._bundle_dir(env) / "carryover.yaml").read_text()
            )
            self.assertEqual(
                carryover["carryover"][0]["reason"], "acceptance_criteria_failed"
            )
            review = yaml.safe_load((self._bundle_dir(env) / "review.yaml").read_text())
            self.assertEqual(review["acceptance_results"]["failed"], 1)

    def test_carryover_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = self._make_env(tmp)
            self._run("init-roadmap", "--project", "lifecycle", env=env)
            self._run("create-bundle", "--goal", "origin bundle", env=env)
            carryover = {
                "bundle": "B-001",
                "carryover": [
                    {
                        "ticket_id": "TKT-001",
                        "title": "Retry worker",
                        "carryover_from": "B-001/TKT-001",
                        "reason": "acceptance_criteria_failed",
                        "failed_criteria": [{"criterion_index": 1}],
                        "original_description": "original description",
                    }
                ],
            }
            (self._bundle_dir(env) / "carryover.yaml").write_text(
                yaml.safe_dump(carryover, sort_keys=False), encoding="utf-8"
            )
            self._run(
                "create-bundle",
                "--goal",
                "retry bundle",
                "--carryover",
                "B-001",
                env=env,
            )
            ticket = yaml.safe_load(
                (self._bundle_dir(env, "B-002") / "TKT-001.yaml").read_text()
            )
            self.assertEqual(ticket["title"], "Retry worker")
            self.assertIn("carryover_from: B-001/TKT-001", ticket["description"])


if __name__ == "__main__":
    unittest.main()
