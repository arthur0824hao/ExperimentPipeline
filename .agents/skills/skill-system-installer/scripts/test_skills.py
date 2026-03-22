#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("skills.sh")


class TestSkillsSync(unittest.TestCase):
    def _write_lockfile(self, path: Path, skills: dict[str, dict[str, str]]) -> None:
        payload = {"version": 1, "skills": skills}
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def test_sync_skipped_skill_not_added_to_local_lockfile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            global_home = root / "global"
            workspace = root / "workspace"
            (global_home / "skills").mkdir(parents=True)
            workspace.mkdir()

            self._write_lockfile(
                global_home / "skills-lock.json",
                {
                    "missing-skill": {
                        "source": "example/repo",
                        "sourceType": "github",
                        "computedHash": "abc123",
                    }
                },
            )

            env = os.environ.copy()
            env["SKILLS_GLOBAL_HOME"] = str(global_home)
            result = subprocess.run(
                ["bash", str(SCRIPT_PATH), "sync", "missing-skill"],
                cwd=workspace,
                env=env,
                text=True,
                capture_output=True,
                check=True,
            )

            self.assertIn("Skipping missing-skill", result.stdout)
            lockfile = workspace / "skills-lock.json"
            if lockfile.exists():
                payload = json.loads(lockfile.read_text(encoding="utf-8"))
                self.assertNotIn("missing-skill", payload.get("skills", {}))

    def test_sync_copied_skill_added_to_local_lockfile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            global_home = root / "global"
            workspace = root / "workspace"
            skill_dir = global_home / "skills" / "present-skill"
            skill_dir.mkdir(parents=True)
            (skill_dir / "SKILL.md").write_text(
                "---\nname: present-skill\ndescription: test\n---\n", encoding="utf-8"
            )
            workspace.mkdir()

            self._write_lockfile(
                global_home / "skills-lock.json",
                {
                    "present-skill": {
                        "source": "example/repo",
                        "sourceType": "github",
                        "computedHash": "def456",
                    }
                },
            )

            env = os.environ.copy()
            env["SKILLS_GLOBAL_HOME"] = str(global_home)
            result = subprocess.run(
                ["bash", str(SCRIPT_PATH), "sync", "present-skill"],
                cwd=workspace,
                env=env,
                text=True,
                capture_output=True,
                check=True,
            )

            self.assertIn("Synced present-skill (copy)", result.stdout)
            payload = json.loads(
                (workspace / "skills-lock.json").read_text(encoding="utf-8")
            )
            self.assertIn("present-skill", payload.get("skills", {}))


if __name__ == "__main__":
    unittest.main()
