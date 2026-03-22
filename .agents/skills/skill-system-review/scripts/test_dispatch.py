#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml


MODULE_PATH = Path(__file__).with_name("review_prompt.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("review_prompt_dispatch", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReviewDispatchTests(unittest.TestCase):
    def _write_ticket(
        self,
        bundle_dir: Path,
        ticket_id: str,
        title: str,
        status: str = "open",
        depends_on: list[str] | None = None,
        wave: int | None = None,
        effort: str | None = None,
    ) -> None:
        payload = {
            "id": ticket_id,
            "bundle": bundle_dir.name,
            "type": "worker",
            "title": title,
            "status": status,
            "depends_on": depends_on or [],
            "effort_estimate": effort,
        }
        if wave is not None:
            payload["wave"] = wave
        (bundle_dir / f"{ticket_id}.yaml").write_text(
            yaml.safe_dump(payload, sort_keys=False), encoding="utf-8"
        )

    def test_dispatch_outputs_open_tickets(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundles = root / ".tkt" / "bundles"
            bundle_dir = bundles / "B-123"
            bundle_dir.mkdir(parents=True)
            self._write_ticket(bundle_dir, "TKT-001", "First task")
            self._write_ticket(bundle_dir, "TKT-002", "Done task", status="done")
            out = io.StringIO()
            with patch.object(mod, "BUNDLES_DIR", bundles), patch("sys.stdout", out):
                mod.cmd_generate_dispatch(
                    type("Args", (), {"bundle": "B-123", "tickets": None})()
                )
            lines = out.getvalue().strip().splitlines()
            payload = json.loads(lines[-1])
            self.assertEqual(
                [ticket["ticket_id"] for ticket in payload["tickets"]], ["TKT-001"]
            )
            self.assertIn("Wave 1", out.getvalue())

    def test_dispatch_dependency_ordering(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundles = root / ".tkt" / "bundles"
            bundle_dir = bundles / "B-456"
            bundle_dir.mkdir(parents=True)
            self._write_ticket(bundle_dir, "TKT-001", "Base task")
            self._write_ticket(
                bundle_dir, "TKT-002", "Dependent task", depends_on=["TKT-001"]
            )
            out = io.StringIO()
            with patch.object(mod, "BUNDLES_DIR", bundles), patch("sys.stdout", out):
                mod.cmd_generate_dispatch(
                    type("Args", (), {"bundle": "B-456", "tickets": None})()
                )
            payload = json.loads(out.getvalue().strip().splitlines()[-1])
            waves = {
                ticket["ticket_id"]: ticket["dispatch_wave"]
                for ticket in payload["tickets"]
            }
            self.assertEqual(waves["TKT-001"], 1)
            self.assertEqual(waves["TKT-002"], 2)


if __name__ == "__main__":
    unittest.main()
