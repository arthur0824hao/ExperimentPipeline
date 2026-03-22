#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
import graph_cli
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[3]
CLI_PATH = Path(__file__).with_name("graph_cli.py")


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    child_env = os.environ.copy()
    child_env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, str(CLI_PATH), *args],
        cwd=ROOT_DIR,
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
    )


def parse_json_stdout(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise AssertionError(
            f"Expected JSON on stdout but got empty output. stderr={result.stderr!r}"
        )
    try:
        payload = json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"Failed to parse stdout JSON from line: {lines[-1]!r}; stderr={result.stderr!r}"
        ) from exc
    if not isinstance(payload, dict):
        raise AssertionError(f"Expected JSON object payload, got: {type(payload)}")
    return payload


class GraphCliBlackBoxTests(unittest.TestCase):
    def _assert_structured_error(self, payload: dict[str, Any]) -> None:
        self.assertEqual(payload.get("status"), "error")
        self.assertIn("error_code", payload)
        self.assertIn("message", payload)

    def test_show_json_output_has_expected_structure(self) -> None:
        result = run_cli("show")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        payload = parse_json_stdout(result)
        self.assertEqual(payload.get("status"), "ok")

        nodes = payload.get("nodes")
        edges = payload.get("edges")
        self.assertIsInstance(nodes, list)
        self.assertIsInstance(edges, list)
        if not isinstance(nodes, list) or not isinstance(edges, list):
            self.fail("show payload must provide list nodes and edges")
        self.assertEqual(payload.get("node_count"), len(nodes))
        self.assertEqual(payload.get("edge_count"), len(edges))

        self.assertGreater(len(nodes), 0)
        first_node = nodes[0]
        self.assertIsInstance(first_node, dict)
        if not isinstance(first_node, dict):
            self.fail("node entry must be an object")
        self.assertIn("skill_name", first_node)
        self.assertIn("description", first_node)
        self.assertIn("spec_path", first_node)
        self.assertIn("operations_count", first_node)
        self.assertIn("content_hash", first_node)
        self.assertIn("stub", first_node)
        self.assertIsInstance(first_node.get("operations_count"), int)
        self.assertIsInstance(first_node.get("stub"), bool)

        if edges:
            first_edge = edges[0]
            self.assertIsInstance(first_edge, dict)
            if not isinstance(first_edge, dict):
                self.fail("edge entry must be an object")
            self.assertIn("source", first_edge)
            self.assertIn("target", first_edge)
            self.assertIn("edge_type", first_edge)

    def test_show_text_output_is_human_readable(self) -> None:
        result = run_cli("show", "--format", "text")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("Skill graph", result.stdout)
        self.assertIn("Nodes:", result.stdout)
        self.assertIn("Edges:", result.stdout)
        self.assertIn("Nodes", result.stdout)
        self.assertIn("Edges", result.stdout)

    def test_help_output_lists_core_commands(self) -> None:
        result = run_cli("--help")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("Skill graph CLI", result.stdout)
        self.assertIn("show", result.stdout)
        self.assertIn("neighbors", result.stdout)
        self.assertIn("path", result.stdout)
        self.assertIn("impact", result.stdout)
        self.assertIn("refresh", result.stdout)

    def test_show_help_output_describes_skills_dir_option(self) -> None:
        result = run_cli("show", "--help")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("usage: graph_cli.py show", result.stdout)
        self.assertIn("--skills-dir", result.stdout)

    def test_invalid_command_returns_argument_error_contract(self) -> None:
        result = run_cli("not-a-command")
        self.assertEqual(result.returncode, 2)

        payload = parse_json_stdout(result)
        self._assert_structured_error(payload)
        self.assertEqual(payload.get("error_code"), "GRAPH-CLI-ARG")
        self.assertIn("invalid choice", str(payload.get("message")))

    def test_neighbors_invalid_skill_returns_structured_error(self) -> None:
        missing_skill = "skill-that-should-not-exist-xyz"
        result = run_cli("neighbors", missing_skill)
        self.assertNotEqual(result.returncode, 0)

        payload = parse_json_stdout(result)
        self.assertEqual(payload.get("status"), "error")
        self.assertEqual(payload.get("error_code"), "GRAPH-CLI-SKILL")
        self.assertIn("Unknown skill", str(payload.get("message")))

        details = payload.get("details")
        self.assertIsInstance(details, dict)
        if not isinstance(details, dict):
            self.fail("error details must be an object")
        self.assertEqual(details.get("skill"), missing_skill)
        self.assertIsInstance(details.get("known_skills"), int)

    def test_path_command_emits_structured_json_on_success_or_failure(self) -> None:
        result = run_cli(
            "path", "skill-system-graph", "skill-system-cli", "--max-depth", "2"
        )
        payload = parse_json_stdout(result)

        if result.returncode == 0:
            self.assertEqual(payload.get("status"), "ok")
            self.assertEqual(payload.get("from_skill"), "skill-system-graph")
            self.assertEqual(payload.get("to_skill"), "skill-system-cli")
            self.assertIsInstance(payload.get("path"), list)
            self.assertIsInstance(payload.get("found"), bool)
            self.assertEqual(payload.get("max_depth"), 2)
        else:
            self._assert_structured_error(payload)

    def test_neighbors_contract_for_known_skill_success_or_structured_error(
        self,
    ) -> None:
        result = run_cli("neighbors", "skill-system-graph")
        payload = parse_json_stdout(result)

        if result.returncode == 0:
            self.assertEqual(payload.get("status"), "ok")
            self.assertEqual(payload.get("skill"), "skill-system-graph")
            self.assertIsInstance(payload.get("outgoing"), list)
            self.assertIsInstance(payload.get("incoming"), list)
            self.assertIsInstance(payload.get("neighbors"), list)
            self.assertIsInstance(payload.get("neighbor_count"), int)
        else:
            self._assert_structured_error(payload)

    def test_impact_contract_for_known_skill_success_or_structured_error(self) -> None:
        result = run_cli("impact", "skill-system-graph", "--max-depth", "3")
        payload = parse_json_stdout(result)

        if result.returncode == 0:
            self.assertEqual(payload.get("status"), "ok")
            self.assertEqual(payload.get("skill"), "skill-system-graph")
            self.assertIsInstance(payload.get("impact"), list)
            self.assertIsInstance(payload.get("impact_count"), int)
            self.assertEqual(payload.get("max_depth"), 3)
        else:
            self._assert_structured_error(payload)

    def test_normalize_impact_payload_accepts_graph_queries_keys(self) -> None:
        raw = {
            "skill": "skill-system-graph",
            "max_depth": 3,
            "count": 2,
            "impacts": [
                {
                    "impact_skill": "skill-system-router",
                    "depth": 1,
                    "path": ["skill-system-router", "skill-system-graph"],
                },
                {
                    "impact_skill": "skill-system-cli",
                    "depth": 2,
                    "path": [
                        "skill-system-cli",
                        "skill-system-router",
                        "skill-system-graph",
                    ],
                },
            ],
        }

        payload = graph_cli._normalize_impact_payload("skill-system-graph", 3, raw)

        self.assertEqual(payload.get("skill"), "skill-system-graph")
        self.assertEqual(payload.get("max_depth"), 3)
        self.assertEqual(payload.get("impact_count"), 2)
        self.assertEqual(payload.get("impact"), raw["impacts"])
        self.assertEqual(payload.get("count"), 2)
        self.assertEqual(payload.get("impacts"), raw["impacts"])

    def test_refresh_contract_or_structured_error(self) -> None:
        result = run_cli("refresh", "--force")
        payload = parse_json_stdout(result)

        if result.returncode == 0:
            self.assertEqual(payload.get("status"), "ok")
            self.assertIsInstance(payload.get("parsed"), int)
            self.assertIsInstance(payload.get("inserted"), int)
            self.assertIsInstance(payload.get("updated"), int)
            self.assertIsInstance(payload.get("skipped"), int)
            self.assertIsInstance(payload.get("removed"), int)
            self.assertTrue(payload.get("force"))
        else:
            self._assert_structured_error(payload)


if __name__ == "__main__":
    unittest.main()
