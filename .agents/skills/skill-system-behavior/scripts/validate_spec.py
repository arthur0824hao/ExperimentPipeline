#!/usr/bin/env python3
"""Validate a SKILL.spec.yaml against the v1 schema."""

import json
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    # Fallback: parse YAML manually for simple cases
    yaml = None


def _load_yaml(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if yaml:
        return yaml.safe_load(text)
    # Minimal YAML parsing fallback — only handles flat/simple structures
    import re

    # Try JSON first (some specs might be JSON)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    raise RuntimeError("PyYAML not installed. Install with: pip install pyyaml")


def _is_non_empty_string(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_governance(governance, violations: list[str]) -> None:
    if governance is None:
        return
    if not isinstance(governance, dict):
        violations.append("governance must be a mapping")
        return
    tier = governance.get("tier")
    if tier is not None and tier not in {"architecture", "behavior", "test"}:
        violations.append(
            "governance.tier must be one of: architecture, behavior, test"
        )
    traces_to = governance.get("traces_to")
    if traces_to is None:
        return
    if not isinstance(traces_to, list):
        violations.append("governance.traces_to must be a list of strings")
        return
    if not all(_is_non_empty_string(item) for item in traces_to):
        violations.append("governance.traces_to entries must be non-empty strings")


def _validate_v1(spec: dict) -> list[str]:
    violations = []

    for field in ("schema_version", "skill_name", "description", "operations"):
        if field not in spec:
            violations.append(f"Missing required field: {field}")

    if violations:
        return violations

    if spec.get("schema_version") != 1:
        violations.append(
            f"schema_version must be 1, got: {spec.get('schema_version')}"
        )

    name = spec.get("skill_name", "")
    if not _is_non_empty_string(name):
        violations.append("skill_name must be a non-empty string")

    desc = spec.get("description", "")
    if not isinstance(desc, str) or len(desc.strip()) < 10:
        violations.append("description must be at least 10 characters")

    for dep_field in ("depends_on", "delegates_to"):
        dep = spec.get(dep_field)
        if dep is None:
            continue
        if not isinstance(dep, list):
            violations.append(f"{dep_field} must be a list of skill IDs")
        elif not all(_is_non_empty_string(item) for item in dep):
            violations.append(f"{dep_field} entries must be non-empty strings")

    _validate_governance(spec.get("governance"), violations)

    ops = spec.get("operations", [])
    if not isinstance(ops, list) or len(ops) == 0:
        violations.append("operations must be a non-empty list")
    else:
        for i, op in enumerate(ops):
            prefix = f"operations[{i}]"
            if not isinstance(op, dict):
                violations.append(f"{prefix}: must be a mapping")
                continue
            if not op.get("name"):
                violations.append(f"{prefix}: missing 'name'")
            if not op.get("intent"):
                violations.append(f"{prefix}: missing 'intent'")

            inputs = op.get("inputs", [])
            outputs = op.get("outputs", [])
            if not inputs and not outputs:
                violations.append(
                    f"{prefix} ({op.get('name', '?')}): must have at least one input or output"
                )

            constraints = op.get("constraints", [])
            if not constraints:
                violations.append(
                    f"{prefix} ({op.get('name', '?')}): missing constraints (at least one expected)"
                )

    tests = spec.get("acceptance_tests", {})
    if isinstance(tests, dict):
        structural = tests.get("structural", [])
        if isinstance(structural, list):
            test_ids = {t.get("id") for t in structural if isinstance(t, dict)}
            if "manifest-valid" not in test_ids:
                violations.append(
                    "acceptance_tests.structural: missing 'manifest-valid' test"
                )
            if "scripts-exist" not in test_ids:
                violations.append(
                    "acceptance_tests.structural: missing 'scripts-exist' test"
                )

        behavioral = tests.get("behavioral", [])
        if isinstance(behavioral, list):
            for i, test in enumerate(behavioral):
                if not isinstance(test, dict):
                    continue
                if "tested_by" in test:
                    tested_by = test.get("tested_by")
                    if not _is_non_empty_string(tested_by):
                        violations.append(
                            f"acceptance_tests.behavioral[{i}].tested_by must be a non-empty string"
                        )

    return violations


def _validate_v2_behavior(behavior, prefix: str, violations: list[str]) -> None:
    if not isinstance(behavior, dict):
        violations.append(f"{prefix}: must be a mapping")
        return

    if not _is_non_empty_string(behavior.get("name")):
        violations.append(f"{prefix}.name must be a non-empty string")
    if not _is_non_empty_string(behavior.get("intent")):
        violations.append(f"{prefix}.intent must be a non-empty string")

    input_types = {"string", "number", "boolean", "path", "json", "list", "map", "any"}
    inputs = behavior.get("inputs", [])
    if not isinstance(inputs, list):
        violations.append(f"{prefix}.inputs must be a list")
    else:
        for i, item in enumerate(inputs):
            ip = f"{prefix}.inputs[{i}]"
            if not isinstance(item, dict):
                violations.append(f"{ip}: must be a mapping")
                continue
            if not _is_non_empty_string(item.get("name")):
                violations.append(f"{ip}.name must be a non-empty string")
            item_type = item.get("type")
            if item_type not in input_types:
                violations.append(
                    f"{ip}.type must be one of: {', '.join(sorted(input_types))}"
                )
            if not isinstance(item.get("required"), bool):
                violations.append(f"{ip}.required must be a boolean")
            if not _is_non_empty_string(item.get("description")):
                violations.append(f"{ip}.description must be a non-empty string")

    output_types = {"markdown", "json", "file", "side_effect", "text", "metric", "any"}
    outputs = behavior.get("outputs", [])
    if not isinstance(outputs, list) or not outputs:
        violations.append(f"{prefix}.outputs must be a non-empty list")
    else:
        for i, item in enumerate(outputs):
            op = f"{prefix}.outputs[{i}]"
            if not isinstance(item, dict):
                violations.append(f"{op}: must be a mapping")
                continue
            if not _is_non_empty_string(item.get("name")):
                violations.append(f"{op}.name must be a non-empty string")
            item_type = item.get("type")
            if item_type not in output_types:
                violations.append(
                    f"{op}.type must be one of: {', '.join(sorted(output_types))}"
                )
            if not _is_non_empty_string(item.get("description")):
                violations.append(f"{op}.description must be a non-empty string")

    constraints = behavior.get("constraints", [])
    if not isinstance(constraints, list) or not constraints:
        violations.append(f"{prefix}.constraints must be a non-empty list")
    elif not all(_is_non_empty_string(item) for item in constraints):
        violations.append(f"{prefix}.constraints entries must be non-empty strings")

    effect_types = {
        "returns",
        "fs.read",
        "fs.write",
        "proc.exec",
        "db.read",
        "db.write",
        "net.fetch",
        "event.emit",
        "event.consume",
    }
    effects = behavior.get("expected_effects", [])
    if not isinstance(effects, list) or not effects:
        violations.append(f"{prefix}.expected_effects must be a non-empty list")
    else:
        for i, item in enumerate(effects):
            ep = f"{prefix}.expected_effects[{i}]"
            if not isinstance(item, dict):
                violations.append(f"{ep}: must be a mapping")
                continue
            effect_type = item.get("type")
            if effect_type not in effect_types:
                violations.append(
                    f"{ep}.type must be one of: {', '.join(sorted(effect_types))}"
                )
            if not _is_non_empty_string(item.get("description")):
                violations.append(f"{ep}.description must be a non-empty string")


def _validate_v2(spec: dict) -> list[str]:
    violations = []

    for field in ("schema_version", "spec_id", "description", "nodes"):
        if field not in spec:
            violations.append(f"Missing required field: {field}")

    if violations:
        return violations

    if spec.get("schema_version") != 2:
        violations.append(
            f"schema_version must be 2, got: {spec.get('schema_version')}"
        )

    if not _is_non_empty_string(spec.get("spec_id")):
        violations.append("spec_id must be a non-empty string")

    desc = spec.get("description", "")
    if not isinstance(desc, str) or len(desc.strip()) < 10:
        violations.append("description must be at least 10 characters")

    spec_kind = spec.get("spec_kind")
    if spec_kind is not None and spec_kind not in {
        "skill",
        "project",
        "component",
        "runtime",
    }:
        violations.append(
            "spec_kind must be one of: skill, project, component, runtime"
        )

    metadata = spec.get("metadata")
    if metadata is not None:
        if not isinstance(metadata, dict):
            violations.append("metadata must be a mapping")
        else:
            for field in ("owner", "version"):
                if field in metadata and not _is_non_empty_string(metadata.get(field)):
                    violations.append(f"metadata.{field} must be a non-empty string")
            tags = metadata.get("tags")
            if tags is not None:
                if not isinstance(tags, list):
                    violations.append("metadata.tags must be a list of strings")
                elif not all(_is_non_empty_string(item) for item in tags):
                    violations.append("metadata.tags entries must be non-empty strings")

    node_types = {
        "skill",
        "script",
        "doc",
        "config",
        "service",
        "table",
        "workflow",
        "runtime",
        "other",
    }

    nodes = spec.get("nodes", [])
    if not isinstance(nodes, list) or not nodes:
        violations.append("nodes must be a non-empty list")
        nodes = []

    node_ids = set()
    behavior_count = 0
    for i, node in enumerate(nodes):
        prefix = f"nodes[{i}]"
        if not isinstance(node, dict):
            violations.append(f"{prefix}: must be a mapping")
            continue

        node_id = node.get("id")
        if not _is_non_empty_string(node_id):
            violations.append(f"{prefix}.id must be a non-empty string")
        elif node_id in node_ids:
            violations.append(f"{prefix}.id must be unique, duplicate: {node_id}")
        else:
            node_ids.add(node_id)

        node_type = node.get("type")
        if node_type not in node_types:
            violations.append(
                f"{prefix}.type must be one of: {', '.join(sorted(node_types))}"
            )

        path = node.get("path")
        if path is not None and not _is_non_empty_string(path):
            violations.append(f"{prefix}.path must be a non-empty string when present")

        if not _is_non_empty_string(node.get("description")):
            violations.append(f"{prefix}.description must be a non-empty string")

        behaviors = node.get("behaviors")
        if behaviors is None:
            continue
        if not isinstance(behaviors, list):
            violations.append(f"{prefix}.behaviors must be a list")
            continue

        for j, behavior in enumerate(behaviors):
            behavior_count += 1
            _validate_v2_behavior(behavior, f"{prefix}.behaviors[{j}]", violations)

    if behavior_count == 0:
        violations.append(
            "nodes must include at least one behavior entry across all nodes"
        )

    relation_types = {
        "depends_on",
        "delegates_to",
        "reads",
        "writes",
        "executes",
        "validates",
        "documents",
        "configures",
        "emits",
        "consumes",
        "owns",
        "references",
        "other",
    }
    relationships = spec.get("relationships", [])
    if relationships is not None and not isinstance(relationships, list):
        violations.append("relationships must be a list when present")
        relationships = []

    for i, relation in enumerate(relationships or []):
        prefix = f"relationships[{i}]"
        if not isinstance(relation, dict):
            violations.append(f"{prefix}: must be a mapping")
            continue
        source = relation.get("from")
        target = relation.get("to")
        if not _is_non_empty_string(source):
            violations.append(f"{prefix}.from must be a non-empty string")
        if not _is_non_empty_string(target):
            violations.append(f"{prefix}.to must be a non-empty string")
        relation_type = relation.get("type")
        if relation_type not in relation_types:
            violations.append(
                f"{prefix}.type must be one of: {', '.join(sorted(relation_types))}"
            )
        relation_desc = relation.get("description")
        if relation_desc is not None and not _is_non_empty_string(relation_desc):
            violations.append(
                f"{prefix}.description must be a non-empty string when present"
            )

    tests = spec.get("acceptance_tests")
    if tests is not None:
        if not isinstance(tests, dict):
            violations.append("acceptance_tests must be a mapping when present")
        else:
            structural = tests.get("structural", [])
            if not isinstance(structural, list):
                violations.append("acceptance_tests.structural must be a list")
            else:
                ids = set()
                for i, item in enumerate(structural):
                    prefix = f"acceptance_tests.structural[{i}]"
                    if not isinstance(item, dict):
                        violations.append(f"{prefix}: must be a mapping")
                        continue
                    test_id = item.get("id")
                    if not _is_non_empty_string(test_id):
                        violations.append(f"{prefix}.id must be a non-empty string")
                    elif test_id in ids:
                        violations.append(
                            f"{prefix}.id must be unique, duplicate: {test_id}"
                        )
                    else:
                        ids.add(test_id)
                    if not _is_non_empty_string(item.get("assert")):
                        violations.append(f"{prefix}.assert must be a non-empty string")

            behavioral = tests.get("behavioral", [])
            if not isinstance(behavioral, list):
                violations.append("acceptance_tests.behavioral must be a list")
            else:
                ids = set()
                for i, item in enumerate(behavioral):
                    prefix = f"acceptance_tests.behavioral[{i}]"
                    if not isinstance(item, dict):
                        violations.append(f"{prefix}: must be a mapping")
                        continue
                    test_id = item.get("id")
                    if not _is_non_empty_string(test_id):
                        violations.append(f"{prefix}.id must be a non-empty string")
                    elif test_id in ids:
                        violations.append(
                            f"{prefix}.id must be unique, duplicate: {test_id}"
                        )
                    else:
                        ids.add(test_id)
                    for required_key in ("given", "when", "then"):
                        if not _is_non_empty_string(item.get(required_key)):
                            violations.append(
                                f"{prefix}.{required_key} must be a non-empty string"
                            )
                    if "tested_by" in item and not _is_non_empty_string(
                        item.get("tested_by")
                    ):
                        violations.append(
                            f"{prefix}.tested_by must be a non-empty string"
                        )

    _validate_governance(spec.get("governance"), violations)
    return violations


def validate_spec(spec_path: Path) -> dict:
    """Validate spec and return {status, violations}."""
    if not spec_path.exists():
        return {"status": "FAIL", "violations": [f"File not found: {spec_path}"]}

    try:
        spec = _load_yaml(spec_path)
    except Exception as e:
        return {"status": "FAIL", "violations": [f"Parse error: {e}"]}

    if not isinstance(spec, dict):
        return {"status": "FAIL", "violations": ["Spec must be a YAML mapping"]}

    schema_version = spec.get("schema_version")
    if schema_version == 1:
        violations = _validate_v1(spec)
    elif schema_version == 2:
        violations = _validate_v2(spec)
    else:
        violations = [
            f"Unsupported schema_version: {schema_version}. Supported versions: 1, 2"
        ]

    status = "PASS" if not violations else "FAIL"
    return {"status": status, "violations": violations}


def main():
    if len(sys.argv) < 2:
        print(
            json.dumps(
                {"status": "FAIL", "violations": ["Usage: validate_spec.py <path>"]}
            )
        )
        sys.exit(1)

    spec_path = Path(sys.argv[1])
    result = validate_spec(spec_path)

    # Print human-readable summary
    if result["status"] == "PASS":
        print(f"✓ Spec validation PASSED: {spec_path}")
    else:
        print(f"✗ Spec validation FAILED: {spec_path}")
        for v in result["violations"]:
            print(f"  - {v}")

    # Last line JSON (stdout_contract)
    print(json.dumps(result))
    sys.exit(0 if result["status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
