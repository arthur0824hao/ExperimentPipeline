#!/usr/bin/env python3
"""Generate SKILL.behavior.yaml (Mermaid DAG) from SKILL.spec.yaml."""

import json
import sys
from datetime import datetime
from pathlib import Path

try:
    import yaml

    def _dump_yaml(data: dict) -> str:
        return yaml.dump(
            data,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            width=120,
        )

    def _load_yaml(path: Path) -> dict:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
except ImportError:
    yaml = None

    def _dump_yaml(data: dict) -> str:
        return json.dumps(data, indent=2, ensure_ascii=False)

    def _load_yaml(path: Path) -> dict:
        raise RuntimeError("PyYAML not installed")


def _generate_mermaid(spec: dict) -> str:
    """Generate Mermaid flowchart from spec operations."""
    lines = ["flowchart TD"]
    ops = spec.get("operations", [])

    # Input nodes
    for i, op in enumerate(ops):
        for inp in op.get("inputs", []):
            node_id = f"in_{i}_{inp['name'].replace('-', '_').replace(' ', '_')}"
            lines.append(f'  {node_id}[/"{inp["name"]}"/]:::input')

    # Operation nodes
    for i, op in enumerate(ops):
        op_id = f"op_{op['name'].replace('-', '_')}"
        lines.append(f'  {op_id}(["{op["name"]}"]):::stage')

    # Output nodes
    for i, op in enumerate(ops):
        for out in op.get("outputs", []):
            node_id = f"out_{i}_{out['name'].replace('-', '_').replace(' ', '_')}"
            lines.append(f'  {node_id}[\\"{out["name"]}"\\]:::output')

    lines.append("")

    # Edges: inputs → operations
    for i, op in enumerate(ops):
        op_id = f"op_{op['name'].replace('-', '_')}"
        for inp in op.get("inputs", []):
            in_id = f"in_{i}_{inp['name'].replace('-', '_').replace(' ', '_')}"
            lines.append(f"  {in_id} --> {op_id}")

    # Edges: operations → outputs
    for i, op in enumerate(ops):
        op_id = f"op_{op['name'].replace('-', '_')}"
        for out in op.get("outputs", []):
            out_id = f"out_{i}_{out['name'].replace('-', '_').replace(' ', '_')}"
            lines.append(f"  {op_id} --> {out_id}")

    # Constraint error paths
    for i, op in enumerate(ops):
        op_id = f"op_{op['name'].replace('-', '_')}"
        for j, c in enumerate(op.get("constraints", [])):
            err_id = f"err_{i}_{j}"
            short = c[:40] + "..." if len(c) > 40 else c
            lines.append(f'  {op_id} -->|"violation"| {err_id}["{short}"]:::error')

    depends_on = spec.get("depends_on", [])
    delegates_to = spec.get("delegates_to", [])
    for dep in depends_on:
        dep_id = dep.replace("-", "_")
        lines.append(f'  dep_{dep_id}{{{{"{dep}"}}}}:::dep')
        lines.append(
            f"  dep_{dep_id} -.->|depends_on| op_{ops[0]['name'].replace('-', '_') if ops else 'root'}"
        )
    for dlg in delegates_to:
        dlg_id = dlg.replace("-", "_")
        last_op = ops[-1]["name"].replace("-", "_") if ops else "root"
        lines.append(f"  op_{last_op} -.->|delegates_to| dlg_{dlg_id}")
        lines.append(f'  dlg_{dlg_id}{{{{"{dlg}"}}}}:::dep')

    lines.append("")
    lines.append(
        "  classDef stage fill:#dbeafe,stroke:#3b82f6,stroke-width:2px,color:#1e40af"
    )
    lines.append(
        "  classDef error fill:#fee2e2,stroke:#ef4444,stroke-width:2px,color:#991b1b,stroke-dasharray: 5 5"
    )
    lines.append(
        "  classDef input fill:#f0fdf4,stroke:#22c55e,stroke-width:2px,color:#166534"
    )
    lines.append(
        "  classDef output fill:#fef3c7,stroke:#f59e0b,stroke-width:2px,color:#92400e"
    )
    lines.append(
        "  classDef dep fill:#ede9fe,stroke:#8b5cf6,stroke-width:2px,color:#5b21b6,stroke-dasharray: 5 5"
    )

    return "\n".join(lines)


def generate(skill_dir: Path) -> dict:
    """Generate behavior contract from spec."""
    spec_path = skill_dir / "SKILL.spec.yaml"
    if not spec_path.exists():
        return {"status": "FAIL", "error": "SKILL.spec.yaml not found"}

    spec = _load_yaml(spec_path)

    # Build contract
    contract = {
        "schema_version": 1,
        "skill_name": spec.get("skill_name", "unknown"),
        "title": f"{spec.get('skill_name', 'unknown')} Behavior Contract",
        "purpose": spec.get("description", ""),
        "generated_from": "SKILL.spec.yaml",
        "updated_at": datetime.now().isoformat(),
        "depends_on": spec.get("depends_on", []),
        "delegates_to": spec.get("delegates_to", []),
        "operations": [],
        "mermaid": _generate_mermaid(spec),
    }

    for op in spec.get("operations", []):
        contract["operations"].append(
            {
                "name": op["name"],
                "intent": op.get("intent", ""),
                "inputs": [inp["name"] for inp in op.get("inputs", [])],
                "outputs": [out["name"] for out in op.get("outputs", [])],
                "constraints": op.get("constraints", []),
            }
        )

    # Write contract
    output_path = skill_dir / "SKILL.behavior.yaml"
    output_path.write_text(_dump_yaml(contract), encoding="utf-8")

    return {"status": "OK", "path": str(output_path)}


def main():
    if len(sys.argv) < 2:
        print(
            json.dumps(
                {"status": "FAIL", "error": "Usage: generate_contract.py <skill-dir>"}
            )
        )
        sys.exit(1)

    result = generate(Path(sys.argv[1]))
    if result["status"] == "OK":
        print(f"✓ Generated behavior contract: {result['path']}")
    else:
        print(f"✗ {result.get('error', 'Unknown error')}")

    print(json.dumps(result))
    sys.exit(0 if result["status"] == "OK" else 1)


if __name__ == "__main__":
    main()
