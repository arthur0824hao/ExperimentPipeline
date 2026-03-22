#!/usr/bin/env python3
"""Generate a v2 behavior spec from a source file."""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path

try:
    import yaml as _yaml

    def _dump_yaml(data: dict) -> str:
        return _yaml.safe_dump(
            data,
            sort_keys=False,
            allow_unicode=True,
            width=120,
        )

except ImportError:

    def _dump_yaml(data: dict) -> str:
        return json.dumps(data, indent=2, ensure_ascii=False)


ALLOWED_INPUT_TYPES = {
    "string",
    "number",
    "boolean",
    "path",
    "json",
    "list",
    "map",
    "any",
}
ALLOWED_OUTPUT_TYPES = {
    "markdown",
    "json",
    "file",
    "side_effect",
    "text",
    "metric",
    "any",
}


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "generated"


def _kebab_from_snake(name: str) -> str:
    return _slug(name.replace("_", "-"))


def _first_sentence(text: str) -> str:
    clean = " ".join((text or "").strip().split())
    if not clean:
        return ""
    for sep in (". ", "。", "! ", "? "):
        if sep in clean:
            return clean.split(sep, 1)[0].strip()
    return clean


def _annotation_to_type(annotation: ast.AST | None, arg_name: str) -> str:
    if "path" in arg_name.lower() or "file" in arg_name.lower():
        return "path"
    if annotation is None:
        return "any"
    text = ast.unparse(annotation).lower()
    if any(key in text for key in ("bool",)):
        return "boolean"
    if any(key in text for key in ("int", "float", "decimal", "number")):
        return "number"
    if any(key in text for key in ("str", "string")):
        return "string"
    if any(key in text for key in ("path", "pathlib")):
        return "path"
    if any(key in text for key in ("dict", "mapping")):
        return "map"
    if any(key in text for key in ("list", "tuple", "set", "sequence")):
        return "list"
    if any(key in text for key in ("json", "object")):
        return "json"
    return "any"


def _literal_default(node: ast.AST | None):
    if node is None:
        return None
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        values = []
        for item in node.elts:
            if isinstance(item, ast.Constant):
                values.append(item.value)
            else:
                return ast.unparse(node)
        return values
    if isinstance(node, ast.Dict):
        out = {}
        for key, value in zip(node.keys, node.values):
            if not isinstance(key, ast.Constant) or not isinstance(value, ast.Constant):
                return ast.unparse(node)
            out[key.value] = value.value
        return out
    return ast.unparse(node)


def _call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        base = (
            _call_name(ast.Call(func=node.func.value, args=[], keywords=[]))
            if isinstance(node.func.value, ast.Call)
            else ""
        )
        if isinstance(node.func.value, ast.Name):
            base = node.func.value.id
        elif isinstance(node.func.value, ast.Attribute):
            parts = []
            cur = node.func.value
            while isinstance(cur, ast.Attribute):
                parts.append(cur.attr)
                cur = cur.value
            if isinstance(cur, ast.Name):
                parts.append(cur.id)
            base = ".".join(reversed(parts))
        if base:
            return f"{base}.{node.func.attr}"
        return node.func.attr
    return ""


def _infer_output_type(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    return_types: set[str] = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Return):
            value = node.value
            if value is None:
                continue
            if isinstance(value, ast.Dict):
                return_types.add("json")
            elif isinstance(value, (ast.List, ast.Tuple, ast.Set)):
                return_types.add("json")
            elif isinstance(value, ast.Constant):
                if isinstance(value.value, bool):
                    return_types.add("metric")
                elif isinstance(value.value, (int, float)):
                    return_types.add("metric")
                elif isinstance(value.value, str):
                    return_types.add("text")
                elif value.value is None:
                    continue
                else:
                    return_types.add("any")
            else:
                return_types.add("any")

    if not return_types:
        for node in ast.walk(fn):
            if isinstance(node, ast.Call) and _call_name(node).endswith("print"):
                return "side_effect"
        return "side_effect"

    if "json" in return_types:
        return "json"
    if "text" in return_types:
        return "text"
    if "metric" in return_types:
        return "metric"
    return "any"


def _expected_effects(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[dict]:
    effects: dict[str, str] = {}
    sql_literals: list[str] = []

    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node).lower()

        if name.endswith("read_text") or name.endswith("read_bytes"):
            effects["fs.read"] = "Read local files"
        if name == "open":
            mode = "r"
            if (
                len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
            ):
                mode = node.args[1].value
            if any(flag in mode for flag in ("w", "a", "x", "+")):
                effects["fs.write"] = "Write local files"
            else:
                effects["fs.read"] = "Read local files"
        if name.endswith("write_text") or name.endswith("write_bytes"):
            effects["fs.write"] = "Write local files"
        if name in {"subprocess.run", "subprocess.popen", "os.system"}:
            effects["proc.exec"] = "Execute subprocess commands"
        if name.endswith("execute"):
            effects["db.read"] = "Execute database queries"
            if (
                node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                sql_literals.append(node.args[0].value.lower())

    if any(
        token in " ".join(sql_literals)
        for token in ("insert ", "update ", "delete ", "store_", "create ", "drop ")
    ):
        effects["db.write"] = "Mutate database state"

    effects["returns"] = "Return function result or completion status"

    result = [
        {"type": k, "description": v}
        for k, v in effects.items()
        if k in {"returns", "fs.read", "fs.write", "proc.exec", "db.read", "db.write"}
    ]

    if not result:
        return [
            {
                "type": "returns",
                "description": "Return function result or completion status",
            }
        ]
    return result


def _build_inputs(
    fn: ast.FunctionDef | ast.AsyncFunctionDef, is_method: bool
) -> list[dict]:
    items = []
    args = fn.args
    positional = list(args.posonlyargs) + list(args.args)

    default_offset = len(positional) - len(args.defaults)
    defaults: dict[int, ast.AST] = {
        idx + default_offset: val for idx, val in enumerate(args.defaults)
    }

    for idx, arg in enumerate(positional):
        if is_method and idx == 0 and arg.arg in {"self", "cls"}:
            continue
        inferred_type = _annotation_to_type(arg.annotation, arg.arg)
        if inferred_type not in ALLOWED_INPUT_TYPES:
            inferred_type = "any"
        default_node = defaults.get(idx)
        default = _literal_default(default_node)
        item = {
            "name": arg.arg,
            "type": inferred_type,
            "required": default_node is None,
            "description": f"Input parameter `{arg.arg}`",
        }
        if default_node is not None:
            item["default"] = default
        items.append(item)

    for idx, arg in enumerate(args.kwonlyargs):
        inferred_type = _annotation_to_type(arg.annotation, arg.arg)
        if inferred_type not in ALLOWED_INPUT_TYPES:
            inferred_type = "any"
        kw_default = args.kw_defaults[idx]
        item = {
            "name": arg.arg,
            "type": inferred_type,
            "required": kw_default is None,
            "description": f"Keyword parameter `{arg.arg}`",
        }
        if kw_default is not None:
            item["default"] = _literal_default(kw_default)
        items.append(item)

    if args.vararg:
        items.append(
            {
                "name": args.vararg.arg,
                "type": "list",
                "required": False,
                "description": f"Variadic arguments `{args.vararg.arg}`",
                "default": [],
            }
        )

    if args.kwarg:
        items.append(
            {
                "name": args.kwarg.arg,
                "type": "map",
                "required": False,
                "description": f"Variadic keyword arguments `{args.kwarg.arg}`",
                "default": {},
            }
        )

    return items


def _collect_python_function_behaviors(tree: ast.Module) -> list[dict]:
    behaviors = []

    def append_behavior(
        fn: ast.FunctionDef | ast.AsyncFunctionDef,
        prefix: str = "",
        is_method: bool = False,
    ):
        fn_name = f"{prefix}.{fn.name}" if prefix else fn.name
        doc = ast.get_docstring(fn) or ""
        intent = _first_sentence(doc) or f"Run `{fn_name}` logic"
        inputs = _build_inputs(fn, is_method=is_method)
        out_type = _infer_output_type(fn)
        if out_type not in ALLOWED_OUTPUT_TYPES:
            out_type = "any"
        constraints = []
        if any(isinstance(node, (ast.Assert, ast.Raise)) for node in ast.walk(fn)):
            constraints.append(
                "Must enforce preconditions and fail fast for invalid states"
            )
        else:
            constraints.append(
                "Must remain deterministic for equivalent inputs and environment"
            )

        behaviors.append(
            {
                "name": _kebab_from_snake(fn_name),
                "intent": intent,
                "inputs": inputs,
                "outputs": [
                    {
                        "name": "result",
                        "type": out_type,
                        "description": f"Primary output from `{fn_name}`",
                    }
                ],
                "constraints": constraints,
                "expected_effects": _expected_effects(fn),
            }
        )

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            append_behavior(node)
        elif isinstance(node, ast.ClassDef):
            for member in node.body:
                if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    append_behavior(member, prefix=node.name, is_method=True)

    return behaviors


def _collect_import_nodes(tree: ast.Module) -> list[dict]:
    imports: dict[str, dict] = {}

    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                module = alias.name
                node_id = f"module-{_slug(module)}"
                imports[node_id] = {
                    "id": node_id,
                    "type": "runtime",
                    "path": module,
                    "description": f"Imported module `{module}`",
                }
        elif isinstance(node, ast.ImportFrom):
            module = node.module or "relative"
            display = "." * node.level + module if node.level else module
            node_id = f"module-{_slug(display)}"
            imports[node_id] = {
                "id": node_id,
                "type": "runtime",
                "path": display,
                "description": f"Imported module `{display}`",
            }

    return [imports[key] for key in sorted(imports.keys())]


def _build_python_spec(
    source_path: Path, output_path: Path | None = None, spec_id: str | None = None
) -> dict:
    content = source_path.read_text(encoding="utf-8")
    tree = ast.parse(content)
    module_doc = ast.get_docstring(tree) or ""

    script_node_id = f"script-{_slug(source_path.stem)}"
    script_behaviors = _collect_python_function_behaviors(tree)

    function_nodes = []
    for behavior in script_behaviors:
        function_nodes.append(
            {
                "id": f"function-{behavior['name']}",
                "type": "runtime",
                "path": source_path.as_posix(),
                "description": behavior["intent"],
                "behaviors": [behavior],
            }
        )

    script_node = {
        "id": script_node_id,
        "type": "script",
        "path": source_path.as_posix(),
        "description": _first_sentence(module_doc)
        or f"Behavior model for `{source_path.name}`",
    }

    import_nodes = _collect_import_nodes(tree)
    nodes = [script_node] + function_nodes + import_nodes

    relationships = [
        {
            "from": script_node_id,
            "to": node["id"],
            "type": "owns",
            "description": f"`{source_path.name}` defines `{node['behaviors'][0]['name']}`",
        }
        for node in function_nodes
    ] + [
        {
            "from": script_node_id,
            "to": node["id"],
            "type": "depends_on",
            "description": f"`{source_path.name}` imports `{node['path']}`",
        }
        for node in import_nodes
    ]

    behavioral_tests = []
    for idx, behavior in enumerate(script_behaviors[:3], start=1):
        behavioral_tests.append(
            {
                "id": f"generated-behavior-{idx}",
                "given": f"Valid inputs for `{behavior['name']}`",
                "when": f"`{behavior['name']}` is executed",
                "then": "Output matches declared shape and expected effects stay within constraints",
            }
        )

    result = {
        "schema_version": 2,
        "spec_id": spec_id or _slug(source_path.with_suffix("").as_posix()),
        "spec_kind": "component",
        "description": f"Auto-generated behavior specification for `{source_path.as_posix()}`.",
        "nodes": nodes,
        "relationships": relationships,
        "acceptance_tests": {
            "structural": [
                {
                    "id": "nodes-present",
                    "assert": "At least one behavior-bearing function/runtime node is present",
                },
                {
                    "id": "relationship-endpoints-valid",
                    "assert": "Relationship endpoints are valid node ids or accepted external references",
                },
            ],
            "behavioral": behavioral_tests,
        },
    }

    if output_path is None:
        output_path = source_path.with_suffix(".behavior.yaml")
    output_path.write_text(_dump_yaml(result), encoding="utf-8")
    return {
        "status": "OK",
        "path": str(output_path),
        "spec_id": result["spec_id"],
        "node_count": len(nodes),
        "behavior_count": len(script_behaviors),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate behavior spec (v2) from source"
    )
    parser.add_argument("source", help="Path to source file")
    parser.add_argument(
        "--output", help="Output YAML path (default: <source>.behavior.yaml)"
    )
    parser.add_argument("--spec-id", help="Override generated spec_id")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    source_path = Path(args.source)
    output_path = Path(args.output) if args.output else None

    if not source_path.exists():
        result = {"status": "FAIL", "error": f"Source file not found: {source_path}"}
        print(result["error"], file=sys.stderr)
        print(json.dumps(result, ensure_ascii=False))
        return 1

    if source_path.suffix != ".py":
        result = {
            "status": "FAIL",
            "error": f"Unsupported source type: {source_path.suffix}. Python (.py) is currently supported.",
        }
        print(result["error"], file=sys.stderr)
        print(json.dumps(result, ensure_ascii=False))
        return 1

    try:
        result = _build_python_spec(
            source_path=source_path,
            output_path=output_path,
            spec_id=args.spec_id,
        )
    except SyntaxError as exc:
        result = {
            "status": "FAIL",
            "error": f"Python parse failed at line {exc.lineno}: {exc.msg}",
        }
        print(result["error"], file=sys.stderr)
        print(json.dumps(result, ensure_ascii=False))
        return 1
    except Exception as exc:
        result = {"status": "FAIL", "error": str(exc)}
        print(result["error"], file=sys.stderr)
        print(json.dumps(result, ensure_ascii=False))
        return 1

    print(f"Generated v2 behavior spec: {result['path']}")
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
