import json
from pathlib import Path
from typing import Any, Dict, List, Set

try:
    from artifact import _artifact_timestamp, _load_json_dict
except ModuleNotFoundError:
    from .artifact import _artifact_timestamp, _load_json_dict

try:
    from db_registry import derive_progression_status
except ModuleNotFoundError:
    from .db_registry import derive_progression_status

try:
    from runtime_config import cfg_bool, get_runtime_section
except ModuleNotFoundError:
    from .runtime_config import cfg_bool, get_runtime_section

try:
    from formatting import normalize_status
except ModuleNotFoundError:
    from .formatting import normalize_status

BASE_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = BASE_DIR.parent
RESULTS_DB_DIR = BASE_DIR / "results_db"

_CONDITION_NODES_CFG = get_runtime_section("condition_nodes")
_STAGED_MATRIX_CFG = get_runtime_section("staged_matrix")

STATUS_NEEDS_RERUN = "NEEDS_RERUN"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"


def _normalize_name_list(raw: Any) -> List[str]:
    if isinstance(raw, str):
        raw_items = [raw]
    elif isinstance(raw, list):
        raw_items = raw
    else:
        raw_items = []
    names: List[str] = []
    seen: Set[str] = set()
    for item in raw_items:
        value = str(item).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        names.append(value)
    return names


def _load_condition_nodes_from_runtime() -> List[Dict[str, Any]]:
    nodes_raw = _CONDITION_NODES_CFG.get("nodes", [])
    if not isinstance(nodes_raw, list):
        return []
    parsed: List[Dict[str, Any]] = []
    seen_names: Set[str] = set()
    for item in nodes_raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name or name in seen_names:
            continue
        seen_names.add(name)
        condition_parent = str(item.get("condition_parent") or "").strip()
        depends_on = _normalize_name_list(item.get("depends_on"))
        if condition_parent and condition_parent not in depends_on:
            depends_on = [condition_parent] + depends_on
        parsed.append(
            {
                "name": name,
                "description": str(item.get("description") or "").strip(),
                "role": "condition_node",
                "condition_parent": condition_parent,
                "depends_on": depends_on,
                "gate_type": str(item.get("gate_type") or "").strip(),
                "gate_evidence_ref": str(item.get("gate_evidence_ref") or "").strip(),
            }
        )
    return parsed


RUNTIME_CONDITION_NODES = _load_condition_nodes_from_runtime()


def _load_staged_matrix_entries() -> List[Dict[str, Any]]:
    if not cfg_bool(_STAGED_MATRIX_CFG, "enabled", False):
        return []
    manifest_rel = str(_STAGED_MATRIX_CFG.get("manifest_path") or "").strip()
    if not manifest_rel:
        return []
    manifest_path = PROJECT_ROOT / manifest_rel
    if not manifest_path.exists():
        return []
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []

    condition_parent = str(_STAGED_MATRIX_CFG.get("condition_parent") or "").strip()
    role = str(_STAGED_MATRIX_CFG.get("role") or "staged_matrix_leaf").strip()
    rows: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        rows.append(
            {
                "name": name,
                "description": str(item.get("description") or "").strip(),
                "script": str(item.get("script") or "").strip(),
                "features": list(item.get("features") or []),
                "condition_parent": condition_parent,
                "role": role,
                "gate_type": "staged_after_root_cause",
            }
        )
    return rows


RUNTIME_STAGED_MATRIX = _load_staged_matrix_entries()


def _resolve_gate_evidence_status(
    gate_evidence_ref: str,
    status_lookup_by_name: Dict[str, str],
    fallback_name: str = "",
) -> str:
    evidence_name = str(gate_evidence_ref or "").strip()
    if not evidence_name:
        evidence_name = str(fallback_name or "").strip()
    if evidence_name:
        in_memory = normalize_status(status_lookup_by_name.get(evidence_name, ""))
        if in_memory in {STATUS_RUNNING, STATUS_COMPLETED}:
            return in_memory
        result_file = RESULTS_DB_DIR / f"{evidence_name}.json"
        if result_file.exists():
            return STATUS_COMPLETED
        return in_memory
    return ""


def _build_condition_node_rows(
    status_lookup_by_name: Dict[str, str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    local_status_lookup = dict(status_lookup_by_name)
    for node in RUNTIME_CONDITION_NODES:
        name = str(node.get("name") or "").strip()
        if not name:
            continue
        condition_parent = str(node.get("condition_parent") or "").strip()
        depends_on = _normalize_name_list(node.get("depends_on"))
        gate_evidence_ref = str(node.get("gate_evidence_ref") or "").strip()
        gate_type = str(node.get("gate_type") or "").strip()
        evidence_status = _resolve_gate_evidence_status(
            gate_evidence_ref, local_status_lookup, fallback_name=name
        )
        if evidence_status in {STATUS_RUNNING, STATUS_COMPLETED}:
            status = evidence_status
        else:
            status = STATUS_NEEDS_RERUN

        unmet_dependencies: List[str] = []
        for dependency in depends_on:
            dep_status = normalize_status(local_status_lookup.get(dependency, ""))
            if dep_status != STATUS_COMPLETED:
                unmet_dependencies.append(dependency)

        progression_status = ""
        block_reason = ""
        if unmet_dependencies:
            progression_status = "BLOCKED_CONDITION"
            block_reason = "condition_dependencies_unmet:" + ",".join(
                unmet_dependencies
            )
        elif status == STATUS_RUNNING:
            progression_status = "RUNNING"
        elif status == STATUS_COMPLETED:
            progression_status = "COMPLETED"
        else:
            progression_status = "READY"

        row = {
            "name": name,
            "description": str(node.get("description") or "").strip(),
            "batch_id": "-",
            "status": status,
            "role": "condition_node",
            "condition_parent": condition_parent,
            "depends_on": depends_on,
            "gate_type": gate_type,
            "gate_evidence_ref": gate_evidence_ref,
            "progression_status": progression_status,
            "block_reason": block_reason,
            "_synthetic_reason": "condition_node",
            "_non_actionable": True,
        }
        rows.append(row)
        local_status_lookup[name] = status
    return rows


def _build_staged_matrix_rows(
    status_lookup_by_name: Dict[str, str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for leaf in RUNTIME_STAGED_MATRIX:
        name = str(leaf.get("name") or "").strip()
        if not name:
            continue
        condition_parent = str(leaf.get("condition_parent") or "").strip()
        parent_status = normalize_status(
            status_lookup_by_name.get(condition_parent, "")
        )
        progression_status, block_reason = derive_progression_status(
            STATUS_NEEDS_RERUN,
            condition_parent=condition_parent,
            condition_parent_status=parent_status,
        )
        rows.append(
            {
                "name": name,
                "description": str(leaf.get("description") or "").strip(),
                "script": str(leaf.get("script") or "").strip(),
                "features": list(leaf.get("features") or []),
                "batch_id": "matrix-staged",
                "status": STATUS_NEEDS_RERUN,
                "role": str(leaf.get("role") or "staged_matrix_leaf"),
                "condition_parent": condition_parent,
                "gate_type": str(leaf.get("gate_type") or "staged_after_root_cause"),
                "progression_status": progression_status,
                "block_reason": block_reason,
                "parent_experiment": condition_parent,
                "_synthetic_reason": "staged_matrix_leaf",
                "_non_actionable": True,
            }
        )
    return rows
