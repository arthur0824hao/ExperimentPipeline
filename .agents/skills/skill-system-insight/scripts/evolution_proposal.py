#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT_DIR = Path(__file__).resolve().parents[3]
MEMORY_SCRIPTS_DIR = ROOT_DIR / "skills" / "skill-system-memory" / "scripts"


def load_mem_module():
    module_path = MEMORY_SCRIPTS_DIR / "mem.py"
    spec = importlib.util.spec_from_file_location(
        "skill_system_memory_mem", module_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MEM_MODULE = load_mem_module()
_die = MEM_MODULE._die


CANONICAL_WORKFLOW_TARGET = "agent_memory"
ALLOWED_KINDS = {"friction", "decision", "next_step", "experiment_result", "unknown"}
ALLOWED_STATUS = {"PROPOSED", "ACCEPTED", "REJECTED"}
ACCEPTED_CATEGORY = "evolution-node"
REJECTED_CATEGORY = "evolution-rejected"
AGENT_MEMORY_ROLE = (
    "dual-write summary/backref surface with explicit legacy compatibility reads"
)
TASK_AUTHORITY_MODEL = "agent_tasks_is_lifecycle_authority"
EVOLUTION_TASKS_ROLE = "mapping_only"
FAIL_AFTER_STEP_ENV = "EVOLUTION_PROPOSAL_FAIL_AFTER_STEP"
DECISION_ACTION_TO_STATUS = {
    "approve": "ACCEPTED",
    "reject": "REJECTED",
    "dismiss": "REJECTED",
}
REQUIRED_PROPOSAL_FIELDS = {
    "proposal_id",
    "kind",
    "summary",
    "rationale",
    "suggested_change",
    "evidence_refs",
    "status",
    "created_at",
}


class DecisionReplayError(RuntimeError):
    def __init__(
        self, code: str, message: str, *, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def resolve_workflow_db_target() -> tuple[str, str]:
    explicit = os.environ.get("SKILL_PGDATABASE", "").strip()
    ambient = os.environ.get("PGDATABASE", "").strip()

    if explicit:
        if explicit != CANONICAL_WORKFLOW_TARGET:
            _die(
                "proposal/evolution workflow currently requires "
                f"SKILL_PGDATABASE={CANONICAL_WORKFLOW_TARGET}; got {explicit}."
            )
        if ambient and ambient != explicit:
            return explicit, f"SKILL_PGDATABASE(overrides:{ambient})"
        return explicit, "SKILL_PGDATABASE"

    if ambient:
        return (
            CANONICAL_WORKFLOW_TARGET,
            f"canonical:{CANONICAL_WORKFLOW_TARGET}(ambient_ignored:{ambient})",
        )

    return CANONICAL_WORKFLOW_TARGET, f"canonical:{CANONICAL_WORKFLOW_TARGET}"


def connect_workflow_db(db_target: str):
    import psycopg2  # type: ignore

    kwargs: dict[str, Any] = {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": db_target,
    }
    user = os.environ.get("PGUSER", "").strip()
    if user:
        kwargs["user"] = user
    return psycopg2.connect(**kwargs)


def normalize_kind(kind: str) -> str:
    normalized = kind.strip().lower()
    if normalized not in ALLOWED_KINDS:
        _die(f"Unsupported proposal kind: {kind}. Allowed: {sorted(ALLOWED_KINDS)}")
    return normalized


def make_proposal(
    *,
    kind: str,
    summary: str,
    rationale: str,
    suggested_change: str,
    evidence_refs: list[str],
    proposal_id: str | None = None,
    created_at: str | None = None,
    status: str = "PROPOSED",
) -> dict[str, Any]:
    if status not in ALLOWED_STATUS:
        _die(f"Unsupported proposal status: {status}")

    return {
        "proposal_id": proposal_id or f"evop-{uuid.uuid4().hex[:12]}",
        "kind": normalize_kind(kind),
        "summary": summary.strip(),
        "rationale": rationale.strip(),
        "suggested_change": suggested_change.strip(),
        "evidence_refs": [item.strip() for item in evidence_refs if item.strip()],
        "status": status,
        "created_at": created_at or now_utc(),
    }


def validate_proposal_payload(proposal: dict[str, Any]) -> dict[str, Any]:
    missing = sorted(REQUIRED_PROPOSAL_FIELDS - set(proposal.keys()))
    if missing:
        _die(f"Proposal payload missing required fields: {missing}")
    proposal = dict(proposal)
    proposal["kind"] = normalize_kind(str(proposal["kind"]))
    status = str(proposal["status"])
    if status not in ALLOWED_STATUS:
        _die(f"Unsupported proposal status: {status}")
    if not isinstance(proposal["evidence_refs"], list):
        _die("Proposal field evidence_refs must be a list")
    proposal["summary"] = str(proposal["summary"]).strip()
    proposal["rationale"] = str(proposal["rationale"]).strip()
    proposal["suggested_change"] = str(proposal["suggested_change"]).strip()
    proposal["evidence_refs"] = normalize_evidence_refs(proposal["evidence_refs"])
    return proposal


def normalize_evidence_refs(values: list[Any]) -> list[str]:
    refs = sorted({str(value).strip() for value in values if str(value).strip()})
    return refs


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def build_semantic_identity(
    proposal: dict[str, Any],
    *,
    action: str,
    requested_parent_node_id: str | None = None,
) -> dict[str, Any]:
    if action not in DECISION_ACTION_TO_STATUS:
        raise DecisionReplayError(
            "UNSUPPORTED_ACTION", f"Unsupported decision action: {action}"
        )
    return {
        "action": action,
        "kind": proposal["kind"],
        "summary": proposal["summary"],
        "rationale": proposal["rationale"],
        "suggested_change": proposal["suggested_change"],
        "evidence_refs": normalize_evidence_refs(proposal["evidence_refs"]),
        "requested_parent_node_id": requested_parent_node_id,
    }


def fingerprint_semantic_identity(identity: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()


def proposal_lock_keys(proposal_id: str) -> tuple[int, int]:
    digest = hashlib.sha256(proposal_id.encode("utf-8")).digest()
    return (
        int.from_bytes(digest[:4], "big", signed=True),
        int.from_bytes(digest[4:8], "big", signed=True),
    )


def lock_proposal_decision(conn: Any, proposal_id: str) -> None:
    key_one, key_two = proposal_lock_keys(proposal_id)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (key_one, key_two))


def maybe_inject_failure(step: str) -> None:
    configured = os.environ.get(FAIL_AFTER_STEP_ENV, "").strip()
    if configured == step:
        raise RuntimeError(f"Injected failure after step: {step}")


def reject_proposal(proposal: dict[str, Any], *, mode: str) -> dict[str, Any]:
    rejected = dict(proposal)
    rejected["status"] = "REJECTED"
    rejected["rejected_via"] = mode
    rejected["rejected_at"] = now_utc()
    return rejected


def parse_json_object(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def reconstruct_existing_semantic_identity(
    record: dict[str, Any], *, fallback_action: str
) -> dict[str, Any] | None:
    semantic_identity = record.get("semantic_identity")
    if isinstance(semantic_identity, dict) and semantic_identity:
        return semantic_identity

    payload = parse_json_object(record.get("content"))
    action = str(payload.get("rejected_via") or fallback_action)
    if action not in DECISION_ACTION_TO_STATUS:
        action = fallback_action

    return {
        "action": action,
        "kind": str(payload.get("kind") or record.get("kind") or "").strip(),
        "summary": str(payload.get("summary") or record.get("summary") or "").strip(),
        "rationale": str(payload.get("rationale") or "").strip(),
        "suggested_change": str(payload.get("suggested_change") or "").strip(),
        "evidence_refs": normalize_evidence_refs(payload.get("evidence_refs") or []),
        "requested_parent_node_id": payload.get("requested_parent_node_id"),
    }


def replay_matches(
    record: dict[str, Any],
    *,
    expected_identity: dict[str, Any],
    fallback_action: str,
) -> bool:
    existing_fingerprint = str(record.get("semantic_fingerprint") or "").strip()
    expected_fingerprint = fingerprint_semantic_identity(expected_identity)
    if existing_fingerprint:
        return existing_fingerprint == expected_fingerprint
    existing_identity = reconstruct_existing_semantic_identity(
        record, fallback_action=fallback_action
    )
    if existing_identity is None:
        return False
    return canonical_json(existing_identity) == canonical_json(expected_identity)


def build_existing_accepted_result(record: dict[str, Any]) -> dict[str, Any]:
    payload = parse_json_object(record.get("content"))
    payload.setdefault("proposal_id", record["proposal_id"])
    payload.setdefault("node_id", record["node_id"])
    payload.setdefault("kind", record["kind"])
    payload.setdefault("status", record["status"])
    payload.setdefault("parent_node_id", record["parent_node_id"])
    payload["task_id"] = record.get("task_id")
    if record.get("semantic_identity") is not None:
        payload["semantic_identity"] = record["semantic_identity"]
    if record.get("semantic_fingerprint"):
        payload["semantic_fingerprint"] = record["semantic_fingerprint"]
    return payload


def build_existing_rejected_result(record: dict[str, Any]) -> dict[str, Any]:
    payload = parse_json_object(record.get("content"))
    payload.setdefault("proposal_id", record["proposal_id"])
    payload.setdefault("kind", record["kind"])
    payload.setdefault("status", record["status"])
    payload.setdefault("rejected_via", record["rejected_via"])
    if record.get("semantic_identity") is not None:
        payload["semantic_identity"] = record["semantic_identity"]
    if record.get("semantic_fingerprint"):
        payload["semantic_fingerprint"] = record["semantic_fingerprint"]
    return payload


def build_payload_mismatch_error(
    *, action: str, proposal_id: str, terminal_state: str
) -> DecisionReplayError:
    return DecisionReplayError(
        "PAYLOAD_MISMATCH",
        f"Replay payload mismatch for proposal {proposal_id} on {action}.",
        details={
            "proposal_id": proposal_id,
            "action": action,
            "terminal_state": terminal_state,
        },
    )


def build_terminal_conflict_error(
    *,
    proposal_id: str,
    action: str,
    existing_terminal_state: str,
    existing_action: str,
) -> DecisionReplayError:
    return DecisionReplayError(
        "TERMINAL_CONFLICT",
        f"Proposal {proposal_id} is already terminal via {existing_action}; cannot apply {action}.",
        details={
            "proposal_id": proposal_id,
            "action": action,
            "existing_terminal_state": existing_terminal_state,
            "existing_action": existing_action,
        },
    )


def get_latest_accepted_node(conn: Any) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT node_id, proposal_id, parent_node_id, accepted_at
            FROM evolution_nodes
            WHERE status = 'ACCEPTED'
            ORDER BY accepted_at DESC, created_at DESC
            LIMIT 1
            """,
        )
        row = cur.fetchone()

    if row is None:
        return None

    return {
        "node_id": row[0],
        "proposal_id": row[1],
        "parent_node_id": row[2],
        "accepted_at": row[3],
    }


def create_materialized_task(
    conn: Any, *, node_id: str, summary: str, memory_id: int | None = None
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_tasks (task_key, title, description, status, created_by, metadata)
            VALUES (%s, %s, %s, %s::task_status, %s, %s::jsonb)
            RETURNING id
            """,
            (
                f"evolution:{node_id}",
                summary,
                f"Materialized from evolution node {node_id}",
                "open",
                "evolution-proposal",
                json.dumps(
                    {"source_node_id": node_id, "workflow": "durable-evolution-model"}
                ),
            ),
        )
        row = cur.fetchone()
        task_id = int(row[0]) if row else 0
        maybe_inject_failure("approve_after_agent_task")

        cur.execute(
            """
            INSERT INTO evolution_tasks (task_id, source_node_id, summary, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source_node_id) DO UPDATE SET
                task_id = EXCLUDED.task_id,
                summary = EXCLUDED.summary,
                created_at = evolution_tasks.created_at
            """,
            (task_id, node_id, summary, "open"),
        )
        maybe_inject_failure("approve_after_evolution_task")

        if memory_id is not None:
            cur.execute(
                """
                INSERT INTO task_memory_links (task_id, memory_id, link_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (task_id, memory_id, link_type) DO NOTHING
                """,
                (task_id, memory_id, "source-summary"),
            )
            maybe_inject_failure("approve_after_task_memory_link")

    return task_id


def persist_accepted_proposal(
    conn: Any,
    proposal: dict[str, Any],
    *,
    target_source: str,
    parent_node_id: str | None = None,
    requested_parent_node_id: str | None = None,
    semantic_identity: dict[str, Any],
    semantic_fingerprint: str,
) -> tuple[int, dict[str, Any]]:
    accepted = validate_proposal_payload(proposal)
    accepted["status"] = "ACCEPTED"
    accepted["accepted_at"] = now_utc()
    accepted["persistence_target"] = CANONICAL_WORKFLOW_TARGET
    accepted["target_source"] = target_source
    accepted["node_id"] = f"evo-node-{uuid.uuid4().hex[:12]}"
    accepted["requested_parent_node_id"] = requested_parent_node_id
    accepted["semantic_identity"] = semantic_identity
    accepted["semantic_fingerprint"] = semantic_fingerprint

    if parent_node_id is None:
        latest = get_latest_accepted_node(conn)
        parent_node_id = str(latest["node_id"]) if latest is not None else None
    accepted["parent_node_id"] = parent_node_id

    title = f"Evolution Node Accepted: {accepted['summary']}"
    content = json.dumps(accepted, ensure_ascii=True, indent=2)
    metadata = {
        "node_id": accepted["node_id"],
        "proposal_id": accepted["proposal_id"],
        "kind": accepted["kind"],
        "status": accepted["status"],
        "parent_node_id": accepted["parent_node_id"],
        "target_source": target_source,
        "workflow": "dual-lane-evolution-ledger",
        "semantic_identity": semantic_identity,
        "semantic_fingerprint": semantic_fingerprint,
    }
    tags = [
        "evolution",
        "accepted",
        f"kind:{accepted['kind']}",
        f"proposal_id:{accepted['proposal_id']}",
        f"node_id:{accepted['node_id']}",
        "status:ACCEPTED",
    ]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT store_memory(
                %s::memory_type,
                %s,
                %s::text[],
                %s,
                %s,
                %s::jsonb,
                %s,
                NULL,
                %s::numeric
            )
            """,
            (
                "episodic",
                ACCEPTED_CATEGORY,
                tags,
                title,
                content,
                json.dumps(metadata),
                "evolution-proposal",
                7,
            ),
        )
        row = cur.fetchone()
        memory_id = int(row[0]) if row else 0
        maybe_inject_failure("approve_after_summary_backref")

        cur.execute(
            """
            INSERT INTO evolution_nodes (
                node_id,
                proposal_id,
                kind,
                summary,
                status,
                created_at,
                accepted_at,
                parent_node_id,
                memory_id,
                semantic_identity,
                semantic_fingerprint
            )
            VALUES (%s, %s, %s, %s, %s, %s::timestamptz, %s::timestamptz, %s, %s, %s::jsonb, %s)
            ON CONFLICT (node_id) DO UPDATE SET
                proposal_id = EXCLUDED.proposal_id,
                kind = EXCLUDED.kind,
                summary = EXCLUDED.summary,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at,
                accepted_at = EXCLUDED.accepted_at,
                parent_node_id = EXCLUDED.parent_node_id,
                memory_id = EXCLUDED.memory_id,
                semantic_identity = EXCLUDED.semantic_identity,
                semantic_fingerprint = EXCLUDED.semantic_fingerprint
            """,
            (
                accepted["node_id"],
                accepted["proposal_id"],
                accepted["kind"],
                accepted["summary"],
                accepted["status"],
                accepted["created_at"],
                accepted["accepted_at"],
                accepted["parent_node_id"],
                memory_id,
                canonical_json(semantic_identity),
                semantic_fingerprint,
            ),
        )
        maybe_inject_failure("approve_after_canonical_node")

    accepted["task_id"] = create_materialized_task(
        conn,
        node_id=accepted["node_id"],
        summary=accepted["summary"],
        memory_id=memory_id,
    )
    return memory_id, accepted


def persist_rejected_proposal(
    conn: Any,
    proposal: dict[str, Any],
    *,
    target_source: str,
    mode: str,
    semantic_identity: dict[str, Any],
    semantic_fingerprint: str,
) -> tuple[int, dict[str, Any]]:
    rejected = reject_proposal(validate_proposal_payload(proposal), mode=mode)
    rejected["semantic_identity"] = semantic_identity
    rejected["semantic_fingerprint"] = semantic_fingerprint
    title = f"Evolution Proposal Rejected: {rejected['summary']}"
    content = json.dumps(rejected, ensure_ascii=True, indent=2)
    metadata = {
        "proposal_id": rejected["proposal_id"],
        "kind": rejected["kind"],
        "status": rejected["status"],
        "rejected_via": mode,
        "target_source": target_source,
        "workflow": "dual-lane-evolution-ledger",
        "semantic_identity": semantic_identity,
        "semantic_fingerprint": semantic_fingerprint,
    }
    tags = [
        "evolution",
        "rejected",
        f"kind:{rejected['kind']}",
        f"proposal_id:{rejected['proposal_id']}",
        f"mode:{mode}",
        "status:REJECTED",
    ]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT store_memory(
                %s::memory_type,
                %s,
                %s::text[],
                %s,
                %s,
                %s::jsonb,
                %s,
                NULL,
                %s::numeric
            )
            """,
            (
                "episodic",
                REJECTED_CATEGORY,
                tags,
                title,
                content,
                json.dumps(metadata),
                "evolution-proposal",
                5,
            ),
        )
        row = cur.fetchone()
        memory_id = int(row[0]) if row else 0
        maybe_inject_failure("reject_after_summary_backref")

        cur.execute(
            """
            INSERT INTO evolution_rejections (
                proposal_id,
                kind,
                summary,
                status,
                rejected_via,
                rejected_at,
                memory_id,
                semantic_identity,
                semantic_fingerprint
            )
            VALUES (%s, %s, %s, %s, %s, %s::timestamptz, %s, %s::jsonb, %s)
            ON CONFLICT (proposal_id) DO UPDATE SET
                kind = EXCLUDED.kind,
                summary = EXCLUDED.summary,
                status = EXCLUDED.status,
                rejected_via = EXCLUDED.rejected_via,
                rejected_at = EXCLUDED.rejected_at,
                memory_id = EXCLUDED.memory_id,
                semantic_identity = EXCLUDED.semantic_identity,
                semantic_fingerprint = EXCLUDED.semantic_fingerprint
            """,
            (
                rejected["proposal_id"],
                rejected["kind"],
                rejected["summary"],
                rejected["status"],
                rejected["rejected_via"],
                rejected["rejected_at"],
                memory_id,
                canonical_json(semantic_identity),
                semantic_fingerprint,
            ),
        )
        maybe_inject_failure("reject_after_canonical_rejection")

    return memory_id, rejected


def list_accepted_records(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                n.node_id,
                n.proposal_id,
                COALESCE(m.title, 'Evolution Node Accepted: ' || n.summary) AS title,
                n.kind,
                n.status,
                n.parent_node_id,
                t.id,
                n.semantic_identity,
                n.semantic_fingerprint,
                n.memory_id,
                m.content,
                n.summary
            FROM evolution_nodes n
            LEFT JOIN agent_memories m ON m.id = n.memory_id
            LEFT JOIN evolution_tasks et ON et.source_node_id = n.node_id
            LEFT JOIN agent_tasks t ON t.id = et.task_id AND t.deleted_at IS NULL
            ORDER BY n.accepted_at DESC, n.created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [
        {
            "node_id": row[0],
            "proposal_id": row[1],
            "title": row[2],
            "kind": row[3],
            "status": row[4],
            "parent_node_id": row[5],
            "task_id": row[6],
            "semantic_identity": row[7],
            "semantic_fingerprint": row[8],
            "memory_id": int(row[9]) if row[9] is not None else None,
            "content": row[10],
            "summary": row[11],
        }
        for row in rows
    ]


def read_accepted_record(conn: Any, proposal_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                n.node_id,
                n.proposal_id,
                COALESCE(m.title, 'Evolution Node Accepted: ' || n.summary) AS title,
                m.content,
                n.kind,
                n.status,
                n.parent_node_id,
                n.memory_id,
                t.id,
                n.semantic_identity,
                n.semantic_fingerprint,
                n.summary
            FROM evolution_nodes n
            LEFT JOIN agent_memories m ON m.id = n.memory_id
            LEFT JOIN evolution_tasks et ON et.source_node_id = n.node_id
            LEFT JOIN agent_tasks t ON t.id = et.task_id AND t.deleted_at IS NULL
            WHERE n.proposal_id = %s OR n.node_id = %s
            ORDER BY n.accepted_at DESC, n.created_at DESC
            LIMIT 1
            """,
            (proposal_id, proposal_id),
        )
        row = cur.fetchone()

    if row is None:
        return None

    return {
        "node_id": row[0],
        "proposal_id": row[1],
        "title": row[2],
        "content": row[3],
        "kind": row[4],
        "status": row[5],
        "parent_node_id": row[6],
        "memory_id": int(row[7]) if row[7] is not None else None,
        "task_id": row[8],
        "semantic_identity": row[9],
        "semantic_fingerprint": row[10],
        "summary": row[11],
    }


def read_rejected_record(conn: Any, proposal_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                r.proposal_id,
                COALESCE(m.title, 'Evolution Proposal Rejected: ' || r.summary) AS title,
                m.content,
                r.kind,
                r.status,
                r.rejected_via,
                r.memory_id,
                r.semantic_identity,
                r.semantic_fingerprint,
                r.summary
            FROM evolution_rejections r
            LEFT JOIN agent_memories m ON m.id = r.memory_id
            WHERE r.proposal_id = %s
            LIMIT 1
            """,
            (proposal_id,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    return {
        "proposal_id": row[0],
        "title": row[1],
        "content": row[2],
        "kind": row[3],
        "status": row[4],
        "rejected_via": row[5],
        "memory_id": int(row[6]) if row[6] is not None else None,
        "semantic_identity": row[7],
        "semantic_fingerprint": row[8],
        "summary": row[9],
    }


def list_rejected_records(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                r.proposal_id,
                COALESCE(m.title, 'Evolution Proposal Rejected: ' || r.summary) AS title,
                r.kind,
                r.status,
                r.rejected_via,
                r.semantic_identity,
                r.semantic_fingerprint,
                r.memory_id,
                m.content,
                r.summary
            FROM evolution_rejections r
            LEFT JOIN agent_memories m ON m.id = r.memory_id
            ORDER BY r.rejected_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [
        {
            "proposal_id": row[0],
            "title": row[1],
            "kind": row[2],
            "status": row[3],
            "rejected_via": row[4],
            "semantic_identity": row[5],
            "semantic_fingerprint": row[6],
            "memory_id": int(row[7]) if row[7] is not None else None,
            "content": row[8],
            "summary": row[9],
        }
        for row in rows
    ]


def list_materialized_tasks(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.id, et.source_node_id, t.title, t.status, t.created_at
            FROM evolution_tasks et
            JOIN agent_tasks t ON t.id = et.task_id
            WHERE t.deleted_at IS NULL
            ORDER BY t.created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [
        {
            "task_id": row[0],
            "source_node_id": row[1],
            "summary": row[2],
            "status": row[3],
            "created_at": row[4],
        }
        for row in rows
    ]


def decide_accept_proposal(
    conn: Any,
    proposal: dict[str, Any],
    *,
    target_source: str,
    requested_parent_node_id: str | None = None,
) -> tuple[int, dict[str, Any], str]:
    validated = validate_proposal_payload(proposal)
    semantic_identity = build_semantic_identity(
        validated,
        action="approve",
        requested_parent_node_id=requested_parent_node_id,
    )
    lock_proposal_decision(conn, validated["proposal_id"])

    existing_accepted = read_accepted_record(conn, validated["proposal_id"])
    if existing_accepted is not None:
        if not replay_matches(
            existing_accepted,
            expected_identity=semantic_identity,
            fallback_action="approve",
        ):
            raise build_payload_mismatch_error(
                action="approve",
                proposal_id=validated["proposal_id"],
                terminal_state="ACCEPTED",
            )
        return (
            existing_accepted["memory_id"] or 0,
            build_existing_accepted_result(existing_accepted),
            "REPLAYED_EXISTING",
        )

    existing_rejected = read_rejected_record(conn, validated["proposal_id"])
    if existing_rejected is not None:
        raise build_terminal_conflict_error(
            proposal_id=validated["proposal_id"],
            action="approve",
            existing_terminal_state="REJECTED",
            existing_action=str(existing_rejected["rejected_via"]),
        )

    memory_id, accepted = persist_accepted_proposal(
        conn,
        validated,
        target_source=target_source,
        parent_node_id=requested_parent_node_id,
        requested_parent_node_id=requested_parent_node_id,
        semantic_identity=semantic_identity,
        semantic_fingerprint=fingerprint_semantic_identity(semantic_identity),
    )
    return memory_id, accepted, "CREATED"


def decide_reject_proposal(
    conn: Any,
    proposal: dict[str, Any],
    *,
    target_source: str,
    mode: str,
) -> tuple[int, dict[str, Any], str]:
    validated = validate_proposal_payload(proposal)
    semantic_identity = build_semantic_identity(validated, action=mode)
    lock_proposal_decision(conn, validated["proposal_id"])

    existing_accepted = read_accepted_record(conn, validated["proposal_id"])
    if existing_accepted is not None:
        raise build_terminal_conflict_error(
            proposal_id=validated["proposal_id"],
            action=mode,
            existing_terminal_state="ACCEPTED",
            existing_action="approve",
        )

    existing_rejected = read_rejected_record(conn, validated["proposal_id"])
    if existing_rejected is not None:
        if not replay_matches(
            existing_rejected,
            expected_identity=semantic_identity,
            fallback_action=str(existing_rejected["rejected_via"]),
        ):
            raise build_payload_mismatch_error(
                action=mode,
                proposal_id=validated["proposal_id"],
                terminal_state="REJECTED",
            )
        return (
            existing_rejected["memory_id"] or 0,
            build_existing_rejected_result(existing_rejected),
            "REPLAYED_EXISTING",
        )

    memory_id, rejected = persist_rejected_proposal(
        conn,
        validated,
        target_source=target_source,
        mode=mode,
        semantic_identity=semantic_identity,
        semantic_fingerprint=fingerprint_semantic_identity(semantic_identity),
    )
    return memory_id, rejected, "CREATED"


def run_transactional(conn: Any, operation: Callable[[], Any]):
    conn.autocommit = False
    try:
        result = operation()
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise


def build_lineage(conn: Any, node_id: str) -> list[dict[str, Any]]:
    lineage: list[dict[str, Any]] = []
    current: str | None = node_id
    while current:
        record = read_accepted_record(conn, current)
        if record is None:
            break
        lineage.append(
            {
                "node_id": record["node_id"],
                "parent_node_id": record["parent_node_id"],
                "proposal_id": record["proposal_id"],
                "title": record["title"],
                "kind": record["kind"],
            }
        )
        current = record["parent_node_id"]
    return lineage


def render_feedback_surface(
    conn: Any,
    *,
    proposal: dict[str, Any] | None = None,
    accepted_limit: int = 3,
    rejected_limit: int = 3,
) -> str:
    accepted = list_accepted_records(conn, limit=accepted_limit)
    rejected = list_rejected_records(conn, limit=rejected_limit)
    lines = [
        "### Evolution Ledger",
    ]
    if proposal is not None:
        lines.append(
            f"- proposal: [{proposal['status']}] {proposal['kind']} :: {proposal['summary']} (id={proposal['proposal_id']})"
        )
    else:
        lines.append("- proposal: (none supplied)")

    if accepted:
        item = accepted[0]
        lines.append(
            f"- accepted: [{item['kind']}] {item['title']} (node={item['node_id']}, parent={item['parent_node_id'] or 'root'})"
        )
    else:
        lines.append("- accepted: (none)")

    if rejected:
        item = rejected[0]
        lines.append(
            f"- rejected: [{item['kind']}] {item['title']} (mode={item['rejected_via']})"
        )
    else:
        lines.append("- rejected: (none)")

    return "\n".join(lines)


def parse_proposal_payload(args: argparse.Namespace) -> dict[str, Any]:
    if args.proposal_json:
        return json.loads(args.proposal_json)
    if args.proposal_file:
        return json.loads(Path(args.proposal_file).read_text(encoding="utf-8"))
    raw = sys.stdin.read().strip()
    if raw:
        return json.loads(raw)
    _die("Proposal payload missing. Use --proposal-json, --proposal-file, or stdin.")
    raise AssertionError("unreachable")


def emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=True, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(prog="evolution_proposal.py")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_generate = sub.add_parser(
        "generate", help="Generate a proposal without persistence"
    )
    p_generate.add_argument("--kind", default="unknown")
    p_generate.add_argument("--summary", required=True)
    p_generate.add_argument("--rationale", required=True)
    p_generate.add_argument("--suggested-change", required=True)
    p_generate.add_argument("--evidence-ref", action="append", default=[])

    p_approve = sub.add_parser("approve", help="Persist an approved proposal")
    p_approve.add_argument("--proposal-json")
    p_approve.add_argument("--proposal-file")
    p_approve.add_argument("--parent-node-id")

    p_reject = sub.add_parser(
        "reject", help="Reject a proposal and retain it in the rejected log"
    )
    p_reject.add_argument("--proposal-json")
    p_reject.add_argument("--proposal-file")

    p_dismiss = sub.add_parser(
        "dismiss", help="Dismiss a proposal and retain it in the rejected log"
    )
    p_dismiss.add_argument("--proposal-json")
    p_dismiss.add_argument("--proposal-file")

    p_list = sub.add_parser("list", help="List accepted evolution proposals")
    p_list.add_argument("--limit", type=int, default=10)

    p_read = sub.add_parser("read", help="Read one accepted evolution proposal")
    p_read.add_argument("proposal_id")

    p_lineage = sub.add_parser(
        "lineage", help="Show lineage for an accepted evolution node"
    )
    p_lineage.add_argument("node_id")

    p_list_rejected = sub.add_parser(
        "list-rejected", help="List rejected evolution proposals"
    )
    p_list_rejected.add_argument("--limit", type=int, default=10)

    p_list_tasks = sub.add_parser(
        "list-tasks",
        help="List materialized tasks linked from accepted evolution nodes",
    )
    p_list_tasks.add_argument("--limit", type=int, default=10)

    p_render = sub.add_parser(
        "render-feedback", help="Render concise user-facing feedback surface"
    )
    p_render.add_argument("--proposal-json")
    p_render.add_argument("--proposal-file")

    args = parser.parse_args()
    db_target, target_source = resolve_workflow_db_target()

    if args.cmd == "generate":
        proposal = make_proposal(
            kind=args.kind,
            summary=args.summary,
            rationale=args.rationale,
            suggested_change=args.suggested_change,
            evidence_refs=args.evidence_ref,
        )
        emit(
            {
                "status": "ok",
                "mode": "dry-run",
                "db_target": db_target,
                "target_source": target_source,
                "proposal": proposal,
            }
        )
        return 0

    if args.cmd in {
        "approve",
        "list",
        "read",
        "lineage",
        "list-rejected",
        "list-tasks",
        "render-feedback",
    }:
        conn = connect_workflow_db(db_target)
        try:
            if args.cmd == "approve":
                proposal = parse_proposal_payload(args)
                try:
                    memory_id, accepted, replay_status = run_transactional(
                        conn,
                        lambda: decide_accept_proposal(
                            conn,
                            proposal,
                            target_source=target_source,
                            requested_parent_node_id=args.parent_node_id,
                        ),
                    )
                except DecisionReplayError as exc:
                    emit(
                        {
                            "status": "error",
                            "mode": "approve",
                            "db_target": db_target,
                            "target_source": target_source,
                            "error_code": exc.code,
                            "message": str(exc),
                            "details": exc.details,
                        }
                    )
                    return 1
                emit(
                    {
                        "status": "ok",
                        "db_target": db_target,
                        "target_source": target_source,
                        "memory_id": memory_id,
                        "replay_status": replay_status,
                        "proposal": accepted,
                    }
                )
            elif args.cmd == "list":
                conn.autocommit = True
                emit(
                    {
                        "status": "ok",
                        "db_target": db_target,
                        "target_source": target_source,
                        "records": list_accepted_records(conn, limit=args.limit),
                    }
                )
            elif args.cmd == "read":
                conn.autocommit = True
                emit(
                    {
                        "status": "ok",
                        "db_target": db_target,
                        "target_source": target_source,
                        "record": read_accepted_record(conn, args.proposal_id),
                    }
                )
            elif args.cmd == "lineage":
                conn.autocommit = True
                emit(
                    {
                        "status": "ok",
                        "db_target": db_target,
                        "target_source": target_source,
                        "lineage": build_lineage(conn, args.node_id),
                    }
                )
            elif args.cmd == "list-rejected":
                conn.autocommit = True
                emit(
                    {
                        "status": "ok",
                        "db_target": db_target,
                        "target_source": target_source,
                        "records": list_rejected_records(conn, limit=args.limit),
                    }
                )
            elif args.cmd == "list-tasks":
                conn.autocommit = True
                emit(
                    {
                        "status": "ok",
                        "db_target": db_target,
                        "target_source": target_source,
                        "records": list_materialized_tasks(conn, limit=args.limit),
                    }
                )
            else:
                conn.autocommit = True
                proposal = (
                    parse_proposal_payload(args)
                    if (
                        args.proposal_json
                        or args.proposal_file
                        or not sys.stdin.isatty()
                    )
                    else None
                )
                print(render_feedback_surface(conn, proposal=proposal))
        finally:
            conn.close()
        return 0

    proposal = parse_proposal_payload(args)
    mode = "dismiss" if args.cmd == "dismiss" else "reject"
    conn = connect_workflow_db(db_target)
    try:
        try:
            memory_id, rejected, replay_status = run_transactional(
                conn,
                lambda: decide_reject_proposal(
                    conn, proposal, target_source=target_source, mode=mode
                ),
            )
        except DecisionReplayError as exc:
            emit(
                {
                    "status": "error",
                    "mode": mode,
                    "db_target": db_target,
                    "target_source": target_source,
                    "error_code": exc.code,
                    "message": str(exc),
                    "details": exc.details,
                }
            )
            return 1
        emit(
            {
                "status": "ok",
                "mode": mode,
                "db_target": db_target,
                "target_source": target_source,
                "memory_id": memory_id,
                "replay_status": replay_status,
                "proposal": rejected,
            }
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
