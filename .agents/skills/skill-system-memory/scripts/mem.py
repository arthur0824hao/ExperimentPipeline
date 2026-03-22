#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional


ROOT_DIR = Path(__file__).resolve().parents[3]
DEFAULT_DB_TARGET = "agent_memory"
DB_CONNECT_TIMEOUT = 10
VALID_SCOPES = {"global", "project", "session"}


def resolve_db_target() -> tuple[str, str]:
    explicit = os.environ.get("SKILL_PGDATABASE", "").strip()
    ambient = os.environ.get("PGDATABASE", "").strip()

    if explicit:
        if ambient and ambient != explicit:
            return explicit, f"SKILL_PGDATABASE(overrides:{ambient})"
        return explicit, "SKILL_PGDATABASE"

    if ambient:
        _die(
            "SK-MEM-002",
            f"偵測到 ambient PGDATABASE={ambient}。記憶系統不再默默沿用 ambient PGDATABASE；請明確設定 SKILL_PGDATABASE。",
        )

    return DEFAULT_DB_TARGET, f"default:{DEFAULT_DB_TARGET}"


def _load_psycopg2() -> Any:
    try:
        return importlib.import_module("psycopg2")
    except ImportError:
        _die("SK-SYS-002", "psycopg2 未安裝。請執行：pip install psycopg2-binary")


def _connect_kwargs(db_target: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": db_target,
        "connect_timeout": DB_CONNECT_TIMEOUT,
    }
    user = os.environ.get("PGUSER", "").strip()
    if user:
        kwargs["user"] = user
    return kwargs


def _connect_failure_message(kwargs: dict[str, Any], exc: Exception) -> str:
    return (
        "DB 連線失敗，請確認 PGUSER / .pgpass 設定。\n"
        f"  PGHOST={kwargs.get('host')}  PGPORT={kwargs.get('port')}  PGDATABASE={kwargs.get('dbname')}\n"
        f"  libpq 錯誤：{exc}"
    )


def _get_conn():
    psycopg2 = _load_psycopg2()
    db_target, _source = resolve_db_target()
    kwargs = _connect_kwargs(db_target)
    conn: Any = None
    try:
        conn = psycopg2.connect(**kwargs)
    except Exception as exc:
        _die("SK-MEM-001", _connect_failure_message(kwargs, exc))
    if conn is None:
        _die("SK-MEM-001", "DB 連線失敗。")
    conn.autocommit = True
    return conn


def _safe_connect() -> tuple[Any | None, str, str]:
    psycopg2 = _load_psycopg2()
    db_target, source = resolve_db_target()
    kwargs = _connect_kwargs(db_target)
    try:
        conn: Any = psycopg2.connect(**kwargs)
    except Exception as exc:
        return None, source, _connect_failure_message(kwargs, exc)
    conn.autocommit = True
    return conn, source, ""


def _pending_dir() -> Path:
    path = ROOT_DIR / ".memory" / "pending"
    path.mkdir(parents=True, exist_ok=True)
    return path


def current_project_id() -> str:
    return ROOT_DIR.name


def current_session_id() -> str:
    return (
        os.environ.get("OPENCODE_SESSION_ID", "").strip()
        or os.environ.get("SESSION_ID", "").strip()
        or "session-local"
    )


def normalize_scope(scope: str | None) -> str | None:
    if scope is None:
        return None
    value = scope.strip().lower()
    if value not in VALID_SCOPES:
        _die(
            "SK-MEM-010",
            f"Invalid scope: {scope}. Expected one of global|project|session",
        )
    return value


def scope_metadata(scope: str | None) -> dict[str, Any]:
    resolved = normalize_scope(scope) or "session"
    metadata: dict[str, Any] = {"scope": resolved}
    if resolved == "project":
        metadata["project_id"] = current_project_id()
    elif resolved == "session":
        metadata["session_id"] = current_session_id()
    return metadata


def pending_scope(item: dict[str, Any]) -> str:
    metadata = item.get("metadata") or {}
    if isinstance(metadata, dict) and metadata.get("scope"):
        return str(metadata["scope"])
    return "session"


def pending_matches_scope(item: dict[str, Any], scope: str | None) -> bool:
    resolved = normalize_scope(scope)
    if resolved is None:
        return True
    return pending_scope(item) == resolved


def arg_scope(args: argparse.Namespace) -> str | None:
    value = getattr(args, "scope", None)
    return value if isinstance(value, str) else None


def _warn(message: str, error_code: str = "SK-MEM-008") -> None:
    print(f"[mem.py {error_code}] {message}", file=sys.stderr)


def _die(error_code_or_msg: str, msg: str | None = None, code: int = 1) -> None:
    if msg is None:
        display_msg = error_code_or_msg
        error_code = "SK-MEM-002"
    else:
        display_msg = msg
        error_code = error_code_or_msg
    print(f"[mem.py {error_code}] {display_msg}", file=sys.stderr)
    sys.exit(code)


def _print_rows(cur, rows):
    if not rows:
        print("(no results)")
        return
    cols = [d[0] for d in cur.description]
    col_widths = [
        max(len(str(c)), max((len(str(r[i])) for r in rows), default=0))
        for i, c in enumerate(cols)
    ]
    header = "  ".join(str(c).ljust(w) for c, w in zip(cols, col_widths))
    sep = "  ".join("-" * w for w in col_widths)
    print(header)
    print(sep)
    for row in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(row, col_widths)))


def _memory_schema_ready(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              to_regclass('public.agent_memories') IS NOT NULL,
              EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'store_memory'),
              EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'search_memories')
            """
        )
        row = cur.fetchone()
    return bool(row and all(row))


def _read_content_from_stdin() -> str:
    if sys.stdin.isatty():
        _die(
            '請提供 --content "..." 或透過 stdin 傳入內容。\n'
            "例：echo '...' | python3 mem.py store semantic cat 'Title'"
        )
    content = sys.stdin.read().strip()
    if not content:
        _die("內容為空，已取消。")
    return content


def _store_to_db(
    conn,
    memory_type: str,
    category: str,
    title: str,
    tags_csv: str = "",
    importance: float = 5,
    content: Optional[str] = None,
    metadata: dict[str, Any] | None = None,
    session_id: str | None = None,
):
    if content is None:
        content = _read_content_from_stdin()
    tags = [t.strip() for t in tags_csv.split(",") if t.strip()] if tags_csv else []
    payload = metadata or {}
    sql = """
        SELECT store_memory(
            %s::memory_type,
            %s,
            %s::text[],
            %s,
            %s,
            %s::jsonb,
            'user',
            %s,
            %s::numeric
        )
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                memory_type,
                category,
                tags,
                title,
                content,
                json.dumps(payload),
                session_id,
                importance,
            ),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _scope_column_exists(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'agent_memories' AND column_name = 'scope'
            )
            """
        )
        row = cur.fetchone()
    return bool(row and row[0])


def _scope_sql(alias: str, has_scope_column: bool) -> str:
    if has_scope_column:
        return (
            f"COALESCE({alias}.scope::text, {alias}.metadata->>'scope', "
            f"CASE WHEN {alias}.session_id IS NULL THEN 'global' ELSE 'session' END)"
        )
    return (
        f"COALESCE({alias}.metadata->>'scope', "
        f"CASE WHEN {alias}.session_id IS NULL THEN 'global' ELSE 'session' END)"
    )


def _write_pending_memory(
    memory_type: str,
    category: str,
    title: str,
    tags_csv: str,
    importance: float,
    content: str,
    metadata: dict[str, Any] | None = None,
) -> Path:
    pending_path = (
        _pending_dir() / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    )
    pending_path.write_text(
        json.dumps(
            {
                "memory_type": memory_type,
                "category": category,
                "title": title,
                "tags_csv": tags_csv,
                "importance": importance,
                "content": content,
                "metadata": metadata or {},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return pending_path


def _pending_records() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(_pending_dir().glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        payload["_path"] = str(path)
        records.append(payload)
    return records


def _search_pending(query: str, limit: int = 10) -> list[dict[str, Any]]:
    needle = query.casefold()
    results: list[dict[str, Any]] = []
    for item in _pending_records():
        haystack = " ".join(
            [
                str(item.get("title", "")),
                str(item.get("content", "")),
                str(item.get("category", "")),
                str(item.get("tags_csv", "")),
            ]
        ).casefold()
        if needle in haystack:
            results.append(item)
        if len(results) >= limit:
            break
    return results


def _list_pending(
    scope: str | None = None, category: str | None = None, limit: int = 10
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in _pending_records():
        if not pending_matches_scope(item, scope):
            continue
        if category and str(item.get("category", "")) != category:
            continue
        items.append(item)
        if len(items) >= limit:
            break
    return items


def _print_pending_matches(matches: list[dict[str, Any]]) -> None:
    if not matches:
        print("(no results)")
        return
    print("title  category  source")
    print("-----  --------  ------")
    for item in matches:
        print(
            f"{item.get('title', '')}  {item.get('category', '')}  {Path(item.get('_path', '')).name}"
        )


def cmd_status(
    conn, db_target: str = DEFAULT_DB_TARGET, target_source: str = "unknown"
):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM agent_memories WHERE deleted_at IS NULL")
        (total,) = cur.fetchone()
        cur.execute(
            "SELECT MAX(updated_at) FROM agent_memories WHERE deleted_at IS NULL"
        )
        (last_update,) = cur.fetchone()

    print("DB 連線：OK")
    print(f"記憶資料庫：{db_target}")
    print(f"記憶資料庫來源：{target_source}")
    print(f"記憶總數：{total}")
    print(f"最後更新：{last_update or '（尚無記憶）'}")


def cmd_search(conn, query: str, limit: int = 10):
    cmd_search_with_scope(conn, query, None, limit)


def cmd_search_with_scope(conn, query: str, scope: str | None = None, limit: int = 10):
    has_scope = _scope_column_exists(conn) if normalize_scope(scope) else False
    scope_expr = _scope_sql("m", has_scope)
    sql = f"""
        SELECT s.id, s.memory_type, s.category, s.title, {scope_expr} AS scope, s.relevance_score, s.match_type
        FROM search_memories(%s, NULL, NULL, NULL, NULL, 0.0, %s) AS s
        JOIN agent_memories m ON m.id = s.id
        WHERE m.deleted_at IS NULL
    """
    params: list[Any] = [query, limit]
    resolved_scope = normalize_scope(scope)
    if resolved_scope:
        sql += f" AND {scope_expr} = %s"
        params.append(resolved_scope)
    with conn.cursor() as cur:
        cur.execute(sql + " ORDER BY s.relevance_score DESC", tuple(params))
        rows = cur.fetchall()
        _print_rows(cur, rows)


def cmd_hybrid_search(conn, query: str, limit: int = 10, half_life: float = 30.0):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_proc WHERE proname = 'search_memories_hybrid' LIMIT 1"
        )
        has_hybrid = cur.fetchone() is not None

    if has_hybrid:
        sql = """
            SELECT id, memory_type, category, title, relevance_score, match_type
            FROM search_memories_hybrid(%s, NULL, %s, 60, NULL, NULL, NULL, NULL, 0.0, %s)
            ORDER BY relevance_score DESC
        """
        with conn.cursor() as cur:
            cur.execute(sql, (query, half_life, limit))
            rows = cur.fetchall()
            _print_rows(cur, rows)
        return

    sql = """
        SELECT id, memory_type, category, title, relevance_score, match_type
        FROM search_memories(%s, NULL, NULL, NULL, NULL, 0.0, %s, %s)
        ORDER BY relevance_score DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (query, limit, half_life))
        rows = cur.fetchall()
        _print_rows(cur, rows)


def cmd_store(
    conn,
    memory_type: str,
    category: str,
    title: str,
    tags_csv: str = "",
    importance: float = 5,
    content: Optional[str] = None,
    scope: str | None = None,
):
    metadata = scope_metadata(scope)
    session_id = current_session_id() if metadata["scope"] == "session" else None
    mem_id = _store_to_db(
        conn,
        memory_type,
        category,
        title,
        tags_csv,
        importance,
        content,
        metadata=metadata,
        session_id=session_id,
    )
    print(f"✓ 已儲存記憶 id={mem_id} scope={metadata['scope']}")


def cmd_list(
    conn, scope: str | None = None, category: str | None = None, limit: int = 20
):
    has_scope = _scope_column_exists(conn)
    scope_expr = _scope_sql("m", has_scope)
    sql = f"""
        SELECT m.id, m.memory_type, m.category, m.title, {scope_expr} AS scope, m.importance_score, m.updated_at
        FROM agent_memories m
        WHERE m.deleted_at IS NULL
    """
    params: list[Any] = []
    resolved_scope = normalize_scope(scope)
    if resolved_scope:
        sql += f" AND {scope_expr} = %s"
        params.append(resolved_scope)
    if category:
        sql += " AND m.category = %s"
        params.append(category)
    sql += " ORDER BY m.updated_at DESC LIMIT %s"
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        _print_rows(cur, rows)


def cmd_export(conn, fmt: str = "json", scope: str | None = None, limit: int = 1000):
    has_scope = _scope_column_exists(conn)
    scope_expr = _scope_sql("m", has_scope)
    sql = f"""
        SELECT m.id, m.memory_type, m.category, m.title, m.content, {scope_expr} AS scope, m.importance_score, m.updated_at
        FROM agent_memories m
        WHERE m.deleted_at IS NULL
    """
    params: list[Any] = []
    resolved_scope = normalize_scope(scope)
    if resolved_scope:
        sql += f" AND {scope_expr} = %s"
        params.append(resolved_scope)
    sql += " ORDER BY m.updated_at DESC LIMIT %s"
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    records = [dict(zip(cols, row)) for row in rows]
    if fmt == "csv":
        if not records:
            print("")
            return
        print(",".join(cols))
        for record in records:
            print(",".join(str(record[col]).replace(",", " ") for col in cols))
        return
    print(json.dumps(records, ensure_ascii=False, default=str))


def cmd_compact(conn, scope: str | None = None) -> None:
    has_scope = _scope_column_exists(conn)
    scope_expr = _scope_sql("agent_memories", has_scope)
    sql = f"""
        WITH ranked AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY content_hash, {scope_expr}
                       ORDER BY importance_score DESC, updated_at DESC, id DESC
                   ) AS rn
            FROM agent_memories
            WHERE deleted_at IS NULL AND content_hash IS NOT NULL
        )
        UPDATE agent_memories
        SET deleted_at = NOW()
        FROM ranked
        WHERE agent_memories.id = ranked.id AND ranked.rn > 1
    """
    params: list[Any] = []
    resolved_scope = normalize_scope(scope)
    if resolved_scope:
        sql += f" AND {scope_expr} = %s"
        params.append(resolved_scope)
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        affected = cur.rowcount if cur.rowcount is not None else 0
    print(f"✓ compacted {affected} duplicate memories")


def cmd_tags(conn):
    sql = """
        SELECT unnest(tags) AS tag, COUNT(*) AS count
        FROM agent_memories
        WHERE deleted_at IS NULL
        GROUP BY tag
        ORDER BY count DESC, tag
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        _print_rows(cur, rows)


def cmd_categories(conn):
    sql = """
        SELECT category, COUNT(*) AS count
        FROM agent_memories
        WHERE deleted_at IS NULL
        GROUP BY category
        ORDER BY count DESC, category
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        _print_rows(cur, rows)


def cmd_context(conn, keywords: str, limit: int = 5):
    sql = """
        SELECT id, memory_type, category, title, content,
               importance_score, relevance_score, match_type
        FROM search_memories(%s, NULL, NULL, NULL, NULL, 0.0, %s)
        ORDER BY relevance_score DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (keywords, limit))
        rows = cur.fetchall()

    if not rows:
        print("(此關鍵字找不到相關記憶)")
        return

    print(f"── context: top-{len(rows)} 相關記憶 for 「{keywords}」 ──")
    for row in rows:
        mem_id, mtype, cat, title, content, imp, rel, _match_type = row
        snippet = (content or "")[:120].replace("\n", " ")
        print(f"[#{mem_id}] ({mtype}/{cat}) {title}  [imp={imp} rel={rel:.2f}]")
        print(f"  {snippet}{'…' if len(content or '') > 120 else ''}")


def cmd_flush(conn) -> None:
    pending = _pending_records()
    flushed = 0
    for item in pending:
        mem_id = _store_to_db(
            conn,
            memory_type=item.get("memory_type", "semantic"),
            category=item.get("category", "pending"),
            title=item.get("title", "pending memory"),
            tags_csv=item.get("tags_csv", ""),
            importance=float(item.get("importance", 5)),
            content=item.get("content", ""),
            metadata=item.get("metadata") or {},
        )
        Path(item["_path"]).unlink(missing_ok=True)
        flushed += 1
        print(f"✓ flushed id={mem_id} from {Path(item['_path']).name}")
    if flushed == 0:
        print("(no pending memories)")


def _handle_missing_schema(args: argparse.Namespace) -> int:
    _warn("memory schema 不存在。請先執行 init.sql；目前以降級模式處理。", "SK-MEM-008")
    if args.cmd == "store":
        if args.content is None:
            if sys.stdin.isatty():
                _die(
                    '請提供 --content "..." 或透過 stdin 傳入內容。\n'
                    "例：echo '...' | python3 mem.py store semantic cat 'Title'"
                )
            args.content = sys.stdin.read().strip()
        print("⚠ schema 缺失：store 已略過（no-op）")
        return 0
    if args.cmd in {"search", "hybrid-search", "tags", "categories"}:
        print("(no results)")
        return 0
    if args.cmd == "context":
        print("(此關鍵字找不到相關記憶)")
        return 0
    if args.cmd == "flush":
        print("⚠ schema 缺失：flush 已略過（no-op）")
        return 0
    if args.cmd in {"list", "export", "compact"}:
        print("(no results)")
        return 0
    return 0


def _handle_no_db(args: argparse.Namespace, db_error: str) -> int:
    _warn(db_error, "SK-MEM-009")
    if args.cmd == "store":
        if args.content is None:
            args.content = _read_content_from_stdin()
        metadata = scope_metadata(arg_scope(args))
        pending = _write_pending_memory(
            args.memory_type,
            args.category,
            args.title,
            args.tags_csv,
            args.importance,
            args.content,
            metadata,
        )
        print(f"⚠ DB 不可用：已寫入 fallback 檔案 {pending.relative_to(ROOT_DIR)}")
        return 0
    if args.cmd in {"search", "hybrid-search"}:
        matches = [
            item
            for item in _search_pending(args.query, args.limit)
            if pending_matches_scope(item, arg_scope(args))
        ]
        _print_pending_matches(matches)
        return 0
    if args.cmd == "context":
        matches = _search_pending(args.keywords, args.limit)
        if not matches:
            print("(此關鍵字找不到相關記憶)")
            return 0
        print(
            f"── context: top-{len(matches)} 本地 fallback 記憶 for 「{args.keywords}」 ──"
        )
        for item in matches:
            snippet = str(item.get("content", ""))[:120].replace("\n", " ")
            print(f"({item.get('category', '')}) {item.get('title', '')}")
            print(
                f"  {snippet}{'…' if len(str(item.get('content', ''))) > 120 else ''}"
            )
        return 0
    if args.cmd == "tags":
        tags = sorted(
            {
                tag.strip()
                for item in _pending_records()
                for tag in str(item.get("tags_csv", "")).split(",")
                if tag.strip()
            }
        )
        if not tags:
            print("(no results)")
            return 0
        print("tag")
        print("---")
        for tag in tags:
            print(tag)
        return 0
    if args.cmd == "categories":
        categories = sorted(
            {
                str(item.get("category", "")).strip()
                for item in _pending_records()
                if str(item.get("category", "")).strip()
            }
        )
        if not categories:
            print("(no results)")
            return 0
        print("category")
        print("--------")
        for category in categories:
            print(category)
        return 0
    if args.cmd == "list":
        items = _list_pending(
            arg_scope(args), getattr(args, "category", None), args.limit
        )
        if not items:
            print("(no results)")
            return 0
        print("title  category  scope")
        print("-----  --------  -----")
        for item in items:
            print(
                f"{item.get('title', '')}  {item.get('category', '')}  {pending_scope(item)}"
            )
        return 0
    if args.cmd == "export":
        items = _list_pending(arg_scope(args), None, args.limit)
        if args.format == "csv":
            print("title,category,scope")
            for item in items:
                print(
                    f"{item.get('title', '')},{item.get('category', '')},{pending_scope(item)}"
                )
            return 0
        print(json.dumps(items, ensure_ascii=False, default=str))
        return 0
    if args.cmd == "flush":
        print("⚠ DB 不可用：flush 已略過")
        return 0
    if args.cmd == "compact":
        print("⚠ DB 不可用：compact 已略過")
        return 0
    return 0


def main_status_fail():
    conn = _get_conn()
    db_target, target_source = resolve_db_target()
    cmd_status(conn, db_target, target_source)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="mem.py",
        description="Persistent memory wrapper (psycopg2, parameterized queries).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="DB 連線狀態 + 記憶數量 + 最後更新時間")

    p_search = sub.add_parser("search", help="搜尋記憶")
    p_search.add_argument("query", help="自然語言查詢")
    p_search.add_argument("legacy_limit", nargs="?", type=int)
    p_search.add_argument(
        "--limit", type=int, default=10, help="結果數量上限 (預設 10)"
    )
    p_search.add_argument("--scope", choices=sorted(VALID_SCOPES))

    p_store = sub.add_parser("store", help="儲存記憶")
    p_store.add_argument("memory_type", nargs="?")
    p_store.add_argument("category", nargs="?")
    p_store.add_argument("title", nargs="?")
    p_store.add_argument("tags_csv", nargs="?", default="")
    p_store.add_argument("importance", nargs="?", type=float, default=5)
    p_store.add_argument("--type", "memory_type_opt")
    p_store.add_argument("--category", dest="category_opt")
    p_store.add_argument("--title", dest="title_opt")
    p_store.add_argument("--tags", dest="tags_opt", default="")
    p_store.add_argument("--importance", dest="importance_opt", type=float)
    p_store.add_argument("--scope", choices=sorted(VALID_SCOPES), default="session")
    p_store.add_argument(
        "--content", "-c", default=None, help="記憶內容 (亦可從 stdin 傳入)"
    )

    p_list = sub.add_parser("list", help="列出記憶")
    p_list.add_argument("--scope", choices=sorted(VALID_SCOPES))
    p_list.add_argument("--category")
    p_list.add_argument("--limit", type=int, default=20)

    p_compact = sub.add_parser("compact", help="壓縮重複記憶")
    p_compact.add_argument("--scope", choices=sorted(VALID_SCOPES))

    p_export = sub.add_parser("export", help="匯出記憶")
    p_export.add_argument("--format", choices=["json", "csv"], default="json")
    p_export.add_argument("--scope", choices=sorted(VALID_SCOPES))
    p_export.add_argument("--limit", type=int, default=1000)

    sub.add_parser("tags", help="列出所有已使用的 tags")
    sub.add_parser("categories", help="列出所有已使用的 categories")

    p_hybrid = sub.add_parser(
        "hybrid-search", help="Hybrid search (text + vector RRF) with temporal decay"
    )
    p_hybrid.add_argument("query", help="Natural language query")
    p_hybrid.add_argument(
        "--limit", type=int, default=10, help="Max results (default 10)"
    )
    p_hybrid.add_argument(
        "--half-life",
        type=float,
        default=30.0,
        help="Temporal decay half-life in days (default 30)",
    )

    p_ctx = sub.add_parser("context", help="session 開頭自動撈出相關記憶摘要")
    p_ctx.add_argument("keywords", help="關鍵字 (空白分隔亦可)")
    p_ctx.add_argument("--limit", type=int, default=5, help="回傳筆數 (預設 5)")

    sub.add_parser("flush", help="將 .memory/pending/ 內容寫回 DB")

    args = parser.parse_args()

    if args.cmd == "status":
        conn = _get_conn()
        db_target, target_source = resolve_db_target()
        try:
            cmd_status(conn, db_target, target_source)
        finally:
            conn.close()
        return 0

    conn, _source, db_error = _safe_connect()
    if conn is None:
        return _handle_no_db(args, db_error)

    try:
        if not _memory_schema_ready(conn):
            return _handle_missing_schema(args)

        if args.cmd == "search":
            limit = args.legacy_limit if args.legacy_limit is not None else args.limit
            cmd_search_with_scope(conn, args.query, args.scope, limit)
        elif args.cmd == "store":
            memory_type = args.memory_type_opt or args.memory_type
            category = args.category_opt or args.category
            title = args.title_opt or args.title
            tags_csv = args.tags_opt or args.tags_csv
            importance = (
                args.importance_opt
                if args.importance_opt is not None
                else args.importance
            )
            if not memory_type or not category or not title:
                _die("SK-MEM-010", "store requires memory_type, category, and title")
            cmd_store(
                conn,
                memory_type=memory_type,
                category=category,
                title=title,
                tags_csv=tags_csv,
                importance=importance,
                content=args.content,
                scope=args.scope,
            )
        elif args.cmd == "list":
            cmd_list(conn, args.scope, args.category, args.limit)
        elif args.cmd == "compact":
            cmd_compact(conn, args.scope)
        elif args.cmd == "export":
            cmd_export(conn, args.format, args.scope, args.limit)
        elif args.cmd == "tags":
            cmd_tags(conn)
        elif args.cmd == "categories":
            cmd_categories(conn)
        elif args.cmd == "hybrid-search":
            cmd_hybrid_search(conn, args.query, args.limit, args.half_life)
        elif args.cmd == "context":
            cmd_context(conn, args.keywords, args.limit)
        elif args.cmd == "flush":
            cmd_flush(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
