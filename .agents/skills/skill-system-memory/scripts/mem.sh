#!/usr/bin/env bash
# mem.sh — shell wrapper for skill-system-memory
# Fix A1: search uses $() shell escape instead of :'q' psql variable
# Fix A3: psql failure prints friendly message
# New:   status, tags, categories subcommands (B3, C2)
set -euo pipefail

cmd="${1:-help}"

pg_host="${PGHOST:-localhost}"
pg_port="${PGPORT:-5432}"
resolve_memory_db_target() {
  local explicit="${SKILL_PGDATABASE:-}"
  local ambient="${PGDATABASE:-}"

  if [ -n "$explicit" ]; then
    pg_db="$explicit"
    if [ -n "$ambient" ] && [ "$explicit" != "$ambient" ]; then
      pg_db_source="SKILL_PGDATABASE(overrides:${ambient})"
    else
      pg_db_source="SKILL_PGDATABASE"
    fi
    return
  fi

  if [ -n "$ambient" ]; then
    echo "[mem.sh 錯誤] 偵測到 ambient PGDATABASE=$ambient。" >&2
    echo "  記憶系統不再默默沿用 ambient PGDATABASE；請明確設定 SKILL_PGDATABASE。" >&2
    exit 1
  fi

  pg_db="agent_memory"
  pg_db_source="default:agent_memory"
}

resolve_memory_db_target
# Fix A2: fallback chain PGUSER → whoami → postgres
pg_user="${PGUSER:-}"
if [ -z "$pg_user" ]; then
  pg_user="$(whoami 2>/dev/null || echo postgres)"
fi

# Build psql command array
psql_cmd=(psql -w -h "$pg_host" -p "$pg_port" -U "$pg_user" -d "$pg_db" \
               -v ON_ERROR_STOP=1 -t -A)

# Fix A3: wrapper around psql that prints friendly error on failure
_psql() {
  if ! "${psql_cmd[@]}" "$@"; then
    echo "[mem.sh 錯誤] psql 執行失敗。" >&2
    echo "  請確認 PGUSER (現在: $pg_user) 與 ~/.pgpass 設定是否正確。" >&2
    echo "  連線目標: ${pg_host}:${pg_port}/${pg_db}" >&2
    exit 1
  fi
}

usage() {
  cat <<'EOF'
Usage:
  scripts/mem.sh types
  scripts/mem.sh health
  scripts/mem.sh status
  scripts/mem.sh search <query> [limit]
  scripts/mem.sh store <memory_type> <category> <title> [tags_csv] [importance]
  scripts/mem.sh tags
  scripts/mem.sh categories

Notes:
  - Prefer python3 mem.py for complex queries to avoid shell quoting issues.
  - For store: content is read from STDIN.
    Example: echo "content" | scripts/mem.sh store semantic project "Title" "tag1,tag2" 8
EOF
}

case "$cmd" in
  types)
    _psql -c "SELECT unnest(enum_range(NULL::memory_type))" ;;

  health)
    _psql -c "SELECT * FROM memory_health_check();" ;;

  # B3: New status subcommand
  status)
    total=$(_psql -c "SELECT COUNT(*) FROM agent_memories WHERE deleted_at IS NULL")
    last=$(_psql -c "SELECT COALESCE(MAX(updated_at)::text, '(尚無記憶)') FROM agent_memories WHERE deleted_at IS NULL")
    echo "DB 連線：OK  (${pg_user}@${pg_host}:${pg_port}/${pg_db})"
    echo "記憶資料庫來源：${pg_db_source}"
    echo "記憶總數：${total}"
    echo "最後更新：${last}" ;;

  search)
    q="${2:-}"
    limit="${3:-10}"
    [ -n "$q" ] || { usage; exit 2; }
    # Fix A1: escape single quotes by doubling them, then embed safely
    escaped_q="${q//\'/\'\'}"
    _psql -c "SELECT id, memory_type, category, title, relevance_score, match_type
              FROM search_memories('${escaped_q}', NULL, NULL, NULL, NULL, 0.0, ${limit})
              ORDER BY relevance_score DESC;" ;;

  store)
    mtype="${2:-}"
    category="${3:-}"
    title="${4:-}"
    tags_csv="${5:-}"
    importance="${6:-5}"
    [ -n "$mtype" ] && [ -n "$category" ] && [ -n "$title" ] || { usage; exit 2; }
    content=""
    if [ ! -t 0 ]; then
      content=$(cat)
    fi
    if [ -z "$content" ]; then
      echo "[mem.sh 錯誤] stdin 無內容，已取消。請用 echo '...' | mem.sh store ..." >&2
      exit 2
    fi
    # Escape single quotes in all string fields
    escaped_mtype="${mtype//\'/\'\'}"
    escaped_cat="${category//\'/\'\'}"
    escaped_title="${title//\'/\'\'}"
    escaped_tags="${tags_csv//\'/\'\'}"
    escaped_content="${content//\'/\'\'}"
    _psql -c "SELECT store_memory(
                '${escaped_mtype}'::memory_type,
                '${escaped_cat}',
                CASE WHEN length('${escaped_tags}')=0
                     THEN ARRAY[]::text[]
                     ELSE string_to_array('${escaped_tags}', ',')
                END,
                '${escaped_title}',
                '${escaped_content}',
                '{}'::jsonb,
                'user',
                NULL,
                ${importance}::numeric
              );" ;;

  # C2: New tags subcommand
  tags)
    _psql -c "SELECT unnest(tags) AS tag, COUNT(*) AS count
              FROM agent_memories WHERE deleted_at IS NULL
              GROUP BY tag ORDER BY count DESC, tag;" ;;

  # C2: New categories subcommand
  categories)
    _psql -c "SELECT category, COUNT(*) AS count
              FROM agent_memories WHERE deleted_at IS NULL
              GROUP BY category ORDER BY count DESC, category;" ;;

  help|--help|-h)
    usage ;;

  *)
    usage
    exit 2
    ;;
esac
