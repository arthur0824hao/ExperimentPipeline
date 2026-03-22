#!/usr/bin/env bash
set -euo pipefail

cmd="${1:-types}"

pg_host="${PGHOST:-localhost}"
pg_port="${PGPORT:-5432}"
pg_user="${PGUSER:-}"
if [ -z "$pg_user" ]; then
  pg_user="$(whoami 2>/dev/null || echo postgres)"
fi

fail() {
  local msg="$1"
  printf '%s\n' "{\"status\":\"error\",\"summary\":\"$msg\",\"errors\":[{\"code\":\"MEM_ROUTER_ADAPTER\",\"message\":\"$msg\"}],\"artifacts\":[],\"metrics\":{}}"
  exit 1
}

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
    fail "ambient PGDATABASE=$ambient is not accepted; set SKILL_PGDATABASE explicitly for memory operations"
  fi

  pg_db="agent_memory"
  pg_db_source="default:agent_memory"
}

resolve_memory_db_target

psql_cmd=(psql -w -h "$pg_host" -p "$pg_port" -U "$pg_user" -d "$pg_db" -v ON_ERROR_STOP=1 -t -A)

sql_json() {
  local sql="$1"
  "${psql_cmd[@]}" -c "$sql" | tr -d '\r'
}

usage() {
  cat <<'EOF'
Usage:
  scripts/router_mem.sh types
  scripts/router_mem.sh health
  scripts/router_mem.sh search <query> [limit]
  scripts/router_mem.sh store <memory_type> <category> <title> [tags_csv] [importance]

Notes:
  - For store: content is read from STDIN.
  - Emits a single JSON object on stdout (for Router last-line JSON contract).
EOF
}

case "$cmd" in
  types)
    sql_json "SELECT jsonb_build_object('status','ok','summary','types','db_target','${pg_db}','target_source','${pg_db_source}','results',COALESCE((SELECT jsonb_agg(x) FROM (SELECT unnest(enum_range(NULL::memory_type)) AS x) t),'[]'::jsonb))::text;" || fail "types query failed"
    ;;

  health)
    sql_json "SELECT jsonb_build_object('status','ok','summary','health','db_target','${pg_db}','target_source','${pg_db_source}','results',COALESCE((SELECT jsonb_agg(to_jsonb(t)) FROM memory_health_check() t),'[]'::jsonb))::text;" || fail "health query failed"
    ;;

  search)
    q="${2:-}"
    limit="${3:-10}"
    [ -n "$q" ] || { usage; exit 2; }
    # Fix: embed query via shell interpolation instead of psql :'var' (unreliable with -c)
    escaped_q="${q//\'/\'\'}"
    sql_json "SELECT jsonb_build_object('status','ok','summary','search','db_target','${pg_db}','target_source','${pg_db_source}','results',COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT id, memory_type, category, title, relevance_score, match_type FROM search_memories('${escaped_q}', NULL, NULL, NULL, NULL, 0.0, ${limit}) ORDER BY relevance_score DESC) r),'[]'::jsonb))::text;" || fail "search failed"
    ;;

  store)
    mtype="${2:-}"
    category="${3:-}"
    title="${4:-}"
    tags_csv="${5:-}"
    importance="${6:-5}"
    [ -n "$mtype" ] && [ -n "$category" ] && [ -n "$title" ] || { usage; exit 2; }
    if [ -t 0 ]; then
      fail "No content on stdin"
    fi
    content=$(cat)
    if [ -z "$content" ]; then
      fail "Empty content on stdin"
    fi
    stored_id=$(printf '%s' "$content" | bash "$(dirname "$0")/mem.sh" store "$mtype" "$category" "$title" "$tags_csv" "$importance" 2>/dev/null | tr -d '\r' | tail -n 1 || true)
    [ -n "$stored_id" ] || fail "store failed"
    printf '%s\n' "{\"status\":\"ok\",\"summary\":\"store\",\"db_target\":\"${pg_db}\",\"target_source\":\"${pg_db_source}\",\"results\":[{\"stored_id\":$stored_id}],\"artifacts\":[],\"metrics\":{},\"errors\":[]}" 
    ;;

  *)
    usage
    exit 2
    ;;
esac
