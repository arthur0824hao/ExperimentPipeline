#!/usr/bin/env bash
set -euo pipefail

# TKT System — Ticket-based agent work management
# Usage: tkt.sh <command> [options]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILLS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SKILLS_DIR/.." && pwd)}"
CONFIG_DIR="${CONFIG_DIR:-$PROJECT_ROOT/config}"
TKT_ROOT="${TKT_ROOT:-.tkt}"

# --- Helpers ---

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# Unified error output (SK- error codes)
sk_error() {
  local code="$1" message="$2"
  echo "{\"status\":\"error\",\"error_code\":\"$code\",\"message\":\"$message\"}"
  exit 1
}

# Read lock timeout from config (H3 fix)
lock_timeout() {
  if [ -n "$CONFIG_DIR" ] && [ -f "$CONFIG_DIR/tkt.yaml" ]; then
    local val
    val="$(grep 'lock_timeout:' "$CONFIG_DIR/tkt.yaml" 2>/dev/null | head -1 | sed 's/.*lock_timeout: *//' | tr -d ' ')"
    [ -n "$val" ] && echo "$val" && return
  fi
  echo "30"
}

log_event() {
  local event="$1" detail="${2:-}"
  echo "[$(timestamp)] $event $detail" >> "$TKT_ROOT/history.log"
}

next_bundle_id() {
  local max=0
  if [ -d "$TKT_ROOT/bundles" ]; then
    for d in "$TKT_ROOT/bundles"/B-*/; do
      [ -d "$d" ] || continue
      local num="${d##*B-}"
      num="${num%/}"
      num=$((10#$num))
      [ "$num" -gt "$max" ] && max=$num
    done
  fi
  printf "B-%03d" $((max + 1))
}

next_ticket_num() {
  local bundle_dir="$1"
  local max=0
  for f in "$bundle_dir"/TKT-*.yaml; do
    [ -f "$f" ] || continue
    local base
    base="$(basename "$f" .yaml)"
    local num="${base#TKT-}"
    # skip integrate (000) and audit (A00)
    [[ "$num" == "000" || "$num" == A* ]] && continue
    num=$((10#$num))
    [ "$num" -gt "$max" ] && max=$num
  done
  printf "%03d" $((max + 1))
}

next_express_id() {
  local max=0
  if [ -d "$TKT_ROOT/express" ]; then
    for f in "$TKT_ROOT/express"/EXP-*.yaml; do
      [ -f "$f" ] || continue
      local base
      base="$(basename "$f" .yaml)"
      local num="${base#EXP-}"
      [[ "$num" =~ ^[0-9]+$ ]] || continue
      num=$((10#$num))
      [ "$num" -gt "$max" ] && max=$num
    done
  fi
  printf "EXP-%03d" $((max + 1))
}

yaml_field() {
  local file="$1" field="$2"
  grep "^${field}:" "$file" 2>/dev/null | sed "s/^${field}: *//" | sed 's/^"//' | sed 's/"$//' || true
}

yaml_list_from_csv() {
  local raw="$1"
  local yaml_list="[]"
  if [ -n "$raw" ]; then
    yaml_list=""
    IFS=',' read -ra items <<< "$raw"
    for item in "${items[@]}"; do
      item="$(echo "$item" | xargs)"
      [ -n "$item" ] || continue
      yaml_list+="
  - \"$item\""
    done
    [ -n "$yaml_list" ] || yaml_list="[]"
  fi
  printf '%s' "$yaml_list"
}

update_yaml_status() {
  local file="$1" status="$2" summary="${3:-}" evidence="${4:-}" notes="${5:-}"
  TKT_FILE="$file" TKT_STATUS="$status" TKT_SUMMARY="$summary" TKT_EVIDENCE="$evidence" TKT_NOTES="$notes" TKT_TIMESTAMP="$(timestamp)" python3 - <<'PY'
import os
from pathlib import Path

import yaml

path = Path(os.environ["TKT_FILE"])
status = os.environ["TKT_STATUS"]
summary = os.environ.get("TKT_SUMMARY", "")
evidence = os.environ.get("TKT_EVIDENCE", "")
notes = os.environ.get("TKT_NOTES", "")
timestamp = os.environ["TKT_TIMESTAMP"]

data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
data["status"] = status
if status == "in_progress" and not data.get("started_at"):
    data["started_at"] = timestamp
elif status in {"done", "failed"}:
    data["completed_at"] = timestamp

result = data.setdefault("result", {})
result.setdefault("summary", None)
result.setdefault("artifacts", [])
result.setdefault("notes", None)
result.setdefault("evidence", None)
if summary:
    result["summary"] = summary
if evidence:
    result["evidence"] = evidence
if notes:
    result["notes"] = notes

path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
PY
}

claim_yaml_ticket() {
  local file="$1" agent="$2"
  TKT_FILE="$file" TKT_AGENT="$agent" TKT_TIMESTAMP="$(timestamp)" python3 - <<'PY'
import os
from pathlib import Path

import yaml

path = Path(os.environ["TKT_FILE"])
agent = os.environ["TKT_AGENT"]
timestamp = os.environ["TKT_TIMESTAMP"]

data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
data["status"] = "claimed"
data["claimed_by"] = agent
data["claimed_at"] = timestamp

path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
PY
}

config_value() {
  local dotted_key="$1"
  TKT_CONFIG_DIR="$CONFIG_DIR" TKT_CONFIG_KEY="$dotted_key" python3 - <<'PY'
import os
from pathlib import Path

import yaml

config_dir = Path(os.environ["TKT_CONFIG_DIR"])
key = os.environ["TKT_CONFIG_KEY"].split(".")
path = config_dir / "tkt.yaml"
if not path.exists():
    raise SystemExit(0)
data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
value = data
for part in key:
    if not isinstance(value, dict) or part not in value:
        raise SystemExit(0)
    value = value[part]
if value is None:
    raise SystemExit(0)
print(value)
PY
}

resolve_evidence_value() {
  local evidence="${1:-}" evidence_file="${2:-}"
  if [ -n "$evidence" ] && [ -n "$evidence_file" ]; then
    sk_error "SK-TKT-014" "Use either --evidence or --evidence-file, not both"
  fi
  if [ -n "$evidence_file" ]; then
    [ -f "$evidence_file" ] || sk_error "SK-SYS-004" "Evidence file not found: $evidence_file"
    python3 - "$evidence_file" <<'PY'
from pathlib import Path
import sys

print(Path(sys.argv[1]).read_text(encoding="utf-8"))
PY
    return
  fi
  printf '%s' "$evidence"
}

run_command_acceptance_checks() {
  local bundle_dir="$1"
  TKT_BUNDLE_DIR="$bundle_dir" TKT_PROJECT_ROOT="$PROJECT_ROOT" python3 - <<'PY'
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

bundle_dir = Path(os.environ["TKT_BUNDLE_DIR"])
project_root = Path(os.environ["TKT_PROJECT_ROOT"])
failures = []
results = []
passed = 0
failed = 0
skipped = 0

for ticket_file in sorted(bundle_dir.glob("TKT-*.yaml")):
    data = yaml.safe_load(ticket_file.read_text(encoding="utf-8")) or {}
    ticket_id = data.get("id", ticket_file.stem)
    title = data.get("title", ticket_id)
    criteria = data.get("acceptance_criteria") or []
    if not isinstance(criteria, list):
        continue
    for index, criterion in enumerate(criteria, start=1):
        if not isinstance(criterion, dict):
            skipped += 1
            results.append(
                {
                    "ticket_id": ticket_id,
                    "title": title,
                    "criterion_index": index,
                    "status": "skipped",
                    "reason": "non-command criterion",
                }
            )
            continue
        if criterion.get("type") != "command":
            skipped += 1
            results.append(
                {
                    "ticket_id": ticket_id,
                    "title": title,
                    "criterion_index": index,
                    "status": "skipped",
                    "reason": "non-command criterion",
                }
            )
            continue
        run = criterion.get("run")
        if not isinstance(run, str) or not run.strip():
            failed += 1
            failure = {
                "ticket_id": ticket_id,
                "title": title,
                "criterion_index": index,
                "reason": "missing run command",
            }
            failures.append(failure)
            results.append({**failure, "status": "failed"})
            continue
        expected_exit = criterion.get("expect_exit_code", 0)
        expect_contains = criterion.get("expect_contains")
        completed = subprocess.run(
            run,
            shell=True,
            cwd=project_root,
            capture_output=True,
            text=True,
        )
        combined = (completed.stdout or "") + (completed.stderr or "")
        result = {
            "ticket_id": ticket_id,
            "title": title,
            "criterion_index": index,
            "run": run,
            "exit_code": completed.returncode,
        }
        if completed.returncode != expected_exit:
            failed += 1
            failure = {
                **result,
                "reason": f"expected exit {expected_exit}, got {completed.returncode}",
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            }
            failures.append(failure)
            results.append({**failure, "status": "failed"})
            continue
        if expect_contains is not None and str(expect_contains) not in combined:
            failed += 1
            failure = {
                **result,
                "reason": f"expected output to contain {expect_contains!r}",
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            }
            failures.append(failure)
            results.append({**failure, "status": "failed"})
            continue
        passed += 1
        results.append({**result, "status": "passed"})

payload = {
    "summary": {"passed": passed, "failed": failed, "skipped": skipped},
    "results": results,
    "failures": failures,
}
print(json.dumps(payload, ensure_ascii=False))
PY
}

bundle_matches_track() {
  local bundle_dir="$1" track_filter="$2"
  [ -z "$track_filter" ] && return 0
  local bundle_track
  bundle_track="$(yaml_field "$bundle_dir/bundle.yaml" "track")"
  [ "$bundle_track" = "$track_filter" ]
}

# --- Commands ---

cmd_init_roadmap() {
  local project="" force=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --project) project="$2"; shift 2;;
      --force) force="true"; shift;;
      *) shift;;
    esac
  done
  [ -z "$project" ] && sk_error "SK-TKT-014" "--project is required"

  # C5 fix: idempotent — refuse if already exists unless --force
  if [ -f "$TKT_ROOT/roadmap.yaml" ] && [ "$force" != "true" ]; then
    sk_error "SK-TKT-021" "Roadmap already exists at $TKT_ROOT/roadmap.yaml. Use --force to overwrite."
  fi

  mkdir -p "$TKT_ROOT/bundles"
  cat > "$TKT_ROOT/roadmap.yaml" <<YAML
project: "$project"
version: 2
updated_at: "$(timestamp)"
maintained_by: pm-agent
stage: planning
stage_history:
  - stage: planning
    entered_at: "$(timestamp)"
    reason: "initial"
goals: []
success_criteria: []
decision_gates: []
dependencies: []
YAML
  touch "$TKT_ROOT/history.log"
  log_event "INIT" "project=$project"
  echo "{\"roadmap_path\":\"$TKT_ROOT/roadmap.yaml\",\"project\":\"$project\"}"
}

cmd_create_bundle() {
  local goal="" context="" depends_on="" priority="" track="" source_plan="" worktree="" carryover_bundle=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --goal) goal="$2"; shift 2;;
      --context) context="$2"; shift 2;;
      --depends-on) depends_on="$2"; shift 2;;
      --priority) priority="$2"; shift 2;;
      --track) track="$2"; shift 2;;
      --source-plan) source_plan="$2"; shift 2;;
      --worktree) worktree="true"; shift;;
      --carryover) carryover_bundle="$2"; shift 2;;
      *) shift;;
    esac
  done
  [ -z "$goal" ] && sk_error "SK-TKT-014" "--goal is required"

  # H14 fix: check .tkt/ exists
  [ ! -d "$TKT_ROOT" ] && sk_error "SK-TKT-018" ".tkt/ not initialized. Run init-roadmap first."

  # Validate depends-on bundles exist
  if [ -n "$depends_on" ]; then
    IFS=',' read -ra deps <<< "$depends_on"
    for dep in "${deps[@]}"; do
      dep="$(echo "$dep" | xargs)"  # trim whitespace
      [ ! -d "$TKT_ROOT/bundles/$dep" ] && sk_error "SK-TKT-026" "Dependency bundle not found: $dep"
    done
  fi

  local bid
  bid="$(next_bundle_id)"
  local bdir="$TKT_ROOT/bundles/$bid"
  mkdir -p "$bdir"
  local worktree_path_yaml="null"
  local worktree_branch_yaml="null"
  local worktree_path=""
  local worktree_branch="bundle/$bid"
  if [ "$worktree" = "true" ]; then
    git -C "$PROJECT_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 || sk_error "SK-GIT-007" "Git worktree creation requires a git repository"
    mkdir -p "$PROJECT_ROOT/.worktrees"
    worktree_path="$PROJECT_ROOT/.worktrees/$bid"
    rm -rf "$worktree_path"
    if ! git -C "$PROJECT_ROOT" worktree add "$worktree_path" -b "$worktree_branch" >/dev/null 2>&1; then
      sk_error "SK-TKT-036" "Failed to create worktree for bundle $bid"
    fi
    worktree_path_yaml="\".worktrees/$bid\""
    worktree_branch_yaml="\"$worktree_branch\""
  fi

  if [ -n "$carryover_bundle" ] && [ ! -f "$TKT_ROOT/bundles/$carryover_bundle/carryover.yaml" ]; then
    sk_error "SK-TKT-038" "Carryover file not found for bundle $carryover_bundle"
  fi

  # Build depends_on YAML list
  local deps_yaml="[]"
  local track_yaml="null"
  local source_plan_yaml="null"
  if [ -n "$depends_on" ]; then
    deps_yaml=""
    IFS=',' read -ra deps <<< "$depends_on"
    for dep in "${deps[@]}"; do
      dep="$(echo "$dep" | xargs)"
      deps_yaml+="
  - \"$dep\""
    done
  fi

  if [ -n "$track" ]; then
    track_yaml="\"$track\""
  fi
  if [ -n "$source_plan" ]; then
    source_plan_yaml="\"$source_plan\""
  fi

  cat > "$bdir/bundle.yaml" <<YAML
id: "$bid"
goal_ref: null
title: "$goal"
status: open
created_at: "$(timestamp)"
created_by: pm-agent
main_agent: null
ticket_count: 2
priority: ${priority:-normal}
track: ${track_yaml}
source_plan: ${source_plan_yaml}
worktree_path: ${worktree_path_yaml}
worktree_branch: ${worktree_branch_yaml}
depends_on: ${deps_yaml}
YAML

  # TKT-000: Integrate ticket
  cat > "$bdir/TKT-000.yaml" <<YAML
id: TKT-000
bundle: "$bid"
type: integrate
title: "Integrate and coordinate $bid"
description: |
  Coordinate all worker tickets in this bundle.
  Close this ticket when all worker TKTs are done.
acceptance_criteria:
  - "All worker tickets completed or accounted for"
  - "Outputs integrated and consistent"
status: open
claimed_by: null
claimed_at: null
started_at: null
completed_at: null
category: null
effort_estimate: null
agent_type: null
source_plan: null
source_ticket_index: null
result:
  summary: null
  artifacts: []
  notes: null
  evidence: null
depends_on: []
YAML

  # TKT-A00: Audit ticket
  cat > "$bdir/TKT-A00.yaml" <<YAML
id: TKT-A00
bundle: "$bid"
type: audit
title: "Spot-check quality for $bid"
description: |
  Randomly select and verify engineering quality of completed work.
  Check code standards, test coverage, documentation accuracy.
acceptance_criteria:
  - "At least 2 random items checked"
  - "Findings documented with quality score"
status: open
claimed_by: null
claimed_at: null
started_at: null
completed_at: null
category: null
effort_estimate: null
agent_type: null
source_plan: null
source_ticket_index: null
result:
  summary: null
  artifacts: []
  notes: null
  evidence: null
depends_on: []
YAML

  log_event "BUNDLE_CREATED" "id=$bid goal=\"$goal\""
  if [ -n "$carryover_bundle" ]; then
    while IFS=$'\t' read -r title origin description; do
      [ -n "$title" ] || continue
      cmd_add --bundle "$bid" --title "$title" --description "carryover_from: $origin | $description" >/dev/null
    done < <(
      CARRYOVER_FILE="$TKT_ROOT/bundles/$carryover_bundle/carryover.yaml" python3 - <<'PY'
from pathlib import Path
import os

import yaml

path = Path(os.environ["CARRYOVER_FILE"])
data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
for item in data.get("carryover", []):
    title = str(item.get("title", "")).replace("\t", " ").replace("\n", " ").strip()
    origin = str(item.get("carryover_from", "")).replace("\t", " ").replace("\n", " ").strip()
    description = str(item.get("original_description", "")).replace("\t", " ").replace("\n", " ").strip()
    print(f"{title}\t{origin}\t{description}")
PY
    )
  fi
  echo "{\"bundle_id\":\"$bid\",\"path\":\"$bdir\",\"tickets\":[\"TKT-000\",\"TKT-A00\"]}"
}

cmd_claim() {
  local bundle="" ticket="" agent=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --bundle) bundle="$2"; shift 2;;
      --ticket) ticket="$2"; shift 2;;
      --agent) agent="$2"; shift 2;;
      *) shift;;
    esac
  done
  [ -z "$bundle" ] || [ -z "$ticket" ] || [ -z "$agent" ] && {
    sk_error "SK-TKT-014" "--bundle, --ticket, --agent required"
  }

  local tkt_file="$TKT_ROOT/bundles/$bundle/$ticket.yaml"
  [ ! -f "$tkt_file" ] && sk_error "SK-TKT-002" "Ticket not found: $tkt_file"

  local current_status
  current_status="$(yaml_field "$tkt_file" "status")"
  [[ "$current_status" != "open" && "$current_status" != "blocked" ]] && {
    sk_error "SK-TKT-035" "Ticket transition $current_status -> claimed is not allowed"
  }

  # Atomic claim via lock (H3 fix: use timeout instead of non-blocking)
  local lockfile="$TKT_ROOT/bundles/$bundle/.lock"
  local lt
  lt="$(lock_timeout)"
  (
    flock -w "$lt" 200 || { sk_error "SK-TKT-017" "Bundle locked after ${lt}s timeout"; }
    claim_yaml_ticket "$tkt_file" "$agent"
  ) 200>"$lockfile"

  local role="worker"
  if [ "$ticket" = "TKT-000" ]; then
    role="main"
    sed -i "s/^main_agent: null/main_agent: \"$agent\"/" "$TKT_ROOT/bundles/$bundle/bundle.yaml"
  elif [[ "$ticket" == TKT-A* ]]; then
    role="audit"
  fi

  log_event "TICKET_CLAIMED" "bundle=$bundle ticket=$ticket agent=$agent role=$role"
  echo "{\"claimed\":true,\"ticket\":\"$ticket\",\"role\":\"$role\",\"agent\":\"$agent\"}"
}

cmd_update() {
  local bundle="" ticket="" status="" summary="" evidence="" evidence_file="" reason=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --bundle) bundle="$2"; shift 2;;
      --ticket) ticket="$2"; shift 2;;
      --status) status="$2"; shift 2;;
      --summary) summary="$2"; shift 2;;
      --evidence) evidence="$2"; shift 2;;
      --evidence-file) evidence_file="$2"; shift 2;;
      --reason) reason="$2"; shift 2;;
      *) shift;;
    esac
  done
  [ -z "$bundle" ] || [ -z "$ticket" ] || [ -z "$status" ] && {
    sk_error "SK-TKT-014" "--bundle, --ticket, --status required"
  }

  local tkt_file="$TKT_ROOT/bundles/$bundle/$ticket.yaml"
  [ ! -f "$tkt_file" ] && { sk_error "SK-TKT-002" "Ticket not found"; }

  local current_status
  current_status="$(yaml_field "$tkt_file" "status")"
  case "$current_status:$status" in
    open:done|open:failed|claimed:in_progress|claimed:done|claimed:failed|claimed:blocked|in_progress:done|in_progress:failed|in_progress:blocked|blocked:failed) ;;
    *) sk_error "SK-TKT-035" "Ticket transition $current_status -> $status is not allowed" ;;
  esac

  local evidence_value=""
  if [ "$status" = "done" ]; then
    [ -n "$evidence" ] || [ -n "$evidence_file" ] || sk_error "SK-TKT-030" "--evidence or --evidence-file is required when marking a ticket done"
    evidence_value="$(resolve_evidence_value "$evidence" "$evidence_file")"
  elif [ -n "$evidence" ] || [ -n "$evidence_file" ]; then
    evidence_value="$(resolve_evidence_value "$evidence" "$evidence_file")"
  fi

  [ "$status" != "blocked" ] || [ -n "$reason" ] || sk_error "SK-TKT-014" "--reason is required when blocking a ticket"
  update_yaml_status "$tkt_file" "$status" "$summary" "$evidence_value" "$reason"

  # Update bundle status to in_progress if still open
  local bstatus
  bstatus="$(yaml_field "$TKT_ROOT/bundles/$bundle/bundle.yaml" "status")"
  if [ "$bstatus" = "open" ]; then
    sed -i "s/^status: open/status: in_progress/" "$TKT_ROOT/bundles/$bundle/bundle.yaml"
  fi

  log_event "TICKET_UPDATED" "bundle=$bundle ticket=$ticket status=$status"
  echo "{\"ticket_id\":\"$ticket\",\"new_status\":\"$status\"}"
}

cmd_add() {
  local bundle="" type="worker" title="" description="" category="" effort_estimate="" agent_type="" source_plan="" source_ticket_index="" acceptance="" depends_on="" skills="" wave="" qa_scenarios=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --bundle) bundle="$2"; shift 2;;
      --type) type="$2"; shift 2;;
      --title) title="$2"; shift 2;;
      --description) description="$2"; shift 2;;
      --category) category="$2"; shift 2;;
      --effort-estimate) effort_estimate="$2"; shift 2;;
      --agent-type) agent_type="$2"; shift 2;;
      --source-plan) source_plan="$2"; shift 2;;
      --source-ticket-index) source_ticket_index="$2"; shift 2;;
      --acceptance) acceptance="$2"; shift 2;;
      --depends-on) depends_on="$2"; shift 2;;
      --skills) skills="$2"; shift 2;;
      --wave) wave="$2"; shift 2;;
      --qa-scenarios) qa_scenarios="$2"; shift 2;;
      *) shift;;
    esac
  done
  [ -z "$bundle" ] || [ -z "$title" ] && {
    sk_error "SK-TKT-014" "--bundle, --title required"
  }

  local bdir="$TKT_ROOT/bundles/$bundle"
  [ ! -d "$bdir" ] && { sk_error "SK-TKT-018" "Bundle not found: $bundle"; }

  local num
  num="$(next_ticket_num "$bdir")"
  local tkt_id="TKT-$num"

  local category_yaml="null" effort_yaml="null" agent_type_yaml="null" source_plan_yaml="null" source_index_yaml="null" wave_yaml="null" acceptance_text deps_yaml="[]" skills_yaml="[]" qa_yaml="[]"
  [ -n "$category" ] && category_yaml="\"$category\""
  [ -n "$effort_estimate" ] && effort_yaml="\"$effort_estimate\""
  [ -n "$agent_type" ] && agent_type_yaml="\"$agent_type\""
  [ -n "$source_plan" ] && source_plan_yaml="\"$source_plan\""
  [ -n "$source_ticket_index" ] && source_index_yaml="$source_ticket_index"
  [ -n "$wave" ] && wave_yaml="$wave"
  acceptance_text="${acceptance:-Task completed as described}"
  deps_yaml="$(yaml_list_from_csv "$depends_on")"
  skills_yaml="$(yaml_list_from_csv "$skills")"
  qa_yaml="$(yaml_list_from_csv "$qa_scenarios")"

  cat > "$bdir/$tkt_id.yaml" <<YAML
id: $tkt_id
bundle: "$bundle"
type: $type
title: "$title"
description: |
  ${description:-$title}
acceptance_criteria:
  - "$acceptance_text"
status: open
claimed_by: null
claimed_at: null
started_at: null
completed_at: null
category: ${category_yaml}
effort_estimate: ${effort_yaml}
agent_type: ${agent_type_yaml}
skills: ${skills_yaml}
wave: ${wave_yaml}
qa_scenarios: ${qa_yaml}
source_plan: ${source_plan_yaml}
source_ticket_index: ${source_index_yaml}
result:
  summary: null
  artifacts: []
  notes: null
  evidence: null
depends_on: ${deps_yaml}
YAML

  # Update ticket count
  local count
  count=$(ls "$bdir"/TKT-*.yaml 2>/dev/null | wc -l)
  sed -i "s/^ticket_count: .*/ticket_count: $count/" "$bdir/bundle.yaml"

  log_event "TICKET_ADDED" "bundle=$bundle ticket=$tkt_id title=\"$title\""
  echo "{\"ticket_id\":\"$tkt_id\",\"bundle\":\"$bundle\"}"
}

cmd_express_create() {
  local title="" acceptance="" category="" effort_estimate="" agent_type="" source_plan="" source_ticket_index="" wave=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --title) title="$2"; shift 2;;
      --acceptance) acceptance="$2"; shift 2;;
      --category) category="$2"; shift 2;;
      --effort-estimate) effort_estimate="$2"; shift 2;;
      --agent-type) agent_type="$2"; shift 2;;
      --source-plan) source_plan="$2"; shift 2;;
      --source-ticket-index) source_ticket_index="$2"; shift 2;;
      --wave) wave="$2"; shift 2;;
      *) shift;;
    esac
  done

  [ -z "$title" ] && sk_error "SK-TKT-014" "--title is required"
  [ -z "$acceptance" ] && sk_error "SK-TKT-014" "--acceptance is required"

  mkdir -p "$TKT_ROOT/express"
  touch "$TKT_ROOT/history.log"

  local express_id
  express_id="$(next_express_id)"
  local express_file="$TKT_ROOT/express/$express_id.yaml"
  local category_yaml="null" effort_yaml="null" agent_type_yaml="null" source_plan_yaml="null" source_index_yaml="null" wave_yaml="null"
  [ -n "$category" ] && category_yaml="\"$category\""
  [ -n "$effort_estimate" ] && effort_yaml="\"$effort_estimate\""
  [ -n "$agent_type" ] && agent_type_yaml="\"$agent_type\""
  [ -n "$source_plan" ] && source_plan_yaml="\"$source_plan\""
  [ -n "$source_ticket_index" ] && source_index_yaml="$source_ticket_index"
  [ -n "$wave" ] && wave_yaml="$wave"

  cat > "$express_file" <<YAML
id: "$express_id"
type: express
title: "$title"
description: |
  $title
acceptance_criteria:
  - "$acceptance"
status: open
claimed_by: null
claimed_at: null
completed_at: null
created_at: "$(timestamp)"
updated_at: "$(timestamp)"
category: ${category_yaml}
effort_estimate: ${effort_yaml}
agent_type: ${agent_type_yaml}
wave: ${wave_yaml}
source_plan: ${source_plan_yaml}
source_ticket_index: ${source_index_yaml}
result:
  summary: null
  artifacts: []
  notes: null
  evidence: null
YAML

  log_event "EXPRESS_CREATED" "ticket=$express_id title=\"$title\""
  echo "{\"ticket_id\":\"$express_id\",\"path\":\"$express_file\",\"status\":\"open\"}"
}

cmd_express_list() {
  [ ! -d "$TKT_ROOT/express" ] && { echo '{"tickets":[]}'; exit 0; }

  local result="["
  local first=true
  local files=("$TKT_ROOT/express"/EXP-*.yaml)
  local idx
  for ((idx=${#files[@]}-1; idx>=0; idx--)); do
    local f="${files[$idx]}"
    [ -f "$f" ] || continue
    local tid tstatus ttitle created_at completed_at category
    tid="$(yaml_field "$f" "id")"
    tstatus="$(yaml_field "$f" "status")"
    ttitle="$(yaml_field "$f" "title")"
    created_at="$(yaml_field "$f" "created_at")"
    completed_at="$(yaml_field "$f" "completed_at")"
    category="$(yaml_field "$f" "category")"
    $first || result+=","
    first=false
    result+="{\"id\":\"$tid\",\"status\":\"$tstatus\",\"title\":\"$ttitle\",\"category\":\"$category\",\"created_at\":\"$created_at\",\"completed_at\":\"$completed_at\"}"
  done
  result+="]"
  echo "{\"tickets\":$result}"
}

cmd_express_claim() {
  local express_id="" agent=""
  if [[ $# -gt 0 && "$1" != --* ]]; then
    express_id="$1"
    shift
  fi
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --agent) agent="$2"; shift 2;;
      *) shift;;
    esac
  done

  [ -z "$express_id" ] && sk_error "SK-TKT-014" "express ticket id is required (e.g. EXP-001)"
  [ -z "$agent" ] && sk_error "SK-TKT-014" "--agent is required"

  local express_file="$TKT_ROOT/express/$express_id.yaml"
  [ ! -f "$express_file" ] && sk_error "SK-TKT-002" "Express ticket not found: $express_id"

  local current_status
  current_status="$(yaml_field "$express_file" "status")"
  [ "$current_status" != "open" ] && {
    sk_error "SK-TKT-019" "Express ticket $express_id status is $current_status, expected open"
  }

  sed -i "s/^status: open/status: claimed/" "$express_file"
  sed -i "s/^claimed_by: null/claimed_by: \"$agent\"/" "$express_file"
  sed -i "s/^claimed_at: null/claimed_at: \"$(timestamp)\"/" "$express_file"
  sed -i "s/^updated_at: .*/updated_at: \"$(timestamp)\"/" "$express_file"

  log_event "EXPRESS_CLAIMED" "ticket=$express_id agent=$agent"
  echo "{\"claimed\":true,\"ticket_id\":\"$express_id\",\"status\":\"claimed\",\"agent\":\"$agent\"}"
}

cmd_express_close() {
  local express_id="" files_changed="" evidence="" evidence_file=""
  if [[ $# -gt 0 && "$1" != --* ]]; then
    express_id="$1"
    shift
  fi
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --files-changed) files_changed="$2"; shift 2;;
      --evidence) evidence="$2"; shift 2;;
      --evidence-file) evidence_file="$2"; shift 2;;
      *) shift;;
    esac
  done

  [ -z "$express_id" ] && sk_error "SK-TKT-014" "express ticket id is required (e.g. EXP-001)"
  [ -n "$evidence" ] || [ -n "$evidence_file" ] || sk_error "SK-TKT-030" "--evidence or --evidence-file is required when closing an express ticket"
  local express_file="$TKT_ROOT/express/$express_id.yaml"
  [ ! -f "$express_file" ] && sk_error "SK-TKT-002" "Express ticket not found: $express_id"

  if [ -n "$files_changed" ] && [[ ! "$files_changed" =~ ^[0-9]+$ ]]; then
    sk_error "SK-TKT-014" "--files-changed must be a non-negative integer"
  fi

  local current_status
  current_status="$(yaml_field "$express_file" "status")"
  [ "$current_status" = "done" ] && sk_error "SK-TKT-019" "Express ticket $express_id is already done"

  local evidence_value
  evidence_value="$(resolve_evidence_value "$evidence" "$evidence_file")"

  if [ -n "$files_changed" ] && [ "$files_changed" -gt 3 ]; then
    local title description acceptance category effort_estimate agent_type source_plan source_ticket_index wave
    title="$(yaml_field "$express_file" "title")"
    description="$(sed -n '/^description: |$/,/^acceptance_criteria:/p' "$express_file" | sed '1d;$d' | sed 's/^  //')"
    acceptance="$(sed -n '/^acceptance_criteria:/,/^status:/p' "$express_file" | sed -n '2p' | sed 's/^  - "\(.*\)"$/\1/' | sed 's/^  - //')"
    category="$(yaml_field "$express_file" "category")"
    effort_estimate="$(yaml_field "$express_file" "effort_estimate")"
    agent_type="$(yaml_field "$express_file" "agent_type")"
    wave="$(yaml_field "$express_file" "wave")"
    source_plan="$(yaml_field "$express_file" "source_plan")"
    source_ticket_index="$(yaml_field "$express_file" "source_ticket_index")"
    [ "$category" = "null" ] && category=""
    [ "$effort_estimate" = "null" ] && effort_estimate=""
    [ "$agent_type" = "null" ] && agent_type=""
    [ "$wave" = "null" ] && wave=""
    [ "$source_plan" = "null" ] && source_plan=""
    [ "$source_ticket_index" = "null" ] && source_ticket_index=""
    [ -z "$description" ] && description="$title"
    [ -z "$acceptance" ] && acceptance="Express ticket acceptance criteria preserved"

    local create_output bid add_output tkt_id
    create_output="$(cmd_create_bundle --goal "$title")"
    bid="$(echo "$create_output" | sed -n 's/.*"bundle_id":"\([^"]*\)".*/\1/p')"
    [ -z "$bid" ] && sk_error "SK-TKT-020" "Failed to allocate bundle for express ticket upgrade"

    add_output="$(cmd_add --bundle "$bid" --title "$title" --description "$description" --acceptance "$acceptance" ${category:+--category "$category"} ${effort_estimate:+--effort-estimate "$effort_estimate"} ${agent_type:+--agent-type "$agent_type"} ${wave:+--wave "$wave"} ${source_plan:+--source-plan "$source_plan"} ${source_ticket_index:+--source-ticket-index "$source_ticket_index"})"
    tkt_id="$(echo "$add_output" | sed -n 's/.*"ticket_id":"\([^"]*\)".*/\1/p')"
    [ -z "$tkt_id" ] && sk_error "SK-TKT-020" "Failed to create worker ticket during express upgrade"

    local upgraded_ticket_file="$TKT_ROOT/bundles/$bid/$tkt_id.yaml"
    sed -i "s/^  - \"Task completed as described\"/  - \"$acceptance\"/" "$upgraded_ticket_file"
    update_yaml_status "$express_file" "done" "Auto-upgraded to bundle $bid as $tkt_id (files_changed=$files_changed)" "$evidence_value"
    sed -i "s/^updated_at: .*/updated_at: \"$(timestamp)\"/" "$express_file"
    {
      echo "upgraded_to_bundle: \"$bid\""
      echo "upgraded_to_ticket: \"$tkt_id\""
      echo "upgraded_reason: \"files_changed=$files_changed (>3)\""
    } >> "$express_file"

    log_event "EXPRESS_UPGRADED" "ticket=$express_id files_changed=$files_changed bundle=$bid worker_ticket=$tkt_id"
    echo "{\"closed\":true,\"ticket_id\":\"$express_id\",\"status\":\"done\",\"files_changed\":$files_changed,\"upgraded\":true,\"bundle_id\":\"$bid\",\"worker_ticket\":\"$tkt_id\"}"
    return
  fi

  update_yaml_status "$express_file" "done" "" "$evidence_value"
  sed -i "s/^updated_at: .*/updated_at: \"$(timestamp)\"/" "$express_file"

  log_event "EXPRESS_CLOSED" "ticket=$express_id files_changed=${files_changed:-unknown}"
  if [ -n "$files_changed" ]; then
    echo "{\"closed\":true,\"ticket_id\":\"$express_id\",\"status\":\"done\",\"files_changed\":$files_changed}"
  else
    echo "{\"closed\":true,\"ticket_id\":\"$express_id\",\"status\":\"done\"}"
  fi
}

cmd_express() {
  local subcommand="create"
  if [[ $# -gt 0 && "$1" != --* ]]; then
    subcommand="$1"
    shift
  fi

  case "$subcommand" in
    create) cmd_express_create "$@" ;;
    list) cmd_express_list "$@" ;;
    claim) cmd_express_claim "$@" ;;
    close) cmd_express_close "$@" ;;
    *) sk_error "SK-CLI-002" "Unknown express subcommand: $subcommand. Usage: tkt.sh express [--title ... --acceptance ...|list|claim EXP-001 --agent ...|close EXP-001 --files-changed N]" ;;
  esac
}

cmd_status() {
  local bundle=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --bundle) bundle="$2"; shift 2;;
      *) shift;;
    esac
  done
  [ -z "$bundle" ] && { sk_error "SK-TKT-014" "--bundle required"; }

  local bdir="$TKT_ROOT/bundles/$bundle"
  [ ! -d "$bdir" ] && { sk_error "SK-TKT-018" "Bundle not found"; }

  local bstatus btitle main_agent
  bstatus="$(yaml_field "$bdir/bundle.yaml" "status")"
  btitle="$(yaml_field "$bdir/bundle.yaml" "title")"
  main_agent="$(yaml_field "$bdir/bundle.yaml" "main_agent")"

  local tickets="[]"
  local first=true
  tickets="["
  for f in "$bdir"/TKT-*.yaml; do
    [ -f "$f" ] || continue
    local tid tstatus ttype ttitle tclaimed
    tid="$(yaml_field "$f" "id")"
    tstatus="$(yaml_field "$f" "status")"
    ttype="$(yaml_field "$f" "type")"
    ttitle="$(yaml_field "$f" "title")"
    tclaimed="$(yaml_field "$f" "claimed_by")"
    $first || tickets+=","
    first=false
    tickets+="{\"id\":\"$tid\",\"type\":\"$ttype\",\"status\":\"$tstatus\",\"title\":\"$ttitle\",\"claimed_by\":\"$tclaimed\"}"
  done
  tickets+="]"

  echo "{\"bundle\":\"$bundle\",\"title\":\"$btitle\",\"status\":\"$bstatus\",\"main_agent\":\"$main_agent\",\"tickets\":$tickets}"
}

cmd_close() {
  local bundle="" merge=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --bundle) bundle="$2"; shift 2;;
      --merge) merge="true"; shift;;
      *) shift;;
    esac
  done
  [ -z "$bundle" ] && { sk_error "SK-TKT-014" "--bundle required"; }

  local bdir="$TKT_ROOT/bundles/$bundle"
  [ -d "$bdir" ] || sk_error "SK-TKT-018" "Bundle not found"

  # Check dependencies are satisfied (all depended-on bundles must be closed)
  if [ -f "$bdir/bundle.yaml" ]; then
    local dep_line
    dep_line="$(grep '^ *- ' "$bdir/bundle.yaml" 2>/dev/null || true)"
    if [ -n "$dep_line" ]; then
      while IFS= read -r line; do
        local dep
        dep="$(echo "$line" | sed 's/^ *- *//' | sed 's/"//g' | xargs)"
        [ -z "$dep" ] && continue
        if [ -d "$TKT_ROOT/bundles/$dep" ]; then
          local dep_status
          dep_status="$(yaml_field "$TKT_ROOT/bundles/$dep/bundle.yaml" "status")"
          if [ "$dep_status" != "closed" ] && [ "$dep_status" != "archived" ]; then
            sk_error "SK-TKT-027" "Dependency $dep is still '$dep_status'. Close dependencies before closing $bundle."
          fi
        fi
      done <<< "$dep_line"
    fi
  fi

  local worktree_path worktree_branch worktree_abs=""
  worktree_path="$(yaml_field "$bdir/bundle.yaml" "worktree_path")"
  worktree_branch="$(yaml_field "$bdir/bundle.yaml" "worktree_branch")"
  if [ -n "$worktree_path" ] && [ "$worktree_path" != "null" ]; then
    if [[ "$worktree_path" = /* ]]; then
      worktree_abs="$worktree_path"
    else
      worktree_abs="$PROJECT_ROOT/$worktree_path"
    fi
    [ -d "$worktree_abs" ] || sk_error "SK-GIT-001" "Worktree path not found: $worktree_abs"
    local worktree_dirty
    worktree_dirty="$(git -C "$worktree_abs" status --porcelain 2>/dev/null || true)"
    [ -z "$worktree_dirty" ] || sk_error "SK-TKT-037" "Worktree has uncommitted changes: $worktree_path"
  fi

  # Check all worker tickets are done or failed
  local worker_claimed_by=()
  for f in "$bdir"/TKT-*.yaml; do
    [ -f "$f" ] || continue
    local tid ttype tstatus tclaimed
    tid="$(yaml_field "$f" "id")"
    ttype="$(yaml_field "$f" "type")"
    tstatus="$(yaml_field "$f" "status")"
    tclaimed="$(yaml_field "$f" "claimed_by")"
    # Skip integrate and audit tickets
    [ "$ttype" = "integrate" ] || [ "$ttype" = "audit" ] && continue
    [ -n "$tclaimed" ] && [ "$tclaimed" != "null" ] && worker_claimed_by+=("$tclaimed")
    if [ "$tstatus" != "done" ] && [ "$tstatus" != "failed" ]; then
      sk_error "SK-TKT-019" "Worker ticket $tid is still $tstatus, cannot close bundle"
    fi
  done

  local audit_ticket="$bdir/TKT-A00.yaml"
  if [ -f "$audit_ticket" ]; then
    local audit_claimed_by audit_status
    audit_claimed_by="$(yaml_field "$audit_ticket" "claimed_by")"
    audit_status="$(yaml_field "$audit_ticket" "status")"
    [ -n "$audit_claimed_by" ] && [ "$audit_claimed_by" != "null" ] || sk_error "SK-TKT-012" "Audit ticket must be claimed before bundle close"
    [ "$audit_status" = "done" ] || sk_error "SK-TKT-012" "Audit ticket must be done before bundle close"
    local claimed_by
    for claimed_by in "${worker_claimed_by[@]}"; do
      if [ "$claimed_by" = "$audit_claimed_by" ]; then
        sk_error "SK-TKT-031" "Audit ticket must be claimed by a different agent than worker tickets"
      fi
    done
  fi

  local structural_output
  if ! structural_output="$(cd "$PROJECT_ROOT" && python3 spec/validate_repo_structural.py 2>&1)"; then
    printf '%s\n' "$structural_output" >&2
    sk_error "SK-TKT-032" "Structural validation failed before close"
  fi

  local close_gate_cmd
  close_gate_cmd="$(config_value "close_gate.command")"
  if [ -n "$close_gate_cmd" ]; then
    local close_gate_output
    if ! close_gate_output="$(cd "$PROJECT_ROOT" && bash -lc "$close_gate_cmd" 2>&1)"; then
      printf '%s\n' "$close_gate_output" >&2
      sk_error "SK-TKT-033" "Configured close gate failed"
    fi
  fi

  local acceptance_output
  if ! acceptance_output="$(run_command_acceptance_checks "$bdir" 2>&1)"; then
    sk_error "SK-TKT-034" "Executable acceptance criteria failed"
  fi

  if [ "$merge" = "true" ] && [ -n "$worktree_abs" ]; then
    git -C "$PROJECT_ROOT" merge --no-ff "$worktree_branch" >/dev/null 2>&1 || sk_error "SK-GIT-002" "Failed to merge worktree branch $worktree_branch"
    git -C "$PROJECT_ROOT" worktree remove "$worktree_abs" >/dev/null 2>&1 || sk_error "SK-GIT-003" "Failed to remove worktree $worktree_abs"
  fi

  update_yaml_status "$bdir/TKT-000.yaml" "done" "Bundle $bundle integrated and closed" ""
  sed -i "s/^status: .*/status: closed/" "$bdir/bundle.yaml"

  local report_json
  if ! report_json="$({
    ACCEPTANCE_JSON="$acceptance_output" BUNDLE_DIR="$bdir" REVIEW_FILE="$bdir/review.yaml" CARRYOVER_FILE="$bdir/carryover.yaml" python3 - <<'PY'
import json
import os
from pathlib import Path

import yaml

bundle_dir = Path(os.environ["BUNDLE_DIR"])
review_file = Path(os.environ["REVIEW_FILE"])
carryover_file = Path(os.environ["CARRYOVER_FILE"])
acceptance = json.loads(os.environ["ACCEPTANCE_JSON"])
failures = acceptance.get("failures", [])
summary = acceptance.get("summary", {})

tickets = []
for ticket_file in sorted(bundle_dir.glob("TKT-*.yaml")):
    data = yaml.safe_load(ticket_file.read_text(encoding="utf-8")) or {}
    tickets.append(data)

evidence_summary = [
    {
        "ticket_id": ticket.get("id"),
        "title": ticket.get("title"),
        "evidence": ((ticket.get("result") or {}).get("evidence")),
    }
    for ticket in tickets
]

per_ticket: dict[str, dict] = {}
for item in acceptance.get("results", []):
    key = item.get("ticket_id")
    bucket = per_ticket.setdefault(
        key,
        {
            "ticket_id": key,
            "title": item.get("title"),
            "passed": 0,
            "failed": 0,
            "skipped": 0,
        },
    )
    bucket[item.get("status", "skipped")] += 1

ticket_map = {ticket.get("id"): ticket for ticket in tickets}
carryover = []
if failures:
    failures_by_ticket: dict[str, list[dict]] = {}
    for failure in failures:
        failures_by_ticket.setdefault(failure.get("ticket_id"), []).append(failure)
    for ticket_id, items in failures_by_ticket.items():
        ticket = ticket_map.get(ticket_id, {})
        description = str(ticket.get("description", "")).strip().replace("\n", " ")
        carryover.append(
            {
                "ticket_id": ticket_id,
                "title": ticket.get("title"),
                "carryover_from": f"{bundle_dir.name}/{ticket_id}",
                "reason": "acceptance_criteria_failed",
                "failed_criteria": items,
                "original_description": description,
            }
        )

audit_ticket = ticket_map.get("TKT-A00", {})
audit_acceptance = audit_ticket.get("acceptance_criteria") or []
audit_summary = (audit_ticket.get("result") or {}).get("summary")
audit_evidence = (audit_ticket.get("result") or {}).get("evidence")
checked_items = [
    {"item": str(item), "status": "checked"}
    for item in audit_acceptance
]
quality_score = 1.0 if audit_ticket.get("status") == "done" else None

review_payload = {
    "bundle": bundle_dir.name,
    "generated_at": os.popen("date -u +%Y-%m-%dT%H:%M:%SZ").read().strip(),
    "generated_by": "main-agent",
    "summary": f"Bundle {bundle_dir.name} completed",
    "tickets_completed": sum(1 for ticket in tickets if ticket.get("status") == "done"),
    "tickets_failed": sum(1 for ticket in tickets if ticket.get("status") == "failed"),
    "evidence_summary": evidence_summary,
    "acceptance_results": {
        "passed": summary.get("passed", 0),
        "failed": summary.get("failed", 0),
        "skipped": summary.get("skipped", 0),
        "by_ticket": list(per_ticket.values()),
        "failures": failures,
    },
    "audit_result": {
        "checked_items": checked_items,
        "findings": [] if not audit_summary else [audit_summary],
        "quality_score": quality_score,
        "evidence": audit_evidence,
    },
    "discussion_points": [],
    "next_actions": [],
}
review_file.write_text(yaml.safe_dump(review_payload, sort_keys=False, allow_unicode=True), encoding="utf-8")

if carryover:
    carryover_file.write_text(
        yaml.safe_dump({"bundle": bundle_dir.name, "carryover": carryover}, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
else:
    carryover_file.unlink(missing_ok=True)

print(json.dumps({"carryover_count": len(carryover)}))
PY
  } 2>&1)"; then
    printf '%s\n' "$report_json" >&2
    sk_error "SK-TKT-034" "Executable acceptance criteria failed"
  fi
  local carryover_count
  carryover_count="$(REPORT_JSON="$report_json" python3 - <<'PY'
import json
import os

print(json.loads(os.environ["REPORT_JSON"])["carryover_count"])
PY
)"

  local done_count=0 failed_count=0
  for f in "$bdir"/TKT-*.yaml; do
    [ -f "$f" ] || continue
    local s
    s="$(yaml_field "$f" "status")"
    [ "$s" = "done" ] && done_count=$((done_count + 1))
    [ "$s" = "failed" ] && failed_count=$((failed_count + 1))
  done

  log_event "BUNDLE_CLOSED" "bundle=$bundle done=$done_count failed=$failed_count"
  echo "{\"closed\":true,\"bundle\":\"$bundle\",\"review_path\":\"$bdir/review.yaml\",\"done\":$done_count,\"failed\":$failed_count,\"carryover_count\":$carryover_count}"
}

cmd_list() {
  [ ! -d "$TKT_ROOT/bundles" ] && { echo '{"bundles":[]}'; exit 0; }

  local result="["
  local first=true
  local bundle_dirs=("$TKT_ROOT/bundles"/B-*/)
  local idx
  for ((idx=${#bundle_dirs[@]}-1; idx>=0; idx--)); do
    local d="${bundle_dirs[$idx]}"
    [ -d "$d" ] || continue
    local bid bstatus btitle
    bid="$(basename "$d")"
    bstatus="$(yaml_field "$d/bundle.yaml" "status")"
    btitle="$(yaml_field "$d/bundle.yaml" "title")"
    $first || result+=","
    first=false
    result+="{\"id\":\"$bid\",\"status\":\"$bstatus\",\"title\":\"$btitle\"}"
  done
  result+="]"
  echo "{\"bundles\":$result}"
}

# Valid roadmap stages and allowed transitions
# planning → active → review → done
# planning → archived (skip)
# active → blocked → active (resume)
# any → archived
VALID_ROADMAP_STAGES="planning active review blocked done archived"

roadmap_stage_valid() {
  local stage="$1"
  for s in $VALID_ROADMAP_STAGES; do
    [ "$s" = "$stage" ] && return 0
  done
  return 1
}

roadmap_transition_allowed() {
  local from="$1" to="$2"
  case "$from:$to" in
    planning:active|planning:archived) return 0;;
    active:review|active:blocked|active:archived) return 0;;
    review:done|review:active|review:archived) return 0;;
    blocked:active|blocked:archived) return 0;;
    done:archived) return 0;;
    *) return 1;;
  esac
}

cmd_roadmap_transition() {
  local to_stage="" reason="" force="" track=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --stage) to_stage="$2"; shift 2;;
      --reason) reason="$2"; shift 2;;
      --force) force="true"; shift;;
      --track) track="$2"; shift 2;;
      *) shift;;
    esac
  done
  [ -z "$to_stage" ] && sk_error "SK-TKT-014" "--stage is required"
  [ -z "$reason" ] && sk_error "SK-TKT-014" "--reason is required for stage transitions"

  local rmfile="$TKT_ROOT/roadmap.yaml"
  [ ! -f "$rmfile" ] && sk_error "SK-TKT-018" "No roadmap.yaml found. Run init-roadmap first."

  # Validate target stage
  roadmap_stage_valid "$to_stage" || sk_error "SK-TKT-022" "Invalid stage: $to_stage. Valid: $VALID_ROADMAP_STAGES"

  # Read current stage
  local current_stage
  current_stage="$(yaml_field "$rmfile" "stage")"
  [ -z "$current_stage" ] && current_stage="planning"

  # Same stage — no-op
  [ "$current_stage" = "$to_stage" ] && {
    echo "{\"transition\":\"no-op\",\"stage\":\"$current_stage\",\"message\":\"Already in stage $current_stage\"}"
    exit 0
  }

  # Gate check: is transition allowed?
  if [ "$force" != "true" ]; then
    roadmap_transition_allowed "$current_stage" "$to_stage" || \
      sk_error "SK-TKT-023" "Transition $current_stage → $to_stage not allowed. Allowed from $current_stage: $(
        for s in $VALID_ROADMAP_STAGES; do
          roadmap_transition_allowed "$current_stage" "$s" 2>/dev/null && printf '%s ' "$s"
        done
      ). Use --force to override."

    # Gate check: if transitioning to 'done', verify all bundles are closed
    if [ "$to_stage" = "done" ] && [ -d "$TKT_ROOT/bundles" ]; then
      for d in "$TKT_ROOT/bundles"/B-*/; do
        [ -d "$d" ] || continue
        bundle_matches_track "$d" "$track" || continue
        local bstatus
        bstatus="$(yaml_field "$d/bundle.yaml" "status")"
        if [ "$bstatus" != "closed" ] && [ "$bstatus" != "archived" ]; then
          local bid
          bid="$(basename "$d")"
          if [ -n "$track" ]; then
            sk_error "SK-TKT-024" "Cannot transition to 'done': bundle $bid (track '$track') is still '$bstatus'. Close or archive track bundles first."
          else
            sk_error "SK-TKT-024" "Cannot transition to 'done': bundle $bid is still '$bstatus'. Close or archive all bundles first."
          fi
        fi
      done
    fi

    # Gate check: if transitioning to 'active', verify at least one goal or bundle exists
    if [ "$to_stage" = "active" ]; then
      local has_bundles=false
      if [ -d "$TKT_ROOT/bundles" ]; then
        for d in "$TKT_ROOT/bundles"/B-*/; do
          [ -d "$d" ] || continue
          bundle_matches_track "$d" "$track" || continue
          has_bundles=true
          break
        done
      fi
      if [ "$has_bundles" = "false" ]; then
        if [ -n "$track" ]; then
          sk_error "SK-TKT-025" "Cannot transition to 'active': no bundles exist for track '$track'. Create at least one bundle first."
        else
          sk_error "SK-TKT-025" "Cannot transition to 'active': no bundles exist. Create at least one bundle first."
        fi
      fi
    fi
  fi

  # Apply transition
  sed -i "s/^stage: .*/stage: $to_stage/" "$rmfile"
  sed -i "s/^updated_at: .*/updated_at: \"$(timestamp)\"/" "$rmfile"

  local history_entry="  - stage: $to_stage\n    entered_at: \"$(timestamp)\"\n    reason: \"$reason\"\n    from: \"$current_stage\""
  sed -i "/^stage_history:/a\\$history_entry" "$rmfile"

  log_event "ROADMAP_TRANSITION" "from=$current_stage to=$to_stage reason=\"$reason\" track=${track:-all}"
  echo "{\"transition\":\"$current_stage→$to_stage\",\"new_stage\":\"$to_stage\",\"reason\":\"$reason\",\"track\":\"${track:-all}\",\"gated\":$([ \"$force\" = \"true\" ] && echo 'false' || echo 'true')}"
}

cmd_roadmap_status() {
  local rmfile="$TKT_ROOT/roadmap.yaml"
  [ ! -f "$rmfile" ] && sk_error "SK-TKT-018" "No roadmap.yaml found."

  local project stage updated
  project="$(yaml_field "$rmfile" "project")"
  stage="$(yaml_field "$rmfile" "stage")"
  updated="$(yaml_field "$rmfile" "updated_at")"

  # Count bundles by status
  local total=0 open=0 in_progress=0 closed=0
  if [ -d "$TKT_ROOT/bundles" ]; then
    for d in "$TKT_ROOT/bundles"/B-*/; do
      [ -d "$d" ] || continue
      total=$((total + 1))
      local bstatus
      bstatus="$(yaml_field "$d/bundle.yaml" "status")"
      case "$bstatus" in
        open) open=$((open + 1));;
        in_progress) in_progress=$((in_progress + 1));;
        closed) closed=$((closed + 1));;
      esac
    done
  fi

  # Determine allowed transitions
  local allowed=""
  local first_t=true
  for s in $VALID_ROADMAP_STAGES; do
    if roadmap_transition_allowed "$stage" "$s" 2>/dev/null; then
      $first_t || allowed+=","
      first_t=false
      allowed+="\"$s\""
    fi
  done

  echo "{\"project\":\"$project\",\"stage\":\"$stage\",\"updated_at\":\"$updated\",\"bundles\":{\"total\":$total,\"open\":$open,\"in_progress\":$in_progress,\"closed\":$closed},\"allowed_transitions\":[$allowed]}"
}

# --- Main ---

case "${1:-help}" in
  init-roadmap)        shift; cmd_init_roadmap "$@";;
  create-bundle)       shift; cmd_create_bundle "$@";;
  claim)               shift; cmd_claim "$@";;
  update)              shift; cmd_update "$@";;
  add)                 shift; cmd_add "$@";;
  express)             shift; cmd_express "$@";;
  status)              shift; cmd_status "$@";;
  close)               shift; cmd_close "$@";;
  list)                shift; cmd_list "$@";;
  roadmap-transition)  shift; cmd_roadmap_transition "$@";;
  roadmap-status)      shift; cmd_roadmap_status "$@";;
  *)
    sk_error "SK-CLI-002" "Unknown command: ${1:-}. Usage: tkt.sh <init-roadmap|create-bundle|claim|update|add|express|status|close|list|roadmap-transition|roadmap-status>"
    ;;
esac
