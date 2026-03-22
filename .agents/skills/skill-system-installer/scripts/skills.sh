#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILLS_GLOBAL_HOME="${SKILLS_GLOBAL_HOME:-$HOME/.skills-system}"
SKILLS_LOCAL_DIR="./skills"

usage() {
  cat <<'EOF'
Usage: scripts/skills.sh <subcommand> [options]

Subcommands:
  install     Install skill(s) from GitHub into local or global scope
  list        List curated skills, then local/global installed skills
  update      Check or update installed skills against selected lockfile
  sync        Sync global skills into local workspace
  bootstrap   First-run setup: detect, scaffold, compute hashes, validate

Scope flags:
  --local         Use local scope (default)
  --global, -g    Use global scope

sync options:
  --strategy copy|symlink  Sync strategy (default: copy)
  --force                  Overwrite existing local skills
  skill1,skill2,...        Optional comma-separated skill names
EOF
}

SUBCOMMAND="${1:-}"
if [[ -z "$SUBCOMMAND" ]]; then
  usage
  exit 1
fi
if [[ "$SUBCOMMAND" == "--help" || "$SUBCOMMAND" == "-h" ]]; then
  usage
  exit 0
fi
shift

SCOPE="local"
PASSTHROUGH=()
while (($# > 0)); do
  case "$1" in
    --global|-g)
      SCOPE="global"
      ;;
    --local)
      SCOPE="local"
      ;;
    *)
      PASSTHROUGH+=("$1")
      ;;
  esac
  shift
done

if [[ "$SCOPE" == "global" ]]; then
  DEST_DIR="$SKILLS_GLOBAL_HOME/skills"
  LOCKFILE="$SKILLS_GLOBAL_HOME/skills-lock.json"
else
  DEST_DIR="$SKILLS_LOCAL_DIR"
  LOCKFILE="./skills-lock.json"
fi

GLOBAL_SKILLS_DIR="$SKILLS_GLOBAL_HOME/skills"
GLOBAL_LOCKFILE="$SKILLS_GLOBAL_HOME/skills-lock.json"
LOCAL_SKILLS_DIR="$SKILLS_LOCAL_DIR"
LOCAL_LOCKFILE="./skills-lock.json"

print_installed() {
  local label="$1"
  local directory="$2"
  echo "$label:"
  if [[ ! -d "$directory" ]]; then
    echo "  (none)"
    return
  fi
  local found=0
  shopt -s nullglob
  local entry
  for entry in "$directory"/*; do
    if [[ -d "$entry" || -L "$entry" ]]; then
      echo "  - $(basename "$entry")"
      found=1
    fi
  done
  shopt -u nullglob
  if [[ "$found" -eq 0 ]]; then
    echo "  (none)"
  fi
}

sync_lockfile_entries() {
  local skills_csv="$1"
  python3 - "$GLOBAL_LOCKFILE" "$LOCAL_LOCKFILE" "$skills_csv" <<'PY'
import json
import os
import sys

global_lockfile, local_lockfile, skills_csv = sys.argv[1:4]
if not os.path.isfile(global_lockfile):
    raise SystemExit(f"Error: global lockfile not found: {global_lockfile}")
with open(global_lockfile, "r", encoding="utf-8") as file_handle:
    global_payload = json.load(file_handle)
if not isinstance(global_payload, dict) or not isinstance(global_payload.get("skills"), dict):
    raise SystemExit("Error: global lockfile must contain a top-level 'skills' object")

if os.path.isfile(local_lockfile):
    with open(local_lockfile, "r", encoding="utf-8") as file_handle:
        local_payload = json.load(file_handle)
else:
    local_payload = {"version": global_payload.get("version", 1), "skills": {}}

if not isinstance(local_payload, dict):
    local_payload = {"version": global_payload.get("version", 1), "skills": {}}
if "version" not in local_payload:
    local_payload["version"] = global_payload.get("version", 1)
if not isinstance(local_payload.get("skills"), dict):
    local_payload["skills"] = {}

for skill_name in [name for name in skills_csv.split(",") if name]:
    if skill_name in global_payload["skills"]:
        local_payload["skills"][skill_name] = global_payload["skills"][skill_name]

with open(local_lockfile, "w", encoding="utf-8") as file_handle:
    json.dump(local_payload, file_handle, indent=2)
    file_handle.write("\n")
PY
}

case "$SUBCOMMAND" in
  install)
    mkdir -p "$DEST_DIR"
    python3 "$SCRIPT_DIR/install-skill-from-github.py" "${PASSTHROUGH[@]}" --dest "$DEST_DIR"
    ;;
  list)
    python3 "$SCRIPT_DIR/list-curated-skills.py" "${PASSTHROUGH[@]}"
    print_installed "Local installed skills" "$LOCAL_SKILLS_DIR"
    print_installed "Global installed skills" "$GLOBAL_SKILLS_DIR"
    ;;
  update)
    mkdir -p "$DEST_DIR"
    if [[ "$SCOPE" == "global" ]]; then
      mkdir -p "$SKILLS_GLOBAL_HOME"
    fi
    python3 "$SCRIPT_DIR/update-skills.py" "${PASSTHROUGH[@]}" --skills-dir "$DEST_DIR" --lockfile "$LOCKFILE"
    ;;
  sync)
    strategy="copy"
    force="false"
    requested_skills=()
    index=0
    while ((index < ${#PASSTHROUGH[@]})); do
      token="${PASSTHROUGH[$index]}"
      case "$token" in
        --strategy)
          index=$((index + 1))
          if ((index >= ${#PASSTHROUGH[@]})); then
            echo "Error: --strategy requires a value" >&2
            exit 1
          fi
          strategy="${PASSTHROUGH[$index]}"
          ;;
        --strategy=*)
          strategy="${token#--strategy=}"
          ;;
        --force)
          force="true"
          ;;
        --help|-h)
          usage
          exit 0
          ;;
        *)
          requested_skills+=("$token")
          ;;
      esac
      index=$((index + 1))
    done

    if [[ "$strategy" != "copy" && "$strategy" != "symlink" ]]; then
      echo "Error: --strategy must be copy or symlink" >&2
      exit 1
    fi

    if [[ ! -d "$GLOBAL_SKILLS_DIR" ]]; then
      echo "Error: global skills directory not found: $GLOBAL_SKILLS_DIR" >&2
      exit 1
    fi

    mkdir -p "$LOCAL_SKILLS_DIR"

    target_skills=()
    if [[ ${#requested_skills[@]} -eq 0 ]]; then
      shopt -s nullglob
      for global_path in "$GLOBAL_SKILLS_DIR"/*; do
        if [[ -d "$global_path" ]]; then
          target_skills+=("$(basename "$global_path")")
        fi
      done
      shopt -u nullglob
    else
      for item in "${requested_skills[@]}"; do
        IFS=',' read -r -a split_names <<< "$item"
        for skill_name in "${split_names[@]}"; do
          if [[ -n "$skill_name" ]]; then
            target_skills+=("$skill_name")
          fi
        done
      done
    fi

    if [[ ${#target_skills[@]} -eq 0 ]]; then
      echo "No skills to sync."
      sync_lockfile_entries ""
      exit 0
    fi

    synced_lockfile_skills=()
    for skill_name in "${target_skills[@]}"; do
      src_path="$GLOBAL_SKILLS_DIR/$skill_name"
      dest_path="$LOCAL_SKILLS_DIR/$skill_name"

      if [[ ! -d "$src_path" ]]; then
        echo "Skipping $skill_name (not found in global scope)"
        continue
      fi

      synced_lockfile_skills+=("$skill_name")

      if [[ -e "$dest_path" || -L "$dest_path" ]]; then
        if [[ "$force" == "true" ]]; then
          rm -rf "$dest_path"
        else
          echo "Skipping $skill_name (already exists locally)"
          continue
        fi
      fi

      if [[ "$strategy" == "copy" ]]; then
        cp -R "$src_path" "$dest_path"
        echo "Synced $skill_name (copy)"
      else
        rel_src=$(python3 - "$src_path" "$LOCAL_SKILLS_DIR" <<'PY'
import os
import sys

print(os.path.relpath(sys.argv[1], sys.argv[2]))
PY
)
        ln -s "$rel_src" "$dest_path"
        echo "Synced $skill_name (symlink)"
      fi
    done

    skills_csv=""
    for skill_name in "${synced_lockfile_skills[@]}"; do
      if [[ -z "$skills_csv" ]]; then
        skills_csv="$skill_name"
      else
        skills_csv="$skills_csv,$skill_name"
      fi
    done
    sync_lockfile_entries "$skills_csv"
    echo "Updated local lockfile: $LOCAL_LOCKFILE"
    ;;
  bootstrap)
    # === First-run bootstrap: detect, scaffold, compute hashes, validate ===
    echo '{"phase":"start","message":"Starting skill system bootstrap..."}'

    # 1. Detect project root (parent of skills/)
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
    LOCK="$PROJECT_ROOT/skills-lock.json"
    SK_DIR="$PROJECT_ROOT/skills"
    CONFIG="$PROJECT_ROOT/config"
    NOTE="$PROJECT_ROOT/note"
    TKT="$PROJECT_ROOT/.tkt"

    checks=()
    actions=()

    # 2. Check config/
    if [[ -d "$CONFIG" ]]; then
      checks+='"config": "present"'
      # Check for missing config files
      missing_cfg=()
      for f in tkt.yaml insight.yaml router.yaml workflow.yaml; do
        [[ -f "$CONFIG/$f" ]] || missing_cfg+=("$f")
      done
      if [[ ${#missing_cfg[@]} -gt 0 ]]; then
        checks+='"config_missing": ["'"$(IFS=','; echo "${missing_cfg[*]}" | sed 's/,/","/g')"'"]'
      fi
    else
      checks+='"config": "missing"'
      actions+='"scaffold_config"'
      mkdir -p "$CONFIG" "$CONFIG/local"
      echo '*' > "$CONFIG/local/.gitignore"
    fi

    # 3. Check note/
    if [[ -d "$NOTE" ]]; then
      checks+='"note": "present"'
    else
      checks+='"note": "missing"'
      actions+='"scaffold_note"'
      mkdir -p "$NOTE"
      # Create stub files
      for f in note_rules.md note_tasks.md note_feedback.md; do
        if [[ ! -f "$NOTE/$f" ]]; then
          echo "# ${f%.md}" > "$NOTE/$f"
        fi
      done
    fi

    # 4. Check .tkt/
    if [[ -d "$TKT" ]]; then
      checks+='"tkt": "present"'
    else
      checks+='"tkt": "missing"'
      actions+='"scaffold_tkt"'
    fi

    # 5. Check skills-lock.json exists
    if [[ -f "$LOCK" ]]; then
      checks+='"lockfile": "present"'
    else
      checks+='"lockfile": "missing"'
      actions+='"create_lockfile"'
      # Generate lockfile from installed skills
      python3 - "$SK_DIR" "$LOCK" <<'PY'
import hashlib, json, os, sys

skills_dir, lockfile_path = sys.argv[1:3]
skills = {}
if os.path.isdir(skills_dir):
    for entry in sorted(os.listdir(skills_dir)):
        skill_path = os.path.join(skills_dir, entry)
        if not os.path.isdir(skill_path):
            continue
        skill_md = os.path.join(skill_path, "SKILL.md")
        if not os.path.isfile(skill_md):
            continue
        # Compute hash
        digest = hashlib.sha256()
        files = []
        for root, _, fnames in os.walk(skill_path):
            for fname in fnames:
                files.append(os.path.join(root, fname))
        for fp in sorted(files, key=lambda v: os.path.relpath(v, skill_path)):
            with open(fp, "rb") as fh:
                while True:
                    chunk = fh.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
                digest.update(b"\0")
        skills[entry] = {
            "source": "local",
            "sourceType": "local",
            "computedHash": digest.hexdigest()
        }

payload = {"version": 1, "skills": skills}
with open(lockfile_path, "w") as fh:
    json.dump(payload, fh, indent=2)
    fh.write("\n")
print(f"Created lockfile with {len(skills)} skills")
PY
    fi

    # 6. Compute hashes for any "pending" entries in lockfile
    if [[ -f "$LOCK" ]]; then
      pending_count=$(python3 - "$SK_DIR" "$LOCK" <<'PY'
import hashlib, json, os, sys

skills_dir, lockfile_path = sys.argv[1:3]
with open(lockfile_path, "r") as fh:
    payload = json.load(fh)

changed = 0
skills = payload.get("skills", {})
for name, entry in skills.items():
    h = entry.get("computedHash", "")
    if h and h != "pending":
        continue
    skill_path = os.path.join(skills_dir, name)
    if not os.path.isdir(skill_path):
        continue
    digest = hashlib.sha256()
    files = []
    for root, _, fnames in os.walk(skill_path):
        for fname in fnames:
            files.append(os.path.join(root, fname))
    for fp in sorted(files, key=lambda v: os.path.relpath(v, skill_path)):
        with open(fp, "rb") as fh2:
            while True:
                chunk = fh2.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
            digest.update(b"\0")
    entry["computedHash"] = digest.hexdigest()
    changed += 1

if changed:
    with open(lockfile_path, "w") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")
print(changed)
PY
      )
      if [[ "$pending_count" -gt 0 ]]; then
        actions+='"computed_pending_hashes"'
        checks+='"pending_hashes_fixed": '$pending_count''
      fi
    fi

    # 7. Validate all installed skills have SKILL.md
    invalid_skills=()
    if [[ -d "$SK_DIR" ]]; then
      for d in "$SK_DIR"/*/; do
        [[ -d "$d" ]] || continue
        [[ -f "$d/SKILL.md" ]] || invalid_skills+=("$(basename "$d")")
      done
    fi
    if [[ ${#invalid_skills[@]} -gt 0 ]]; then
      checks+='"invalid_skills": ["'"$(IFS=','; echo "${invalid_skills[*]}" | sed 's/,/","/g')"'"]'
    fi

    # 8. Count installed skills
    skill_count=0
    if [[ -d "$SK_DIR" ]]; then
      for d in "$SK_DIR"/*/; do
        [[ -d "$d" && -f "$d/SKILL.md" ]] && skill_count=$((skill_count + 1))
      done
    fi
    checks+='"installed_skills": '$skill_count''

    # 9. Check dependencies (python3, git, bash)
    deps_ok=true
    for cmd in python3 git bash; do
      if ! command -v "$cmd" &>/dev/null; then
        checks+='"missing_dep": "'"$cmd"'"'
        deps_ok=false
      fi
    done
    [[ "$deps_ok" == true ]] && checks+='"dependencies": "ok"'

    # Build JSON report
    checks_json="{"
    first=true
    for c in "${checks[@]}"; do
      $first || checks_json+=","
      first=false
      checks_json+="$c"
    done
    checks_json+="}"

    actions_json="["
    first=true
    for a in "${actions[@]}"; do
      $first || actions_json+=","
      first=false
      actions_json+="$a"
    done
    actions_json+="]"

    echo "{\"status\":\"ok\",\"phase\":\"complete\",\"checks\":$checks_json,\"actions_taken\":$actions_json}"
    ;;
  *)
    usage
    exit 1
    ;;
esac
