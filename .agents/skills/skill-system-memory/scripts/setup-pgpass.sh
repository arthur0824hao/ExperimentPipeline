#!/usr/bin/env bash
set -euo pipefail

interactive="false"
if [ -t 0 ] && [ -t 1 ]; then
  interactive="true"
fi

PGHOST_IN="${PGHOST:-localhost}"
PGPORT_IN="${PGPORT:-5432}"
PGDATABASE_IN="${SKILL_PGDATABASE:-${PGDATABASE:-agent_memory}}"
_default_user="${PGUSER:-$(whoami)}"
PGUSER_IN="$_default_user"

if [ "$interactive" = "true" ]; then
  echo "PGHOST (default: $PGHOST_IN): "
  read -r input || true
  PGHOST_IN="${input:-$PGHOST_IN}"

  echo "PGPORT (default: $PGPORT_IN): "
  read -r input || true
  PGPORT_IN="${input:-$PGPORT_IN}"

  echo "PGDATABASE (default: $PGDATABASE_IN): "
  read -r input || true
  PGDATABASE_IN="${input:-$PGDATABASE_IN}"

  echo "PGUSER (default: $_default_user): "
  read -r input || true
  PGUSER_IN="${input:-$_default_user}"
fi

if [ -n "${PGPASSWORD:-}" ]; then
  PGPASS_IN="$PGPASSWORD"
elif [ "$interactive" = "true" ]; then
  echo -n "Password: "
  stty -echo
  read -r PGPASS_IN
  stty echo
  echo
else
  echo "Skipping .pgpass setup in non-interactive mode because PGPASSWORD is not set."
  exit 0
fi

if [ -z "$PGPASS_IN" ]; then
  echo "Password cannot be empty" >&2
  exit 1
fi

pgpass_path="$HOME/.pgpass"
esc_pass="${PGPASS_IN//\\/\\\\}"
esc_pass="${esc_pass//:/\\:}"

printf '%s:%s:%s:%s:%s\n' "$PGHOST_IN" "$PGPORT_IN" "$PGDATABASE_IN" "$PGUSER_IN" "$esc_pass" > "$pgpass_path"
chmod 0600 "$pgpass_path"

echo "Wrote $pgpass_path (mode 0600)"
