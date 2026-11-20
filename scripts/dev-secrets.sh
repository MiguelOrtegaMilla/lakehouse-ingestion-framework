#!/usr/bin/env bash
# Generates the local development secrets used by docker/docker-compose.yml: one random
# password per PostgreSQL role. Idempotent: existing files are kept. Never committed.
set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/docker/secrets"
roles=(
  pg_superuser ingest_migrator ingest_control ingest_coordination ingest_maintenance
  sales_owner sales_ro
)

umask 077
mkdir -p "$dir"
# Other host users cannot reach the files: the directory is private to the owner.
chmod 700 "$dir"

for role in "${roles[@]}"; do
  file="$dir/${role}_password"
  if [[ ! -s "$file" ]]; then
    # 128 random bits as 32 hex characters: safe in JDBC URLs, env vars and SQL literals.
    od -An -tx1 -N16 /dev/urandom | tr -d ' \n' > "$file"
    echo "created ${file#"$dir"/}"
  fi
  # The bind-mounted file must be readable by the container's postgres user (uid 999).
  # Compose without Swarm ignores uid/gid/mode on secrets, and the 0700 directory above
  # still keeps other host users out.
  chmod 644 "$file"
done
