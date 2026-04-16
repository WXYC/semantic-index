#!/bin/sh
# Entrypoint for the Railway cron service.
# Runs the nightly sync pipeline against Backend-Service PG
# and atomically swaps the production SQLite database.
#
# Required env vars:
#   DATABASE_URL_BACKEND  — PostgreSQL DSN for Backend-Service
#   DB_PATH               — Path to production SQLite (default: /data/wxyc_artist_graph.db)

set -e

DB_PATH="${DB_PATH:-/data/wxyc_artist_graph.db}"

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Starting nightly sync..."
echo "  DB_PATH=$DB_PATH"

if [ -z "$DATABASE_URL_BACKEND" ]; then
    echo "ERROR: DATABASE_URL_BACKEND not set"
    exit 1
fi

exec python -m semantic_index.nightly_sync \
    --db-path "$DB_PATH" \
    --dsn "$DATABASE_URL_BACKEND" \
    --verbose
