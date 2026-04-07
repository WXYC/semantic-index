#!/bin/sh
# Copy database to volume if not already present or if the bundled copy is newer.
# The volume at /data persists across deploys; the bundled copy in /app/data
# comes from the Docker build (may be an LFS pointer on Railway).

DB_VOL="/data/wxyc_artist_graph.db"
DB_BUNDLED="/app/data/wxyc_artist_graph.db"

# Check if volume DB exists and is a real SQLite file (not empty or pointer)
if [ -f "$DB_VOL" ] && [ "$(head -c 6 "$DB_VOL")" = "SQLite" ]; then
    echo "Using existing database on volume: $DB_VOL"
else
    # Check if bundled copy is a real SQLite file (not an LFS pointer)
    if [ -f "$DB_BUNDLED" ] && [ "$(head -c 6 "$DB_BUNDLED")" = "SQLite" ]; then
        echo "Copying bundled database to volume..."
        cp "$DB_BUNDLED" "$DB_VOL"
        echo "Done."
    else
        echo "WARNING: No valid database found. The bundled file may be a Git LFS pointer."
        echo "Upload the database to the volume at $DB_VOL manually."
    fi
fi

exec python -m semantic_index.api
