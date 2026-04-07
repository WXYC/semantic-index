#!/bin/bash
# Deploy the semantic-index service to Railway.
#
# Handles the Git LFS limitation: Railway doesn't pull LFS files during
# builds, so the database must be uploaded to the volume separately.
#
# Usage:
#   ./scripts/deploy.sh              # deploy code only
#   ./scripts/deploy.sh --update-db  # deploy code + upload database to volume
#
# Prerequisites:
#   - railway CLI authenticated and linked to the project
#   - git LFS installed (for database pulls)

set -euo pipefail

SERVICE="semantic-index"
VOLUME_DB_PATH="/data/wxyc_artist_graph.db"
LOCAL_DB_PATH="data/wxyc_artist_graph.db"
LFS_URL="https://media.githubusercontent.com/media/WXYC/semantic-index/main/data/wxyc_artist_graph.db"

update_db=false
for arg in "$@"; do
    case "$arg" in
        --update-db) update_db=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

echo "Pushing to main..."
git push origin HEAD:main

echo "Waiting for Railway to pick up the deploy..."
sleep 5

if [ "$update_db" = true ]; then
    echo "Uploading database to Railway volume..."

    # Install curl in the container if needed, then download the database
    # from GitHub LFS directly to the volume (faster than piping through SSH)
    railway ssh -s "$SERVICE" -- sh -c "
        command -v curl >/dev/null 2>&1 || (apt-get update -qq && apt-get install -y -qq curl)
        echo 'Downloading database from GitHub LFS...'
        curl -L -o ${VOLUME_DB_PATH} '${LFS_URL}'
        echo 'Clearing stale caches...'
        rm -f ${VOLUME_DB_PATH}.bio-cache.db ${VOLUME_DB_PATH}-shm ${VOLUME_DB_PATH}-wal
        ls -lh ${VOLUME_DB_PATH}
        echo 'Database updated.'
    "

    echo "Redeploying service to pick up new database..."
    railway redeploy -s "$SERVICE" --yes
fi

echo "Verifying deployment..."
sleep 30

health=$(curl -sf "https://explore.wxyc.org/health" 2>/dev/null || echo '{"status":"unreachable"}')
echo "Health: $health"

echo "Done."
