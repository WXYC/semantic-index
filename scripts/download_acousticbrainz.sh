#!/bin/bash
#
# Download AcousticBrainz high-level feature dumps from MetaBrainz.
#
# The dumps are split into 30 shards (~1-2GB each, ~37GB total).
# Files are compressed with zstd (.tar.zst).
#
# Downloads up to 4 shards in parallel. Handles interrupted downloads
# with curl -C - resume and validates against sha256sums.
#
# Usage:
#   ./scripts/download_acousticbrainz.sh /Volumes/Peak\ Twins/acousticbrainz
#   PARALLEL=8 ./scripts/download_acousticbrainz.sh /path/to/dest
#
# Requires: curl, zstd (brew install zstd), shasum
#
set -euo pipefail

DEST="${1:?Usage: $0 <destination_dir>}"
BASE_URL="https://data.metabrainz.org/pub/musicbrainz/acousticbrainz/dumps/acousticbrainz-highlevel-json-20220623"
SHARD_COUNT=30
MAX_RETRIES=3
PARALLEL="${PARALLEL:-4}"

mkdir -p "$DEST"

echo "Downloading AcousticBrainz high-level features to $DEST"
echo "  $SHARD_COUNT shards, ~37GB total, $PARALLEL parallel downloads"
echo ""

# Download checksums first so we can validate
CHECKSUMS_FILE="${DEST}/sha256sums"
if [ ! -f "$CHECKSUMS_FILE" ]; then
    echo "Downloading checksums..."
    curl -L -o "$CHECKSUMS_FILE" "${BASE_URL}/sha256sums" --progress-bar
fi

# Export variables for the subshell
export DEST BASE_URL CHECKSUMS_FILE MAX_RETRIES

download_shard() {
    local i="$1"
    local FILE="acousticbrainz-highlevel-json-20220623-${i}.tar.zst"
    local URL="${BASE_URL}/${FILE}"
    local DEST_FILE="${DEST}/${FILE}"

    # Check if file exists and passes checksum
    if [ -f "$DEST_FILE" ]; then
        local EXPECTED
        EXPECTED=$(grep "$FILE" "$CHECKSUMS_FILE" 2>/dev/null | awk '{print $1}' || true)
        if [ -n "$EXPECTED" ]; then
            local ACTUAL
            ACTUAL=$(shasum -a 256 "$DEST_FILE" | awk '{print $1}')
            if [ "$ACTUAL" = "$EXPECTED" ]; then
                echo "  Shard $i: ✓ verified, skipping"
                return 0
            else
                echo "  Shard $i: checksum mismatch, resuming download..."
            fi
        else
            echo "  Shard $i: no checksum available, skipping (file exists)"
            return 0
        fi
    fi

    # Download with resume support and retries
    for attempt in $(seq 1 "$MAX_RETRIES"); do
        echo "  Shard $i: downloading (attempt $attempt/$MAX_RETRIES)..."
        if curl -L -C - -o "$DEST_FILE" "$URL" --silent --show-error --retry 3 --retry-delay 5; then
            # Verify checksum after download
            local EXPECTED
            EXPECTED=$(grep "$FILE" "$CHECKSUMS_FILE" 2>/dev/null | awk '{print $1}' || true)
            if [ -n "$EXPECTED" ]; then
                local ACTUAL
                ACTUAL=$(shasum -a 256 "$DEST_FILE" | awk '{print $1}')
                if [ "$ACTUAL" = "$EXPECTED" ]; then
                    echo "  Shard $i: ✓ download verified"
                    return 0
                else
                    echo "  Shard $i: ✗ checksum mismatch after download"
                    rm -f "$DEST_FILE"
                    if [ "$attempt" -eq "$MAX_RETRIES" ]; then
                        echo "  Shard $i: FAILED after $MAX_RETRIES attempts"
                        return 1
                    fi
                fi
            else
                echo "  Shard $i: downloaded (no checksum to verify)"
                return 0
            fi
        else
            echo "  Shard $i: curl failed (attempt $attempt)"
            if [ "$attempt" -eq "$MAX_RETRIES" ]; then
                echo "  Shard $i: FAILED after $MAX_RETRIES attempts"
                return 1
            fi
        fi
    done
}
export -f download_shard

# Download shards in parallel
seq 0 $((SHARD_COUNT - 1)) | xargs -P "$PARALLEL" -I {} bash -c 'download_shard "$@"' _ {}

echo ""
echo "Done. To extract a shard:"
echo "  zstd -d shard.tar.zst && tar xf shard.tar"
