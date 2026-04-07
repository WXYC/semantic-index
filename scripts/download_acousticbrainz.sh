#!/bin/bash
#
# Download AcousticBrainz high-level feature dumps from MetaBrainz.
#
# The dumps are split into 30 shards (~1-2GB each, ~37GB total).
# Files are compressed with zstd (.tar.zst).
#
# Handles interrupted downloads: uses curl -C - to resume partial files,
# and validates completed downloads against sha256sums before skipping.
#
# Usage:
#   ./scripts/download_acousticbrainz.sh /Volumes/Peak\ Twins/acousticbrainz
#
# Requires: curl, zstd (brew install zstd), shasum
#
set -euo pipefail

DEST="${1:?Usage: $0 <destination_dir>}"
BASE_URL="https://data.metabrainz.org/pub/musicbrainz/acousticbrainz/dumps/acousticbrainz-highlevel-json-20220623"
SHARD_COUNT=30
MAX_RETRIES=3

mkdir -p "$DEST"

echo "Downloading AcousticBrainz high-level features to $DEST"
echo "  $SHARD_COUNT shards, ~37GB total"
echo ""

# Download checksums first so we can validate
CHECKSUMS_FILE="${DEST}/sha256sums"
if [ ! -f "$CHECKSUMS_FILE" ]; then
    echo "Downloading checksums..."
    curl -L -o "$CHECKSUMS_FILE" "${BASE_URL}/sha256sums" --progress-bar
fi

for i in $(seq 0 $((SHARD_COUNT - 1))); do
    FILE="acousticbrainz-highlevel-json-20220623-${i}.tar.zst"
    URL="${BASE_URL}/${FILE}"
    DEST_FILE="${DEST}/${FILE}"

    # Check if file exists and passes checksum
    if [ -f "$DEST_FILE" ]; then
        EXPECTED=$(grep "$FILE" "$CHECKSUMS_FILE" 2>/dev/null | awk '{print $1}' || true)
        if [ -n "$EXPECTED" ]; then
            ACTUAL=$(shasum -a 256 "$DEST_FILE" | awk '{print $1}')
            if [ "$ACTUAL" = "$EXPECTED" ]; then
                echo "  Shard $i: ✓ verified, skipping"
                continue
            else
                echo "  Shard $i: checksum mismatch, resuming download..."
            fi
        else
            echo "  Shard $i: no checksum available, skipping (file exists)"
            continue
        fi
    fi

    # Download with resume support and retries
    for attempt in $(seq 1 $MAX_RETRIES); do
        echo "  Shard $i: downloading (attempt $attempt/$MAX_RETRIES)..."
        if curl -L -C - -o "$DEST_FILE" "$URL" --progress-bar --retry 3 --retry-delay 5; then
            # Verify checksum after download
            EXPECTED=$(grep "$FILE" "$CHECKSUMS_FILE" 2>/dev/null | awk '{print $1}' || true)
            if [ -n "$EXPECTED" ]; then
                ACTUAL=$(shasum -a 256 "$DEST_FILE" | awk '{print $1}')
                if [ "$ACTUAL" = "$EXPECTED" ]; then
                    echo "  Shard $i: ✓ download verified"
                    break
                else
                    echo "  Shard $i: ✗ checksum mismatch after download"
                    rm -f "$DEST_FILE"
                    if [ "$attempt" -eq "$MAX_RETRIES" ]; then
                        echo "  Shard $i: FAILED after $MAX_RETRIES attempts"
                    fi
                fi
            else
                echo "  Shard $i: downloaded (no checksum to verify)"
                break
            fi
        else
            echo "  Shard $i: curl failed (attempt $attempt)"
            if [ "$attempt" -eq "$MAX_RETRIES" ]; then
                echo "  Shard $i: FAILED after $MAX_RETRIES attempts, continuing to next shard"
            fi
        fi
    done
done

echo ""
echo "Done. To extract a shard:"
echo "  zstd -d shard.tar.zst && tar xf shard.tar"
