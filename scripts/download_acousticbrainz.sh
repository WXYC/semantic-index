#!/bin/bash
#
# Download AcousticBrainz high-level feature dumps from MetaBrainz.
#
# The dumps are split into 30 shards (~1-2GB each, ~37GB total).
# Files are compressed with zstd (.tar.zst).
#
# Usage:
#   ./scripts/download_acousticbrainz.sh /Volumes/Peak\ Twins/acousticbrainz
#
# Requires: curl, zstd (brew install zstd)
#
set -euo pipefail

DEST="${1:?Usage: $0 <destination_dir>}"
BASE_URL="https://data.metabrainz.org/pub/musicbrainz/acousticbrainz/dumps/acousticbrainz-highlevel-json-20220623"
SHARD_COUNT=30

mkdir -p "$DEST"

echo "Downloading AcousticBrainz high-level features to $DEST"
echo "  $SHARD_COUNT shards, ~37GB total"
echo ""

for i in $(seq 0 $((SHARD_COUNT - 1))); do
    FILE="acousticbrainz-highlevel-json-20220623-${i}.tar.zst"
    URL="${BASE_URL}/${FILE}"
    DEST_FILE="${DEST}/${FILE}"

    if [ -f "$DEST_FILE" ]; then
        echo "  Shard $i: already downloaded, skipping"
        continue
    fi

    echo "  Shard $i: downloading..."
    curl -L -o "$DEST_FILE" "$URL" --progress-bar
done

# Download checksums
echo ""
echo "Downloading checksums..."
curl -L -o "${DEST}/sha256sums" "${BASE_URL}/sha256sums"

echo ""
echo "Done. Verify with:"
echo "  cd '$DEST' && sha256sum -c sha256sums"
echo ""
echo "To extract a shard:"
echo "  zstd -d shard.tar.zst && tar xf shard.tar"
