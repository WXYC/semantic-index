#!/usr/bin/env bash
# Pull the [mem] profile from the latest nightly_sync run on production and
# summarize whether sync succeeded or was OOM-killed. Designed to be run by
# hand any time after ~09:10 UTC, or wired to an OS-level cron.
#
# Background: WXYC/semantic-index#329 -- the nightly sync was OOM-killed at
# 09:00 UTC every day until per-phase RSS instrumentation (PR #331) and a
# --memory-swap bump (PR #332) shipped. This script reads the resulting
# [mem]<phase> current=N peak=N MiB log lines to identify which phase
# allocates the most, so we can pick a targeted memory-reduction fix.
#
# Requirements: ssh access to wxyc-ec2 (works with the user's SSH config).

set -euo pipefail

SINCE="${1:-$(date -u -v-1H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u --date='1 hour ago' +%Y-%m-%dT%H:%M:%S)}"

echo "=== Container memory cap ==="
ssh wxyc-ec2 'docker inspect semantic-index --format "{{.HostConfig.Memory}} bytes RAM / {{.HostConfig.MemorySwap}} bytes total (RAM+swap)"' \
    | awk '{printf "%.2f GiB RAM / %.2f GiB total\n", $1/1073741824, $4/1073741824}'

echo ""
echo "=== [mem] profile since $SINCE UTC ==="
PROFILE=$(ssh wxyc-ec2 "docker logs semantic-index --since $SINCE 2>&1 | grep -E '\[mem\]|Pipeline complete|Atomic swap'") || true
echo "$PROFILE"

LINE_COUNT=$(echo "$PROFILE" | grep -c '\[mem\]' || true)
echo ""
echo "[mem] lines emitted: $LINE_COUNT"
if [ "$LINE_COUNT" = "12" ] && echo "$PROFILE" | grep -q "Atomic swap"; then
    echo "STATUS: sync completed successfully"
elif [ "$LINE_COUNT" = "0" ]; then
    echo "STATUS: no [mem] lines found -- sync may not have started, or container restarted recently. Check 'docker logs semantic-index --since $SINCE | head -50'"
else
    echo "STATUS: sync killed mid-pipeline after $LINE_COUNT phase markers"
fi

echo ""
echo "=== Production DB mtime ==="
ssh wxyc-ec2 'stat -c "%y  %s bytes" /home/ec2-user/semantic-index-data/wxyc_artist_graph.db'
echo "  (mtime today = swap succeeded; mtime stale = sync did not complete)"

echo ""
echo "=== Recent OOM kills (last 3) ==="
ssh wxyc-ec2 'sudo dmesg -T 2>/dev/null | grep -iE "killed process.*uvicorn" | tail -3' || \
    echo "  (need passwordless sudo for dmesg; skip)"

echo ""
echo "=== Peak phase ranking ==="
echo "$PROFILE" | grep '\[mem\]' \
    | awk -F'peak=' '{phase=$1; sub(/.*\[mem\] +/, "", phase); sub(/ +current.*/, "", phase); peak=$2; sub(/ +MiB.*/, "", peak); printf "  %s peak=%s MiB\n", phase, peak}' \
    | sort -k3 -n -r \
    | head -5

cat <<EOF

=== Next-step decision tree ===
  If "after graph_metrics" is the peak: file PR to swap NetworkX for igraph
    in semantic_index/graph_metrics.py (~5-10x memory reduction for Louvain /
    betweenness / PageRank).
  If "after _load_from_pg" is the peak: stream/chunk loaders in
    semantic_index/pg_source.py via server-side cursors.
  If "after dedup" is the peak: rewrite PipelineDB.deduplicate_by_qid() to
    use SQL UPDATE rather than materializing edges in Python.
  If no single phase dominant (peak grows steadily across many): split sync
    into a one-shot container so it gets the full host's memory.
  If sync succeeded and peak <= 1.5 GiB: swap bump is fine long-term; close
    #329 and just add a CloudWatch alarm for "DB mtime > 36h" as a backstop.
EOF
