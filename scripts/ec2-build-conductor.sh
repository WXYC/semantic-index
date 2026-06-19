#!/usr/bin/env bash
#
# Nightly conductor for the out-of-process graph rebuild (WXYC/semantic-index#347).
# Runs on the Backend-Service EC2 serving host (installed via the systemd units
# in deploy/). It is the single driver of the round-trip:
#
#   1. snapshot the live production DB and capture its enrichment row counts
#   2. upload the snapshot to S3 as the build's SEED
#   3. launch the Fargate build task (aws ecs run-task) and wait for it
#   4. download the build artifact; validate it (fail-closed gate)
#   5. atomically swap it into the live serving path (no API restart)
#
# All heavy work (the ~4 GiB rebuild) happens in Fargate; the conductor only does
# light, low-memory I/O. Every step is logged; any failure leaves the currently
# serving DB untouched and exits non-zero.
#
# Requires (on the host): aws CLI + instance-profile creds (see infra/README.md
# step 3), docker, and the semantic-index image locally (the serving image).
#
# Config via environment (systemd unit sets these; defaults below):
#   DATA_DIR        /home/ec2-user/semantic-index-data
#   DB_NAME         wxyc_artist_graph.db
#   IMAGE           semantic-index image ref used for sqlite backup + validation
#   BUCKET          wxyc-semantic-index-build
#   CLUSTER         semantic-index-build
#   TASK_DEF        semantic-index-build
#   SUBNETS         comma-separated subnet ids (public, BS VPC)
#   BUILD_SG        BuildSecurityGroup id
#   AWS_REGION      us-east-1
#   MIN_ARTISTS     artist-count floor for validation (default 1000)

set -euo pipefail

DATA_DIR="${DATA_DIR:-/home/ec2-user/semantic-index-data}"
DB_NAME="${DB_NAME:-wxyc_artist_graph.db}"
IMAGE="${IMAGE:-semantic-index:latest}"
BUCKET="${BUCKET:-wxyc-semantic-index-build}"
CLUSTER="${CLUSTER:-semantic-index-build}"
TASK_DEF="${TASK_DEF:-semantic-index-build}"
AWS_REGION="${AWS_REGION:-us-east-1}"
MIN_ARTISTS="${MIN_ARTISTS:-1000}"
# Ceiling for waiting on the Fargate build. Must exceed the build's worst case
# (graph_metrics is unmeasured under load) but stay under the systemd unit's
# TimeoutStartSec. NOT `aws ecs wait tasks-stopped`, whose botocore waiter is
# fixed at 100x6s = 600s (10 min) and would abort a longer build without
# swapping — the unit budgets 40+ min precisely because runs exceed 10 min.
BUILD_WAIT_CEILING_SECS="${BUILD_WAIT_CEILING_SECS:-2700}"
BUILD_POLL_INTERVAL_SECS="${BUILD_POLL_INTERVAL_SECS:-15}"
export AWS_REGION

PROD_DB="$DATA_DIR/$DB_NAME"
SEED_DB="$DATA_DIR/${DB_NAME}.seed"
SEED_COUNTS="$DATA_DIR/${DB_NAME}.seed-counts.json"
INCOMING_DB="$DATA_DIR/${DB_NAME}.incoming"
SEED_KEY="seed/$DB_NAME"
BUILD_KEY="build/$DB_NAME"

log() { echo "[conductor $(date -u +%FT%TZ)] $*"; }
fail() { log "ERROR: $*"; exit 1; }

cleanup() { rm -f "$SEED_DB" "$SEED_COUNTS" "$INCOMING_DB" 2>/dev/null || true; }
trap cleanup EXIT

: "${SUBNETS:?set SUBNETS (comma-separated public subnet ids)}"
: "${BUILD_SG:?set BUILD_SG (BuildSecurityGroup id)}"

# Run a one-shot python in the semantic-index image against the mounted data dir.
in_image() { docker run --rm -v "$DATA_DIR:/data" "$IMAGE" "$@"; }

# --- 1. Consistent snapshot of the live DB + capture enrichment baseline ------
[[ -f "$PROD_DB" ]] || fail "production DB not found: $PROD_DB"
log "Snapshotting live DB -> $SEED_DB (sqlite .backup, consistent under concurrent reads)"
in_image python -c "import sqlite3,sys; src=sqlite3.connect('/data/$DB_NAME'); dst=sqlite3.connect('/data/${DB_NAME}.seed'); src.backup(dst); dst.close(); src.close()" \
  || fail "snapshot failed"

log "Capturing seed enrichment counts -> $SEED_COUNTS"
in_image python scripts/validate_graph_db.py "/data/${DB_NAME}.seed" --emit-counts > "$SEED_COUNTS" \
  || fail "seed count capture failed"
log "seed counts: $(cat "$SEED_COUNTS")"

# --- 2. Upload seed ----------------------------------------------------------
log "Uploading seed -> s3://$BUCKET/$SEED_KEY"
aws s3 cp "$SEED_DB" "s3://$BUCKET/$SEED_KEY" --only-show-errors || fail "seed upload failed"

# --- 3. Launch the Fargate build and wait ------------------------------------
log "Launching Fargate build task..."
TASK_ARN="$(aws ecs run-task \
  --cluster "$CLUSTER" \
  --task-definition "$TASK_DEF" \
  --launch-type FARGATE \
  --count 1 \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$BUILD_SG],assignPublicIp=ENABLED}" \
  --query 'tasks[0].taskArn' --output text)"
[[ -n "$TASK_ARN" && "$TASK_ARN" != "None" ]] || fail "run-task did not return a task ARN"
log "task: $TASK_ARN — polling for completion (ceiling ${BUILD_WAIT_CEILING_SECS}s)..."

# Poll describe-tasks until STOPPED with a ceiling matching the build's real
# runtime (see BUILD_WAIT_CEILING_SECS above). A single `aws ecs wait
# tasks-stopped` would cap at 10 min and abort the build mid-flight.
elapsed=0
while :; do
  # Tolerate a transient describe-tasks failure (API throttle/blip) over the long
  # poll window: treat it as "not STOPPED yet" and retry on the next tick rather
  # than letting set -e abort the whole night's rebuild on one failed call. A
  # persistent failure still trips the ceiling below and fails closed.
  STATUS="$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$TASK_ARN" \
    --query 'tasks[0].lastStatus' --output text 2>/dev/null)" || STATUS="DESCRIBE_FAILED"
  [[ "$STATUS" == "STOPPED" ]] && break
  if (( elapsed >= BUILD_WAIT_CEILING_SECS )); then
    fail "build task still '$STATUS' after ${BUILD_WAIT_CEILING_SECS}s ($TASK_ARN) — NOT swapping"
  fi
  sleep "$BUILD_POLL_INTERVAL_SECS"
  elapsed=$(( elapsed + BUILD_POLL_INTERVAL_SECS ))
done

# Fetch exit code + reason once STOPPED. Capture into a var first (with set -e
# tolerance) so a describe failure surfaces as a clear 'fail' instead of a silent
# EOF-abort of `read`. Empty/None exitCode -> the guard below fails closed.
DESC="$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$TASK_ARN" \
  --query 'tasks[0].[containers[0].exitCode, stoppedReason]' --output text 2>/dev/null)" || DESC=""
read -r EXIT_CODE STOP_REASON <<<"$DESC" || true
log "task stopped: exitCode=${EXIT_CODE:-<none>} reason=${STOP_REASON:-<none>}"
[[ "$EXIT_CODE" == "0" ]] || fail "build task exit '${EXIT_CODE:-<none>}' (${STOP_REASON:-no reason}) — NOT swapping"

# --- 4. Download + validate (fail-closed) ------------------------------------
log "Downloading build artifact -> $INCOMING_DB"
aws s3 cp "s3://$BUCKET/$BUILD_KEY" "$INCOMING_DB" --only-show-errors || fail "build download failed"

log "Validating build artifact (header + artist>=$MIN_ARTISTS + enrichment vs seed)..."
in_image python scripts/validate_graph_db.py "/data/${DB_NAME}.incoming" \
  --seed-counts "/data/${DB_NAME}.seed-counts.json" \
  --min-artists "$MIN_ARTISTS" \
  || fail "validation failed — keeping current DB, NOT swapping"

# --- 5. Atomic swap (same filesystem) ----------------------------------------
# Clear the prior generation's WAL/SHM so a stale journal can't shadow the new
# inode, then rename (atomic on the same FS). The API opens read-only per request
# from app.state.db_path, so the next request picks up the new inode — no restart.
log "Swapping in the new DB (clearing stale -wal/-shm, then rename)"
rm -f "${PROD_DB}-wal" "${PROD_DB}-shm"
mv -f "$INCOMING_DB" "$PROD_DB" || fail "atomic swap failed"

# Best-effort freshness signal (seeds the 'DB mtime > 36h' alarm follow-up).
aws cloudwatch put-metric-data --namespace WXYC/SemanticIndex \
  --metric-name GraphRebuildSuccess --value 1 --unit Count 2>/dev/null || true

log "Rebuild complete. $PROD_DB mtime: $(date -u -r "$PROD_DB" +%FT%TZ 2>/dev/null || stat -c %y "$PROD_DB")"
