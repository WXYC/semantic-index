#!/usr/bin/env bash
#
# Nightly conductor for the out-of-process graph rebuild (WXYC/semantic-index#347).
# Runs on the Backend-Service EC2 serving host (installed via the systemd units
# in deploy/). It is the single driver of the round-trip:
#
#   0. take a non-blocking run lock (a manual run must not overlap the timer),
#      clear the conductor's own leftover artifacts from a run that never
#      reached its exit trap, then confirm free disk space before each step
#      that writes a DB-sized file locally
#   1. snapshot the live production DB and capture its enrichment row counts
#   2. upload the snapshot to S3 as the build's SEED, then free the local copy
#   3. launch the Fargate build task (aws ecs run-task) and wait for it
#   4. download the build artifact; validate it (fail-closed gate)
#   5. atomically swap it into the live serving path (no API restart)
#
# All heavy work (the ~4 GiB rebuild) happens in Fargate; the conductor only does
# light, low-memory I/O. Every step is logged; any failure leaves the currently
# serving DB untouched and exits non-zero.
#
# Disk-space and leftover-artifact checks (WXYC/semantic-index#385) live in
# scripts/conductor_preflight.py, unit-tested like validate_graph_db.py, rather
# than as inline bash — see that module's docstring for why the leftover-sweep
# is scoped narrowly (the data dir also holds permanent API cache sidecars in
# a naming shape that overlaps the conductor's own working files) and why a
# ".preprune-*" file is flagged rather than deleted. It runs under the HOST's
# own python3, never through in_image: in_image runs whatever `$IMAGE` tag
# happens to be on the host, which can be stale (WXYC/semantic-index#371) or,
# at 100% disk, can fail to even start before a check gets a chance to speak.
# It needs only the standard library, so no venv/install step is required —
# just the file itself, alongside this script (see the install instructions
# below and in infra/README.md).
#
# Requires (on the host): aws CLI + instance-profile creds (see infra/README.md
# step 3), docker + the semantic-index image locally (the serving image, used
# for the sqlite snapshot and validate_graph_db.py), flock (util-linux, present
# on the standard Amazon Linux image), and python3 (any 3.9+; stdlib only).
#
# Install on the host: copy BOTH scripts/ec2-build-conductor.sh AND
# scripts/conductor_preflight.py to the same directory (today, by hand --
# see deploy/semantic-index-build.service). This script resolves the preflight
# script's path relative to its own location ($PREFLIGHT_SCRIPT overrides).
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
#   MIN_MAPPED_ARTISTS  library-code-mapped-artist floor (default 10000; see #358)
#   PYTHON3         host python3 interpreter for conductor_preflight.py (default: python3)
#   PREFLIGHT_SCRIPT  path to conductor_preflight.py (default: alongside this script)
#   LOCK_FILE       run-lock path (default: $DATA_DIR/.semantic-index-build.lock --
#                   under DATA_DIR, not /run/lock, because /run/lock is root-owned
#                   and not writable by the ec2-user this service runs as)

set -euo pipefail

DATA_DIR="${DATA_DIR:-/home/ec2-user/semantic-index-data}"
DB_NAME="${DB_NAME:-wxyc_artist_graph.db}"
IMAGE="${IMAGE:-semantic-index:latest}"
BUCKET="${BUCKET:-wxyc-semantic-index-build}"
CLUSTER="${CLUSTER:-semantic-index-build}"
TASK_DEF="${TASK_DEF:-semantic-index-build}"
AWS_REGION="${AWS_REGION:-us-east-1}"
MIN_ARTISTS="${MIN_ARTISTS:-1000}"
# Absolute floor on graph artists mapped to a Backend library-code id
# (WXYC/semantic-index#358). Closes the bootstrap-night hole in the seed
# ratchet. On a transition night where the builder image predates the mapping
# code, this fail-closes and the conductor keeps serving the previous DB — the
# gate working as designed. Set to 0 to disable (e.g. a temporary rollback).
MIN_MAPPED_ARTISTS="${MIN_MAPPED_ARTISTS:-10000}"
# Ceiling for waiting on the Fargate build. Must exceed the build's worst case
# (graph_metrics is unmeasured under load) but stay under the systemd unit's
# TimeoutStartSec. NOT `aws ecs wait tasks-stopped`, whose botocore waiter is
# fixed at 100x6s = 600s (10 min) and would abort a longer build without
# swapping — the unit budgets 40+ min precisely because runs exceed 10 min.
BUILD_WAIT_CEILING_SECS="${BUILD_WAIT_CEILING_SECS:-2700}"
BUILD_POLL_INTERVAL_SECS="${BUILD_POLL_INTERVAL_SECS:-15}"
export AWS_REGION

# Resolve the preflight script relative to this script's own location, so the
# "copy both files to the same place" install convention (see header) just
# works without hardcoding a host path here.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON3="${PYTHON3:-python3}"
PREFLIGHT="${PREFLIGHT_SCRIPT:-$SCRIPT_DIR/conductor_preflight.py}"
LOCK_FILE="${LOCK_FILE:-$DATA_DIR/.semantic-index-build.lock}"

PROD_DB="$DATA_DIR/$DB_NAME"
SEED_DB="$DATA_DIR/${DB_NAME}.seed"
SEED_COUNTS="$DATA_DIR/${DB_NAME}.seed-counts.json"
INCOMING_DB="$DATA_DIR/${DB_NAME}.incoming"
SEED_KEY="seed/$DB_NAME"
BUILD_KEY="build/$DB_NAME"

log() { echo "[conductor $(date -u +%FT%TZ)] $*"; }
fail() { log "ERROR: $*"; exit 1; }

# Clears this run's own working files, INCLUDING their SQLite -wal/-shm
# companions (opened during the snapshot .backup() and the validation reads) --
# previously omitted here, which is why a killed-not-exited run left
# *.seed-wal/-shm and *.incoming-wal/-shm behind for step 0 below to find. The
# seed itself is normally already gone by now (freed right after upload, step
# 2) -- this remains a backstop for a failure before that point.
cleanup() {
  rm -f "$SEED_DB" "${SEED_DB}-wal" "${SEED_DB}-shm" "$SEED_COUNTS" \
        "$INCOMING_DB" "${INCOMING_DB}-wal" "${INCOMING_DB}-shm" 2>/dev/null || true
}
trap cleanup EXIT

: "${SUBNETS:?set SUBNETS (comma-separated public subnet ids)}"
: "${BUILD_SG:?set BUILD_SG (BuildSecurityGroup id)}"
[[ -f "$PREFLIGHT" ]] || fail "preflight script not found: $PREFLIGHT (install it alongside this script — see infra/README.md)"

# Non-blocking run lock: a manual `systemctl start` must not overlap the
# nightly timer (or another manual run) mid-round-trip, which would race two
# snapshots/swaps against the same files. Held for the process lifetime via fd
# 9; released automatically on exit, success or failure.
exec 9>"$LOCK_FILE" || fail "could not open lock file: $LOCK_FILE"
flock -n 9 || fail "another conductor run is already in progress (lock: $LOCK_FILE) — not starting a second one"

# Run a one-shot python in the semantic-index image against the mounted data dir.
# Used only for steps that need the image itself (the sqlite snapshot, and
# validate_graph_db.py's dependencies) -- NOT for the preflight checks, which
# run directly against the host's python3 (see header).
in_image() { docker run --rm -v "$DATA_DIR:/data" "$IMAGE" "$@"; }

# --- 0. Clear stale artifacts from a run that never reached its exit trap ----
log "Checking for leftover artifacts from a previous run..."
"$PYTHON3" "$PREFLIGHT" clear-stale \
  --data-dir "$DATA_DIR" --db-name "$DB_NAME" \
  || fail "leftover-artifact check failed — see the clear-stale output above"

# --- 1. Consistent snapshot of the live DB + capture enrichment baseline ------
[[ -f "$PROD_DB" ]] || fail "production DB not found: $PROD_DB"
PROD_DB_BYTES="$(stat -c%s "$PROD_DB")" || fail "could not stat production DB: $PROD_DB"
# ~2x the current DB size, not ~1x: this doubles as a fail-fast estimate of the
# LATER download step's footprint too (the incoming build is normally close to
# the same size as today's DB), so a run that's going to run out of room fails
# now rather than after ~20 minutes of Fargate. The download step below still
# runs its own precise check against the artifact's real size once it's known.
SNAPSHOT_PREFLIGHT_BYTES=$(( PROD_DB_BYTES * 2 ))
log "Checking free disk space before snapshot (payload ~${SNAPSHOT_PREFLIGHT_BYTES} bytes: current DB x2)..."
"$PYTHON3" "$PREFLIGHT" check-space \
  --path "$DATA_DIR" --needed-bytes "$SNAPSHOT_PREFLIGHT_BYTES" --step snapshot \
  || fail "disk-space preflight failed before the snapshot step — see the check-space output above"

log "Snapshotting live DB -> $SEED_DB (sqlite .backup, consistent under concurrent reads)"
in_image python -c "import sqlite3,sys; src=sqlite3.connect('/data/$DB_NAME'); dst=sqlite3.connect('/data/${DB_NAME}.seed'); src.backup(dst); dst.close(); src.close()" \
  || fail "snapshot failed"

log "Capturing seed enrichment counts -> $SEED_COUNTS"
in_image python scripts/validate_graph_db.py "/data/${DB_NAME}.seed" --emit-counts > "$SEED_COUNTS" \
  || fail "seed count capture failed"
log "seed counts: $(cat "$SEED_COUNTS")"

# --- 2. Upload seed, then free the local copy --------------------------------
log "Uploading seed -> s3://$BUCKET/$SEED_KEY"
aws s3 cp "$SEED_DB" "s3://$BUCKET/$SEED_KEY" --only-show-errors || fail "seed upload failed"

# Only seed-counts.json (a few bytes) is needed from here on -- validation
# (step 4) compares the build against those counts, never re-reads the full
# seed DB. Freeing the multi-GB seed now, rather than waiting for the exit
# trap, cuts the peak local footprint back down to just the production DB
# while Fargate runs, and again while the incoming artifact downloads below.
log "Releasing local seed copy (uploaded; only seed-counts.json is needed from here)"
rm -f "$SEED_DB" "${SEED_DB}-wal" "${SEED_DB}-shm"

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
BUILD_BYTES="$(aws s3api head-object --bucket "$BUCKET" --key "$BUILD_KEY" \
  --query ContentLength --output text)" \
  || fail "could not stat build artifact in S3: s3://$BUCKET/$BUILD_KEY"
log "Checking free disk space before download (payload ${BUILD_BYTES} bytes)..."
"$PYTHON3" "$PREFLIGHT" check-space \
  --path "$DATA_DIR" --needed-bytes "$BUILD_BYTES" --step download \
  || fail "disk-space preflight failed before the download step — see the check-space output above"

log "Downloading build artifact -> $INCOMING_DB"
aws s3 cp "s3://$BUCKET/$BUILD_KEY" "$INCOMING_DB" --only-show-errors || fail "build download failed"

log "Validating build artifact (header + artist>=$MIN_ARTISTS + mapped>=$MIN_MAPPED_ARTISTS + enrichment vs seed)..."
in_image python scripts/validate_graph_db.py "/data/${DB_NAME}.incoming" \
  --seed-counts "/data/${DB_NAME}.seed-counts.json" \
  --min-artists "$MIN_ARTISTS" \
  --min-mapped-artists "$MIN_MAPPED_ARTISTS" \
  || fail "validation failed — keeping current DB, NOT swapping"

# --- 5. Atomic swap (same filesystem) ----------------------------------------
# Clear the prior generation's WAL/SHM so a stale journal can't shadow the new
# inode, then rename (atomic on the same FS). The API opens read-only per request
# from app.state.db_path, so the next request picks up the new inode — no restart.
# NOTE (WXYC/semantic-index#387): removing these while the serving container
# may still have the current $PROD_DB open is a separate, pre-existing risk
# (possible corruption / lost committed WAL frames) tracked there, not fixed here.
log "Swapping in the new DB (clearing stale -wal/-shm, then rename)"
rm -f "${PROD_DB}-wal" "${PROD_DB}-shm"
mv -f "$INCOMING_DB" "$PROD_DB" || fail "atomic swap failed"

# Best-effort freshness signal (seeds the 'DB mtime > 36h' alarm follow-up).
aws cloudwatch put-metric-data --namespace WXYC/SemanticIndex \
  --metric-name GraphRebuildSuccess --value 1 --unit Count 2>/dev/null || true

log "Rebuild complete. $PROD_DB mtime: $(date -u -r "$PROD_DB" +%FT%TZ 2>/dev/null || stat -c %y "$PROD_DB")"
