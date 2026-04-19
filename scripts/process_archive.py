"""Process WXYC audio archives: fingerprint and identify recordings via AcoustID.

Downloads hourly MP3 files from the wxyc-archive S3 bucket, generates
Chromaprint fingerprints for audio segments around flowsheet timestamps,
and submits them to the AcoustID API to obtain MusicBrainz recording IDs.

Uses a checkpoint SQLite database for resumable processing. Each archive
hour is processed atomically: download -> decode -> fingerprint -> lookup.

Usage:
    python scripts/process_archive.py \
        --backend-dsn postgresql://localhost/backend \
        --acoustid-api-key KEY \
        --checkpoint output/archive_progress.db \
        --date-range 2020-01-01:2020-01-07 \
        --max-hours 24 \
        [--skip-essentia] \
        [--retry-failed] \
        [--dry-run]
"""

import argparse
import asyncio
import logging
import os
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from semantic_index.archive_client import (
    ArchiveClient,
    compute_search_windows,
    merge_overlapping_windows,
    timestamp_to_s3_key,
)
from semantic_index.archive_fingerprint import (
    CheckpointDB,
    _best_match_per_play,
    _generate_fingerprint_offsets,
    fingerprint_and_lookup,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Flowsheet queries
# ---------------------------------------------------------------------------

_FLOWSHEET_WITH_TIMESTAMPS_SQL = """\
SELECT f.id, f.artist_name, f.show_id, f.play_order,
       EXTRACT(EPOCH FROM f.add_time)::bigint AS add_time_epoch
FROM wxyc_schema.flowsheet f
WHERE f.entry_type = 'track'
  AND f.add_time IS NOT NULL
  AND f.add_time >= %s
  AND f.add_time < %s
ORDER BY f.add_time
"""


def _load_flowsheet_entries(
    conn: psycopg.Connection,
    start_date: datetime,
    end_date: datetime,
) -> list[dict]:
    """Load flowsheet entries with timestamps in the given date range.

    Args:
        conn: PostgreSQL connection to Backend-Service (dict_row factory).
        start_date: Start of the date range (inclusive).
        end_date: End of the date range (exclusive).

    Returns:
        List of dicts with id, artist_name, show_id, play_order, add_time_epoch.
    """
    rows = conn.execute(
        _FLOWSHEET_WITH_TIMESTAMPS_SQL,
        (start_date, end_date),
    ).fetchall()
    logger.info("Loaded %d flowsheet entries in date range", len(rows))
    return [dict(row) for row in rows]


def _group_entries_by_hour(entries: list[dict]) -> dict[str, list[dict]]:
    """Group flowsheet entries by their archive hour S3 key.

    Args:
        entries: Flowsheet entries with ``add_time_epoch`` (seconds).

    Returns:
        Dict mapping S3 key to list of entries in that hour.
    """
    groups: dict[str, list[dict]] = defaultdict(list)
    for entry in entries:
        ts = datetime.fromtimestamp(entry["add_time_epoch"], tz=UTC)
        key = timestamp_to_s3_key(ts)
        groups[key].append(entry)
    return dict(groups)


def _entries_to_offsets(entries: list[dict], hour_key: str) -> tuple[list[int], list[int]]:
    """Convert flowsheet entries to offsets within an archive hour.

    Args:
        entries: Flowsheet entries for a single hour.
        hour_key: S3 key for the hour (used to determine hour start time).

    Returns:
        Tuple of (offsets_ms, play_ids).
    """
    # Parse hour start from S3 key: "YYYY/MM/DD/YYYYMMDDHH00.mp3"
    filename = Path(hour_key).stem  # "YYYYMMDDHH00"
    hour_start = datetime.strptime(filename, "%Y%m%d%H%M").replace(tzinfo=UTC)
    hour_start_epoch = hour_start.timestamp()

    offsets_ms = []
    play_ids = []
    for entry in entries:
        offset_s = entry["add_time_epoch"] - hour_start_epoch
        offset_ms = int(offset_s * 1000)
        offset_ms = max(0, min(offset_ms, 3_600_000))
        offsets_ms.append(offset_ms)
        play_ids.append(entry["id"])
    return offsets_ms, play_ids


# ---------------------------------------------------------------------------
# Processing pipeline
# ---------------------------------------------------------------------------


async def process_hour(
    archive_client: ArchiveClient,
    checkpoint: CheckpointDB,
    hour_key: str,
    entries: list[dict],
    api_key: str,
    match_threshold: float,
) -> tuple[int, int]:
    """Process a single archive hour: download, fingerprint, lookup.

    Args:
        archive_client: S3 client for downloading archive audio.
        checkpoint: Checkpoint database for progress tracking.
        hour_key: S3 key for the archive hour.
        entries: Flowsheet entries in this hour.
        api_key: AcoustID API key.
        match_threshold: Minimum AcoustID score to accept.

    Returns:
        Tuple of (segments_fingerprinted, segments_matched).
    """
    checkpoint.mark_hour_started(hour_key, play_count=len(entries))

    # Download and decode
    try:
        mp3_path = archive_client.download_hour(hour_key)
    except Exception as e:
        logger.error("Failed to download %s: %s", hour_key, e)
        checkpoint.mark_hour_failed(hour_key, f"download: {e}")
        return 0, 0

    wav_path = None
    try:
        wav_path = ArchiveClient.decode_to_wav(mp3_path)
    except Exception as e:
        logger.error("Failed to decode %s: %s", hour_key, e)
        checkpoint.mark_hour_failed(hour_key, f"decode: {e}")
        mp3_path.unlink(missing_ok=True)
        return 0, 0

    try:
        # Compute search windows
        offsets_ms, play_ids = _entries_to_offsets(entries, hour_key)
        windows = compute_search_windows(offsets_ms, play_ids=play_ids)
        windows = merge_overlapping_windows(windows)

        # Build fingerprint offset list with play_id mapping
        all_offsets: list[int] = []
        all_play_ids: list[int | None] = []
        for window in windows:
            fp_offsets = _generate_fingerprint_offsets(window.start_ms, window.end_ms)
            # Map each fingerprint offset to the nearest play entry
            for fp_offset in fp_offsets:
                all_offsets.append(fp_offset)
                # Assign to the closest play_id in this window
                if window.play_ids:
                    closest_pid = min(
                        window.play_ids,
                        key=lambda pid: abs(offsets_ms[play_ids.index(pid)] - fp_offset),
                    )
                    all_play_ids.append(closest_pid)
                else:
                    all_play_ids.append(None)

        logger.info(
            "  %s: %d entries, %d windows, %d fingerprint offsets",
            hour_key,
            len(entries),
            len(windows),
            len(all_offsets),
        )

        # Fingerprint and lookup
        matches = await fingerprint_and_lookup(
            wav_path,
            all_offsets,
            api_key,
            match_threshold=match_threshold,
            play_ids=all_play_ids,
        )

        # Deduplicate: best match per play entry
        best_matches = _best_match_per_play(matches)

        # Save matches to checkpoint
        entry_lookup = {e["id"]: e for e in entries}
        for match in best_matches:
            artist_name = None
            if match.play_id and match.play_id in entry_lookup:
                artist_name = entry_lookup[match.play_id].get("artist_name")
            checkpoint.save_segment_match(
                archive_key=hour_key,
                match=match,
                duration_ms=15_000,
                artist_name=artist_name,
            )

        checkpoint.mark_hour_complete(
            hour_key,
            segments_fingerprinted=len(all_offsets),
            segments_matched=len(best_matches),
            segments_extracted=0,
        )

        logger.info(
            "  %s: %d fingerprinted, %d matched",
            hour_key,
            len(all_offsets),
            len(best_matches),
        )
        return len(all_offsets), len(best_matches)

    except Exception as e:
        logger.error("Error processing %s: %s", hour_key, e, exc_info=True)
        checkpoint.mark_hour_failed(hour_key, str(e))
        return 0, 0
    finally:
        if wav_path:
            wav_path.unlink(missing_ok=True)
        mp3_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Process WXYC audio archives: fingerprint recordings via "
            "Chromaprint and identify them via AcoustID."
        )
    )
    parser.add_argument(
        "--backend-dsn",
        default=os.environ.get("DATABASE_URL_BACKEND"),
        help="PostgreSQL DSN for Backend-Service (default: DATABASE_URL_BACKEND env var).",
    )
    parser.add_argument(
        "--acoustid-api-key",
        default=os.environ.get("ACOUSTID_API_KEY"),
        help="AcoustID API key (default: ACOUSTID_API_KEY env var).",
    )
    parser.add_argument(
        "--checkpoint",
        default=os.environ.get("ARCHIVE_CHECKPOINT", "output/archive_progress.db"),
        help="Path to checkpoint SQLite database.",
    )
    parser.add_argument(
        "--bucket",
        default="wxyc-archive",
        help="S3 bucket name (default: wxyc-archive).",
    )
    parser.add_argument(
        "--date-range",
        help="Date range to process as START:END (YYYY-MM-DD:YYYY-MM-DD).",
    )
    parser.add_argument(
        "--max-hours",
        type=int,
        default=0,
        help="Maximum number of archive hours to process (0 = unlimited).",
    )
    parser.add_argument(
        "--match-threshold",
        type=float,
        default=0.7,
        help="Minimum AcoustID match score (0-1, default: 0.7).",
    )
    parser.add_argument(
        "--skip-essentia",
        action="store_true",
        help="Run fingerprint + AcoustID only, skip Essentia feature extraction.",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-attempt previously failed archive hours.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be processed without downloading audio.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.backend_dsn:
        logger.error("--backend-dsn or DATABASE_URL_BACKEND is required")
        raise SystemExit(1)

    if not args.acoustid_api_key:
        logger.error("--acoustid-api-key or ACOUSTID_API_KEY is required")
        raise SystemExit(1)

    # Parse date range
    if args.date_range:
        start_str, end_str = args.date_range.split(":")
        start_date = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=UTC)
        end_date = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=UTC)
    else:
        logger.error("--date-range is required")
        raise SystemExit(1)

    # Initialize checkpoint
    checkpoint_dir = Path(args.checkpoint).parent
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = CheckpointDB(args.checkpoint)
    checkpoint.initialize()

    # Load flowsheet entries from Backend PG
    logger.info("Loading flowsheet entries from %s...", start_date.date())
    conn = psycopg.connect(args.backend_dsn, row_factory=dict_row)
    try:
        entries = _load_flowsheet_entries(conn, start_date, end_date)
    finally:
        conn.close()

    if not entries:
        logger.info("No flowsheet entries found in date range")
        return

    # Group by archive hour
    hour_groups = _group_entries_by_hour(entries)
    logger.info("%d archive hours to consider", len(hour_groups))

    # Filter out completed hours
    to_process = {}
    for key, hour_entries in sorted(hour_groups.items()):
        if checkpoint.is_hour_complete(key):
            continue
        if not args.retry_failed:
            # Skip failed hours unless --retry-failed
            pass
        to_process[key] = hour_entries

    if args.retry_failed:
        failed = set(checkpoint.get_failed_hours())
        to_process = {
            k: v
            for k, v in sorted(hour_groups.items())
            if not checkpoint.is_hour_complete(k) or k in failed
        }

    if args.max_hours > 0:
        keys = list(to_process.keys())[: args.max_hours]
        to_process = {k: to_process[k] for k in keys}

    logger.info("%d archive hours to process", len(to_process))

    if args.dry_run:
        for key, hour_entries in to_process.items():
            logger.info("  [dry-run] %s: %d entries", key, len(hour_entries))
        logger.info("Dry run complete. %d hours would be processed.", len(to_process))
        return

    # Process each hour
    archive_client = ArchiveClient(bucket=args.bucket)
    total_fingerprinted = 0
    total_matched = 0

    for i, (key, hour_entries) in enumerate(to_process.items(), 1):
        logger.info("=== Hour %d/%d: %s ===", i, len(to_process), key)
        t0 = time.time()
        fp_count, match_count = asyncio.run(
            process_hour(
                archive_client,
                checkpoint,
                key,
                hour_entries,
                args.acoustid_api_key,
                args.match_threshold,
            )
        )
        elapsed = time.time() - t0
        total_fingerprinted += fp_count
        total_matched += match_count
        logger.info("  Done in %.1fs", elapsed)

    logger.info(
        "Processing complete: %d hours, %d fingerprinted, %d matched",
        len(to_process),
        total_fingerprinted,
        total_matched,
    )

    checkpoint.close()


if __name__ == "__main__":
    main()
