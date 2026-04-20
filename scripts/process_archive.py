"""Process WXYC audio archives: classify segments via Essentia TF.

Downloads hourly MP3 files from the wxyc-archive S3 bucket, extracts
audio segments at flowsheet timestamps, and runs Essentia TF classifiers
(VGGish embeddings + classification heads) to produce per-segment audio
features. Aggregated per-artist profiles are written to the pipeline
SQLite database's audio_profile table.

Uses a checkpoint SQLite database for resumable processing. Each archive
hour is processed atomically: download → decode → classify → checkpoint.

Usage:
    python scripts/process_archive.py \
        --backend-dsn postgresql://localhost/backend \
        --model-dir /path/to/essentia-models \
        --db-path data/wxyc_artist_graph.db \
        --checkpoint output/archive_progress.db \
        --date-range 2021-06-01:2026-01-01 \
        --max-hours 100 \
        [--segment-duration 30] \
        [--retry-failed] \
        [--dry-run]
"""

import argparse
import json
import logging
import os
import sqlite3
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from semantic_index.archive_client import ArchiveClient, timestamp_to_s3_key
from semantic_index.archive_essentia import (
    CLASSIFIERS,
    EssentiaClassifier,
    SegmentFeatures,
    _build_recording_features,
    aggregate_artist_profile,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_SEGMENT_DURATION_S = 30
VGGISH_SAMPLE_RATE = 16000


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


def _entry_offset_in_hour(entry: dict, hour_key: str) -> float:
    """Compute an entry's offset in seconds from the start of its archive hour.

    Args:
        entry: Flowsheet entry with ``add_time_epoch``.
        hour_key: S3 key for the hour.

    Returns:
        Offset in seconds (clamped to [0, 3600]).
    """
    filename = Path(hour_key).stem  # "YYYYMMDDHH00"
    hour_start = datetime.strptime(filename, "%Y%m%d%H%M").replace(tzinfo=UTC)
    offset_s = entry["add_time_epoch"] - hour_start.timestamp()
    return max(0.0, min(offset_s, 3600.0))


# ---------------------------------------------------------------------------
# Checkpoint database
# ---------------------------------------------------------------------------


class ArchiveCheckpointDB:
    """SQLite checkpoint database for archive Essentia processing.

    Tracks which archive hours have been processed and stores
    per-segment classification results.

    Args:
        db_path: Path to the SQLite checkpoint file.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path)
            self._conn.execute("PRAGMA journal_mode=WAL")
        return self._conn

    def initialize(self) -> None:
        """Create checkpoint tables if they don't exist."""
        conn = self._get_conn()
        conn.executescript(
            """\
            CREATE TABLE IF NOT EXISTS hour_progress (
                archive_key TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                play_count INTEGER,
                segments_classified INTEGER,
                started_at TEXT,
                completed_at TEXT,
                error_msg TEXT
            );

            CREATE TABLE IF NOT EXISTS segment_features (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                archive_key TEXT NOT NULL,
                play_id INTEGER NOT NULL,
                artist_name TEXT NOT NULL,
                offset_s REAL NOT NULL,
                duration_s REAL NOT NULL,
                danceability REAL,
                genre TEXT,
                genre_probability REAL,
                voice_instrumental TEXT,
                voice_instrumental_probability REAL,
                feature_vector TEXT,
                created_at TEXT NOT NULL,
                UNIQUE (archive_key, play_id)
            );
            """
        )
        conn.commit()

    def is_hour_complete(self, archive_key: str) -> bool:
        """Check if an archive hour has been fully processed."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT status FROM hour_progress WHERE archive_key = ?",
            (archive_key,),
        ).fetchone()
        return row is not None and row[0] == "complete"

    def mark_hour_started(self, archive_key: str, play_count: int) -> None:
        """Record that processing has started for an archive hour."""
        conn = self._get_conn()
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO hour_progress "
            "(archive_key, status, play_count, started_at) "
            "VALUES (?, 'processing', ?, ?)",
            (archive_key, play_count, now),
        )
        conn.commit()

    def mark_hour_complete(self, archive_key: str, segments_classified: int) -> None:
        """Record that an archive hour has been fully processed."""
        conn = self._get_conn()
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "UPDATE hour_progress SET status = 'complete', "
            "segments_classified = ?, completed_at = ? "
            "WHERE archive_key = ?",
            (segments_classified, now, archive_key),
        )
        conn.commit()

    def mark_hour_failed(self, archive_key: str, error_msg: str) -> None:
        """Record that processing failed for an archive hour."""
        conn = self._get_conn()
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "UPDATE hour_progress SET status = 'failed', "
            "error_msg = ?, completed_at = ? "
            "WHERE archive_key = ?",
            (error_msg, now, archive_key),
        )
        conn.commit()

    def get_failed_hours(self) -> list[str]:
        """Get archive keys for all failed hours."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT archive_key FROM hour_progress WHERE status = 'failed'"
        ).fetchall()
        return [row[0] for row in rows]

    def save_segment(
        self,
        seg: SegmentFeatures,
        archive_key: str,
        play_id: int,
        offset_s: float,
        duration_s: float,
    ) -> None:
        """Save classified segment features. Idempotent via UNIQUE constraint."""
        conn = self._get_conn()
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO segment_features "
            "(archive_key, play_id, artist_name, offset_s, duration_s, "
            "danceability, genre, genre_probability, voice_instrumental, "
            "voice_instrumental_probability, feature_vector, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                archive_key,
                play_id,
                seg.artist_name,
                offset_s,
                duration_s,
                seg.danceability,
                seg.genre,
                seg.genre_probability,
                seg.voice_instrumental,
                seg.voice_instrumental_probability,
                json.dumps(seg.feature_vector),
                now,
            ),
        )
        conn.commit()

    def load_all_segments(self) -> list[SegmentFeatures]:
        """Load all successfully classified segments from the checkpoint."""
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT sf.* FROM segment_features sf "
            "JOIN hour_progress hp ON sf.archive_key = hp.archive_key "
            "WHERE hp.status = 'complete'"
        ).fetchall()
        segments = []
        for row in rows:
            fv = json.loads(row["feature_vector"])
            segments.append(
                SegmentFeatures(
                    artist_name=row["artist_name"],
                    danceability=row["danceability"],
                    genre=row["genre"],
                    genre_probability=row["genre_probability"],
                    genre_vector=fv[:9],
                    mood_vector=fv[9:16],
                    voice_instrumental=row["voice_instrumental"],
                    voice_instrumental_probability=row["voice_instrumental_probability"],
                    feature_vector=fv,
                )
            )
        conn.row_factory = None
        return segments

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None


# ---------------------------------------------------------------------------
# Processing pipeline
# ---------------------------------------------------------------------------


def process_hour(
    classifier: EssentiaClassifier,
    archive_client: ArchiveClient,
    checkpoint: ArchiveCheckpointDB,
    hour_key: str,
    entries: list[dict],
    segment_duration_s: int,
) -> int:
    """Process a single archive hour: download, classify segments.

    Loads the full hour audio once, then slices and classifies each
    flowsheet entry's segment without repeated disk I/O.

    Args:
        classifier: Pre-loaded Essentia classifier pipeline.
        archive_client: S3 client for downloading archive audio.
        checkpoint: Checkpoint database for progress tracking.
        hour_key: S3 key for the archive hour.
        entries: Flowsheet entries in this hour.
        segment_duration_s: Duration of each segment in seconds.

    Returns:
        Number of segments successfully classified.
    """
    checkpoint.mark_hour_started(hour_key, play_count=len(entries))

    # Download and decode
    try:
        mp3_path = archive_client.download_hour(hour_key)
    except Exception as e:
        logger.error("Failed to download %s: %s", hour_key, e)
        checkpoint.mark_hour_failed(hour_key, f"download: {e}")
        return 0

    wav_path = None
    try:
        wav_path = ArchiveClient.decode_to_wav(mp3_path, sample_rate=VGGISH_SAMPLE_RATE)
    except Exception as e:
        logger.error("Failed to decode %s: %s", hour_key, e)
        checkpoint.mark_hour_failed(hour_key, f"decode: {e}")
        mp3_path.unlink(missing_ok=True)
        return 0

    try:
        from essentia.standard import MonoLoader

        audio = MonoLoader(filename=str(wav_path), sampleRate=VGGISH_SAMPLE_RATE)()
        total_samples = len(audio)
        classified = 0

        for entry in entries:
            offset_s = _entry_offset_in_hour(entry, hour_key)
            start_sample = int(offset_s * VGGISH_SAMPLE_RATE)
            end_sample = min(
                start_sample + segment_duration_s * VGGISH_SAMPLE_RATE,
                total_samples,
            )

            if end_sample - start_sample < VGGISH_SAMPLE_RATE:
                continue  # less than 1 second — skip

            segment_audio = audio[start_sample:end_sample]
            results = classifier.classify_array(segment_audio)

            if len(results) < len(CLASSIFIERS):
                missing_names = set(CLASSIFIERS) - set(results)
                logger.debug(
                    "Skipping play %d: missing classifiers %s",
                    entry["id"],
                    missing_names,
                )
                continue

            rf = _build_recording_features(results)
            fv = rf.feature_vector()
            seg = SegmentFeatures(
                artist_name=entry["artist_name"],
                danceability=rf.danceability,
                genre=rf.genre,
                genre_probability=rf.genre_probability,
                genre_vector=rf.genre_vector,
                mood_vector=rf.mood_vector,
                voice_instrumental=rf.voice_instrumental,
                voice_instrumental_probability=rf.voice_instrumental_probability,
                feature_vector=fv,
            )
            checkpoint.save_segment(
                seg, hour_key, entry["id"], offset_s, segment_duration_s,
            )
            classified += 1

        checkpoint.mark_hour_complete(hour_key, classified)
        logger.info(
            "  %s: %d/%d entries classified",
            hour_key,
            classified,
            len(entries),
        )
        return classified

    except Exception as e:
        logger.error("Error processing %s: %s", hour_key, e, exc_info=True)
        checkpoint.mark_hour_failed(hour_key, str(e))
        return 0
    finally:
        if wav_path:
            wav_path.unlink(missing_ok=True)
        mp3_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Profile aggregation and DB write
# ---------------------------------------------------------------------------


def write_profiles_to_db(db_path: str, segments: list[SegmentFeatures]) -> int:
    """Aggregate per-artist profiles and write to the audio_profile table.

    Only writes profiles for artists that exist in the ``artist`` table
    and don't already have a profile (preserves existing AcousticBrainz data).

    Args:
        db_path: Path to the pipeline SQLite database.
        segments: All classified segments from the checkpoint.

    Returns:
        Number of profiles written.
    """
    by_artist: dict[str, list[SegmentFeatures]] = defaultdict(list)
    for seg in segments:
        by_artist[seg.artist_name].append(seg)

    logger.info(
        "Aggregating profiles for %d artists from %d segments",
        len(by_artist),
        len(segments),
    )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    written = 0

    try:
        for artist_name, artist_segments in sorted(by_artist.items()):
            row = conn.execute(
                "SELECT id FROM artist WHERE canonical_name = ?",
                (artist_name,),
            ).fetchone()
            if row is None:
                continue
            artist_id = row["id"]

            # Don't overwrite existing profiles (AcousticBrainz data)
            existing = conn.execute(
                "SELECT recording_count FROM audio_profile WHERE artist_id = ?",
                (artist_id,),
            ).fetchone()
            if existing is not None:
                continue

            profile = aggregate_artist_profile(artist_name, artist_segments)
            now = datetime.now(UTC).isoformat()

            conn.execute(
                "INSERT OR IGNORE INTO audio_profile "
                "(artist_id, avg_danceability, primary_genre, primary_genre_probability, "
                "voice_instrumental_ratio, feature_centroid, recording_count, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    artist_id,
                    profile["avg_danceability"],
                    profile["primary_genre"],
                    profile["primary_genre_probability"],
                    profile["voice_instrumental_ratio"],
                    json.dumps(profile["feature_centroid"]),
                    profile["recording_count"],
                    now,
                ),
            )
            written += 1

        conn.commit()
        logger.info("Wrote %d new audio profiles", written)
    finally:
        conn.close()

    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Process WXYC audio archives: classify segments via Essentia TF "
            "and build per-artist audio profiles."
        )
    )
    parser.add_argument(
        "--backend-dsn",
        default=os.environ.get("DATABASE_URL_BACKEND"),
        help="PostgreSQL DSN for Backend-Service (default: DATABASE_URL_BACKEND env var).",
    )
    parser.add_argument(
        "--model-dir",
        default=os.environ.get("ESSENTIA_MODEL_DIR"),
        help="Directory containing Essentia TF models (default: ESSENTIA_MODEL_DIR env var).",
    )
    parser.add_argument(
        "--db-path",
        default=os.environ.get("DB_PATH"),
        help="Pipeline SQLite database for writing audio profiles (default: DB_PATH env var).",
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
        "--segment-duration",
        type=int,
        default=DEFAULT_SEGMENT_DURATION_S,
        help=f"Duration of each segment in seconds (default: {DEFAULT_SEGMENT_DURATION_S}).",
    )
    parser.add_argument(
        "--aggregate-only",
        action="store_true",
        help="Skip processing; aggregate existing checkpoint data into the DB.",
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

    # Initialize checkpoint
    checkpoint_dir = Path(args.checkpoint).parent
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = ArchiveCheckpointDB(args.checkpoint)
    checkpoint.initialize()

    if not args.aggregate_only:
        if not args.backend_dsn:
            logger.error("--backend-dsn or DATABASE_URL_BACKEND is required")
            raise SystemExit(1)

        if not args.model_dir:
            logger.error("--model-dir or ESSENTIA_MODEL_DIR is required")
            raise SystemExit(1)

        if not args.date_range:
            logger.error("--date-range is required")
            raise SystemExit(1)

        # Parse date range
        start_str, end_str = args.date_range.split(":")
        start_date = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=UTC)
        end_date = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=UTC)

        # Load flowsheet entries
        logger.info(
            "Loading flowsheet entries %s to %s...",
            start_date.date(),
            end_date.date(),
        )
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
        to_process = {
            k: v
            for k, v in sorted(hour_groups.items())
            if not checkpoint.is_hour_complete(k)
        }

        if args.retry_failed:
            failed = set(checkpoint.get_failed_hours())
            to_process.update(
                {k: v for k, v in sorted(hour_groups.items()) if k in failed}
            )

        if args.max_hours > 0:
            keys = list(to_process.keys())[: args.max_hours]
            to_process = {k: to_process[k] for k in keys}

        logger.info("%d archive hours to process", len(to_process))

        if args.dry_run:
            total_entries = sum(len(v) for v in to_process.values())
            for key, hour_entries in to_process.items():
                logger.info("  [dry-run] %s: %d entries", key, len(hour_entries))
            logger.info(
                "Dry run complete. %d hours, %d entries would be processed.",
                len(to_process),
                total_entries,
            )
            return

        # Initialize classifier and S3 client
        classifier = EssentiaClassifier(args.model_dir)
        archive_client = ArchiveClient(bucket=args.bucket)
        total_classified = 0

        for i, (key, hour_entries) in enumerate(to_process.items(), 1):
            logger.info(
                "=== Hour %d/%d: %s (%d entries) ===",
                i,
                len(to_process),
                key,
                len(hour_entries),
            )
            t0 = time.time()
            classified = process_hour(
                classifier,
                archive_client,
                checkpoint,
                key,
                hour_entries,
                args.segment_duration,
            )
            elapsed = time.time() - t0
            total_classified += classified
            logger.info("  Done in %.1fs", elapsed)

        logger.info(
            "Processing complete: %d hours, %d segments classified",
            len(to_process),
            total_classified,
        )

    # Aggregate and write to production DB
    if args.db_path:
        segments = checkpoint.load_all_segments()
        if segments:
            written = write_profiles_to_db(args.db_path, segments)
            logger.info("Wrote %d audio profiles to %s", written, args.db_path)
        else:
            logger.info("No segments to aggregate")
    else:
        logger.info("No --db-path specified; skipping profile aggregation")

    checkpoint.close()


if __name__ == "__main__":
    main()
