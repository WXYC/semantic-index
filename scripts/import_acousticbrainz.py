"""ETL script: Import AcousticBrainz high-level features from tar archives into PostgreSQL.

Processes tar files one at a time (NAS-friendly), with per-tar checkpointing
to support resume after interruption. Uses COPY for fast bulk inserts and
ON CONFLICT DO NOTHING for idempotent re-runs.

Usage:
    python scripts/import_acousticbrainz.py \
        --tar-dir "/Volumes/Peak Twins/acousticbrainz/" \
        --dsn postgresql://localhost/musicbrainz \
        --checkpoint output/ab_import_progress.db \
        [--retry-failed]
"""

import argparse
import json
import logging
import os
import sqlite3
import tarfile
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Column names matching the ab_recording table, in INSERT order
_COLUMNS = [
    "recording_mbid",
    "danceability",
    "gender_value",
    "gender_probability",
    "genre_dortmund_value",
    "genre_dortmund_prob",
    "genre_electronic_value",
    "genre_electronic_prob",
    "genre_rosamerica_value",
    "genre_rosamerica_prob",
    "genre_tzanetakis_value",
    "genre_tzanetakis_prob",
    "ismir04_rhythm_value",
    "ismir04_rhythm_prob",
    "mood_acoustic",
    "mood_aggressive",
    "mood_electronic",
    "mood_happy",
    "mood_party",
    "mood_relaxed",
    "mood_sad",
    "moods_mirex_value",
    "moods_mirex_prob",
    "timbre_value",
    "timbre_probability",
    "tonal",
    "voice_instrumental_value",
    "voice_instrumental_prob",
    "classifier_distributions",
    "audio_length",
    "audio_codec",
    "audio_sample_rate",
    "audio_bit_rate",
    "replay_gain",
    "metadata_tags",
    "tar_file",
]

_BATCH_SIZE = 10_000


def parse_recording_json(mbid: str, data: dict, tar_name: str) -> dict:
    """Parse an AcousticBrainz JSON into a flat dict for DB insertion.

    Args:
        mbid: MusicBrainz recording UUID.
        data: Parsed JSON data from the AcousticBrainz dump.
        tar_name: Name of the source tar file (provenance tracking).

    Returns:
        Dict with keys matching _COLUMNS.
    """
    hl = data["highlevel"]

    # Build classifier distributions JSONB
    distributions = {}
    for key in [
        "genre_dortmund",
        "genre_electronic",
        "genre_rosamerica",
        "genre_tzanetakis",
        "ismir04_rhythm",
        "moods_mirex",
        "gender",
    ]:
        classifier = hl.get(key, {})
        distributions[key] = classifier.get("all", {})

    # Audio properties
    audio = data.get("metadata", {}).get("audio_properties", {})
    tags = data.get("metadata", {}).get("tags", {})

    return {
        "recording_mbid": mbid,
        "danceability": hl["danceability"]["all"]["danceable"],
        "gender_value": hl["gender"]["value"],
        "gender_probability": hl["gender"]["probability"],
        "genre_dortmund_value": hl["genre_dortmund"]["value"],
        "genre_dortmund_prob": hl["genre_dortmund"]["probability"],
        "genre_electronic_value": hl.get("genre_electronic", {}).get("value", ""),
        "genre_electronic_prob": hl.get("genre_electronic", {}).get("probability", 0.0),
        "genre_rosamerica_value": hl.get("genre_rosamerica", {}).get("value", ""),
        "genre_rosamerica_prob": hl.get("genre_rosamerica", {}).get("probability", 0.0),
        "genre_tzanetakis_value": hl.get("genre_tzanetakis", {}).get("value", ""),
        "genre_tzanetakis_prob": hl.get("genre_tzanetakis", {}).get("probability", 0.0),
        "ismir04_rhythm_value": hl.get("ismir04_rhythm", {}).get("value", ""),
        "ismir04_rhythm_prob": hl.get("ismir04_rhythm", {}).get("probability", 0.0),
        "mood_acoustic": hl["mood_acoustic"]["all"].get("acoustic", 0.0),
        "mood_aggressive": hl["mood_aggressive"]["all"].get("aggressive", 0.0),
        "mood_electronic": hl["mood_electronic"]["all"].get("electronic", 0.0),
        "mood_happy": hl["mood_happy"]["all"].get("happy", 0.0),
        "mood_party": hl["mood_party"]["all"].get("party", 0.0),
        "mood_relaxed": hl["mood_relaxed"]["all"].get("relaxed", 0.0),
        "mood_sad": hl["mood_sad"]["all"].get("sad", 0.0),
        "moods_mirex_value": hl.get("moods_mirex", {}).get("value", ""),
        "moods_mirex_prob": hl.get("moods_mirex", {}).get("probability", 0.0),
        "timbre_value": hl["timbre"]["value"],
        "timbre_probability": hl["timbre"]["probability"],
        "tonal": hl["tonal_atonal"]["all"]["tonal"],
        "voice_instrumental_value": hl["voice_instrumental"]["value"],
        "voice_instrumental_prob": hl["voice_instrumental"]["probability"],
        "classifier_distributions": json.dumps(distributions),
        "audio_length": audio.get("length"),
        "audio_codec": audio.get("codec"),
        "audio_sample_rate": audio.get("analysis_sample_rate"),
        "audio_bit_rate": audio.get("bit_rate"),
        "replay_gain": audio.get("replay_gain"),
        "metadata_tags": json.dumps(tags) if tags else None,
        "tar_file": tar_name,
    }


def process_tar(tar_path: str) -> list[dict]:
    """Read all AcousticBrainz JSON files from a tar archive.

    Args:
        tar_path: Path to the tar file.

    Returns:
        List of parsed recording dicts.
    """
    tar_name = Path(tar_path).name
    rows = []
    with tarfile.open(tar_path) as tf:
        for member in tf:
            if not member.isfile() or not member.name.endswith(".json"):
                continue
            filename = member.name.rsplit("/", 1)[-1]
            mbid = filename.rsplit("-", 1)[0]
            try:
                f = tf.extractfile(member)
                if f is None:
                    continue
                data = json.load(f)
                row = parse_recording_json(mbid, data, tar_name)
                rows.append(row)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning("  Skipping %s: %s", member.name, e)
                continue
    return rows


def init_checkpoint(checkpoint_path: str) -> None:
    """Initialize the checkpoint SQLite database."""
    conn = sqlite3.connect(checkpoint_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS progress ("
        "tar_file TEXT PRIMARY KEY, status TEXT NOT NULL, "
        "rows_imported INTEGER, completed_at TEXT, error_msg TEXT)"
    )
    conn.commit()
    conn.close()


def get_completed_tars(checkpoint_path: str) -> set[str]:
    """Get the set of tar filenames that have been fully imported."""
    conn = sqlite3.connect(checkpoint_path)
    rows = conn.execute("SELECT tar_file FROM progress WHERE status = 'complete'").fetchall()
    conn.close()
    return {r[0] for r in rows}


def get_failed_tars(checkpoint_path: str) -> set[str]:
    """Get the set of tar filenames that failed during import."""
    conn = sqlite3.connect(checkpoint_path)
    rows = conn.execute("SELECT tar_file FROM progress WHERE status = 'failed'").fetchall()
    conn.close()
    return {r[0] for r in rows}


def mark_tar_complete(checkpoint_path: str, tar_name: str, rows_imported: int) -> None:
    """Mark a tar file as successfully imported."""
    conn = sqlite3.connect(checkpoint_path)
    conn.execute(
        "INSERT OR REPLACE INTO progress (tar_file, status, rows_imported, completed_at) "
        "VALUES (?, 'complete', ?, ?)",
        (tar_name, rows_imported, datetime.now(UTC).isoformat()),
    )
    conn.commit()
    conn.close()


def mark_tar_failed(checkpoint_path: str, tar_name: str, error_msg: str) -> None:
    """Mark a tar file as failed."""
    conn = sqlite3.connect(checkpoint_path)
    conn.execute(
        "INSERT OR REPLACE INTO progress (tar_file, status, rows_imported, completed_at, error_msg) "
        "VALUES (?, 'failed', 0, ?, ?)",
        (tar_name, datetime.now(UTC).isoformat(), error_msg),
    )
    conn.commit()
    conn.close()


def insert_batch(conn: psycopg.Connection, rows: list[dict]) -> int:
    """Insert a batch of rows using multi-row INSERT with ON CONFLICT DO NOTHING.

    Args:
        conn: PostgreSQL connection.
        rows: List of recording dicts to insert.

    Returns:
        Number of rows actually inserted (excluding conflicts).
    """
    if not rows:
        return 0

    placeholders = ", ".join([f"({', '.join(['%s'] * len(_COLUMNS))})"] * len(rows))
    col_names = ", ".join(_COLUMNS)
    query = (
        f"INSERT INTO ab_recording ({col_names}) VALUES "
        f"{placeholders} "
        f"ON CONFLICT (recording_mbid) DO NOTHING"
    )

    values = []
    for row in rows:
        for col in _COLUMNS:
            values.append(row[col])

    with conn.cursor() as cur:
        cur.execute(query, values)
        return cur.rowcount


def import_tar(dsn: str, tar_path: str, checkpoint_path: str) -> int:
    """Import a single tar file into PostgreSQL.

    Reads all JSON files from the tar, then bulk inserts in batches.
    Commits per batch so partial progress is retained on failure.

    Args:
        dsn: PostgreSQL connection string.
        tar_path: Path to the tar archive.
        checkpoint_path: Path to the checkpoint SQLite database.

    Returns:
        Number of rows inserted.
    """
    tar_name = Path(tar_path).name
    logger.info("Processing %s...", tar_name)
    t0 = time.time()

    try:
        rows = process_tar(tar_path)
    except (tarfile.TarError, OSError) as e:
        logger.error("Failed to read %s: %s", tar_name, e)
        mark_tar_failed(checkpoint_path, tar_name, str(e))
        return 0

    logger.info("  %d recordings parsed from %s (%.1fs)", len(rows), tar_name, time.time() - t0)

    if not rows:
        mark_tar_complete(checkpoint_path, tar_name, 0)
        return 0

    conn = psycopg.connect(dsn)
    total_inserted = 0
    try:
        for i in range(0, len(rows), _BATCH_SIZE):
            batch = rows[i : i + _BATCH_SIZE]
            inserted = insert_batch(conn, batch)
            conn.commit()
            total_inserted += inserted
            logger.info(
                "  Batch %d/%d: %d inserted (%d total)",
                i // _BATCH_SIZE + 1,
                (len(rows) + _BATCH_SIZE - 1) // _BATCH_SIZE,
                inserted,
                total_inserted,
            )
    except Exception as e:
        logger.error("Failed during insert for %s: %s", tar_name, e)
        conn.rollback()
        mark_tar_failed(checkpoint_path, tar_name, str(e))
        conn.close()
        return total_inserted
    finally:
        if not conn.closed:
            conn.close()

    mark_tar_complete(checkpoint_path, tar_name, total_inserted)
    elapsed = time.time() - t0
    logger.info("  %s complete: %d rows in %.1fs", tar_name, total_inserted, elapsed)
    return total_inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import AcousticBrainz high-level features from tar archives into PostgreSQL."
    )
    parser.add_argument(
        "--tar-dir",
        required=True,
        help="Directory containing AcousticBrainz tar files.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL_MUSICBRAINZ", "postgresql://localhost/musicbrainz"),
        help="PostgreSQL DSN (default: DATABASE_URL_MUSICBRAINZ env var).",
    )
    parser.add_argument(
        "--checkpoint",
        default="output/ab_import_progress.db",
        help="Path to checkpoint SQLite database (default: output/ab_import_progress.db).",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-attempt previously failed tars.",
    )
    args = parser.parse_args()

    init_checkpoint(args.checkpoint)

    tar_dir = Path(args.tar_dir)
    tar_files = sorted(tar_dir.glob("*.tar"))
    logger.info("Found %d tar files in %s", len(tar_files), tar_dir)

    completed = get_completed_tars(args.checkpoint)
    failed = get_failed_tars(args.checkpoint) if args.retry_failed else set()
    skipping = completed - failed  # retry_failed removes failed from skip set

    to_process = [t for t in tar_files if t.name not in skipping]
    logger.info(
        "%d tars to process (%d completed, %d skipped)",
        len(to_process),
        len(completed),
        len(skipping),
    )

    grand_total = 0
    for i, tar_path in enumerate(to_process, 1):
        logger.info("=== Tar %d/%d: %s ===", i, len(to_process), tar_path.name)
        count = import_tar(args.dsn, str(tar_path), args.checkpoint)
        grand_total += count

    logger.info(
        "Import complete: %d total rows inserted across %d tars", grand_total, len(to_process)
    )

    # Final verification
    try:
        conn = psycopg.connect(args.dsn)
        total = conn.execute("SELECT COUNT(*) FROM ab_recording").fetchone()[0]
        conn.close()
        logger.info("Total rows in ab_recording: %d", total)
    except Exception:
        logger.warning("Could not verify final count", exc_info=True)


if __name__ == "__main__":
    main()
