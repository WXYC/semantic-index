"""Archive audio fingerprinting and AcoustID identification.

Generates Chromaprint fingerprints for audio segments extracted from
WXYC archive recordings and submits them to the AcoustID API to obtain
MusicBrainz recording IDs.

Uses ``pyacoustid`` (which wraps the ``fpcalc`` binary) for fingerprint
generation and ``httpx`` for rate-limited AcoustID API lookups.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

ACOUSTID_API_URL = "https://api.acoustid.org/v2/lookup"
DEFAULT_MATCH_THRESHOLD = 0.7
DEFAULT_SEGMENT_DURATION_MS = 15_000
DEFAULT_STEP_MS = 10_000
RATE_LIMIT_PER_SECOND = 3


@dataclass
class AcoustIDMatch:
    """Result of an AcoustID fingerprint lookup.

    Attributes:
        offset_ms: Offset within the archive hour where the fingerprint was taken.
        recording_mbid: MusicBrainz recording UUID from AcoustID.
        score: AcoustID match confidence (0-1).
        play_id: Flowsheet entry ID that guided the search window.
    """

    offset_ms: int
    recording_mbid: str
    score: float
    play_id: int | None = None


def _generate_fingerprint_offsets(
    window_start_ms: int,
    window_end_ms: int,
    segment_duration_ms: int = DEFAULT_SEGMENT_DURATION_MS,
    step_ms: int = DEFAULT_STEP_MS,
) -> list[int]:
    """Generate offsets within a search window for fingerprinting.

    Slides a segment of ``segment_duration_ms`` through the window at
    ``step_ms`` intervals. The last offset is adjusted so the segment
    does not exceed the window end.

    Args:
        window_start_ms: Start of the search window (ms).
        window_end_ms: End of the search window (ms).
        segment_duration_ms: Duration of each fingerprint segment (ms).
        step_ms: Step size between consecutive offsets (ms).

    Returns:
        List of offsets (ms) within the window.
    """
    last_valid = window_end_ms - segment_duration_ms
    if last_valid < window_start_ms:
        return [window_start_ms]

    offsets = []
    offset = window_start_ms
    while offset <= last_valid:
        offsets.append(offset)
        offset += step_ms

    if offsets[-1] < last_valid:
        offsets.append(last_valid)

    return offsets


def _best_match_per_play(matches: list[AcoustIDMatch]) -> list[AcoustIDMatch]:
    """Select the highest-scoring AcoustID match for each play entry.

    When multiple fingerprints for the same ``play_id`` return matches,
    keep only the one with the highest score.

    Args:
        matches: All AcoustID matches from an archive hour.

    Returns:
        Deduplicated list with one match per play_id.
    """
    if not matches:
        return []

    best: dict[int | None, AcoustIDMatch] = {}
    for m in matches:
        existing = best.get(m.play_id)
        if existing is None or m.score > existing.score:
            best[m.play_id] = m

    return list(best.values())


async def fingerprint_and_lookup(
    wav_path: Path,
    offsets_ms: list[int],
    api_key: str,
    segment_duration_ms: int = DEFAULT_SEGMENT_DURATION_MS,
    match_threshold: float = DEFAULT_MATCH_THRESHOLD,
    play_ids: list[int | None] | None = None,
) -> list[AcoustIDMatch]:
    """Fingerprint audio segments and look up on AcoustID.

    For each offset, extracts a segment, computes a Chromaprint fingerprint
    via ``pyacoustid``, and submits it to the AcoustID API. Rate-limited
    to :data:`RATE_LIMIT_PER_SECOND` requests per second.

    Args:
        wav_path: Path to the decoded WAV file.
        offsets_ms: List of offsets (ms) to fingerprint.
        api_key: AcoustID API key.
        segment_duration_ms: Duration of each segment (ms).
        match_threshold: Minimum AcoustID score to accept.
        play_ids: Optional parallel list of play IDs for each offset.

    Returns:
        List of :class:`AcoustIDMatch` instances for successful lookups.
    """
    import acoustid

    from semantic_index.archive_client import ArchiveClient

    if play_ids is None:
        play_ids = [None] * len(offsets_ms)

    matches: list[AcoustIDMatch] = []
    semaphore = asyncio.Semaphore(RATE_LIMIT_PER_SECOND)
    last_request_time = 0.0

    async with httpx.AsyncClient(timeout=30.0) as client:
        for offset, pid in zip(offsets_ms, play_ids):
            segment_path = ArchiveClient.extract_segment(
                wav_path, offset, segment_duration_ms
            )
            try:
                duration, fingerprint = acoustid.fingerprint_file(str(segment_path))
            except Exception:
                logger.debug("Fingerprint failed at offset %d", offset)
                continue
            finally:
                segment_path.unlink(missing_ok=True)

            # Rate limiting
            async with semaphore:
                now = time.monotonic()
                wait = (1.0 / RATE_LIMIT_PER_SECOND) - (now - last_request_time)
                if wait > 0:
                    await asyncio.sleep(wait)
                last_request_time = time.monotonic()

                try:
                    resp = await client.get(
                        ACOUSTID_API_URL,
                        params={
                            "client": api_key,
                            "fingerprint": fingerprint,
                            "duration": str(int(duration)),
                            "meta": "recordings",
                            "format": "json",
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception:
                    logger.warning(
                        "AcoustID lookup failed at offset %d", offset, exc_info=True
                    )
                    continue

            for result in data.get("results", []):
                score = result.get("score", 0.0)
                if score < match_threshold:
                    continue
                for recording in result.get("recordings", []):
                    mbid = recording.get("id")
                    if mbid:
                        matches.append(
                            AcoustIDMatch(
                                offset_ms=offset,
                                recording_mbid=mbid,
                                score=score,
                                play_id=pid,
                            )
                        )
                        break  # one recording per result is enough

    return matches


# ---------------------------------------------------------------------------
# Checkpoint database
# ---------------------------------------------------------------------------


class CheckpointDB:
    """SQLite checkpoint database for archive processing progress.

    Tracks which archive hours have been processed and stores
    per-segment AcoustID match results.

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
                segments_fingerprinted INTEGER,
                segments_matched INTEGER,
                segments_extracted INTEGER,
                started_at TEXT,
                completed_at TEXT,
                error_msg TEXT
            );

            CREATE TABLE IF NOT EXISTS segment_match (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                archive_key TEXT NOT NULL,
                offset_ms INTEGER NOT NULL,
                duration_ms INTEGER NOT NULL,
                play_id INTEGER,
                artist_name TEXT,
                chromaprint TEXT,
                acoustid_mbid TEXT,
                acoustid_score REAL,
                essentia_extracted INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE (archive_key, offset_ms)
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

    def mark_hour_complete(
        self,
        archive_key: str,
        segments_fingerprinted: int,
        segments_matched: int,
        segments_extracted: int,
    ) -> None:
        """Record that an archive hour has been fully processed."""
        conn = self._get_conn()
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "UPDATE hour_progress SET status = 'complete', "
            "segments_fingerprinted = ?, segments_matched = ?, "
            "segments_extracted = ?, completed_at = ? "
            "WHERE archive_key = ?",
            (segments_fingerprinted, segments_matched, segments_extracted, now, archive_key),
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

    def save_segment_match(
        self,
        archive_key: str,
        match: AcoustIDMatch,
        duration_ms: int,
        artist_name: str | None = None,
        chromaprint: str | None = None,
    ) -> None:
        """Save an AcoustID match result for a segment.

        Uses ``INSERT OR IGNORE`` for idempotent re-runs.
        """
        conn = self._get_conn()
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO segment_match "
            "(archive_key, offset_ms, duration_ms, play_id, artist_name, "
            "chromaprint, acoustid_mbid, acoustid_score, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                archive_key,
                match.offset_ms,
                duration_ms,
                match.play_id,
                artist_name,
                chromaprint,
                match.recording_mbid,
                match.score,
                now,
            ),
        )
        conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
