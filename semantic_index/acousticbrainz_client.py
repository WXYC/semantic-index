"""AcousticBrainz PostgreSQL client for audio feature retrieval.

Queries ``ab_recording`` in the musicbrainz-cache PostgreSQL database,
joining with ``mb_artist_recording`` for per-artist feature retrieval.
Replaces the two-step MusicBrainzClient + TarLoader flow with a single
JOIN query.
"""

import json
import logging

import psycopg
import psycopg.errors

from semantic_index.acousticbrainz import (
    GENRE_ELECTRONIC_LABELS,
    GENRE_LABELS,
    GENRE_ROSAMERICA_LABELS,
    GENRE_TZANETAKIS_LABELS,
    MIREX_LABELS,
    RHYTHM_LABELS,
    RecordingFeatures,
)

logger = logging.getLogger(__name__)

# Column order in the SELECT query — must match _parse_row()
_SELECT_COLS = """
    mar.artist_id,
    ar.recording_mbid::text,
    ar.danceability,
    ar.gender_value,
    ar.gender_probability,
    ar.genre_dortmund_value,
    ar.genre_dortmund_prob,
    ar.genre_electronic_value,
    ar.genre_electronic_prob,
    ar.genre_rosamerica_value,
    ar.genre_rosamerica_prob,
    ar.genre_tzanetakis_value,
    ar.genre_tzanetakis_prob,
    ar.ismir04_rhythm_value,
    ar.ismir04_rhythm_prob,
    ar.mood_acoustic,
    ar.mood_aggressive,
    ar.mood_electronic,
    ar.mood_happy,
    ar.mood_party,
    ar.mood_relaxed,
    ar.mood_sad,
    ar.moods_mirex_value,
    ar.moods_mirex_prob,
    ar.timbre_value,
    ar.timbre_probability,
    ar.tonal,
    ar.voice_instrumental_value,
    ar.voice_instrumental_prob,
    ar.classifier_distributions
"""


class AcousticBrainzClient:
    """PostgreSQL client for AcousticBrainz features.

    Queries ``ab_recording`` joined with ``mb_artist_recording`` to
    retrieve per-artist audio features in a single query.

    Args:
        cache_dsn: PostgreSQL connection string for the musicbrainz database.
    """

    def __init__(self, cache_dsn: str) -> None:
        self._cache_dsn = cache_dsn
        self._conn: psycopg.Connection | None = None

    def _get_conn(self) -> psycopg.Connection | None:
        """Get or create the PostgreSQL connection."""
        if self._conn is None or self._conn.closed:
            try:
                self._conn = psycopg.connect(self._cache_dsn, autocommit=True)
            except Exception:
                logger.warning("Failed to connect to musicbrainz database", exc_info=True)
                return None
        return self._conn

    def resolve_gids_to_ids(self, gids: list[str]) -> dict[str, int]:
        """Resolve MusicBrainz artist GIDs (UUIDs) to internal integer IDs.

        Queries ``mb_artist`` using the ``gid`` column added by issue #153.
        Unresolved GIDs are silently omitted from the result.

        Args:
            gids: List of MusicBrainz artist UUID strings.

        Returns:
            Dict mapping GID string to internal integer ID.
        """
        if not gids:
            return {}

        conn = self._get_conn()
        if conn is None:
            return {}

        try:
            result: dict[str, int] = {}
            batch_size = 1000
            for i in range(0, len(gids), batch_size):
                batch = gids[i : i + batch_size]
                rows = conn.execute(
                    "SELECT id, gid::text FROM mb_artist WHERE gid = ANY(%s::uuid[])",
                    (batch,),
                ).fetchall()
                for int_id, gid_str in rows:
                    result[gid_str] = int_id

            logger.info(
                "GID resolution: %d/%d GIDs resolved to integer IDs",
                len(result),
                len(gids),
            )
            return result
        except psycopg.errors.UndefinedColumn:
            logger.warning("mb_artist.gid column not found — run #153 migration first")
            return {}
        except Exception:
            logger.warning("GID resolution failed", exc_info=True)
            return {}

    def get_features_for_artists(
        self, mb_artist_ids: list[int]
    ) -> dict[int, list[RecordingFeatures]]:
        """Get audio features for a set of MusicBrainz artist IDs.

        Joins ``ab_recording`` with ``mb_artist_recording`` to retrieve
        all recordings per artist in a single query.

        Args:
            mb_artist_ids: List of MusicBrainz internal artist IDs.

        Returns:
            Dict mapping artist ID to list of RecordingFeatures.
        """
        if not mb_artist_ids:
            return {}

        conn = self._get_conn()
        if conn is None:
            return {}

        try:
            result: dict[int, list[RecordingFeatures]] = {}
            batch_size = 1000
            for i in range(0, len(mb_artist_ids), batch_size):
                batch = mb_artist_ids[i : i + batch_size]
                rows = conn.execute(
                    f"SELECT {_SELECT_COLS} "
                    "FROM mb_artist_recording mar "
                    "JOIN ab_recording ar ON ar.recording_mbid = mar.recording_mbid "
                    "WHERE mar.artist_id = ANY(%s)",
                    (batch,),
                ).fetchall()

                for row in rows:
                    artist_id = row[0]
                    features = _parse_row(row)
                    result.setdefault(artist_id, []).append(features)

                if (i + batch_size) % 5000 == 0:
                    logger.info(
                        "  AB feature lookup: %d/%d artist batches",
                        i // batch_size + 1,
                        (len(mb_artist_ids) + batch_size - 1) // batch_size,
                    )

            total_recordings = sum(len(v) for v in result.values())
            logger.info(
                "AcousticBrainz features: %d recordings across %d artists",
                total_recordings,
                len(result),
            )
            return result
        except Exception:
            logger.warning("AcousticBrainz feature lookup failed", exc_info=True)
            return {}


def _parse_row(row: tuple) -> RecordingFeatures:
    """Parse a PG result row into RecordingFeatures.

    Uses the ``classifier_distributions`` JSONB column for probability
    vectors, and structured columns for top-level values.
    """
    (
        _artist_id,
        recording_mbid,
        danceability,
        gender_value,
        gender_probability,
        genre_dortmund_value,
        genre_dortmund_prob,
        _genre_electronic_value,
        _genre_electronic_prob,
        _genre_rosamerica_value,
        _genre_rosamerica_prob,
        _genre_tzanetakis_value,
        _genre_tzanetakis_prob,
        _ismir04_rhythm_value,
        _ismir04_rhythm_prob,
        mood_acoustic,
        mood_aggressive,
        mood_electronic,
        mood_happy,
        mood_party,
        mood_relaxed,
        mood_sad,
        _moods_mirex_value,
        _moods_mirex_prob,
        timbre_value,
        timbre_probability,
        tonal,
        voice_instrumental_value,
        voice_instrumental_prob,
        classifier_distributions_raw,
    ) = row

    # Parse JSONB distributions
    if isinstance(classifier_distributions_raw, str):
        dists = json.loads(classifier_distributions_raw)
    else:
        dists = classifier_distributions_raw

    genre_all = dists.get("genre_dortmund", {})
    genre_vector = [genre_all.get(label, 0.0) for label in GENRE_LABELS]

    mirex_all = dists.get("moods_mirex", {})
    mirex_vector = [mirex_all.get(label, 0.0) for label in MIREX_LABELS]

    rhythm_all = dists.get("ismir04_rhythm", {})
    rhythm_vector = [rhythm_all.get(label, 0.0) for label in RHYTHM_LABELS]

    gender_all_dist = dists.get("gender", {})
    gender_female = gender_all_dist.get("female", 0.5)

    genre_electronic_all = dists.get("genre_electronic", {})
    genre_electronic_vector = [
        genre_electronic_all.get(label, 0.0) for label in GENRE_ELECTRONIC_LABELS
    ]

    genre_rosamerica_all = dists.get("genre_rosamerica", {})
    genre_rosamerica_vector = [
        genre_rosamerica_all.get(label, 0.0) for label in GENRE_ROSAMERICA_LABELS
    ]

    genre_tzanetakis_all = dists.get("genre_tzanetakis", {})
    genre_tzanetakis_vector = [
        genre_tzanetakis_all.get(label, 0.0) for label in GENRE_TZANETAKIS_LABELS
    ]

    mood_vector = [
        mood_acoustic,
        mood_aggressive,
        mood_electronic,
        mood_happy,
        mood_party,
        mood_relaxed,
        mood_sad,
    ]

    return RecordingFeatures(
        recording_mbid=recording_mbid,
        danceability=danceability,
        genre=genre_dortmund_value,
        genre_probability=genre_dortmund_prob,
        genre_vector=genre_vector,
        mood_vector=mood_vector,
        mirex_vector=mirex_vector,
        rhythm_vector=rhythm_vector,
        gender_female=gender_female,
        timbre=timbre_value,
        timbre_probability=timbre_probability,
        tonal=tonal,
        voice_instrumental=voice_instrumental_value,
        voice_instrumental_probability=voice_instrumental_prob,
        genre_electronic_vector=genre_electronic_vector,
        genre_rosamerica_vector=genre_rosamerica_vector,
        genre_tzanetakis_vector=genre_tzanetakis_vector,
    )
