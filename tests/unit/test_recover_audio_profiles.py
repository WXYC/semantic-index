"""Tests for the audio profile recovery script."""

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

from scripts.recover_audio_profiles import find_recovery_candidates, recover
from semantic_index.acousticbrainz import FEATURE_VECTOR_DIM

# MusicBrainz GIDs (UUIDs) — the format stored in artist.musicbrainz_artist_id
GID_AUTECHRE = "410c9baf-5469-44f6-9852-826524b80c61"
GID_STEREOLAB = "f22942a1-6f70-4f48-866e-238cb2308fbd"
GID_CAT_POWER = "7a47cd48-1a5e-4f0d-8db1-3985f944c3e4"


def _make_test_db(tmp_path: Path, *, with_profiles: dict[int, bool] | None = None) -> Path:
    """Create a minimal SQLite DB with artist and audio_profile tables.

    Args:
        tmp_path: pytest tmp_path fixture.
        with_profiles: Dict of artist_id -> has_profile. If None, uses defaults.
    """
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE artist ("
        "  id INTEGER PRIMARY KEY,"
        "  canonical_name TEXT NOT NULL UNIQUE,"
        "  musicbrainz_artist_id TEXT"
        ")"
    )
    conn.execute(
        "CREATE TABLE audio_profile ("
        "  artist_id INTEGER PRIMARY KEY,"
        "  avg_danceability REAL,"
        "  primary_genre TEXT,"
        "  primary_genre_probability REAL,"
        "  voice_instrumental_ratio REAL,"
        "  feature_centroid TEXT,"
        "  recording_count INTEGER NOT NULL DEFAULT 0,"
        "  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))"
        ")"
    )
    conn.execute(
        "CREATE TABLE acoustic_similarity ("
        "  artist_a_id INTEGER NOT NULL,"
        "  artist_b_id INTEGER NOT NULL,"
        "  similarity REAL NOT NULL,"
        "  PRIMARY KEY (artist_a_id, artist_b_id)"
        ")"
    )

    # Insert artists: 1 and 2 have MB GIDs, 3 does not
    conn.execute("INSERT INTO artist VALUES (1, 'Autechre', ?)", (GID_AUTECHRE,))
    conn.execute("INSERT INTO artist VALUES (2, 'Stereolab', ?)", (GID_STEREOLAB,))
    conn.execute("INSERT INTO artist VALUES (3, 'Cat Power', NULL)")

    if with_profiles is None:
        with_profiles = {}

    centroid = json.dumps([0.5] * FEATURE_VECTOR_DIM)
    for artist_id, has_profile in with_profiles.items():
        if has_profile:
            conn.execute(
                "INSERT INTO audio_profile "
                "(artist_id, avg_danceability, primary_genre, primary_genre_probability, "
                "voice_instrumental_ratio, feature_centroid, recording_count) "
                "VALUES (?, 0.6, 'electronic', 0.9, 0.3, ?, 5)",
                (artist_id, centroid),
            )

    conn.commit()
    conn.close()
    return db_path


def _make_mock_recording_features():
    """Create a mock RecordingFeatures-compatible object for build_audio_profiles_from_features."""
    from semantic_index.acousticbrainz import RecordingFeatures

    return RecordingFeatures(
        recording_mbid="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        danceability=0.7,
        genre="electronic",
        genre_probability=0.9,
        genre_vector=[0.01, 0.01, 0.90, 0.01, 0.02, 0.01, 0.02, 0.01, 0.01],
        mood_vector=[0.2, 0.1, 0.8, 0.6, 0.7, 0.3, 0.15],
        mirex_vector=[0.15, 0.25, 0.20, 0.30, 0.10],
        rhythm_vector=[0.05, 0.15, 0.10, 0.05, 0.05, 0.05, 0.20, 0.15, 0.10, 0.10],
        gender_female=0.7,
        timbre="bright",
        timbre_probability=0.85,
        tonal=0.65,
        voice_instrumental="voice",
        voice_instrumental_probability=0.8,
        genre_electronic_vector=[0.10, 0.05, 0.60, 0.20, 0.05],
        genre_rosamerica_vector=[0.03, 0.40, 0.02, 0.05, 0.20, 0.10, 0.10, 0.10],
        genre_tzanetakis_vector=[0.03, 0.02, 0.02, 0.30, 0.05, 0.03, 0.02, 0.30, 0.03, 0.20],
    )


class TestFindRecoveryCandidates:
    """Test the SQL query that finds recovery candidates."""

    def test_finds_artists_with_gids_and_no_profile(self, tmp_path: Path) -> None:
        """Artists with MB GIDs but no audio profile are returned."""
        db_path = _make_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        candidates = find_recovery_candidates(conn)
        conn.close()

        # Artists 1 and 2 have GIDs, no profiles. Artist 3 has no GID.
        assert len(candidates) == 2
        ids = {c[0] for c in candidates}
        assert ids == {1, 2}

    def test_skips_artists_with_existing_profiles(self, tmp_path: Path) -> None:
        """Artists that already have profiles are not returned."""
        db_path = _make_test_db(tmp_path, with_profiles={1: True})
        conn = sqlite3.connect(str(db_path))
        candidates = find_recovery_candidates(conn)
        conn.close()

        # Only artist 2 lacks a profile
        assert len(candidates) == 1
        assert candidates[0][0] == 2

    def test_no_candidates_when_all_have_profiles(self, tmp_path: Path) -> None:
        """Returns empty when all GID-having artists already have profiles."""
        db_path = _make_test_db(tmp_path, with_profiles={1: True, 2: True})
        conn = sqlite3.connect(str(db_path))
        candidates = find_recovery_candidates(conn)
        conn.close()

        assert candidates == []


class TestRecover:
    """Test the full recovery pipeline."""

    def test_uuid_strings_resolved_correctly(self, tmp_path: Path) -> None:
        """Regression: UUID strings in musicbrainz_artist_id are resolved
        via resolve_gids_to_ids, not cast with int() (the original bug)."""
        db_path = _make_test_db(tmp_path)
        mock_features = {12345: [_make_mock_recording_features()] * 3}

        with (
            patch("semantic_index.acousticbrainz_client.AcousticBrainzClient") as mock_client_cls,
        ):
            client = mock_client_cls.return_value
            client.resolve_gids_to_ids.return_value = {GID_AUTECHRE: 12345}
            client.get_features_for_artists.return_value = mock_features

            stats = recover(
                db_path=str(db_path),
                musicbrainz_cache_dsn="postgresql://localhost/musicbrainz",
                min_recordings=1,
                dry_run=True,
            )

        # resolve_gids_to_ids was called with UUID strings, not int()
        call_args = client.resolve_gids_to_ids.call_args[0][0]
        for gid in call_args:
            assert "-" in gid, f"Expected UUID string, got {gid}"

        assert stats["new_profiles"] >= 1

    def test_stores_new_profiles(self, tmp_path: Path) -> None:
        """New profiles are persisted to the database."""
        db_path = _make_test_db(tmp_path)
        mock_features = {
            12345: [_make_mock_recording_features()] * 3,
            67890: [_make_mock_recording_features()] * 3,
        }

        with patch("semantic_index.acousticbrainz_client.AcousticBrainzClient") as mock_client_cls:
            client = mock_client_cls.return_value
            client.resolve_gids_to_ids.return_value = {
                GID_AUTECHRE: 12345,
                GID_STEREOLAB: 67890,
            }
            client.get_features_for_artists.return_value = mock_features

            stats = recover(
                db_path=str(db_path),
                musicbrainz_cache_dsn="postgresql://localhost/musicbrainz",
                min_recordings=1,
            )

        assert stats["profiles_before"] == 0
        assert stats["profiles_after"] == 2
        assert stats["new_profiles"] == 2

    def test_recomputes_similarity(self, tmp_path: Path) -> None:
        """Acoustic similarity is recomputed after adding new profiles."""
        db_path = _make_test_db(tmp_path)
        mock_features = {
            12345: [_make_mock_recording_features()] * 3,
            67890: [_make_mock_recording_features()] * 3,
        }

        with patch("semantic_index.acousticbrainz_client.AcousticBrainzClient") as mock_client_cls:
            client = mock_client_cls.return_value
            client.resolve_gids_to_ids.return_value = {
                GID_AUTECHRE: 12345,
                GID_STEREOLAB: 67890,
            }
            client.get_features_for_artists.return_value = mock_features

            stats = recover(
                db_path=str(db_path),
                musicbrainz_cache_dsn="postgresql://localhost/musicbrainz",
                min_recordings=1,
                similarity_threshold=0.5,
            )

        # Two identical profiles should produce a similarity edge
        assert stats["similarity_after"] > 0

    def test_dry_run_does_not_swap(self, tmp_path: Path) -> None:
        """Dry run creates profiles in temp but does not swap to production."""
        db_path = _make_test_db(tmp_path)
        mock_features = {12345: [_make_mock_recording_features()] * 3}

        with patch("semantic_index.acousticbrainz_client.AcousticBrainzClient") as mock_client_cls:
            client = mock_client_cls.return_value
            client.resolve_gids_to_ids.return_value = {GID_AUTECHRE: 12345}
            client.get_features_for_artists.return_value = mock_features

            stats = recover(
                db_path=str(db_path),
                musicbrainz_cache_dsn="postgresql://localhost/musicbrainz",
                min_recordings=1,
                dry_run=True,
            )

        assert stats["new_profiles"] >= 1

        # Production DB should still have 0 profiles (dry run)
        conn = sqlite3.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM audio_profile").fetchone()[0]
        conn.close()
        assert count == 0

    def test_no_candidates_exits_early(self, tmp_path: Path) -> None:
        """When all artists have profiles, recovery exits early without querying PG."""
        db_path = _make_test_db(tmp_path, with_profiles={1: True, 2: True})

        # Should not even instantiate the client
        stats = recover(
            db_path=str(db_path),
            musicbrainz_cache_dsn="postgresql://localhost/musicbrainz",
        )

        assert stats["candidates"] == 0
        assert stats["new_profiles"] == 0
