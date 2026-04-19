"""Tests for AcousticBrainz PostgreSQL client."""

from unittest.mock import MagicMock, patch

import pytest

from semantic_index.acousticbrainz import FEATURE_VECTOR_DIM, RecordingFeatures
from semantic_index.acousticbrainz_client import AcousticBrainzClient

MBID_VOGUE = "33f7dd17-1b3e-4f1a-a60c-c0e32e48e9a0"
MBID_MATERIAL = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _make_distributions() -> dict:
    """Build a classifier_distributions JSONB value matching the PG schema."""
    return {
        "genre_dortmund": {
            "alternative": 0.01,
            "blues": 0.01,
            "electronic": 0.90,
            "folkcountry": 0.01,
            "funksoulrnb": 0.02,
            "jazz": 0.01,
            "pop": 0.02,
            "raphiphop": 0.01,
            "rock": 0.01,
        },
        "genre_electronic": {
            "ambient": 0.10,
            "dnb": 0.05,
            "house": 0.60,
            "techno": 0.20,
            "trance": 0.05,
        },
        "genre_rosamerica": {
            "cla": 0.03,
            "dan": 0.40,
            "hip": 0.02,
            "jaz": 0.05,
            "pop": 0.20,
            "rhy": 0.10,
            "roc": 0.10,
            "spe": 0.10,
        },
        "genre_tzanetakis": {
            "blu": 0.03,
            "cla": 0.02,
            "cou": 0.02,
            "dis": 0.30,
            "hip": 0.05,
            "jaz": 0.03,
            "met": 0.02,
            "pop": 0.30,
            "reg": 0.03,
            "roc": 0.20,
        },
        "moods_mirex": {
            "Cluster1": 0.15,
            "Cluster2": 0.25,
            "Cluster3": 0.20,
            "Cluster4": 0.30,
            "Cluster5": 0.10,
        },
        "ismir04_rhythm": {
            "ChaChaCha": 0.05,
            "Jive": 0.15,
            "Quickstep": 0.10,
            "Rumba-American": 0.05,
            "Rumba-International": 0.05,
            "Rumba-Misc": 0.05,
            "Samba": 0.20,
            "Tango": 0.15,
            "VienneseWaltz": 0.10,
            "Waltz": 0.10,
        },
        "gender": {"female": 0.70, "male": 0.30},
    }


def _make_pg_row(
    *,
    mbid: str = MBID_VOGUE,
    artist_id: int = 42,
) -> tuple:
    """Build a mock PG row matching the client's SELECT column order."""
    import json

    return (
        artist_id,  # mar.artist_id
        mbid,  # ar.recording_mbid
        0.75,  # danceability
        "female",  # gender_value
        0.70,  # gender_probability
        "electronic",  # genre_dortmund_value
        0.90,  # genre_dortmund_prob
        "house",  # genre_electronic_value
        0.60,  # genre_electronic_prob
        "dan",  # genre_rosamerica_value
        0.40,  # genre_rosamerica_prob
        "dis",  # genre_tzanetakis_value
        0.30,  # genre_tzanetakis_prob
        "Jive",  # ismir04_rhythm_value
        0.15,  # ismir04_rhythm_prob
        0.20,  # mood_acoustic
        0.10,  # mood_aggressive
        0.80,  # mood_electronic
        0.60,  # mood_happy
        0.70,  # mood_party
        0.30,  # mood_relaxed
        0.15,  # mood_sad
        "Cluster4",  # moods_mirex_value
        0.30,  # moods_mirex_prob
        "bright",  # timbre_value
        0.85,  # timbre_probability
        0.65,  # tonal
        "voice",  # voice_instrumental_value
        0.80,  # voice_instrumental_prob
        json.dumps(_make_distributions()),  # classifier_distributions
    )


class TestAcousticBrainzClient:
    """Test AcousticBrainz PostgreSQL client."""

    def test_get_features_for_artists_parses_recording(self) -> None:
        """RecordingFeatures is correctly parsed from a PG row."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        row = _make_pg_row()

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [row]

        with patch.object(client, "_get_conn", return_value=mock_conn):
            result = client.get_features_for_artists([42])

        assert 42 in result
        recordings = result[42]
        assert len(recordings) == 1
        rf = recordings[0]
        assert isinstance(rf, RecordingFeatures)
        assert rf.recording_mbid == MBID_VOGUE
        assert rf.danceability == pytest.approx(0.75)
        assert rf.genre == "electronic"
        assert rf.timbre == "bright"
        assert rf.voice_instrumental == "voice"

    def test_feature_vector_has_59_dims(self) -> None:
        """Features parsed from PG produce a 59-dim vector."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        row = _make_pg_row()

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [row]

        with patch.object(client, "_get_conn", return_value=mock_conn):
            result = client.get_features_for_artists([42])

        rf = result[42][0]
        vec = rf.feature_vector()
        assert len(vec) == FEATURE_VECTOR_DIM

    def test_multiple_artists(self) -> None:
        """Multiple artists' recordings are grouped correctly."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        row1 = _make_pg_row(mbid=MBID_VOGUE, artist_id=42)
        row2 = _make_pg_row(mbid=MBID_MATERIAL, artist_id=99)

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [row1, row2]

        with patch.object(client, "_get_conn", return_value=mock_conn):
            result = client.get_features_for_artists([42, 99])

        assert len(result) == 2
        assert len(result[42]) == 1
        assert len(result[99]) == 1
        assert result[42][0].recording_mbid == MBID_VOGUE
        assert result[99][0].recording_mbid == MBID_MATERIAL

    def test_empty_artist_list(self) -> None:
        """Empty input returns empty dict without querying."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        result = client.get_features_for_artists([])
        assert result == {}

    def test_connection_failure_returns_empty(self) -> None:
        """Connection failure returns empty dict gracefully."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/nonexistent")

        with patch.object(client, "_get_conn", return_value=None):
            result = client.get_features_for_artists([42])

        assert result == {}


# MusicBrainz artist GIDs used in resolve_gids_to_ids tests
GID_AUTECHRE = "410c9baf-5469-44f6-9852-826524b80c61"
GID_STEREOLAB = "f22942a1-6f70-4f48-866e-238cb2308fbd"
GID_NONEXISTENT = "00000000-0000-0000-0000-000000000000"


class TestResolveGidsToIds:
    """Test GID-to-integer-ID resolution via mb_artist."""

    def test_resolve_gids_to_ids_basic(self) -> None:
        """GIDs are resolved to integer IDs via mb_artist."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            (12345, GID_AUTECHRE),
            (67890, GID_STEREOLAB),
        ]

        with patch.object(client, "_get_conn", return_value=mock_conn):
            result = client.resolve_gids_to_ids([GID_AUTECHRE, GID_STEREOLAB])

        assert result == {GID_AUTECHRE: 12345, GID_STEREOLAB: 67890}

    def test_resolve_gids_to_ids_missing_gids(self) -> None:
        """GIDs not found in mb_artist are silently omitted."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        mock_conn = MagicMock()
        # Only one of two GIDs found
        mock_conn.execute.return_value.fetchall.return_value = [
            (12345, GID_AUTECHRE),
        ]

        with patch.object(client, "_get_conn", return_value=mock_conn):
            result = client.resolve_gids_to_ids([GID_AUTECHRE, GID_NONEXISTENT])

        assert result == {GID_AUTECHRE: 12345}
        assert GID_NONEXISTENT not in result

    def test_resolve_gids_to_ids_empty_input(self) -> None:
        """Empty input returns empty dict without querying."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        result = client.resolve_gids_to_ids([])
        assert result == {}

    def test_resolve_gids_to_ids_connection_failure(self) -> None:
        """Connection failure returns empty dict gracefully."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/nonexistent")

        with patch.object(client, "_get_conn", return_value=None):
            result = client.resolve_gids_to_ids([GID_AUTECHRE])

        assert result == {}

    def test_resolve_gids_to_ids_missing_gid_column(self) -> None:
        """Missing gid column (pre-#153) returns empty dict with warning."""
        from psycopg.errors import UndefinedColumn

        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = UndefinedColumn("column mb_artist.gid does not exist")

        with patch.object(client, "_get_conn", return_value=mock_conn):
            result = client.resolve_gids_to_ids([GID_AUTECHRE])

        assert result == {}

    def test_resolve_gids_to_ids_batching(self) -> None:
        """GIDs are resolved in batches of 1,000."""
        client = AcousticBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        mock_conn = MagicMock()
        # Return empty for each batch call
        mock_conn.execute.return_value.fetchall.return_value = []

        gids = [f"00000000-0000-0000-0000-{i:012d}" for i in range(2500)]

        with patch.object(client, "_get_conn", return_value=mock_conn):
            client.resolve_gids_to_ids(gids)

        # 2500 GIDs / 1000 batch size = 3 batches
        assert mock_conn.execute.call_count == 3
