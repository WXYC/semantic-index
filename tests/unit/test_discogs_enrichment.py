"""Tests for the Discogs enrichment module."""

import logging
from unittest.mock import MagicMock

from semantic_index.discogs_client import DiscogsClient
from semantic_index.discogs_enrichment import DiscogsEnricher
from semantic_index.models import DiscogsSearchResult


def _mock_client(
    artist_name: str = "Autechre",
    artist_id: int = 42,
    release_ids: list[int] | None = None,
    styles: list[str] | None = None,
    extra_artists: list[tuple[str, str | None]] | None = None,
    labels: list[tuple[int | None, str]] | None = None,
    track_artists: list[tuple[int, str]] | None = None,
) -> MagicMock:
    """Create a mock DiscogsClient with bulk enrichment data."""
    client = MagicMock(spec=DiscogsClient)
    client.search_artist.return_value = DiscogsSearchResult(
        artist_name=artist_name, artist_id=artist_id
    )
    client.get_releases_for_artist.return_value = release_ids or [12345]
    client.get_enrichment_for_artist.return_value = {
        "styles": styles or [],
        "extra_artists": extra_artists or [],
        "labels": labels or [],
        "track_artists": track_artists or [],
    }
    return client


class TestSingleReleaseStyles:
    def test_styles_from_single_release(self):
        client = _mock_client(styles=["IDM", "Abstract"])
        result = DiscogsEnricher(client).enrich_artist("Autechre")

        assert result is not None
        assert sorted(result.styles) == ["Abstract", "IDM"]

    def test_styles_deduplicated_across_releases(self):
        client = _mock_client(
            release_ids=[12345, 67890],
            styles=["IDM", "Abstract", "IDM", "Ambient"],
        )
        result = DiscogsEnricher(client).enrich_artist("Autechre")

        assert result is not None
        assert sorted(result.styles) == ["Abstract", "Ambient", "IDM"]


class TestMultiReleaseAggregation:
    def test_labels_deduplicated_by_name(self):
        client = _mock_client(
            release_ids=[12345, 67890],
            labels=[(100, "Warp Records"), (100, "Warp Records"), (200, "Skam")],
        )
        result = DiscogsEnricher(client).enrich_artist("Autechre")

        assert result is not None
        label_names = [lbl.name for lbl in result.labels]
        assert "Warp Records" in label_names
        assert "Skam" in label_names
        assert len(result.labels) == 2

    def test_personnel_deduplicated_with_roles_merged(self):
        client = _mock_client(
            extra_artists=[
                ("Rob Brown", "Written-By"),
                ("Rob Brown", "Performer"),
                ("Sean Booth", "Written-By"),
            ],
        )
        result = DiscogsEnricher(client).enrich_artist("Autechre")

        assert result is not None
        rob = next(p for p in result.personnel if p.name == "Rob Brown")
        assert "Written-By" in rob.roles
        assert "Performer" in rob.roles
        sean = next(p for p in result.personnel if p.name == "Sean Booth")
        assert "Written-By" in sean.roles


class TestExtraArtists:
    def test_extra_artists_become_personnel(self):
        client = _mock_client(
            extra_artists=[("Jonathan Wilson", "Producer")],
        )
        result = DiscogsEnricher(client).enrich_artist("Autechre")

        assert result is not None
        assert len(result.personnel) == 1
        assert result.personnel[0].name == "Jonathan Wilson"
        assert "Producer" in result.personnel[0].roles

    def test_extra_artist_with_no_role(self):
        client = _mock_client(
            extra_artists=[("Unknown Person", None)],
        )
        result = DiscogsEnricher(client).enrich_artist("Autechre")

        assert result is not None
        assert len(result.personnel) == 1
        assert result.personnel[0].roles == []


class TestCompilationDetection:
    def test_tracks_with_per_track_artists_detected_as_compilation(self):
        client = _mock_client(
            track_artists=[
                (12345, "Stereolab"),
                (12345, "Broadcast"),
            ],
        )
        result = DiscogsEnricher(client).enrich_artist("Autechre")

        assert result is not None
        assert len(result.compilation_appearances) == 1
        assert "Stereolab" in result.compilation_appearances[0].other_artists
        assert "Broadcast" in result.compilation_appearances[0].other_artists

    def test_regular_album_not_detected_as_compilation(self):
        client = _mock_client(track_artists=[])
        result = DiscogsEnricher(client).enrich_artist("Autechre")

        assert result is not None
        assert len(result.compilation_appearances) == 0


class TestNoMatch:
    def test_no_search_result_returns_none(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = None

        result = DiscogsEnricher(client).enrich_artist("Unknown Artist")
        assert result is None

    def test_search_found_but_no_releases_returns_empty_enrichment(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Autechre", artist_id=42
        )
        client.get_releases_for_artist.return_value = []

        result = DiscogsEnricher(client).enrich_artist("Autechre")
        assert result is not None
        assert result.styles == []
        assert result.personnel == []


class TestEnrichWithKnownId:
    def test_known_id_skips_search(self):
        client = _mock_client(styles=["IDM"])
        result = DiscogsEnricher(client).enrich_artist("Autechre", discogs_artist_id=42)

        assert result is not None
        client.search_artist.assert_not_called()


class TestBatchEnrichment:
    def test_batch_enriches_multiple_artists(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Autechre", artist_id=42
        )
        client.get_releases_for_artist.return_value = [12345]
        client.get_enrichment_for_artist.return_value = {
            "styles": ["IDM"],
            "extra_artists": [],
            "labels": [(100, "Warp Records")],
            "track_artists": [],
        }

        results = DiscogsEnricher(client).enrich_batch({"Autechre": None, "Stereolab": None})
        assert len(results) == 2

    def test_batch_skips_failed_artists(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = None

        results = DiscogsEnricher(client).enrich_batch({"Unknown": None})
        assert len(results) == 0

    def test_batch_uses_known_ids(self):
        client = MagicMock(spec=DiscogsClient)
        client.get_releases_for_artist.return_value = [12345]
        client.get_enrichment_for_artist.return_value = {
            "styles": [],
            "extra_artists": [],
            "labels": [],
            "track_artists": [],
        }

        results = DiscogsEnricher(client).enrich_batch({"Autechre": 42})
        assert len(results) == 1
        client.search_artist.assert_not_called()


class TestProgressLogging:
    def test_logs_progress_at_intervals(self, caplog):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = None

        artists = {f"Artist{i}": None for i in range(1000)}
        with caplog.at_level(logging.INFO):
            DiscogsEnricher(client).enrich_batch(artists)

        progress_msgs = [r for r in caplog.records if "Enriched" in r.message and "/" in r.message]
        assert len(progress_msgs) == 2  # at 500 and final

    def test_logs_final_summary(self, caplog):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = None

        with caplog.at_level(logging.INFO):
            DiscogsEnricher(client).enrich_batch({"A": None})

        assert any("Enrichment complete" in r.message for r in caplog.records)
