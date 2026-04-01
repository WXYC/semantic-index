"""Tests for the Discogs enrichment module."""

import logging
from unittest.mock import MagicMock

from semantic_index.discogs_client import DiscogsClient
from semantic_index.discogs_enrichment import DiscogsEnricher


def _mock_client(bulk_data: dict | None = None) -> MagicMock:
    """Create a mock DiscogsClient with bulk enrichment data."""
    client = MagicMock(spec=DiscogsClient)
    client.get_bulk_enrichment.return_value = bulk_data or {}
    return client


class TestSingleReleaseStyles:
    def test_styles_from_single_release(self):
        client = _mock_client(
            {
                "Autechre": {
                    "styles": ["IDM", "Abstract"],
                    "extra_artists": [],
                    "labels": [],
                    "track_artists": [],
                }
            }
        )
        result = DiscogsEnricher(client).enrich_batch({"Autechre": None})

        assert "Autechre" in result
        assert sorted(result["Autechre"].styles) == ["Abstract", "IDM"]

    def test_styles_deduplicated(self):
        client = _mock_client(
            {
                "Autechre": {
                    "styles": ["IDM", "Abstract", "IDM", "Ambient"],
                    "extra_artists": [],
                    "labels": [],
                    "track_artists": [],
                }
            }
        )
        result = DiscogsEnricher(client).enrich_batch({"Autechre": None})

        assert sorted(result["Autechre"].styles) == ["Abstract", "Ambient", "IDM"]


class TestMultiReleaseAggregation:
    def test_labels_deduplicated_by_name(self):
        client = _mock_client(
            {
                "Autechre": {
                    "styles": [],
                    "extra_artists": [],
                    "labels": [(100, "Warp Records"), (100, "Warp Records"), (200, "Skam")],
                    "track_artists": [],
                }
            }
        )
        result = DiscogsEnricher(client).enrich_batch({"Autechre": None})

        label_names = [lbl.name for lbl in result["Autechre"].labels]
        assert "Warp Records" in label_names
        assert "Skam" in label_names
        assert len(result["Autechre"].labels) == 2

    def test_personnel_deduplicated_with_roles_merged(self):
        client = _mock_client(
            {
                "Autechre": {
                    "styles": [],
                    "extra_artists": [
                        ("Rob Brown", "Written-By"),
                        ("Rob Brown", "Performer"),
                        ("Sean Booth", "Written-By"),
                    ],
                    "labels": [],
                    "track_artists": [],
                }
            }
        )
        result = DiscogsEnricher(client).enrich_batch({"Autechre": None})

        rob = next(p for p in result["Autechre"].personnel if p.name == "Rob Brown")
        assert "Written-By" in rob.roles
        assert "Performer" in rob.roles


class TestExtraArtists:
    def test_extra_artists_become_personnel(self):
        client = _mock_client(
            {
                "FJM": {
                    "styles": [],
                    "extra_artists": [("Jonathan Wilson", "Producer")],
                    "labels": [],
                    "track_artists": [],
                }
            }
        )
        result = DiscogsEnricher(client).enrich_batch({"FJM": None})

        assert len(result["FJM"].personnel) == 1
        assert result["FJM"].personnel[0].name == "Jonathan Wilson"

    def test_extra_artist_with_no_role(self):
        client = _mock_client(
            {
                "A": {
                    "styles": [],
                    "extra_artists": [("Unknown", None)],
                    "labels": [],
                    "track_artists": [],
                }
            }
        )
        result = DiscogsEnricher(client).enrich_batch({"A": None})

        assert len(result["A"].personnel) == 1
        assert result["A"].personnel[0].roles == []


class TestCompilationDetection:
    def test_tracks_with_per_track_artists_detected(self):
        client = _mock_client(
            {
                "Autechre": {
                    "styles": [],
                    "extra_artists": [],
                    "labels": [],
                    "track_artists": [(12345, "Stereolab"), (12345, "Broadcast")],
                }
            }
        )
        result = DiscogsEnricher(client).enrich_batch({"Autechre": None})

        assert len(result["Autechre"].compilation_appearances) == 1
        assert "Stereolab" in result["Autechre"].compilation_appearances[0].other_artists

    def test_no_track_artists_means_no_compilation(self):
        client = _mock_client(
            {
                "A": {
                    "styles": [],
                    "extra_artists": [],
                    "labels": [],
                    "track_artists": [],
                }
            }
        )
        result = DiscogsEnricher(client).enrich_batch({"A": None})

        assert len(result["A"].compilation_appearances) == 0


class TestNoMatch:
    def test_no_data_means_not_in_results(self):
        client = _mock_client({})
        result = DiscogsEnricher(client).enrich_batch({"Unknown": None})
        assert "Unknown" not in result

    def test_single_artist_not_found_returns_none(self):
        client = _mock_client({})
        result = DiscogsEnricher(client).enrich_artist("Unknown")
        assert result is None


class TestBatchEnrichment:
    def test_batch_enriches_multiple_artists(self):
        client = _mock_client(
            {
                "Autechre": {
                    "styles": ["IDM"],
                    "extra_artists": [],
                    "labels": [],
                    "track_artists": [],
                },
                "Stereolab": {
                    "styles": ["Krautrock"],
                    "extra_artists": [],
                    "labels": [],
                    "track_artists": [],
                },
            }
        )
        result = DiscogsEnricher(client).enrich_batch({"Autechre": None, "Stereolab": None})
        assert len(result) == 2

    def test_batch_passes_all_names_to_bulk(self):
        client = _mock_client({})
        DiscogsEnricher(client).enrich_batch({"A": None, "B": None, "C": None})
        client.get_bulk_enrichment.assert_called_once()
        call_args = client.get_bulk_enrichment.call_args[0][0]
        assert set(call_args) == {"A", "B", "C"}


class TestProgressLogging:
    def test_logs_completion(self, caplog):
        client = _mock_client({})
        with caplog.at_level(logging.INFO):
            DiscogsEnricher(client).enrich_batch({"A": None})
        assert any("Enrichment complete" in r.message for r in caplog.records)
