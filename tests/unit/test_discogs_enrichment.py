"""Tests for the Discogs enrichment module."""

import logging
from unittest.mock import MagicMock

from semantic_index.discogs_client import DiscogsClient
from semantic_index.discogs_enrichment import DiscogsEnricher
from semantic_index.models import (
    DiscogsCredit,
    DiscogsLabel,
    DiscogsSearchResult,
    DiscogsTrack,
)
from tests.conftest import make_discogs_release


class TestSingleReleaseStyles:
    """Test style aggregation from a single release."""

    def test_styles_from_single_release(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Autechre", artist_id=42
        )
        client.get_releases_for_artist.return_value = [12345]
        client.get_release.return_value = make_discogs_release(
            styles=["IDM", "Abstract"],
        )

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Autechre")

        assert result is not None
        assert sorted(result.styles) == ["Abstract", "IDM"]

    def test_styles_deduplicated_across_releases(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Autechre", artist_id=42
        )
        client.get_releases_for_artist.return_value = [12345, 67890]
        client.get_release.side_effect = [
            make_discogs_release(release_id=12345, styles=["IDM", "Abstract"]),
            make_discogs_release(
                release_id=67890,
                title="Amber",
                year=1994,
                styles=["IDM", "Ambient"],
            ),
        ]

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Autechre")

        assert result is not None
        assert sorted(result.styles) == ["Abstract", "Ambient", "IDM"]


class TestMultiReleaseAggregation:
    """Test aggregation across multiple releases."""

    def test_labels_deduplicated_by_name(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Autechre", artist_id=42
        )
        client.get_releases_for_artist.return_value = [12345, 67890]
        client.get_release.side_effect = [
            make_discogs_release(
                release_id=12345,
                labels=[DiscogsLabel(name="Warp Records", label_id=100, catno="WARPCD85")],
            ),
            make_discogs_release(
                release_id=67890,
                title="Amber",
                labels=[
                    DiscogsLabel(name="Warp Records", label_id=100, catno="WARPCD34"),
                    DiscogsLabel(name="Skam", label_id=200, catno="SKA001"),
                ],
            ),
        ]

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Autechre")

        assert result is not None
        label_names = [lbl.name for lbl in result.labels]
        assert sorted(label_names) == ["Skam", "Warp Records"]

    def test_personnel_deduplicated_with_roles_merged(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Autechre", artist_id=42
        )
        client.get_releases_for_artist.return_value = [12345, 67890]
        client.get_release.side_effect = [
            make_discogs_release(
                release_id=12345,
                extra_artists=[
                    DiscogsCredit(name="Rob Brown", role="Written-By"),
                ],
            ),
            make_discogs_release(
                release_id=67890,
                title="Amber",
                extra_artists=[
                    DiscogsCredit(name="Rob Brown", role="Producer"),
                    DiscogsCredit(name="Sean Booth", role="Written-By"),
                ],
            ),
        ]

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Autechre")

        assert result is not None
        personnel_by_name = {p.name: p for p in result.personnel}
        assert "Rob Brown" in personnel_by_name
        assert "Sean Booth" in personnel_by_name
        assert sorted(personnel_by_name["Rob Brown"].roles) == ["Producer", "Written-By"]
        assert personnel_by_name["Sean Booth"].roles == ["Written-By"]


class TestExtraArtists:
    """Test personnel credit extraction from extra_artists."""

    def test_extra_artists_become_personnel(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Father John Misty", artist_id=99
        )
        client.get_releases_for_artist.return_value = [55555]
        client.get_release.return_value = make_discogs_release(
            release_id=55555,
            title="I Love You, Honeybear",
            artist_name="Father John Misty",
            artist_id=99,
            styles=["Folk Rock", "Baroque Pop"],
            labels=[DiscogsLabel(name="Sub Pop", label_id=300)],
            extra_artists=[
                DiscogsCredit(name="Jonathan Wilson", role="Producer"),
                DiscogsCredit(name="Drew Erickson", role="Piano"),
            ],
        )

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Father John Misty")

        assert result is not None
        assert len(result.personnel) == 2
        personnel_names = {p.name for p in result.personnel}
        assert personnel_names == {"Jonathan Wilson", "Drew Erickson"}

    def test_extra_artist_with_no_role(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Stereolab", artist_id=55
        )
        client.get_releases_for_artist.return_value = [11111]
        client.get_release.return_value = make_discogs_release(
            release_id=11111,
            title="Aluminum Tunes",
            artist_name="Stereolab",
            artist_id=55,
            styles=["Post-Rock", "Krautrock"],
            labels=[DiscogsLabel(name="Duophonic", label_id=400)],
            extra_artists=[
                DiscogsCredit(name="John McEntire", role=None),
            ],
        )

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Stereolab")

        assert result is not None
        assert len(result.personnel) == 1
        assert result.personnel[0].name == "John McEntire"
        assert result.personnel[0].roles == []


class TestCompilationDetection:
    """Test compilation appearance detection from per-track artists."""

    def test_tracks_with_per_track_artists_detected_as_compilation(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Chuquimamani-Condori", artist_id=77
        )
        client.get_releases_for_artist.return_value = [33333]
        client.get_release.return_value = make_discogs_release(
            release_id=33333,
            title="SOÑAR Compilation Vol. 1",
            artist_name="Various",
            artist_id=None,
            styles=["Electronic"],
            labels=[DiscogsLabel(name="SOÑAR", label_id=500)],
            tracklist=[
                DiscogsTrack(
                    position="1",
                    title="Call Your Name",
                    artists=["Chuquimamani-Condori"],
                ),
                DiscogsTrack(
                    position="2",
                    title="Night Vision",
                    artists=["Nourished by Time"],
                ),
                DiscogsTrack(
                    position="3",
                    title="Drift",
                    artists=["Rochelle Jordan"],
                ),
            ],
        )

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Chuquimamani-Condori")

        assert result is not None
        assert len(result.compilation_appearances) == 1
        comp = result.compilation_appearances[0]
        assert comp.release_id == 33333
        assert comp.release_title == "SOÑAR Compilation Vol. 1"
        assert "Nourished by Time" in comp.other_artists
        assert "Rochelle Jordan" in comp.other_artists

    def test_regular_album_not_detected_as_compilation(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Jessica Pratt", artist_id=88
        )
        client.get_releases_for_artist.return_value = [44444]
        client.get_release.return_value = make_discogs_release(
            release_id=44444,
            title="On Your Own Love Again",
            artist_name="Jessica Pratt",
            artist_id=88,
            styles=["Folk"],
            labels=[DiscogsLabel(name="Drag City", label_id=600)],
            tracklist=[
                DiscogsTrack(position="1", title="Back, Baby"),
                DiscogsTrack(position="2", title="Greyclouds"),
            ],
        )

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Jessica Pratt")

        assert result is not None
        assert result.compilation_appearances == []


class TestNoMatch:
    """Test behavior when no Discogs match is found."""

    def test_no_search_result_returns_none(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = None

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Obscure Unknown Artist")

        assert result is None

    def test_search_found_but_no_releases_returns_empty_enrichment(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Anne Gillis", artist_id=66
        )
        client.get_releases_for_artist.return_value = []

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Anne Gillis")

        assert result is not None
        assert result.canonical_name == "Anne Gillis"
        assert result.discogs_artist_id == 66
        assert result.styles == []
        assert result.personnel == []
        assert result.labels == []
        assert result.compilation_appearances == []

    def test_release_fetch_returns_none_skipped(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(artist_name="Sessa", artist_id=111)
        client.get_releases_for_artist.return_value = [99999]
        client.get_release.return_value = None

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Sessa")

        assert result is not None
        assert result.canonical_name == "Sessa"
        assert result.styles == []


class TestEnrichWithKnownId:
    """Test enrichment when discogs_artist_id is already known."""

    def test_known_id_skips_search(self):
        client = MagicMock(spec=DiscogsClient)
        client.get_releases_for_artist.return_value = [12345]
        client.get_release.return_value = make_discogs_release()

        enricher = DiscogsEnricher(client)
        result = enricher.enrich_artist("Autechre", discogs_artist_id=42)

        assert result is not None
        assert result.discogs_artist_id == 42
        client.search_artist.assert_not_called()


class TestBatchEnrichment:
    """Test batch enrichment across multiple artists."""

    def test_batch_enriches_multiple_artists(self):
        client = MagicMock(spec=DiscogsClient)

        def search_side_effect(name, release_title=None):
            results = {
                "Autechre": DiscogsSearchResult(artist_name="Autechre", artist_id=42),
                "Stereolab": DiscogsSearchResult(artist_name="Stereolab", artist_id=55),
            }
            return results.get(name)

        def releases_side_effect(name):
            data = {
                "Autechre": [12345],
                "Stereolab": [67890],
            }
            return data.get(name, [])

        def release_side_effect(release_id):
            releases = {
                12345: make_discogs_release(
                    release_id=12345,
                    styles=["IDM"],
                    labels=[DiscogsLabel(name="Warp Records", label_id=100)],
                ),
                67890: make_discogs_release(
                    release_id=67890,
                    title="Aluminum Tunes",
                    artist_name="Stereolab",
                    artist_id=55,
                    styles=["Post-Rock"],
                    labels=[DiscogsLabel(name="Duophonic", label_id=400)],
                ),
            }
            return releases.get(release_id)

        client.search_artist.side_effect = search_side_effect
        client.get_releases_for_artist.side_effect = releases_side_effect
        client.get_release.side_effect = release_side_effect

        enricher = DiscogsEnricher(client)
        results = enricher.enrich_batch({"Autechre": None, "Stereolab": None})

        assert len(results) == 2
        assert "Autechre" in results
        assert "Stereolab" in results
        assert "IDM" in results["Autechre"].styles
        assert "Post-Rock" in results["Stereolab"].styles

    def test_batch_skips_failed_artists(self):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.side_effect = [
            DiscogsSearchResult(artist_name="Cat Power", artist_id=33),
            None,  # Buck Meek not found
        ]
        client.get_releases_for_artist.return_value = [22222]
        client.get_release.return_value = make_discogs_release(
            release_id=22222,
            title="Moon Pix",
            artist_name="Cat Power",
            artist_id=33,
            styles=["Indie Rock"],
            labels=[DiscogsLabel(name="Matador Records", label_id=700)],
        )

        enricher = DiscogsEnricher(client)
        results = enricher.enrich_batch({"Cat Power": None, "Buck Meek": None})

        assert len(results) == 1
        assert "Cat Power" in results
        assert "Buck Meek" not in results

    def test_batch_uses_known_ids(self):
        client = MagicMock(spec=DiscogsClient)
        client.get_releases_for_artist.return_value = [12345]
        client.get_release.return_value = make_discogs_release()

        enricher = DiscogsEnricher(client)
        results = enricher.enrich_batch({"Autechre": 42})

        assert len(results) == 1
        assert results["Autechre"].discogs_artist_id == 42
        client.search_artist.assert_not_called()


class TestProgressLogging:
    """Test that batch enrichment logs progress."""

    def test_logs_progress_at_intervals(self, caplog):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = None

        artists = {f"Artist {i}": None for i in range(1050)}

        enricher = DiscogsEnricher(client)
        with caplog.at_level(logging.INFO, logger="semantic_index.discogs_enrichment"):
            enricher.enrich_batch(artists)

        progress_messages = [r.message for r in caplog.records if "Enriched" in r.message]
        assert len(progress_messages) >= 2  # at 500, 1000
        assert any("500" in msg for msg in progress_messages)
        assert any("1000" in msg for msg in progress_messages)

    def test_logs_final_summary(self, caplog):
        client = MagicMock(spec=DiscogsClient)
        client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Autechre", artist_id=42
        )
        client.get_releases_for_artist.return_value = [12345]
        client.get_release.return_value = make_discogs_release()

        enricher = DiscogsEnricher(client)
        with caplog.at_level(logging.INFO, logger="semantic_index.discogs_enrichment"):
            enricher.enrich_batch({"Autechre": None})

        summary_messages = [r.message for r in caplog.records if "complete" in r.message.lower()]
        assert len(summary_messages) >= 1
