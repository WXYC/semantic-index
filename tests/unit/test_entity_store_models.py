"""Tests for entity store Pydantic models and factory functions."""

import pytest

from semantic_index.models import Entity, ReconciliationEvent, ReconciliationReport
from tests.conftest import make_entity


class TestEntity:
    def test_defaults(self):
        entity = Entity(id=1, name="Autechre")
        assert entity.id == 1
        assert entity.name == "Autechre"
        assert entity.entity_type == "artist"
        assert entity.wikidata_qid is None

    def test_with_wikidata_qid(self):
        entity = Entity(id=1, name="Autechre", wikidata_qid="Q207406")
        assert entity.wikidata_qid == "Q207406"

    def test_entity_type_override(self):
        entity = Entity(id=1, name="Warp Records", entity_type="label")
        assert entity.entity_type == "label"


class TestReconciliationEvent:
    def test_required_fields(self):
        event = ReconciliationEvent(
            source="discogs",
            external_id="42",
            method="exact",
        )
        assert event.source == "discogs"
        assert event.external_id == "42"
        assert event.method == "exact"
        assert event.confidence is None

    def test_with_confidence(self):
        event = ReconciliationEvent(
            source="musicbrainz",
            external_id="a7f7df4a-77d8-4f12-8acd-5c60c93f4de8",
            confidence=0.95,
            method="fuzzy",
        )
        assert event.confidence == 0.95

    @pytest.mark.parametrize(
        "source",
        ["discogs", "musicbrainz", "wikidata"],
    )
    def test_valid_sources(self, source: str):
        event = ReconciliationEvent(source=source, external_id="1", method="exact")
        assert event.source == source

    @pytest.mark.parametrize(
        "method",
        ["exact", "fuzzy", "api_search", "cache_lookup"],
    )
    def test_valid_methods(self, method: str):
        event = ReconciliationEvent(source="discogs", external_id="1", method=method)
        assert event.method == method


class TestReconciliationReport:
    def test_all_fields(self):
        report = ReconciliationReport(
            total=100,
            attempted=80,
            succeeded=60,
            no_match=15,
            errored=5,
            skipped=20,
        )
        assert report.total == 100
        assert report.attempted == 80
        assert report.succeeded == 60
        assert report.no_match == 15
        assert report.errored == 5
        assert report.skipped == 20

    def test_counts_are_consistent(self):
        """attempted + skipped == total; succeeded + no_match + errored == attempted."""
        report = ReconciliationReport(
            total=50,
            attempted=40,
            succeeded=30,
            no_match=8,
            errored=2,
            skipped=10,
        )
        assert report.attempted + report.skipped == report.total
        assert report.succeeded + report.no_match + report.errored == report.attempted

    def test_zero_report(self):
        report = ReconciliationReport(
            total=0, attempted=0, succeeded=0, no_match=0, errored=0, skipped=0
        )
        assert report.total == 0


class TestMakeEntity:
    def test_defaults(self):
        entity = make_entity()
        assert entity.name == "Autechre"
        assert entity.entity_type == "artist"
        assert entity.wikidata_qid is None

    def test_custom_name(self):
        entity = make_entity(name="Stereolab")
        assert entity.name == "Stereolab"

    def test_custom_wikidata_qid(self):
        entity = make_entity(wikidata_qid="Q207406")
        assert entity.wikidata_qid == "Q207406"

    def test_custom_entity_type(self):
        entity = make_entity(entity_type="label")
        assert entity.entity_type == "label"
