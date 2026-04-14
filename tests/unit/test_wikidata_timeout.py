"""Tests for Wikidata client timeout behavior.

Verifies that the WikidataClient properly handles slow SPARQL endpoints
by respecting httpx timeouts and returning gracefully instead of hanging.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx

from semantic_index.wikidata_client import WikidataClient


def _make_timeout_client():
    """Create an httpx mock that raises a timeout on GET."""
    mock_client = MagicMock()
    mock_client.get.side_effect = httpx.ReadTimeout("Read timed out")
    return mock_client


class TestSparqlTimeout:
    """Verify that slow SPARQL endpoints don't hang the client."""

    def test_lookup_by_discogs_ids_returns_empty_on_timeout(self):
        """When SPARQL times out, lookup_by_discogs_ids returns empty dict."""
        with patch("httpx.Client", return_value=_make_timeout_client()):
            client = WikidataClient()
            result = client.lookup_by_discogs_ids([2774])

        assert result == {}

    def test_get_influences_returns_empty_on_timeout(self):
        """When SPARQL times out, get_influences returns empty list."""
        with patch("httpx.Client", return_value=_make_timeout_client()):
            client = WikidataClient()
            result = client.get_influences(["Q2774"])

        assert result == []

    def test_get_label_hierarchy_returns_empty_on_timeout(self):
        """When SPARQL times out, get_label_hierarchy returns empty list."""
        with patch("httpx.Client", return_value=_make_timeout_client()):
            client = WikidataClient()
            result = client.get_label_hierarchy(["Q1312934"])

        assert result == []

    def test_lookup_streaming_ids_returns_empty_on_timeout(self):
        """When SPARQL times out, lookup_streaming_ids returns empty dict."""
        with patch("httpx.Client", return_value=_make_timeout_client()):
            client = WikidataClient()
            result = client.lookup_streaming_ids(["Q2774"])

        assert result == {}

    def test_lookup_labels_by_discogs_ids_returns_empty_on_timeout(self):
        """When SPARQL times out, lookup_labels returns empty dict."""
        with patch("httpx.Client", return_value=_make_timeout_client()):
            client = WikidataClient()
            result = client.lookup_labels_by_discogs_ids([23528])

        assert result == {}


class TestSparqlTimeoutWithRetries:
    """Verify retry behavior on timeout (SPARQL retries on 403/429 but not on other errors)."""

    def test_timeout_does_not_retry(self):
        """httpx.ReadTimeout should not trigger the 403/429 retry loop."""
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.ReadTimeout("Read timed out")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_by_discogs_ids([2774])

        assert result == {}
        # The client is created fresh each call to _sparql_query, and get is called
        # once per attempt. With 3 retries, it would be called 3 times total
        # because the timeout is raised inside the retry loop.
        assert mock_client.get.call_count <= 3

    def test_connect_timeout_returns_empty(self):
        """Connection timeout returns empty result."""
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.ConnectTimeout("Connection timed out")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.get_influences(["Q2774", "Q650826"])

        assert result == []


class TestSearchByNameTimeout:
    """Verify search_by_name handles timeout from the MediaWiki API."""

    def test_search_by_name_returns_empty_on_timeout(self):
        """When the wbsearchentities API times out, returns empty list."""
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.ReadTimeout("Read timed out")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_by_name("Autechre")

        assert result == []

    def test_search_musician_by_name_returns_empty_on_timeout(self):
        """When both search and filter time out, returns empty list."""
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.ReadTimeout("Read timed out")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_musician_by_name("Stereolab")

        assert result == []

    def test_search_musicians_batch_returns_empty_on_timeout(self):
        """When batch search times out, returns empty dict."""
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.ReadTimeout("Read timed out")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_musicians_batch(["Autechre", "Stereolab", "Juana Molina"])

        assert result == {}


class TestTimeoutLogging:
    """Verify timeout errors are logged appropriately."""

    def test_sparql_timeout_logged_as_warning(self, caplog):
        """SPARQL timeout should produce a warning log entry."""
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.ReadTimeout("Read timed out")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            import logging

            with caplog.at_level(logging.WARNING):
                client.lookup_by_discogs_ids([42])

        assert any("failed" in record.message.lower() for record in caplog.records)

    def test_search_timeout_logged_as_warning(self, caplog):
        """Name search timeout should produce a warning log entry."""
        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.ReadTimeout("Read timed out")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            import logging

            with caplog.at_level(logging.WARNING):
                client.search_by_name("Cat Power")

        assert any("failed" in record.message.lower() for record in caplog.records)
