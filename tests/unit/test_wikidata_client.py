"""Tests for the Wikidata SPARQL client."""

from unittest.mock import MagicMock, patch

from semantic_index.wikidata_client import WikidataClient


def _sparql_response(bindings: list[dict]) -> dict:
    """Build a mock SPARQL JSON response."""
    return {"results": {"bindings": bindings}}


def _uri(qid: str) -> dict:
    """Build a SPARQL URI binding value."""
    return {"type": "uri", "value": f"http://www.wikidata.org/entity/{qid}"}


def _literal(value: str) -> dict:
    """Build a SPARQL literal binding value."""
    return {"type": "literal", "value": value}


class TestLookupByDiscogsIds:
    """Tests for Discogs artist ID (P1953) lookups."""

    def test_single_id_returns_entity(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q2774"),
                    "itemLabel": _literal("Autechre"),
                    "discogsId": _literal("2774"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_by_discogs_ids([2774])

        assert 2774 in result
        assert result[2774].qid == "Q2774"
        assert result[2774].name == "Autechre"
        assert result[2774].discogs_artist_id == 2774

    def test_multiple_ids_returns_all(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q2774"),
                    "itemLabel": _literal("Autechre"),
                    "discogsId": _literal("2774"),
                },
                {
                    "item": _uri("Q650826"),
                    "itemLabel": _literal("Stereolab"),
                    "discogsId": _literal("10272"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_by_discogs_ids([2774, 10272])

        assert len(result) == 2
        assert result[2774].name == "Autechre"
        assert result[10272].name == "Stereolab"

    def test_not_found_returns_empty(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response([])
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_by_discogs_ids([99999])

        assert result == {}

    def test_empty_input_returns_empty(self):
        client = WikidataClient()
        result = client.lookup_by_discogs_ids([])
        assert result == {}


class TestGetInfluences:
    """Tests for influence relationship (P737) queries."""

    def test_returns_influences(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "source": _uri("Q2774"),
                    "target": _uri("Q484641"),
                    "targetLabel": _literal("Kraftwerk"),
                },
                {
                    "source": _uri("Q2774"),
                    "target": _uri("Q193815"),
                    "targetLabel": _literal("Brian Eno"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.get_influences(["Q2774"])

        assert len(result) == 2
        assert result[0].source_qid == "Q2774"
        assert result[0].target_qid == "Q484641"
        assert result[0].target_name == "Kraftwerk"

    def test_no_influences_returns_empty(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response([])
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.get_influences(["Q2774"])

        assert result == []

    def test_empty_input_returns_empty(self):
        client = WikidataClient()
        result = client.get_influences([])
        assert result == []

    def test_invalid_qid_skipped(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "source": _uri("Q2774"),
                    "target": _uri("Q484641"),
                    "targetLabel": _literal("Kraftwerk"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.get_influences(["Q2774", "INVALID", "not-a-qid"])

        # Only Q2774 was queried; invalid QIDs were filtered out
        assert len(result) == 1
        assert result[0].source_qid == "Q2774"


class TestGetLabelHierarchy:
    """Tests for label hierarchy (P749/P355) queries."""

    def test_returns_parent_via_p749(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "child": _uri("Q1312934"),
                    "childLabel": _literal("Warp Records"),
                    "parent": _uri("Q21077"),
                    "parentLabel": _literal("Universal Music Group"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.get_label_hierarchy(["Q1312934"])

        assert len(result) == 1
        assert result[0].child_qid == "Q1312934"
        assert result[0].child_name == "Warp Records"
        assert result[0].parent_qid == "Q21077"
        assert result[0].parent_name == "Universal Music Group"

    def test_returns_child_via_p355(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "child": _uri("Q843988"),
                    "childLabel": _literal("Sub Pop"),
                    "parent": _uri("Q21077"),
                    "parentLabel": _literal("Universal Music Group"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.get_label_hierarchy(["Q21077"])

        assert len(result) == 1
        assert result[0].parent_qid == "Q21077"
        assert result[0].child_qid == "Q843988"
        assert result[0].child_name == "Sub Pop"

    def test_no_hierarchy_returns_empty(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response([])
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.get_label_hierarchy(["Q12345"])

        assert result == []

    def test_empty_input_returns_empty(self):
        client = WikidataClient()
        result = client.get_label_hierarchy([])
        assert result == []


class TestSearchByName:
    """Tests for entity name search via wbsearchentities API."""

    def test_returns_matching_entities(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "search": [
                {
                    "id": "Q2774",
                    "label": "Autechre",
                    "description": "British electronic music duo",
                },
            ],
        }
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_by_name("Autechre")

        assert len(result) == 1
        assert result[0].qid == "Q2774"
        assert result[0].name == "Autechre"
        assert result[0].description == "British electronic music duo"

    def test_no_results_returns_empty(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"search": []}
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_by_name("xyznonexistent")

        assert result == []

    def test_blank_name_returns_empty(self):
        client = WikidataClient()
        result = client.search_by_name("   ")
        assert result == []

    def test_limit_capped_at_50(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"search": []}
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            client.search_by_name("Autechre", limit=100)

        assert mock_client.get.call_args.kwargs["params"]["limit"] == 50

    def test_multiple_results_ordered(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "search": [
                {"id": "Q218981", "label": "Cat Power", "description": "American singer"},
                {"id": "Q999999", "label": "Cat Power Band", "description": "tribute act"},
            ],
        }
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_by_name("Cat Power")

        assert len(result) == 2
        assert result[0].qid == "Q218981"
        assert result[1].name == "Cat Power Band"


class TestBatching:
    """Tests for batch splitting with large input."""

    def test_large_batch_splits_into_chunks(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response([])
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient(batch_size=3)
            client.lookup_by_discogs_ids([1, 2, 3, 4, 5])

        # Should have made 2 SPARQL requests (batch of 3 + batch of 2)
        assert mock_client.get.call_count == 2

    def test_influences_batch_splitting(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response([])
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient(batch_size=2)
            client.get_influences(["Q1", "Q2", "Q3"])

        assert mock_client.get.call_count == 2

    def test_results_aggregated_across_batches(self):
        response_batch1 = MagicMock()
        response_batch1.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q2774"),
                    "itemLabel": _literal("Autechre"),
                    "discogsId": _literal("2774"),
                },
            ]
        )
        response_batch2 = MagicMock()
        response_batch2.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q650826"),
                    "itemLabel": _literal("Stereolab"),
                    "discogsId": _literal("10272"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.side_effect = [response_batch1, response_batch2]

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient(batch_size=1)
            result = client.lookup_by_discogs_ids([2774, 10272])

        assert len(result) == 2
        assert result[2774].name == "Autechre"
        assert result[10272].name == "Stereolab"


class TestGracefulDegradation:
    """Tests for error handling."""

    def test_sparql_error_returns_empty_dict(self):
        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("Connection refused")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_by_discogs_ids([2774])

        assert result == {}

    def test_sparql_error_returns_empty_list_for_influences(self):
        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("Timeout")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.get_influences(["Q2774"])

        assert result == []

    def test_sparql_error_returns_empty_list_for_hierarchy(self):
        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("Server error")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.get_label_hierarchy(["Q1312934"])

        assert result == []

    def test_search_error_returns_empty(self):
        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("Network error")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_by_name("Autechre")

        assert result == []

    def test_partial_batch_failure_preserves_successful_results(self):
        """If one batch fails, results from successful batches are still returned."""
        good_response = MagicMock()
        good_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q2774"),
                    "itemLabel": _literal("Autechre"),
                    "discogsId": _literal("2774"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.side_effect = [good_response, Exception("Timeout")]

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient(batch_size=1)
            result = client.lookup_by_discogs_ids([2774, 10272])

        # First batch succeeded, second failed
        assert len(result) == 1
        assert 2774 in result
