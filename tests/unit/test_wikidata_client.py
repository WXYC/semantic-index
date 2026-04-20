"""Tests for the Wikidata SPARQL client (graph-specific methods only)."""

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

        assert len(result) == 1
        assert result[0].source_qid == "Q2774"


class TestLookupLabelsByDiscogsIds:
    """Tests for Discogs label ID (P1902) lookups."""

    def test_single_label_returns_entity(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q1312934"),
                    "itemLabel": _literal("Warp Records"),
                    "discogsLabelId": _literal("23528"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_labels_by_discogs_ids([23528])

        assert 23528 in result
        assert result[23528].qid == "Q1312934"
        assert result[23528].name == "Warp Records"

    def test_multiple_labels(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q1312934"),
                    "itemLabel": _literal("Warp Records"),
                    "discogsLabelId": _literal("23528"),
                },
                {
                    "item": _uri("Q843988"),
                    "itemLabel": _literal("Sub Pop"),
                    "discogsLabelId": _literal("1594"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_labels_by_discogs_ids([23528, 1594])

        assert len(result) == 2
        assert result[23528].name == "Warp Records"
        assert result[1594].name == "Sub Pop"

    def test_empty_input_returns_empty(self):
        client = WikidataClient()
        result = client.lookup_labels_by_discogs_ids([])
        assert result == {}

    def test_not_found_returns_empty(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response([])
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_labels_by_discogs_ids([99999])

        assert result == {}

    def test_sparql_error_returns_empty(self):
        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("Timeout")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_labels_by_discogs_ids([23528])

        assert result == {}


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


class TestBatching:
    """Tests for batch splitting with large input."""

    def test_influences_batch_splitting(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response([])
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient(batch_size=2)
            client.get_influences(["Q1", "Q2", "Q3"])

        assert mock_client.get.call_count == 2


class TestGracefulDegradation:
    """Tests for error handling."""

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


def _make_cache_client(mock_conn: MagicMock) -> WikidataClient:
    """Create a WikidataClient with a mocked cache connection."""
    mock_conn.closed = False
    client = WikidataClient(cache_dsn="mock")
    client._pg._conn = mock_conn
    return client


class TestCacheFirstGetInfluences:
    """Tests for cache-first influence lookups."""

    def test_cache_hit_returns_influences(self):
        """Influences from the cache are returned without SPARQL."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("Q247237", "Q49835", "Kraftwerk"),
            ("Q247237", "Q192540", "Aphex Twin"),
        ]

        client = _make_cache_client(mock_conn)

        with patch("httpx.Client") as http_mock:
            result = client.get_influences(["Q247237"])

        assert len(result) == 2
        assert result[0].source_qid == "Q247237"
        assert result[0].target_qid == "Q49835"
        assert result[0].target_name == "Kraftwerk"
        http_mock.assert_not_called()

    def test_empty_qids_returns_empty(self):
        """Empty input returns empty without any queries."""
        client = WikidataClient()
        result = client.get_influences([])
        assert result == []
