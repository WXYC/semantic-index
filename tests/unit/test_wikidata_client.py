"""Tests for the Wikidata SPARQL client."""

from unittest.mock import MagicMock, patch

from semantic_index.wikidata_client import MUSICAL_GROUP_TYPES, MUSICIAN_OCCUPATIONS, WikidataClient


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


class TestSearchMusicianByName:
    """Tests for musician-filtered name search (search + SPARQL P31/P106 validation)."""

    def _make_search_response(self, items: list[dict]) -> dict:
        """Build a mock wbsearchentities API response."""
        return {"search": items}

    def _make_filter_response(self, qids: list[str]) -> MagicMock:
        """Build a mock SPARQL response that returns the given QIDs as musicians."""
        resp = MagicMock()
        resp.json.return_value = _sparql_response(
            [{"item": _uri(qid), "itemLabel": _literal(qid)} for qid in qids]
        )
        return resp

    def test_returns_musician_entity(self):
        """A search result that passes the musician filter is returned."""
        search_resp = MagicMock()
        search_resp.json.return_value = self._make_search_response(
            [{"id": "Q2774", "label": "Autechre", "description": "British electronic music duo"}]
        )
        filter_resp = self._make_filter_response(["Q2774"])

        mock_client = MagicMock()
        # First call: wbsearchentities; second call: SPARQL filter
        mock_client.get.side_effect = [search_resp, filter_resp]

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_musician_by_name("Autechre")

        assert len(result) == 1
        assert result[0].qid == "Q2774"
        assert result[0].name == "Autechre"

    def test_filters_out_non_musician(self):
        """Search results that don't pass the musician filter are excluded."""
        search_resp = MagicMock()
        search_resp.json.return_value = self._make_search_response(
            [
                {"id": "Q218981", "label": "Cat Power", "description": "American singer"},
                {"id": "Q999999", "label": "Cat Power (film)", "description": "2024 documentary"},
            ]
        )
        # Only Q218981 passes the musician filter
        filter_resp = self._make_filter_response(["Q218981"])

        mock_client = MagicMock()
        mock_client.get.side_effect = [search_resp, filter_resp]

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_musician_by_name("Cat Power")

        assert len(result) == 1
        assert result[0].qid == "Q218981"

    def test_no_search_results_returns_empty(self):
        """If wbsearchentities returns nothing, result is empty without SPARQL call."""
        search_resp = MagicMock()
        search_resp.json.return_value = self._make_search_response([])

        mock_client = MagicMock()
        mock_client.get.return_value = search_resp

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_musician_by_name("xyznonexistent")

        assert result == []
        # Only the search call, no SPARQL filter call
        assert mock_client.get.call_count == 1

    def test_no_musicians_among_results_returns_empty(self):
        """If none of the search results are musicians, result is empty."""
        search_resp = MagicMock()
        search_resp.json.return_value = self._make_search_response(
            [{"id": "Q12345", "label": "Stereolab", "description": "a place"}]
        )
        filter_resp = self._make_filter_response([])

        mock_client = MagicMock()
        mock_client.get.side_effect = [search_resp, filter_resp]

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_musician_by_name("Stereolab")

        assert result == []

    def test_blank_name_returns_empty(self):
        """Blank names skip both search and SPARQL."""
        client = WikidataClient()
        result = client.search_musician_by_name("   ")
        assert result == []

    def test_preserves_search_order(self):
        """Results maintain the original search relevance order."""
        search_resp = MagicMock()
        search_resp.json.return_value = self._make_search_response(
            [
                {"id": "Q218981", "label": "Cat Power", "description": "American singer"},
                {"id": "Q777777", "label": "Cat Power Trio", "description": "jazz trio"},
            ]
        )
        # Both pass the filter, but SPARQL may return them in any order
        filter_resp = self._make_filter_response(["Q777777", "Q218981"])

        mock_client = MagicMock()
        mock_client.get.side_effect = [search_resp, filter_resp]

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_musician_by_name("Cat Power")

        assert len(result) == 2
        assert result[0].qid == "Q218981"  # original search order preserved
        assert result[1].qid == "Q777777"

    def test_sparql_error_returns_empty(self):
        """If the SPARQL filter fails, gracefully return empty."""
        search_resp = MagicMock()
        search_resp.json.return_value = self._make_search_response(
            [{"id": "Q2774", "label": "Autechre", "description": "duo"}]
        )

        mock_client = MagicMock()
        # Search succeeds, then SPARQL filter raises
        mock_client.get.side_effect = [search_resp, Exception("SPARQL timeout")]

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.search_musician_by_name("Autechre")

        assert result == []

    def test_limit_passed_to_search(self):
        """The limit parameter is forwarded to wbsearchentities."""
        search_resp = MagicMock()
        search_resp.json.return_value = self._make_search_response([])
        mock_client = MagicMock()
        mock_client.get.return_value = search_resp

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            client.search_musician_by_name("Autechre", limit=5)

        # The search call should have limit=5
        search_call = mock_client.get.call_args_list[0]
        assert search_call.kwargs["params"]["limit"] == 5

    def test_constants_exported(self):
        """Verify the filter constants are available for inspection."""
        assert "Q639669" in MUSICIAN_OCCUPATIONS  # musician
        assert "Q215380" in MUSICAL_GROUP_TYPES  # musical group/band


def _make_cache_client(mock_conn: MagicMock) -> WikidataClient:
    """Create a WikidataClient with a mocked cache connection."""
    mock_conn.closed = False
    client = WikidataClient()
    client._cache_dsn = "mock"
    client._cache_conn = mock_conn
    return client


class TestCacheFirstLookupByDiscogsIds:
    """Tests for cache-first Discogs artist ID lookups."""

    def test_cache_hit_skips_sparql(self):
        """When the cache has the answer, SPARQL is never called."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("Q247237", "Autechre", "British electronic music duo", "41"),
        ]

        client = _make_cache_client(mock_conn)

        with patch("httpx.Client") as http_mock:
            result = client.lookup_by_discogs_ids([41])

        assert 41 in result
        assert result[41].qid == "Q247237"
        assert result[41].name == "Autechre"
        # SPARQL should not have been called
        http_mock.assert_not_called()

    def test_cache_miss_returns_empty_when_cache_connected(self):
        """IDs not in the cache return empty — cache is authoritative."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        client = _make_cache_client(mock_conn)

        with patch("httpx.Client") as http_mock:
            result = client.lookup_by_discogs_ids([388])

        assert result == {}
        http_mock.assert_not_called()

    def test_no_cache_dsn_uses_sparql_only(self):
        """Without cache_dsn, behaves exactly as before (SPARQL only)."""
        sparql_resp = MagicMock()
        sparql_resp.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q247237"),
                    "itemLabel": _literal("Autechre"),
                    "discogsId": _literal("41"),
                },
            ]
        )
        sparql_resp.status_code = 200
        mock_http = MagicMock()
        mock_http.get.return_value = sparql_resp

        with patch("httpx.Client", return_value=mock_http):
            client = WikidataClient()  # no cache_dsn
            result = client.lookup_by_discogs_ids([41])

        assert 41 in result
        assert result[41].qid == "Q247237"


class TestLookupStreamingIds:
    """Tests for streaming service ID (P1902/P2850/P3283) lookups."""

    def test_all_three_ids_returned(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q2774"),
                    "spotifyId": _literal("5bMqBjPbCOWGgWJpbAqdQq"),
                    "appleMusicId": _literal("15821"),
                    "bandcampId": _literal("autechre"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_streaming_ids(["Q2774"])

        assert "Q2774" in result
        assert result["Q2774"].spotify_artist_id == "5bMqBjPbCOWGgWJpbAqdQq"
        assert result["Q2774"].apple_music_artist_id == "15821"
        assert result["Q2774"].bandcamp_id == "autechre"

    def test_partial_results_spotify_only(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q650826"),
                    "spotifyId": _literal("7x33x5bJkIeVJoamFCgPGj"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_streaming_ids(["Q650826"])

        assert "Q650826" in result
        assert result["Q650826"].spotify_artist_id == "7x33x5bJkIeVJoamFCgPGj"
        assert result["Q650826"].apple_music_artist_id is None
        assert result["Q650826"].bandcamp_id is None

    def test_no_streaming_ids_excluded(self):
        """QIDs that have none of the three properties are excluded from results."""
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q12345"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_streaming_ids(["Q12345"])

        assert result == {}

    def test_multiple_qids(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q2774"),
                    "spotifyId": _literal("5bMqBjPbCOWGgWJpbAqdQq"),
                },
                {
                    "item": _uri("Q650826"),
                    "bandcampId": _literal("stereolab"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_streaming_ids(["Q2774", "Q650826"])

        assert len(result) == 2
        assert result["Q2774"].spotify_artist_id == "5bMqBjPbCOWGgWJpbAqdQq"
        assert result["Q650826"].bandcamp_id == "stereolab"

    def test_empty_input_returns_empty(self):
        client = WikidataClient()
        result = client.lookup_streaming_ids([])
        assert result == {}

    def test_invalid_qids_skipped(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response(
            [
                {
                    "item": _uri("Q2774"),
                    "spotifyId": _literal("5bMqBjPbCOWGgWJpbAqdQq"),
                },
            ]
        )
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_streaming_ids(["Q2774", "INVALID"])

        assert len(result) == 1
        assert "Q2774" in result

    def test_sparql_error_returns_empty(self):
        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("Timeout")

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient()
            result = client.lookup_streaming_ids(["Q2774"])

        assert result == {}

    def test_batch_splitting(self):
        mock_response = MagicMock()
        mock_response.json.return_value = _sparql_response([])
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = WikidataClient(batch_size=2)
            client.lookup_streaming_ids(["Q1", "Q2", "Q3"])

        assert mock_client.get.call_count == 2


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
