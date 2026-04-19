"""Tests for MusicBrainz cache client."""

from unittest.mock import MagicMock, patch

from semantic_index.musicbrainz_client import MusicBrainzClient

GID_AUTECHRE = "410c9baf-5469-44f6-9852-826524b80c61"
GID_STEREOLAB = "f22942a1-6f70-4f48-866c-3f3e3f4e3b5e"
GID_BROADCAST = "aabbccdd-1122-3344-5566-778899aabbcc"


class TestResolveGidsToIds:
    """Tests for MusicBrainzClient.resolve_gids_to_ids()."""

    def test_returns_gid_to_integer_mapping(self) -> None:
        """GIDs are resolved to their integer IDs via mb_artist."""
        client = MusicBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            (42, GID_AUTECHRE),
            (99, GID_STEREOLAB),
        ]

        with patch.object(client, "_get_conn", return_value=mock_conn):
            result = client.resolve_gids_to_ids([GID_AUTECHRE, GID_STEREOLAB])

        assert result == {GID_AUTECHRE: 42, GID_STEREOLAB: 99}

    def test_empty_input_returns_empty(self) -> None:
        """Empty GID list returns empty dict without querying."""
        client = MusicBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")
        result = client.resolve_gids_to_ids([])
        assert result == {}

    def test_connection_failure_returns_empty(self) -> None:
        """Connection failure returns empty dict gracefully."""
        client = MusicBrainzClient(cache_dsn="postgresql://localhost/nonexistent")

        with patch.object(client, "_get_conn", return_value=None):
            result = client.resolve_gids_to_ids([GID_AUTECHRE])

        assert result == {}

    def test_partial_match_omits_unresolved(self) -> None:
        """GIDs not found in mb_artist are omitted from the result."""
        client = MusicBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")

        mock_conn = MagicMock()
        # Only 2 of 3 GIDs resolve
        mock_conn.execute.return_value.fetchall.return_value = [
            (42, GID_AUTECHRE),
            (99, GID_STEREOLAB),
        ]

        with patch.object(client, "_get_conn", return_value=mock_conn):
            result = client.resolve_gids_to_ids([GID_AUTECHRE, GID_STEREOLAB, GID_BROADCAST])

        assert result == {GID_AUTECHRE: 42, GID_STEREOLAB: 99}
        assert GID_BROADCAST not in result

    def test_batching_over_1000(self) -> None:
        """More than 1000 GIDs triggers multiple batched queries."""
        client = MusicBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")

        gids = [f"00000000-0000-0000-0000-{i:012d}" for i in range(1500)]

        mock_conn = MagicMock()
        # Return matching rows for each batch
        mock_conn.execute.return_value.fetchall.return_value = []

        with patch.object(client, "_get_conn", return_value=mock_conn):
            client.resolve_gids_to_ids(gids)

        assert mock_conn.execute.call_count == 2

    def test_query_failure_returns_empty(self) -> None:
        """Query exception returns empty dict with logged warning."""
        client = MusicBrainzClient(cache_dsn="postgresql://localhost/musicbrainz")

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("query failed")

        with patch.object(client, "_get_conn", return_value=mock_conn):
            result = client.resolve_gids_to_ids([GID_AUTECHRE])

        assert result == {}
