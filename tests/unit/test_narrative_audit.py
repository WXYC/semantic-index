"""Tests for the periodic claim-ratio audit (#230)."""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


class TestParseClaimCounts:
    def test_parse_counts_line(self) -> None:
        from semantic_index.narrative_audit import parse_claim_counts

        text = "G: artists co-occur\nU: psychedelic\nCOUNTS: 1g 1u"
        assert parse_claim_counts(text) == (1, 1)

    def test_falls_back_to_line_prefix_count_when_no_summary(self) -> None:
        """Some Haiku responses skip the COUNTS line — fall back to G:/U: prefixes."""
        from semantic_index.narrative_audit import parse_claim_counts

        text = "G: claim one\nG: claim two\nU: hallucinated thing\nU: another\nU: third"
        assert parse_claim_counts(text) == (2, 3)

    def test_empty_response_returns_zero_zero(self) -> None:
        from semantic_index.narrative_audit import parse_claim_counts

        assert parse_claim_counts("") == (0, 0)


class TestAuditDB:
    def test_record_audit_result_persists_row(self) -> None:
        """A single audit run row is persisted with score, flag, narrative excerpt."""
        import sqlite3
        import tempfile

        from semantic_index.narrative_audit import open_audit_db, record_audit_result

        # open_audit_db(path) opens the sidecar at path + ".narrative-audit-cache.db"
        # — same convention as the other sidecar caches.
        base_path = tempfile.mktemp(suffix=".db")
        conn = open_audit_db(base_path)
        record_audit_result(
            conn,
            source_id=1,
            target_id=2,
            month=0,
            dj_id=0,
            edge_type="",
            prompt_version=11,
            narrative="WXYC DJs pair X and Y.",
            claim_ratio=0.0,
            grounded=2,
            ungrounded=0,
            flagged=False,
        )
        conn.close()

        verify = sqlite3.connect(base_path + ".narrative-audit-cache.db")
        verify.row_factory = sqlite3.Row
        rows = verify.execute(
            "SELECT source_id, target_id, claim_ratio, flagged, grounded, ungrounded "
            "FROM narrative_audit"
        ).fetchall()
        verify.close()

        assert len(rows) == 1
        assert rows[0]["source_id"] == 1
        assert rows[0]["target_id"] == 2
        assert rows[0]["claim_ratio"] == 0.0
        assert rows[0]["flagged"] == 0
        assert rows[0]["grounded"] == 2
        assert rows[0]["ungrounded"] == 0


def _build_cache_with_n_entries(path: str, n: int) -> str:
    """Populate a narrative-cache sidecar with ``n`` synthetic rows."""
    import sqlite3
    from datetime import UTC, datetime

    sidecar = path + ".narrative-cache.db"
    conn = sqlite3.connect(sidecar)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS narrative_cache (
            source_id INTEGER NOT NULL,
            target_id INTEGER NOT NULL,
            month INTEGER NOT NULL DEFAULT 0,
            dj_id INTEGER NOT NULL DEFAULT 0,
            edge_type TEXT NOT NULL DEFAULT '',
            prompt_version INTEGER NOT NULL DEFAULT 1,
            insufficient_signal INTEGER NOT NULL DEFAULT 0,
            token_match_score REAL NOT NULL DEFAULT 0.0,
            retry_count INTEGER NOT NULL DEFAULT 0,
            narrative TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (source_id, target_id, month, dj_id, edge_type, prompt_version)
        );
        """
    )
    now = datetime.now(UTC).isoformat()
    rows = [(i, i + 1000, 0, 0, "", 11, 0, 0.0, 0, f"narrative {i}", now) for i in range(n)]
    conn.executemany("INSERT INTO narrative_cache VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return sidecar


class TestSampling:
    def test_samples_at_most_n_rows(self) -> None:
        """``sample_cached_narratives`` returns at most ``n`` rows."""
        import tempfile

        from semantic_index.narrative_audit import sample_cached_narratives

        base_path = tempfile.mktemp(suffix=".db")
        _build_cache_with_n_entries(base_path, 50)
        sample = sample_cached_narratives(base_path, n=10)
        assert len(sample) == 10

    def test_sample_returns_all_when_fewer_than_requested(self) -> None:
        """If the cache has fewer entries than ``n``, return all of them."""
        import tempfile

        from semantic_index.narrative_audit import sample_cached_narratives

        base_path = tempfile.mktemp(suffix=".db")
        _build_cache_with_n_entries(base_path, 3)
        sample = sample_cached_narratives(base_path, n=10)
        assert len(sample) == 3

    def test_sample_skips_insufficient_signal_rows(self) -> None:
        """Insufficient-signal placeholders aren't real narratives — exclude them."""
        import sqlite3
        import tempfile
        from datetime import UTC, datetime

        from semantic_index.narrative_audit import sample_cached_narratives

        base_path = tempfile.mktemp(suffix=".db")
        sidecar = _build_cache_with_n_entries(base_path, 5)
        conn = sqlite3.connect(sidecar)
        # Mark all five as insufficient_signal — no rows should be sampled.
        conn.execute("UPDATE narrative_cache SET insufficient_signal = 1")
        conn.commit()
        # Add a single eligible row.
        conn.execute(
            "INSERT INTO narrative_cache VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                999,
                1999,
                0,
                0,
                "",
                11,
                0,
                0.1,
                0,
                "real narrative",
                datetime.now(UTC).isoformat(),
            ),
        )
        conn.commit()
        conn.close()

        sample = sample_cached_narratives(base_path, n=10)
        assert len(sample) == 1
        assert sample[0]["source_id"] == 999

    def test_sample_returns_empty_when_cache_missing(self) -> None:
        """Fresh deploys have no narrative cache yet — return [] without crashing."""
        import tempfile

        from semantic_index.narrative_audit import sample_cached_narratives

        base_path = tempfile.mktemp(suffix=".db")
        # No cache file created — base_path.narrative-cache.db does not exist.
        assert sample_cached_narratives(base_path, n=10) == []


class TestRunAudit:
    def test_run_audit_scores_each_sample_and_records_results(self) -> None:
        """End-to-end: sample → score → record. Flagged count reflects threshold."""
        import sqlite3
        import tempfile
        from unittest.mock import MagicMock

        from semantic_index.narrative_audit import run_audit

        base_path = tempfile.mktemp(suffix=".db")
        _build_cache_with_n_entries(base_path, 3)
        _build_minimal_app_db(
            base_path,
            artists=[(i, f"artist {i}") for i in range(3)]
            + [(i + 1000, f"target {i}") for i in range(3)],
        )

        # Mock returns alternating responses: clean, dirty, clean.
        responses = [
            "G: claim\nCOUNTS: 1g 0u",  # ratio 0.0, not flagged at 0.2
            "G: a\nU: b\nU: c\nU: d\nCOUNTS: 1g 3u",  # ratio 0.75, flagged
            "G: a\nG: b\nCOUNTS: 2g 0u",  # ratio 0.0, not flagged
        ]
        mock_client = MagicMock()

        def make_msg(text):
            m = MagicMock()
            block = MagicMock()
            block.text = text
            m.content = [block]
            return m

        mock_client.messages.create.side_effect = [make_msg(t) for t in responses]

        summary = run_audit(base_path, client=mock_client, n=3, threshold=0.2)

        assert summary["audited"] == 3
        assert summary["flagged"] == 1

        # Verify rows persisted with the right flag distribution.
        verify = sqlite3.connect(base_path + ".narrative-audit-cache.db")
        verify.row_factory = sqlite3.Row
        rows = verify.execute(
            "SELECT claim_ratio, flagged FROM narrative_audit ORDER BY id"
        ).fetchall()
        verify.close()
        assert len(rows) == 3
        flagged_count = sum(r["flagged"] for r in rows)
        assert flagged_count == 1

    def test_run_audit_passes_artist_metadata_to_verifier(self) -> None:
        """The verifier should see source/target names + styles, not raw integer IDs.

        Without artist names in ``provided_data``, every descriptive claim in a
        narrative is ungroundable by construction, which makes the threshold
        meaningless. The audit looks up metadata from the production DB the
        same way the live narrative endpoint did when it generated the cached
        text.
        """
        import json
        import tempfile
        from unittest.mock import MagicMock

        from semantic_index.narrative_audit import run_audit

        base_path = tempfile.mktemp(suffix=".db")
        _build_cache_with_n_entries(base_path, 1)
        _build_minimal_app_db(
            base_path,
            artists=[(0, "Stereolab"), (1000, "Cat Power")],
        )

        mock_client = MagicMock()
        msg = MagicMock()
        block = MagicMock()
        block.text = "G: a\nCOUNTS: 1g 0u"
        msg.content = [block]
        mock_client.messages.create.return_value = msg

        run_audit(base_path, client=mock_client, n=1, threshold=0.2)

        sent_payload = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
        parsed = json.loads(sent_payload)
        provided = parsed["provided_data"]
        assert provided["source"]["name"] == "Stereolab"
        assert provided["target"]["name"] == "Cat Power"

    def test_run_audit_threshold_is_strict_above(self) -> None:
        """A score equal to the threshold is NOT flagged (strict ``>``)."""
        import tempfile
        from unittest.mock import MagicMock

        from semantic_index.narrative_audit import run_audit

        base_path = tempfile.mktemp(suffix=".db")
        _build_cache_with_n_entries(base_path, 1)
        _build_minimal_app_db(base_path, artists=[(0, "Stereolab"), (1000, "Cat Power")])

        # Exactly threshold: 1g + 1u → ratio 0.5
        mock_client = MagicMock()
        msg = MagicMock()
        block = MagicMock()
        block.text = "G: a\nU: b\nCOUNTS: 1g 1u"
        msg.content = [block]
        mock_client.messages.create.return_value = msg

        summary = run_audit(base_path, client=mock_client, n=1, threshold=0.5)
        assert summary["flagged"] == 0


class TestRecentAudits:
    def test_read_recent_returns_empty_for_fresh_db(self) -> None:
        import tempfile

        from semantic_index.narrative_audit import read_recent_audits

        base_path = tempfile.mktemp(suffix=".db")
        assert read_recent_audits(base_path, limit=10) == []

    def test_read_recent_returns_rows_in_reverse_chronological_order(self) -> None:
        import tempfile

        from semantic_index.narrative_audit import (
            open_audit_db,
            read_recent_audits,
            record_audit_result,
        )

        base_path = tempfile.mktemp(suffix=".db")
        conn = open_audit_db(base_path)
        for i in range(3):
            record_audit_result(
                conn,
                source_id=i,
                target_id=i + 100,
                month=0,
                dj_id=0,
                edge_type="",
                prompt_version=11,
                narrative=f"narrative {i}",
                claim_ratio=0.1 * i,
                grounded=2,
                ungrounded=i,
                flagged=(i == 2),
            )
        conn.close()

        rows = read_recent_audits(base_path, limit=10)
        assert len(rows) == 3
        # Most-recent first: source_ids should descend (2, 1, 0).
        assert [r["source_id"] for r in rows] == [2, 1, 0]
        assert rows[0]["flagged"] is True
        assert rows[1]["flagged"] is False

    def test_read_recent_filters_by_flagged(self) -> None:
        import tempfile

        from semantic_index.narrative_audit import (
            open_audit_db,
            read_recent_audits,
            record_audit_result,
        )

        base_path = tempfile.mktemp(suffix=".db")
        conn = open_audit_db(base_path)
        for i in range(5):
            record_audit_result(
                conn,
                source_id=i,
                target_id=i + 100,
                month=0,
                dj_id=0,
                edge_type="",
                prompt_version=11,
                narrative=f"narrative {i}",
                claim_ratio=0.5 if i < 2 else 0.0,
                grounded=2,
                ungrounded=2 if i < 2 else 0,
                flagged=i < 2,
            )
        conn.close()

        flagged_rows = read_recent_audits(base_path, limit=10, flagged_only=True)
        assert len(flagged_rows) == 2
        assert all(r["flagged"] for r in flagged_rows)


def _build_minimal_app_db(path: str, artists: list[tuple[int, str]] | None = None) -> None:
    """Build the smallest DB the API factory needs.

    Always creates an empty ``artist`` table. When ``artists`` is provided,
    seed those rows so the audit can resolve source/target IDs to names.
    """
    import sqlite3

    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE artist (id INTEGER PRIMARY KEY, canonical_name TEXT NOT NULL UNIQUE, "
        "genre TEXT, total_plays INTEGER NOT NULL DEFAULT 0);"
    )
    if artists:
        conn.executemany(
            "INSERT INTO artist (id, canonical_name) VALUES (?, ?)",
            artists,
        )
    conn.commit()
    conn.close()


class TestAuditEndpoint:
    @pytest_asyncio.fixture
    async def client(self, tmp_path):
        from semantic_index.api.app import create_app

        db_path = str(tmp_path / "graph.db")
        _build_minimal_app_db(db_path)
        app = create_app(db_path)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac, db_path

    @pytest.mark.asyncio
    async def test_recent_endpoint_empty_when_no_audits(self, client) -> None:
        ac, _ = client
        resp = await ac.get("/graph/narrative-audit/recent")
        assert resp.status_code == 200
        assert resp.json() == {"audits": []}

    @pytest.mark.asyncio
    async def test_recent_endpoint_returns_recorded_rows(self, client) -> None:
        from semantic_index.narrative_audit import open_audit_db, record_audit_result

        ac, db_path = client
        conn = open_audit_db(db_path)
        record_audit_result(
            conn,
            source_id=1,
            target_id=2,
            month=0,
            dj_id=0,
            edge_type="",
            prompt_version=11,
            narrative="WXYC DJs pair X and Y.",
            claim_ratio=0.5,
            grounded=1,
            ungrounded=1,
            flagged=True,
        )
        conn.close()

        resp = await ac.get("/graph/narrative-audit/recent")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["audits"]) == 1
        assert body["audits"][0]["flagged"] is True
        assert body["audits"][0]["claim_ratio"] == 0.5

    @pytest.mark.asyncio
    async def test_recent_endpoint_supports_flagged_only(self, client) -> None:
        from semantic_index.narrative_audit import open_audit_db, record_audit_result

        ac, db_path = client
        conn = open_audit_db(db_path)
        record_audit_result(
            conn,
            source_id=1,
            target_id=2,
            month=0,
            dj_id=0,
            edge_type="",
            prompt_version=11,
            narrative="clean",
            claim_ratio=0.0,
            grounded=2,
            ungrounded=0,
            flagged=False,
        )
        record_audit_result(
            conn,
            source_id=3,
            target_id=4,
            month=0,
            dj_id=0,
            edge_type="",
            prompt_version=11,
            narrative="dirty",
            claim_ratio=0.6,
            grounded=2,
            ungrounded=3,
            flagged=True,
        )
        conn.close()

        resp = await ac.get("/graph/narrative-audit/recent?flagged_only=true")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["audits"]) == 1
        assert body["audits"][0]["source_id"] == 3
