"""Tests for `--entity-source=lml` fail-loud behavior when LML PG is unavailable.

The historical audit asked the pipeline to "fall back to local reconciliation"
when LML PG is unreachable. We deliberately rejected that design: silent
fallback masks LML config errors (wrong DSN, expired credentials, network
issues). Instead, the pipeline raises :class:`LmlEntitySourceError` with a
clear message that points the operator at the explicit ``--entity-source=local``
workaround. The file name is preserved for traceability with the audit (#183).
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pytest

from semantic_index.lml_identity import LmlEntitySourceError

pytestmark = pytest.mark.integration

_RELATIVE = "tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql"


def _find_fixture() -> Path:
    override = os.environ.get("TUBAFRENZY_FIXTURE")
    if override:
        return Path(override)
    d = Path(__file__).resolve().parent
    while d != d.parent:
        candidate = d / _RELATIVE
        if candidate.exists():
            return candidate
        d = d.parent
    return Path(_RELATIVE)


FIXTURE_PATH = _find_fixture()


@pytest.fixture
def fixture_dump() -> str:
    if not FIXTURE_PATH.exists():
        pytest.skip(f"Fixture dump not found at {FIXTURE_PATH}")
    return str(FIXTURE_PATH)


def _make_args(**overrides) -> argparse.Namespace:
    """Build a Namespace mimicking parse_args() output, with overrides."""
    base = {
        "dump_path": "dump.sql",
        "entity_source": "lml",
        "discogs_cache_dsn": "postgresql://example/discogs",
        "verbose": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


class _RaisingPgSource:
    """Test double matching the PgSource interface but raising on fetchall.

    Mirrors the real :class:`semantic_index.lml_identity.PgSource` constructor
    signature so it can be substituted via monkeypatch.
    """

    instances: list[_RaisingPgSource] = []

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.closed = False
        _RaisingPgSource.instances.append(self)

    def fetchall(self, query: str):  # noqa: ARG002 — interface compat
        raise ConnectionRefusedError("Connection refused: localhost:5433")

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _reset_raising_instances():
    _RaisingPgSource.instances.clear()
    yield
    _RaisingPgSource.instances.clear()


def _patch_pg_source_to_raise(monkeypatch) -> None:
    """Replace the PgSource class so any LML PG connection attempt fails."""
    monkeypatch.setattr("semantic_index.lml_identity.PgSource", _RaisingPgSource)


class TestValidateLmlEntitySource:
    """Direct tests of the early-fail probe (`_validate_lml_entity_source`)."""

    def test_lml_unavailable_raises_clear_error(self, monkeypatch):
        """When LML PG cannot be reached, raise LmlEntitySourceError with guidance.

        The exception message must mention LML *and* the --entity-source=local
        workaround so the operator can immediately self-recover.
        """
        from run_pipeline import _validate_lml_entity_source

        _patch_pg_source_to_raise(monkeypatch)
        args = _make_args()

        with pytest.raises(LmlEntitySourceError) as excinfo:
            _validate_lml_entity_source(args)

        msg = str(excinfo.value)
        assert "LML" in msg
        assert "--entity-source=local" in msg
        # The original error type/text should be chained for debuggability.
        assert isinstance(excinfo.value.__cause__, ConnectionRefusedError)
        assert "ConnectionRefusedError" in msg
        # And the resource we were probing should be hinted in the chained message.
        assert "Connection refused" in msg

    def test_lml_unavailable_logs_clearly(self, monkeypatch, caplog):
        """An ERROR-level log should describe both the failure and the workaround."""
        from run_pipeline import _validate_lml_entity_source

        _patch_pg_source_to_raise(monkeypatch)
        args = _make_args()

        with caplog.at_level(logging.ERROR, logger="run_pipeline"):
            with pytest.raises(LmlEntitySourceError):
                _validate_lml_entity_source(args)

        assert any(
            "LML entity source unavailable" in rec.message
            and "--entity-source=local" in rec.message
            for rec in caplog.records
        ), f"Expected ERROR mentioning fallback, got: {[r.message for r in caplog.records]}"

    def test_missing_dsn_raises_with_actionable_message(self, monkeypatch):
        """--entity-source=lml without a DSN should fail fast with a clear message."""
        from run_pipeline import _validate_lml_entity_source

        # No PgSource patch needed: we never get that far.
        args = _make_args(discogs_cache_dsn=None)

        with pytest.raises(LmlEntitySourceError) as excinfo:
            _validate_lml_entity_source(args)

        msg = str(excinfo.value)
        assert "--discogs-cache-dsn" in msg
        assert "--entity-source=local" in msg

    def test_pg_source_is_closed_even_on_failure(self, monkeypatch):
        """The PgSource connection is closed in the failure path (no leaks)."""
        from run_pipeline import _validate_lml_entity_source

        _patch_pg_source_to_raise(monkeypatch)
        args = _make_args()

        with pytest.raises(LmlEntitySourceError):
            _validate_lml_entity_source(args)

        assert len(_RaisingPgSource.instances) == 1
        assert _RaisingPgSource.instances[0].closed is True


class TestPipelineRunWithEntitySource:
    """End-to-end checks running the pipeline against the fixture dump."""

    def test_local_entity_source_works_when_lml_down(self, monkeypatch, fixture_dump, tmp_path):
        """`--entity-source=local` succeeds even when LML PG would fail.

        This verifies the user-facing workaround actually works: an operator
        seeing the LmlEntitySourceError message can re-run with =local and
        get a graph DB.
        """
        from run_pipeline import main

        # Even if some code path tried to reach LML, the patched PgSource
        # would raise. With --entity-source=local we should never instantiate
        # it, and the pipeline must complete.
        _patch_pg_source_to_raise(monkeypatch)

        out_dir = tmp_path / "out"
        main(
            [
                fixture_dump,
                "--output-dir",
                str(out_dir),
                "--min-count",
                "1",
                "--entity-source",
                "local",
                # Provide a DSN to prove that "local" ignores it entirely.
                "--discogs-cache-dsn",
                "postgresql://wont-be-used/discogs",
                "--skip-enrichment",
                "--no-graph-metrics",
            ]
        )

        sqlite_path = out_dir / "wxyc_artist_graph.db"
        assert sqlite_path.exists()
        # And no LML PG connection was ever attempted.
        assert _RaisingPgSource.instances == []

    def test_lml_unavailable_pipeline_fails_fast(self, monkeypatch, fixture_dump, tmp_path):
        """`--entity-source=lml` fails before any expensive parsing work.

        We assert the failure surfaces as :class:`LmlEntitySourceError` (not a
        bare exception) and that no SQLite DB was produced.
        """
        from run_pipeline import main

        _patch_pg_source_to_raise(monkeypatch)
        out_dir = tmp_path / "out"

        with pytest.raises(LmlEntitySourceError):
            main(
                [
                    fixture_dump,
                    "--output-dir",
                    str(out_dir),
                    "--min-count",
                    "1",
                    "--entity-source",
                    "lml",
                    "--discogs-cache-dsn",
                    "postgresql://example/discogs",
                ]
            )

        sqlite_path = out_dir / "wxyc_artist_graph.db"
        assert not sqlite_path.exists(), "Pipeline wrote output despite LML failure"

    def test_lml_success_path_still_works(self, monkeypatch, fixture_dump, tmp_path):
        """Sanity check: with a working (mocked) LML PG, the pipeline runs.

        Uses a stub PgSource that returns one identity row matching an artist
        likely to appear in the fixture dump. We don't assert on identity
        rows specifically (the fixture is small) — only that the pipeline
        completes and writes a non-empty `artist` table.
        """
        import sqlite3

        from run_pipeline import main

        class _StubPgSource:
            def __init__(self, dsn: str) -> None:
                self.dsn = dsn

            def fetchall(self, query: str):  # noqa: ARG002
                return [
                    {
                        "library_name": "Aphex Twin",
                        "discogs_artist_id": 45,
                        "wikidata_qid": "Q1397",
                        "musicbrainz_artist_id": ("f22942a1-6f70-4f48-866e-238cb2308fbd"),
                        "spotify_artist_id": "6kBDZFXuLrZgHnvmPu9NsG",
                        "apple_music_artist_id": "3024009",
                        "bandcamp_id": None,
                        "reconciliation_status": "reconciled",
                    },
                ]

            def close(self) -> None:
                pass

        monkeypatch.setattr("semantic_index.lml_identity.PgSource", _StubPgSource)

        out_dir = tmp_path / "out"
        db_path = out_dir / "wxyc_artist_graph.db"
        # `--db-path` triggers the pipeline-DB path that exercises LML import.
        out_dir.mkdir()

        main(
            [
                fixture_dump,
                "--output-dir",
                str(out_dir),
                "--min-count",
                "1",
                "--entity-source",
                "lml",
                "--discogs-cache-dsn",
                "postgresql://example/discogs",
                "--db-path",
                str(db_path),
                "--skip-enrichment",
                "--no-graph-metrics",
            ]
        )

        assert db_path.exists()
        with sqlite3.connect(str(db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        assert count > 0, "Pipeline produced an empty artist table"
