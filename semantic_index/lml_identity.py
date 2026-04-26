"""Import pre-resolved artist identities from LML's entity.identity table.

The pipeline reads identity mappings from the discogs-cache PostgreSQL
database (``entity.identity`` table) and writes them into the local
SQLite pipeline database. This module provides the bridge function
that reads those identities and applies them to local artist rows.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


class LmlEntitySourceError(RuntimeError):
    """Raised when the LML entity source is selected but cannot be reached.

    This is a fail-loud signal: when ``--entity-source=lml`` is requested and
    LML PG is unavailable (connection refused, auth failure, timeout, missing
    DSN), we surface this exception with a clear actionable message instead
    of silently falling back to local reconciliation. Silent fallback masks
    real LML configuration errors (wrong DSN, expired credentials, etc.).
    To skip LML entirely, the operator should pass ``--entity-source=local``.
    """


_FETCH_ALL_IDENTITIES_SQL = """\
SELECT library_name, discogs_artist_id, wikidata_qid,
       musicbrainz_artist_id, spotify_artist_id,
       apple_music_artist_id, bandcamp_id, reconciliation_status
FROM entity.identity
WHERE reconciliation_status != 'unreconciled'
"""


@runtime_checkable
class PgFetchProtocol(Protocol):
    """Protocol for a PostgreSQL connection that can fetch rows as dicts."""

    def fetchall(self, query: str) -> list[dict[str, Any]]: ...
    def close(self) -> None: ...


class PgSource:
    """Synchronous PostgreSQL source using psycopg.

    Matches the interface used by ``discogs_client.py`` in semantic-index.
    Returns rows as dicts.

    Args:
        dsn: PostgreSQL connection string.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: Any | None = None

    def _get_conn(self) -> Any:
        if self._conn is None:
            import psycopg
            from psycopg.rows import dict_row

            self._conn = psycopg.connect(self._dsn, autocommit=True, row_factory=dict_row)
        return self._conn

    def fetchall(self, query: str) -> list[dict[str, Any]]:
        """Execute a query and return all rows as dicts."""
        conn = self._get_conn()
        result: list[dict[str, Any]] = conn.execute(query).fetchall()
        return result

    def close(self) -> None:
        """Close the connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None


@dataclass
class ImportReport:
    """Summary of an LML identity import run."""

    matched: int = 0
    unmatched: int = 0
    entities_created: int = 0


def import_lml_identities(
    pipeline_db: Any,
    pg_source: PgFetchProtocol,
) -> ImportReport:
    """Read identities from LML's entity.identity PG table and apply to local pipeline DB.

    For each identity row, updates the local SQLite artist row with external IDs
    (discogs_artist_id, musicbrainz_artist_id, reconciliation_status) and creates
    an entity row with QID and streaming IDs when a wikidata_qid is present.

    Args:
        pipeline_db: The local SQLite PipelineDB instance.
        pg_source: A PG source connected to the discogs-cache database.

    Returns:
        ImportReport with match/unmatch counts.

    Raises:
        Exception: If the PG connection fails.
    """
    rows = pg_source.fetchall(_FETCH_ALL_IDENTITIES_SQL)

    # Index by library_name for fast lookup
    identity_by_name: dict[str, dict[str, Any]] = {row["library_name"]: row for row in rows}

    # Get all local artist names
    conn = pipeline_db._conn
    conn.row_factory = sqlite3.Row
    local_artists = conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    conn.row_factory = None

    report = ImportReport()
    now_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"

    for artist_row in local_artists:
        artist_id = artist_row[0]
        canonical_name = artist_row[1]
        identity = identity_by_name.get(canonical_name)

        if identity is None:
            report.unmatched += 1
            continue

        report.matched += 1

        # Update artist row with external IDs
        pipeline_db.upsert_artist(
            canonical_name,
            discogs_artist_id=identity["discogs_artist_id"],
            musicbrainz_artist_id=identity["musicbrainz_artist_id"],
        )

        # Update reconciliation status
        conn.execute(
            f"UPDATE artist SET reconciliation_status = ?, updated_at = {now_expr} WHERE id = ?",
            (identity["reconciliation_status"], artist_id),
        )

        # Create/update entity with QID and streaming IDs
        qid = identity.get("wikidata_qid")
        spotify = identity.get("spotify_artist_id")
        apple_music = identity.get("apple_music_artist_id")
        bandcamp = identity.get("bandcamp_id")

        if qid or spotify or apple_music or bandcamp:
            # Check if artist already has an entity
            conn.row_factory = sqlite3.Row
            existing = conn.execute(
                "SELECT entity_id FROM artist WHERE id = ?", (artist_id,)
            ).fetchone()
            conn.row_factory = None
            existing_entity_id = existing[0] if existing and existing[0] else None

            if existing_entity_id:
                # Update existing entity
                conn.execute(
                    f"UPDATE entity SET "
                    f"wikidata_qid = COALESCE(?, wikidata_qid), "
                    f"spotify_artist_id = COALESCE(?, spotify_artist_id), "
                    f"apple_music_artist_id = COALESCE(?, apple_music_artist_id), "
                    f"bandcamp_id = COALESCE(?, bandcamp_id), "
                    f"updated_at = {now_expr} "
                    f"WHERE id = ?",
                    (qid, spotify, apple_music, bandcamp, existing_entity_id),
                )
            else:
                # Create new entity and link to artist
                cur = conn.execute(
                    f"INSERT INTO entity (name, entity_type, wikidata_qid, "
                    f"spotify_artist_id, apple_music_artist_id, bandcamp_id, "
                    f"created_at, updated_at) "
                    f"VALUES (?, 'artist', ?, ?, ?, ?, {now_expr}, {now_expr})",
                    (canonical_name, qid, spotify, apple_music, bandcamp),
                )
                new_entity_id = cur.lastrowid
                conn.execute(
                    "UPDATE artist SET entity_id = ? WHERE id = ?",
                    (new_entity_id, artist_id),
                )
                report.entities_created += 1

    conn.commit()

    logger.info(
        "LML identity import: %d matched, %d unmatched, %d entities created",
        report.matched,
        report.unmatched,
        report.entities_created,
    )
    return report
