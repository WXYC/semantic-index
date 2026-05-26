"""Top-K-per-artist prune for symmetric ``(artist_a_id, artist_b_id)`` edge tables.

Used by ``acousticbrainz.prune_acoustic_similarity``, ``discogs_edges.prune_shared_personnel``,
and ``discogs_edges.prune_label_family``. Each table's public wrapper supplies its
own ranking expression (``similarity``, ``shared_count``, ``json_array_length(shared_labels)``);
the SQL shape — keep-set TEMP table, ROW_NUMBER per endpoint, canonical re-MIN/MAX,
NOT EXISTS delete — is shared.

The function is transaction-neutral: it neither commits nor rolls back, so it
composes with dry-run wrappers and with multi-step pipeline transactions.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)


def prune_symmetric_edge_table(
    conn: sqlite3.Connection,
    *,
    table: str,
    weight_expr: str,
    top_k: int,
) -> tuple[int, int]:
    """Prune ``table`` to top-K per artist by ``weight_expr``, keeping either-side hits.

    For each artist X, rank every neighbor (regardless of canonical direction) by
    ``weight_expr DESC`` with a deterministic tiebreaker on the other endpoint id.
    An edge survives if it appears in *either* endpoint's top-K — so every artist
    always keeps their best matches even when the other endpoint is heavily
    connected. Kept pairs are re-canonicalized to ``artist_a_id < artist_b_id``.

    Args:
        conn: SQLite connection. The caller owns the transaction; this function
            does not commit so it composes cleanly with dry-run wrappers.
        table: Edge table name. Must have ``artist_a_id`` and ``artist_b_id``
            columns with the canonical ``artist_a_id < artist_b_id`` invariant.
        weight_expr: SQL expression evaluated against rows of ``table`` that
            returns a sortable ranking key (higher = stronger edge). Examples:
            ``"similarity"``, ``"shared_count"``,
            ``"json_array_length(shared_labels)"``.
        top_k: Per-artist neighbor cap (must be > 0).

    Returns:
        ``(rows_before, rows_after)`` count tuple for reporting.

    Raises:
        ValueError: If ``top_k <= 0``.
    """
    if top_k <= 0:
        raise ValueError(f"top_k must be positive, got {top_k}")

    before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
    if before == 0:
        return 0, 0

    # _keep_<table> TEMP table holds the (a,b) pairs to keep. Use plain execute()
    # rather than executescript(): the latter issues an implicit COMMIT, which
    # would silently break the "caller manages the transaction" contract.
    conn.execute(f"DROP TABLE IF EXISTS _keep_{table}")
    conn.execute(
        f"""
        CREATE TEMP TABLE _keep_{table} (
            artist_a_id INTEGER NOT NULL,
            artist_b_id INTEGER NOT NULL,
            PRIMARY KEY (artist_a_id, artist_b_id)
        )
        """  # noqa: S608
    )
    conn.execute(
        f"""
        INSERT OR IGNORE INTO _keep_{table} (artist_a_id, artist_b_id)
        SELECT MIN(x_id, y_id), MAX(x_id, y_id) FROM (
            SELECT x_id, y_id, ROW_NUMBER() OVER (
                PARTITION BY x_id ORDER BY w DESC, y_id
            ) AS rn
            FROM (
                SELECT artist_a_id AS x_id, artist_b_id AS y_id, ({weight_expr}) AS w
                FROM {table}
                UNION ALL
                SELECT artist_b_id, artist_a_id, ({weight_expr})
                FROM {table}
            )
        )
        WHERE rn <= ?
        """,  # noqa: S608
        (top_k,),
    )
    conn.execute(
        f"""
        DELETE FROM {table}
        WHERE NOT EXISTS (
            SELECT 1 FROM _keep_{table} k
            WHERE k.artist_a_id = {table}.artist_a_id
              AND k.artist_b_id = {table}.artist_b_id
        )
        """  # noqa: S608
    )
    conn.execute(f"DROP TABLE _keep_{table}")

    after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
    logger.info(
        "%s prune: %d → %d edges (top_k=%d, kept %.1f%%)",
        table,
        before,
        after,
        top_k,
        (after / before * 100) if before else 0,
    )
    return before, after
