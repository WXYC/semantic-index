"""Periodic claim-ratio audit on cached narratives.

Runs a Haiku verifier prompt on a sample of cached narratives, scoring each
as grounded vs ungrounded claims. Catches structural-claim hallucinations
that the always-on token-match gate (the vocabulary check) can miss.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import UTC, datetime
from typing import Any

from semantic_index.api.database import open_cache_db

_AUDIT_SCHEMA = """
CREATE TABLE IF NOT EXISTS narrative_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    month INTEGER NOT NULL DEFAULT 0,
    dj_id INTEGER NOT NULL DEFAULT 0,
    edge_type TEXT NOT NULL DEFAULT '',
    prompt_version INTEGER NOT NULL,
    narrative TEXT NOT NULL,
    claim_ratio REAL NOT NULL,
    grounded INTEGER NOT NULL,
    ungrounded INTEGER NOT NULL,
    flagged INTEGER NOT NULL DEFAULT 0,
    audited_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_narrative_audit_audited_at
    ON narrative_audit (audited_at DESC);

CREATE INDEX IF NOT EXISTS idx_narrative_audit_flagged
    ON narrative_audit (flagged, audited_at DESC);
"""


def sample_cached_narratives(db_path: str, *, n: int) -> list[dict]:
    """Sample up to ``n`` cached narratives from the narrative-cache sidecar.

    Excludes ``insufficient_signal`` placeholders — those carry a deterministic
    canned text that doesn't need auditing. Random-orders the sample so an
    audit doesn't repeatedly hit the same rows on consecutive runs. Returns
    ``[]`` when the cache sidecar doesn't exist (fresh deploy before any
    narratives have been generated).
    """
    sidecar = db_path + ".narrative-cache.db"
    if not os.path.exists(sidecar):
        return []
    conn = sqlite3.connect(sidecar)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT source_id, target_id, month, dj_id, edge_type, prompt_version, "
            "narrative FROM narrative_cache "
            "WHERE insufficient_signal = 0 "
            "ORDER BY RANDOM() LIMIT ?",
            (n,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


_DEFAULT_AUDIT_THRESHOLD = 0.2


def read_recent_audits(db_path: str, *, limit: int = 50, flagged_only: bool = False) -> list[dict]:
    """Return the most-recent audit rows from the audit sidecar.

    Returns ``[]`` when the sidecar doesn't exist yet (fresh deploy before
    the first audit run). Rows are ordered most-recent first.
    """
    sidecar = db_path + ".narrative-audit-cache.db"
    if not _db_has_audit_table(sidecar):
        return []
    conn = sqlite3.connect(sidecar)
    conn.row_factory = sqlite3.Row
    where = "WHERE flagged = 1" if flagged_only else ""
    try:
        rows = conn.execute(
            "SELECT id, source_id, target_id, month, dj_id, edge_type, "
            "prompt_version, narrative, claim_ratio, grounded, ungrounded, "
            "flagged, audited_at "
            f"FROM narrative_audit {where} "  # noqa: S608
            "ORDER BY audited_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    out: list[dict] = []
    for row in rows:
        d = dict(row)
        d["flagged"] = bool(d["flagged"])
        out.append(d)
    return out


def _db_has_audit_table(sidecar_path: str) -> bool:
    """``True`` when the audit sidecar exists and has the ``narrative_audit`` table."""
    if not os.path.exists(sidecar_path):
        return False
    conn = sqlite3.connect(sidecar_path)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='narrative_audit'"
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def run_audit(
    db_path: str,
    *,
    client: Any,
    n: int = 100,
    threshold: float = _DEFAULT_AUDIT_THRESHOLD,
) -> dict:
    """Sample ``n`` cached narratives, score each via Haiku, record results.

    Resolves each sample's ``source_id`` / ``target_id`` against the production
    SQLite at ``db_path`` so the verifier sees the same artist metadata
    (name, genre, styles, audio profile) the live narrative endpoint scored
    against. Without this, integer IDs alone would render every descriptive
    claim ungroundable and the threshold meaningless.

    Returns a summary dict ``{"audited": N, "flagged": M}`` where ``flagged``
    is the count of narratives whose claim-ratio exceeded the threshold.
    Strict ``>`` boundary — a score equal to the threshold is not flagged.

    Note: ``shared_neighbors`` and ``relationships`` (which the live endpoint
    also fed the model) are not reconstructed here. Claims about specific
    shared neighbors will therefore register as ungrounded in the audit —
    that's the intended behaviour, since we want the audit to flag neighbor
    hallucinations.
    """
    samples = sample_cached_narratives(db_path, n=n)
    audit_db = open_audit_db(db_path)
    read_db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    read_db.row_factory = sqlite3.Row
    flagged = 0
    try:
        for sample in samples:
            input_data = _build_audit_input(read_db, sample)
            verify_payload = json.dumps(
                {"narrative": sample["narrative"], "provided_data": input_data},
                separators=(",", ":"),
            )
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                system=_CLAIM_DECOMPOSE_PROMPT,
                messages=[{"role": "user", "content": verify_payload}],
            )
            grounded, ungrounded = parse_claim_counts(response.content[0].text)
            total = grounded + ungrounded
            ratio = (ungrounded / total) if total else 0.0
            is_flagged = ratio > threshold
            if is_flagged:
                flagged += 1
            record_audit_result(
                audit_db,
                source_id=sample["source_id"],
                target_id=sample["target_id"],
                month=sample["month"],
                dj_id=sample["dj_id"],
                edge_type=sample["edge_type"],
                prompt_version=sample["prompt_version"],
                narrative=sample["narrative"],
                claim_ratio=ratio,
                grounded=grounded,
                ungrounded=ungrounded,
                flagged=is_flagged,
            )
    finally:
        read_db.close()
        audit_db.close()
    return {"audited": len(samples), "flagged": flagged}


def _build_audit_input(read_db: sqlite3.Connection, sample: dict) -> dict:
    """Reconstruct the verifier's ``provided_data`` from the production DB.

    Looks up source/target metadata via ``narrative._lookup_artist_metadata``
    so the audit feeds the verifier the same shape of data the live narrative
    endpoint scored against (name, genre, styles, audio profile when present).
    Falls back to ``{"name": "<unknown>"}`` for any artist that's no longer
    present in the production DB (rare — usually a cache-staleness edge case).
    """
    from semantic_index.api.narrative import _lookup_artist_metadata

    source_meta = _resolve_meta(read_db, sample["source_id"], _lookup_artist_metadata)
    target_meta = _resolve_meta(read_db, sample["target_id"], _lookup_artist_metadata)
    return {
        "source": source_meta,
        "target": target_meta,
        "month": sample["month"],
        "dj_id": sample["dj_id"],
        "edge_type": sample["edge_type"],
    }


def _resolve_meta(read_db: sqlite3.Connection, artist_id: int, lookup: Any) -> dict:
    row = read_db.execute(
        "SELECT canonical_name, genre, total_plays FROM artist WHERE id = ?",
        (artist_id,),
    ).fetchone()
    if row is None:
        return {"name": "<unknown>"}
    meta: dict = lookup(read_db, artist_id, row["canonical_name"], row["genre"], row["total_plays"])
    return meta


def open_audit_db(db_path: str) -> sqlite3.Connection:
    """Open (or create) the narrative-audit sidecar database.

    The audit DB is a separate sidecar from the narrative cache so audit
    results survive cache-version bumps that drop the cache table.
    """
    return open_cache_db(db_path, "narrative-audit", _AUDIT_SCHEMA)


def record_audit_result(
    conn: sqlite3.Connection,
    *,
    source_id: int,
    target_id: int,
    month: int,
    dj_id: int,
    edge_type: str,
    prompt_version: int,
    narrative: str,
    claim_ratio: float,
    grounded: int,
    ungrounded: int,
    flagged: bool,
) -> None:
    """Append a single audit-run row."""
    conn.execute(
        "INSERT INTO narrative_audit "
        "(source_id, target_id, month, dj_id, edge_type, prompt_version, "
        "narrative, claim_ratio, grounded, ungrounded, flagged, audited_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            source_id,
            target_id,
            month,
            dj_id,
            edge_type,
            prompt_version,
            narrative,
            float(claim_ratio),
            int(grounded),
            int(ungrounded),
            int(flagged),
            datetime.now(UTC).isoformat(),
        ),
    )
    conn.commit()


_CLAIM_DECOMPOSE_PROMPT = (
    "You are a fact-checking assistant. Decompose the following narrative into individual factual "
    "claims (one per line). For each claim, check whether it is grounded in the provided data.\n\n"
    "Output format — one claim per line:\n"
    "  G: <claim>\n"
    "  U: <claim>\n\n"
    "G = grounded (the claim is stated or directly implied by a data field).\n"
    "U = ungrounded (the claim is not in the provided data).\n\n"
    "Be strict. Describing a neighbor with any adjective is U. Inferring DJ intent is U. "
    "Stating an artist quality not in the styles/audio/genre fields is U.\n\n"
    "End with a count line: COUNTS: Xg Yu"
)


def parse_claim_counts(text: str) -> tuple[int, int]:
    """Extract ``(grounded, ungrounded)`` counts from a verifier response.

    Looks for a ``COUNTS: Xg Yu`` summary line first; falls back to counting
    ``G:`` / ``U:`` line prefixes for resilience against models that drop the
    summary.
    """
    for raw_line in text.strip().split("\n"):
        line = raw_line.strip().upper()
        if line.startswith("COUNTS:"):
            grounded = ungrounded = 0
            g_match = re.findall(r"(\d+)\s*G", line)
            u_match = re.findall(r"(\d+)\s*U", line)
            if g_match:
                grounded = int(g_match[0])
            if u_match:
                ungrounded = int(u_match[0])
            return grounded, ungrounded
    grounded = len(re.findall(r"^\s*G[:|]", text, re.MULTILINE))
    ungrounded = len(re.findall(r"^\s*U[:|]", text, re.MULTILINE))
    return grounded, ungrounded
