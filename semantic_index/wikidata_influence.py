"""Extract Wikidata influence edges (P737) between reconciled artists.

Resolves Wikidata QIDs to canonical artist names via the entity store,
producing directed WikidataInfluenceEdge instances for artist pairs where
both source and target exist in the graph.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from semantic_index.models import WikidataInfluence, WikidataInfluenceEdge

if TYPE_CHECKING:
    from semantic_index.entity_store import EntityStore

logger = logging.getLogger(__name__)


def extract_wikidata_influences(
    entity_store: EntityStore,
    influences: list[WikidataInfluence],
) -> list[WikidataInfluenceEdge]:
    """Build directed influence edges from Wikidata P737 relationships.

    For each influence relationship, resolves both source and target QIDs
    to canonical artist names via the entity store. Only produces edges
    where both artists exist in the graph.

    Args:
        entity_store: Entity store with reconciled artist/entity data.
        influences: Raw Wikidata influence relationships from ``WikidataClient.get_influences()``.

    Returns:
        Sorted, deduplicated list of WikidataInfluenceEdge instances.
    """
    if not influences:
        return []

    # Build QID → canonical_name mapping from entity store
    qid_to_name = _build_qid_to_name_mapping(entity_store)

    seen: set[tuple[str, str]] = set()
    edges: list[WikidataInfluenceEdge] = []

    for inf in influences:
        source_name = qid_to_name.get(inf.source_qid)
        target_name = qid_to_name.get(inf.target_qid)

        if source_name is None or target_name is None:
            continue

        # Skip self-influences
        if source_name == target_name:
            continue

        key = (source_name, target_name)
        if key in seen:
            continue
        seen.add(key)

        edges.append(
            WikidataInfluenceEdge(
                source_artist=source_name,
                target_artist=target_name,
                source_qid=inf.source_qid,
                target_qid=inf.target_qid,
            )
        )

    edges.sort(key=lambda e: (e.source_artist, e.target_artist))
    logger.info("Extracted %d Wikidata influence edges", len(edges))
    return edges


def _build_qid_to_name_mapping(entity_store: EntityStore) -> dict[str, str]:
    """Build a mapping from Wikidata QID to canonical artist name.

    Joins artist rows (via entity_id) to entity rows (with wikidata_qid)
    to resolve QIDs to the canonical names used in the graph.

    Args:
        entity_store: Entity store with reconciled artist/entity data.

    Returns:
        Dict mapping wikidata_qid -> canonical_name.
    """
    rows = entity_store._conn.execute(
        "SELECT e.wikidata_qid, a.canonical_name "
        "FROM artist a "
        "JOIN entity e ON a.entity_id = e.id "
        "WHERE e.wikidata_qid IS NOT NULL"
    ).fetchall()
    return {row[0]: row[1] for row in rows}
