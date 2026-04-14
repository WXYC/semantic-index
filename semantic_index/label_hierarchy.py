"""Populate label and label_hierarchy tables from Wikidata P749/P355.

Collects unique labels from Discogs enrichment data, resolves their Wikidata
QIDs via Discogs label ID (P1902) lookups, queries Wikidata for parent
organization (P749) and subsidiary (P355) relationships, and persists the
results into the entity store's ``label`` and ``label_hierarchy`` tables.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from semantic_index.models import LabelHierarchyReport

if TYPE_CHECKING:
    from semantic_index.label_store import LabelStore
    from semantic_index.models import ArtistEnrichment
    from semantic_index.wikidata_client import WikidataClient

logger = logging.getLogger(__name__)


def populate_label_hierarchy(
    label_store: LabelStore,
    enrichments: dict[str, ArtistEnrichment],
    wikidata_client: WikidataClient,
) -> LabelHierarchyReport:
    """Populate label and label_hierarchy tables from enrichment data and Wikidata.

    Steps:
        1. Collect unique labels (name + Discogs label ID) from enrichments.
        2. Insert them into the entity store's ``label`` table.
        3. Look up Wikidata QIDs for labels with Discogs IDs (via P1902).
        4. Link matched QIDs to label rows.
        5. Query Wikidata for P749/P355 hierarchy relationships.
        6. Insert hierarchy edges (creating parent/child labels as needed).

    Args:
        label_store: The LabelStore managing the SQLite database.
        enrichments: Discogs enrichment data keyed by canonical artist name.
        wikidata_client: WikidataClient for SPARQL lookups.

    Returns:
        A LabelHierarchyReport summarizing what was created.
    """
    if not enrichments:
        return LabelHierarchyReport(labels_created=0, labels_matched=0, hierarchy_edges=0)

    # Step 1: Collect unique labels from enrichments
    # Map label_name -> discogs_label_id (first non-None wins)
    unique_labels: dict[str, int | None] = {}
    for enrich in enrichments.values():
        for label in enrich.labels:
            if label.name not in unique_labels:
                unique_labels[label.name] = label.label_id
            elif unique_labels[label.name] is None and label.label_id is not None:
                unique_labels[label.name] = label.label_id

    logger.info("Collected %d unique labels from enrichments", len(unique_labels))

    # Step 2: Insert labels into entity store
    label_name_to_id: dict[str, int] = {}
    for name, discogs_id in unique_labels.items():
        label_name_to_id[name] = label_store.get_or_create_label(name, discogs_label_id=discogs_id)

    labels_created = len(label_name_to_id)

    # Step 3: Look up Wikidata QIDs for labels with Discogs IDs
    discogs_ids = [did for did in unique_labels.values() if did is not None]
    qid_map = wikidata_client.lookup_labels_by_discogs_ids(discogs_ids)
    logger.info("Wikidata matched %d / %d labels with Discogs IDs", len(qid_map), len(discogs_ids))

    # Step 4: Link QIDs to label rows
    # Build discogs_id -> label_name reverse map
    discogs_to_name: dict[int, str] = {}
    for name, did in unique_labels.items():
        if did is not None:
            discogs_to_name[did] = name

    qid_to_label_name: dict[str, str] = {}
    for discogs_id, wd_entity in qid_map.items():
        label_name = discogs_to_name.get(discogs_id)
        if label_name is None:
            continue
        label_id = label_name_to_id[label_name]
        label_store.update_label_qid(label_id, wd_entity.qid)
        qid_to_label_name[wd_entity.qid] = label_name

    labels_matched = len(qid_map)

    # Step 5: Query Wikidata for hierarchy relationships
    matched_qids = list(qid_to_label_name.keys())
    if not matched_qids:
        logger.info("No labels matched to Wikidata; skipping hierarchy query")
        return LabelHierarchyReport(
            labels_created=labels_created,
            labels_matched=labels_matched,
            hierarchy_edges=0,
        )

    hierarchy = wikidata_client.get_label_hierarchy(matched_qids)
    logger.info("Wikidata returned %d hierarchy relationships", len(hierarchy))

    # Step 6: Insert hierarchy edges
    hierarchy_edges = 0
    for rel in hierarchy:
        # Ensure parent label exists
        parent_name = rel.parent_name
        if parent_name not in label_name_to_id:
            label_name_to_id[parent_name] = label_store.get_or_create_label(
                parent_name, wikidata_qid=rel.parent_qid
            )
        else:
            label_store.update_label_qid(label_name_to_id[parent_name], rel.parent_qid)

        # Ensure child label exists
        child_name = rel.child_name
        if child_name not in label_name_to_id:
            label_name_to_id[child_name] = label_store.get_or_create_label(
                child_name, wikidata_qid=rel.child_qid
            )
        else:
            label_store.update_label_qid(label_name_to_id[child_name], rel.child_qid)

        label_store.insert_label_hierarchy(
            label_name_to_id[parent_name],
            label_name_to_id[child_name],
        )
        hierarchy_edges += 1

    logger.info(
        "Label hierarchy: %d labels, %d matched, %d edges",
        labels_created,
        labels_matched,
        hierarchy_edges,
    )

    return LabelHierarchyReport(
        labels_created=labels_created,
        labels_matched=labels_matched,
        hierarchy_edges=hierarchy_edges,
    )
