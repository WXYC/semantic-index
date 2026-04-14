"""Label CRUD operations for the SQLite graph database.

Extracted from entity_store.py as part of ETL pipeline unification.
Used by label_hierarchy.py to manage label and label_hierarchy tables.
"""

from __future__ import annotations

import logging
import sqlite3

from semantic_index.models import Entity

logger = logging.getLogger(__name__)


class LabelStore:
    """Manages label and label_hierarchy tables within a SQLite database.

    Wraps a SQLite connection and provides CRUD for labels, entity
    creation for label entities, and label hierarchy edges.

    Args:
        conn: An open SQLite connection to the graph database.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def get_or_create_entity(
        self,
        name: str,
        entity_type: str,
        wikidata_qid: str | None = None,
    ) -> Entity:
        """Return an existing entity by name+type, or create a new one.

        If the entity already exists, its wikidata_qid is not overwritten.

        Args:
            name: Display name of the entity.
            entity_type: One of 'artist', 'label', etc.
            wikidata_qid: Optional Wikidata QID to set on creation.

        Returns:
            The existing or newly created Entity.
        """
        row = self._conn.execute(
            "SELECT id, wikidata_qid, name, entity_type FROM entity WHERE name = ? AND entity_type = ?",
            (name, entity_type),
        ).fetchone()
        if row is not None:
            return Entity(id=row[0], wikidata_qid=row[1], name=row[2], entity_type=row[3])

        cur = self._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            (name, entity_type, wikidata_qid),
        )
        self._conn.commit()
        return Entity(
            id=cur.lastrowid,  # type: ignore[arg-type]
            name=name,
            entity_type=entity_type,
            wikidata_qid=wikidata_qid,
        )

    def get_or_create_label(
        self,
        name: str,
        *,
        discogs_label_id: int | None = None,
        wikidata_qid: str | None = None,
    ) -> int:
        """Return an existing label by name, or create a new one.

        On conflict (name), existing discogs_label_id is not overwritten.
        If a wikidata_qid is provided, an entity row is also created/linked.

        Args:
            name: Label name (unique key).
            discogs_label_id: Optional Discogs label ID.
            wikidata_qid: Optional Wikidata QID. Creates an entity if provided.

        Returns:
            The label row's integer primary key.
        """
        row = self._conn.execute("SELECT id FROM label WHERE name = ?", (name,)).fetchone()
        if row is not None:
            return int(row[0])

        entity_id = None
        if wikidata_qid:
            entity = self.get_or_create_entity(name, "label", wikidata_qid=wikidata_qid)
            entity_id = entity.id

        cur = self._conn.execute(
            "INSERT INTO label (name, discogs_label_id, entity_id) VALUES (?, ?, ?)",
            (name, discogs_label_id, entity_id),
        )
        self._conn.commit()
        return int(cur.lastrowid)  # type: ignore[arg-type]

    def update_label_qid(self, label_id: int, wikidata_qid: str) -> None:
        """Set the Wikidata QID for a label by creating/linking an entity.

        Args:
            label_id: The label's primary key.
            wikidata_qid: The Wikidata QID to assign.

        Raises:
            ValueError: If no label with the given id exists.
        """
        row = self._conn.execute(
            "SELECT id, name, entity_id FROM label WHERE id = ?", (label_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"No label with id {label_id}")

        label_name = row[1]
        existing_entity_id = row[2]

        if existing_entity_id is not None:
            self._conn.execute(
                "UPDATE entity SET wikidata_qid = COALESCE(wikidata_qid, ?), "
                "updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?",
                (wikidata_qid, existing_entity_id),
            )
        else:
            entity = self.get_or_create_entity(label_name, "label", wikidata_qid=wikidata_qid)
            self._conn.execute(
                "UPDATE label SET entity_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?",
                (entity.id, label_id),
            )
        self._conn.commit()

    def insert_label_hierarchy(
        self, parent_label_id: int, child_label_id: int, source: str = "wikidata"
    ) -> None:
        """Insert a parent-child label relationship (idempotent).

        Args:
            parent_label_id: The parent label's primary key.
            child_label_id: The child label's primary key.
            source: Source of the relationship (default: 'wikidata').
        """
        self._conn.execute(
            "INSERT OR IGNORE INTO label_hierarchy (parent_label_id, child_label_id, source) VALUES (?, ?, ?)",
            (parent_label_id, child_label_id, source),
        )
        self._conn.commit()
