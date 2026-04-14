"""Wikidata SPARQL client for knowledge graph queries.

Batched SPARQL queries for graph-specific property lookups: influences
via P737, label hierarchy via P749/P355, and label-to-QID bridging
via P1902. Identity resolution methods (Discogs artist ID lookup,
name search, streaming IDs) have been moved to LML.

The client respects Wikidata's rate limit (~1 request/second) and splits
large lookups into batches to avoid SPARQL query timeouts.
"""

import logging
import re
import time

import httpx
import psycopg

from semantic_index.models import (
    WikidataEntity,
    WikidataInfluence,
    WikidataLabelHierarchy,
)

logger = logging.getLogger(__name__)

_QID_PATTERN = re.compile(r"^Q\d+$")
_RATE_INTERVAL = 1.0  # seconds between requests


def _extract_qid(uri: str) -> str:
    """Extract QID from a Wikidata entity URI.

    Args:
        uri: Full entity URI, e.g. ``http://www.wikidata.org/entity/Q2774``.

    Returns:
        The QID portion, e.g. ``Q2774``.
    """
    return uri.rsplit("/", 1)[-1]


def _binding_value(binding: dict, key: str) -> str | None:
    """Extract a string value from a SPARQL result binding.

    Args:
        binding: A single result binding dict from a SPARQL JSON response.
        key: The variable name to extract.

    Returns:
        The value string, or None if the key is absent.
    """
    if key in binding:
        result: str = binding[key]["value"]
        return result
    return None


class WikidataClient:
    """Client for Wikidata SPARQL queries (graph-specific).

    Uses the Wikidata Query Service SPARQL endpoint for influence
    relationships (P737), label hierarchy (P749/P355), and label-to-QID
    bridging (P1902).

    Args:
        sparql_endpoint: SPARQL endpoint URL. Defaults to Wikidata Query Service.
        user_agent: User-Agent header (required by Wikidata).
        batch_size: Max entities per SPARQL VALUES clause.
        cache_dsn: Optional PostgreSQL DSN for wikidata-cache (used by get_influences).
    """

    _DEFAULT_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
    _DEFAULT_USER_AGENT = "WXYCSemanticIndex/0.1 (https://wxyc.org; engineering@wxyc.org)"

    def __init__(
        self,
        sparql_endpoint: str | None = None,
        user_agent: str | None = None,
        batch_size: int = 50,
        cache_dsn: str | None = None,
    ) -> None:
        self._sparql_endpoint = sparql_endpoint or self._DEFAULT_SPARQL_ENDPOINT
        self._user_agent = user_agent or self._DEFAULT_USER_AGENT
        self._batch_size = batch_size
        self._last_request: float = 0
        self._cache_dsn = cache_dsn
        self._cache_conn: psycopg.Connection | None = None

    def _get_cache_conn(self) -> psycopg.Connection | None:
        """Get or create the wikidata-cache PostgreSQL connection."""
        if self._cache_dsn is None:
            return None
        if self._cache_conn is None or self._cache_conn.closed:
            try:
                self._cache_conn = psycopg.connect(self._cache_dsn, autocommit=True)
            except Exception:
                logger.warning("Failed to connect to wikidata-cache", exc_info=True)
                return None
        return self._cache_conn

    def _rate_limit(self) -> None:
        """Sleep to respect Wikidata's rate limit (~1 req/s)."""
        elapsed = time.time() - self._last_request
        if elapsed < _RATE_INTERVAL:
            time.sleep(_RATE_INTERVAL - elapsed)
        self._last_request = time.time()

    def _sparql_query(self, query: str) -> list[dict]:
        """Execute a SPARQL query and return result bindings.

        Args:
            query: SPARQL query string.

        Returns:
            List of binding dicts from the SPARQL JSON response.

        Raises:
            httpx.HTTPStatusError: On non-2xx response.
            Exception: On network or parse errors.
        """
        max_retries = 3
        for attempt in range(max_retries):
            self._rate_limit()
            client = httpx.Client(
                timeout=60,
                headers={
                    "User-Agent": self._user_agent,
                    "Accept": "application/sparql-results+json",
                },
            )
            try:
                resp = client.get(self._sparql_endpoint, params={"query": query})
                if resp.status_code == 403 or resp.status_code == 429:
                    backoff = 2 ** (attempt + 1)
                    logger.warning(
                        "Wikidata rate limited (HTTP %d), backing off %ds (attempt %d/%d)",
                        resp.status_code,
                        backoff,
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(backoff)
                    continue
                resp.raise_for_status()
                bindings: list[dict] = resp.json()["results"]["bindings"]
                return bindings
            finally:
                client.close()
        logger.warning("Wikidata SPARQL query failed after %d retries", max_retries)
        return []

    def _validate_qids(self, qids: list[str]) -> list[str]:
        """Validate and filter QIDs, logging warnings for invalid ones.

        Args:
            qids: List of candidate QID strings.

        Returns:
            List of valid QIDs matching the ``Q\\d+`` pattern.
        """
        valid = []
        for qid in qids:
            if _QID_PATTERN.match(qid):
                valid.append(qid)
            else:
                logger.warning("Invalid QID skipped: %r", qid)
        return valid

    def lookup_labels_by_discogs_ids(
        self, discogs_label_ids: list[int]
    ) -> dict[int, WikidataEntity]:
        """Look up Wikidata entities by Discogs label ID (P1902).

        Batches the lookup into chunks of ``batch_size`` to avoid SPARQL
        query timeouts on large input sets.

        Args:
            discogs_label_ids: Discogs label IDs to look up.

        Returns:
            Dict mapping discogs_label_id -> WikidataEntity for each found match.
        """
        if not discogs_label_ids:
            return {}

        result: dict[int, WikidataEntity] = {}
        for batch_start in range(0, len(discogs_label_ids), self._batch_size):
            batch = discogs_label_ids[batch_start : batch_start + self._batch_size]
            values = " ".join(f'"{lid}"' for lid in batch)
            query = (
                "SELECT ?item ?itemLabel ?discogsLabelId WHERE {\n"
                f"  VALUES ?discogsLabelId {{ {values} }}\n"
                "  ?item wdt:P1902 ?discogsLabelId .\n"
                '  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}\n'
                "}"
            )
            try:
                bindings = self._sparql_query(query)
                for b in bindings:
                    qid = _extract_qid(_binding_value(b, "item") or "")
                    name = _binding_value(b, "itemLabel") or qid
                    label_id_str = _binding_value(b, "discogsLabelId")
                    if label_id_str is not None:
                        try:
                            lid = int(label_id_str)
                        except (ValueError, TypeError):
                            continue
                        result[lid] = WikidataEntity(
                            qid=qid,
                            name=name,
                        )
            except Exception:
                logger.warning(
                    "SPARQL lookup_labels_by_discogs_ids failed for batch starting at %d",
                    batch_start,
                    exc_info=True,
                )

        return result

    def get_influences(self, qids: list[str]) -> list[WikidataInfluence]:
        """Get influence relationships (P737) for given Wikidata entities.

        Cache-first: queries the wikidata-cache PostgreSQL if available,
        then falls back to SPARQL for any QIDs not found in the cache.

        Args:
            qids: Wikidata QIDs to query for influences (e.g. ``["Q2774"]``).

        Returns:
            List of WikidataInfluence relationships.
        """
        valid_qids = self._validate_qids(qids)
        if not valid_qids:
            return []

        result: list[WikidataInfluence] = []
        remaining_qids = list(valid_qids)

        # Try cache -- when available, treat as authoritative (no SPARQL fallback)
        conn = self._get_cache_conn()
        if conn is not None:
            try:
                rows = conn.execute(
                    "SELECT i.source_qid, i.target_qid, COALESCE(e.label, i.target_qid) "
                    "FROM influence i "
                    "LEFT JOIN entity e ON i.target_qid = e.qid "
                    "WHERE i.source_qid = ANY(%s)",
                    (valid_qids,),
                ).fetchall()
                for source_qid, target_qid, target_name in rows:
                    result.append(
                        WikidataInfluence(
                            source_qid=source_qid,
                            target_qid=target_qid,
                            target_name=target_name,
                        )
                    )
                return result
            except Exception:
                logger.warning("Wikidata cache get_influences failed", exc_info=True)

        # SPARQL fallback (only when no cache is available)
        for batch_start in range(0, len(remaining_qids), self._batch_size):
            batch = remaining_qids[batch_start : batch_start + self._batch_size]
            values = " ".join(f"wd:{qid}" for qid in batch)
            query = (
                "SELECT ?source ?target ?targetLabel WHERE {\n"
                f"  VALUES ?source {{ {values} }}\n"
                "  ?source wdt:P737 ?target .\n"
                '  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}\n'
                "}"
            )
            try:
                bindings = self._sparql_query(query)
                for b in bindings:
                    source_qid = _extract_qid(_binding_value(b, "source") or "")
                    target_qid = _extract_qid(_binding_value(b, "target") or "")
                    target_name = _binding_value(b, "targetLabel") or target_qid
                    result.append(
                        WikidataInfluence(
                            source_qid=source_qid,
                            target_qid=target_qid,
                            target_name=target_name,
                        )
                    )
            except Exception:
                logger.warning(
                    "SPARQL get_influences failed for batch starting at %d",
                    batch_start,
                    exc_info=True,
                )

        return result

    def get_label_hierarchy(self, qids: list[str]) -> list[WikidataLabelHierarchy]:
        """Get label parent-child relationships (P749 parent org, P355 subsidiary).

        Queries both directions: finds parents of the given QIDs (via P749)
        and children of the given QIDs (via P355).

        Args:
            qids: Wikidata QIDs for labels (e.g. ``["Q1312934"]`` for Warp Records).

        Returns:
            List of WikidataLabelHierarchy relationships.
        """
        valid_qids = self._validate_qids(qids)
        if not valid_qids:
            return []

        result: list[WikidataLabelHierarchy] = []
        for batch_start in range(0, len(valid_qids), self._batch_size):
            batch = valid_qids[batch_start : batch_start + self._batch_size]
            values = " ".join(f"wd:{qid}" for qid in batch)
            query = (
                "SELECT ?child ?childLabel ?parent ?parentLabel WHERE {\n"
                f"  VALUES ?entity {{ {values} }}\n"
                "  {\n"
                "    BIND(?entity AS ?child)\n"
                "    ?entity wdt:P749 ?parent .\n"
                "  }\n"
                "  UNION\n"
                "  {\n"
                "    BIND(?entity AS ?parent)\n"
                "    ?entity wdt:P355 ?child .\n"
                "  }\n"
                '  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}\n'
                "}"
            )
            try:
                bindings = self._sparql_query(query)
                for b in bindings:
                    child_qid = _extract_qid(_binding_value(b, "child") or "")
                    child_name = _binding_value(b, "childLabel") or child_qid
                    parent_qid = _extract_qid(_binding_value(b, "parent") or "")
                    parent_name = _binding_value(b, "parentLabel") or parent_qid
                    result.append(
                        WikidataLabelHierarchy(
                            parent_qid=parent_qid,
                            parent_name=parent_name,
                            child_qid=child_qid,
                            child_name=child_name,
                        )
                    )
            except Exception:
                logger.warning(
                    "SPARQL get_label_hierarchy failed for batch starting at %d",
                    batch_start,
                    exc_info=True,
                )

        return result
