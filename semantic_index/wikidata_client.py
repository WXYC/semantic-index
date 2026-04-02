"""Wikidata SPARQL client for entity lookups and knowledge graph queries.

Batched SPARQL queries for property lookups (Discogs artist ID via P1953,
influences via P737, label hierarchy via P749/P355) and entity name search
via the MediaWiki wbsearchentities API.

The client respects Wikidata's rate limit (~1 request/second) and splits
large lookups into batches to avoid SPARQL query timeouts.
"""

import logging
import re
import time

import httpx

from semantic_index.models import (
    WikidataEntity,
    WikidataInfluence,
    WikidataLabelHierarchy,
)

logger = logging.getLogger(__name__)

_QID_PATTERN = re.compile(r"^Q\d+$")
_RATE_INTERVAL = 1.0  # seconds between requests

# Wikidata QIDs for musician-related occupations (P106 values).
MUSICIAN_OCCUPATIONS: frozenset[str] = frozenset(
    {
        "Q639669",  # musician
        "Q177220",  # singer
        "Q753110",  # songwriter
        "Q488205",  # singer-songwriter
        "Q36834",  # composer
        "Q855091",  # guitarist
        "Q386854",  # rapper
        "Q183945",  # record producer
        "Q130857",  # disc jockey
        "Q806349",  # bandleader
    }
)

# Wikidata QIDs for musical group types (P31 values).
MUSICAL_GROUP_TYPES: frozenset[str] = frozenset(
    {
        "Q215380",  # musical group/band
        "Q5741069",  # musical duo
        "Q56816954",  # music project
    }
)


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
    """Client for Wikidata SPARQL queries and entity search.

    Uses the Wikidata Query Service SPARQL endpoint for property-based
    batch lookups (Discogs ID, influences, label hierarchy) and the
    MediaWiki wbsearchentities API for name search.

    Args:
        sparql_endpoint: SPARQL endpoint URL. Defaults to Wikidata Query Service.
        api_endpoint: MediaWiki API URL. Defaults to Wikidata API.
        user_agent: User-Agent header (required by Wikidata).
        batch_size: Max entities per SPARQL VALUES clause.
    """

    _DEFAULT_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
    _DEFAULT_API_ENDPOINT = "https://www.wikidata.org/w/api.php"
    _DEFAULT_USER_AGENT = "WXYCSemanticIndex/0.1 (https://wxyc.org; engineering@wxyc.org)"

    def __init__(
        self,
        sparql_endpoint: str | None = None,
        api_endpoint: str | None = None,
        user_agent: str | None = None,
        batch_size: int = 50,
    ) -> None:
        self._sparql_endpoint = sparql_endpoint or self._DEFAULT_SPARQL_ENDPOINT
        self._api_endpoint = api_endpoint or self._DEFAULT_API_ENDPOINT
        self._user_agent = user_agent or self._DEFAULT_USER_AGENT
        self._batch_size = batch_size
        self._last_request: float = 0

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
            resp.raise_for_status()
            bindings: list[dict] = resp.json()["results"]["bindings"]
            return bindings
        finally:
            client.close()

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

    def lookup_by_discogs_ids(self, discogs_ids: list[int]) -> dict[int, WikidataEntity]:
        """Look up Wikidata entities by Discogs artist ID (P1953).

        Batches the lookup into chunks of ``batch_size`` to avoid SPARQL
        query timeouts on large input sets.

        Args:
            discogs_ids: Discogs artist IDs to look up.

        Returns:
            Dict mapping discogs_id -> WikidataEntity for each found match.
        """
        if not discogs_ids:
            return {}

        result: dict[int, WikidataEntity] = {}
        for batch_start in range(0, len(discogs_ids), self._batch_size):
            batch = discogs_ids[batch_start : batch_start + self._batch_size]
            values = " ".join(f'"{did}"' for did in batch)
            query = (
                "SELECT ?item ?itemLabel ?discogsId WHERE {\n"
                f"  VALUES ?discogsId {{ {values} }}\n"
                "  ?item wdt:P1953 ?discogsId .\n"
                '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }\n'
                "}"
            )
            try:
                bindings = self._sparql_query(query)
                for b in bindings:
                    qid = _extract_qid(_binding_value(b, "item") or "")
                    name = _binding_value(b, "itemLabel") or qid
                    discogs_id_str = _binding_value(b, "discogsId")
                    if discogs_id_str is not None:
                        try:
                            did = int(discogs_id_str)
                        except (ValueError, TypeError):
                            continue
                        result[did] = WikidataEntity(
                            qid=qid,
                            name=name,
                            discogs_artist_id=did,
                        )
            except Exception:
                logger.warning(
                    "SPARQL lookup_by_discogs_ids failed for batch starting at %d",
                    batch_start,
                    exc_info=True,
                )

        return result

    def get_influences(self, qids: list[str]) -> list[WikidataInfluence]:
        """Get influence relationships (P737) for given Wikidata entities.

        Returns edges where "source is influenced by target".

        Args:
            qids: Wikidata QIDs to query for influences (e.g. ``["Q2774"]``).

        Returns:
            List of WikidataInfluence relationships.
        """
        valid_qids = self._validate_qids(qids)
        if not valid_qids:
            return []

        result: list[WikidataInfluence] = []
        for batch_start in range(0, len(valid_qids), self._batch_size):
            batch = valid_qids[batch_start : batch_start + self._batch_size]
            values = " ".join(f"wd:{qid}" for qid in batch)
            query = (
                "SELECT ?source ?target ?targetLabel WHERE {\n"
                f"  VALUES ?source {{ {values} }}\n"
                "  ?source wdt:P737 ?target .\n"
                '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }\n'
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
                '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }\n'
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

    def search_by_name(self, name: str, limit: int = 10) -> list[WikidataEntity]:
        """Search for Wikidata entities by name.

        Uses the MediaWiki wbsearchentities API for efficient text search.

        Args:
            name: Search string.
            limit: Maximum results (capped at 50 by the API).

        Returns:
            List of matching WikidataEntity instances.
        """
        if not name.strip():
            return []

        self._rate_limit()
        client = httpx.Client(
            timeout=30,
            headers={"User-Agent": self._user_agent},
        )
        try:
            resp = client.get(
                self._api_endpoint,
                params={
                    "action": "wbsearchentities",
                    "search": name,
                    "language": "en",
                    "type": "item",
                    "limit": min(limit, 50),
                    "format": "json",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return [
                WikidataEntity(
                    qid=item["id"],
                    name=item.get("label", item["id"]),
                    description=item.get("description"),
                )
                for item in data.get("search", [])
            ]
        except Exception:
            logger.warning("Wikidata name search failed for %r", name, exc_info=True)
            return []
        finally:
            client.close()

    def search_musician_by_name(self, name: str, limit: int = 10) -> list[WikidataEntity]:
        """Search for Wikidata entities by name, filtered to musicians.

        Two-step process: first finds candidates via ``wbsearchentities``,
        then validates them with a SPARQL query checking P31 (instance of)
        and P106 (occupation) to keep only humans with musician occupations
        or musical groups/duos.

        Args:
            name: Search string (artist name).
            limit: Maximum candidates to retrieve from search (capped at 50).

        Returns:
            List of WikidataEntity instances that are musicians, in search
            relevance order.
        """
        candidates = self.search_by_name(name, limit=limit)
        if not candidates:
            return []

        candidate_qids = [c.qid for c in candidates]
        musician_qids = self._filter_musicians(candidate_qids)
        if not musician_qids:
            return []

        return [c for c in candidates if c.qid in musician_qids]

    def _filter_musicians(self, qids: list[str]) -> set[str]:
        """Filter QIDs to those that are musicians or musical groups.

        Uses a SPARQL query to check:
        - Human (P31=Q5) with a musician-related occupation (P106), OR
        - Musical group/duo/project (P31 in MUSICAL_GROUP_TYPES).

        Args:
            qids: Candidate Wikidata QIDs to filter.

        Returns:
            Set of QIDs that pass the musician filter.
        """
        valid_qids = self._validate_qids(qids)
        if not valid_qids:
            return set()

        values = " ".join(f"wd:{qid}" for qid in valid_qids)
        occupations = " ".join(f"wd:{qid}" for qid in MUSICIAN_OCCUPATIONS)
        group_types = " ".join(f"wd:{qid}" for qid in MUSICAL_GROUP_TYPES)

        query = (
            "SELECT DISTINCT ?item WHERE {\n"
            f"  VALUES ?item {{ {values} }}\n"
            "  {\n"
            "    ?item wdt:P31 wd:Q5 .\n"
            f"    VALUES ?occupation {{ {occupations} }}\n"
            "    ?item wdt:P106 ?occupation .\n"
            "  }\n"
            "  UNION\n"
            "  {\n"
            f"    VALUES ?groupType {{ {group_types} }}\n"
            "    ?item wdt:P31 ?groupType .\n"
            "  }\n"
            "}"
        )
        try:
            bindings = self._sparql_query(query)
            return {_extract_qid(_binding_value(b, "item") or "") for b in bindings}
        except Exception:
            logger.warning("SPARQL musician filter failed for %s", qids, exc_info=True)
            return set()
