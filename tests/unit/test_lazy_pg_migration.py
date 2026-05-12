"""Regression: pipeline modules consume the shared LazyPgConnection from wxyc-fastapi.

semantic-index used to ship its own ``LazyPgConnection`` in ``utils.py``. Phase E
of the wxyc-fastapi project (issue #309) lifted that class verbatim into
``wxyc_fastapi.db.lazy_pg`` and made semantic-index a consumer. These tests pin
the post-migration contract:

* The four PG-cache pipeline clients hold a ``wxyc_fastapi.db.LazyPgConnection``
  instance (not a local copy that happens to share the name).
* ``semantic_index.utils`` no longer defines the class itself.

If any client regresses to a local class, the isinstance check fails. If a future
change re-introduces a local ``LazyPgConnection`` in ``utils.py``, the absence
check fails.
"""

from __future__ import annotations

from wxyc_fastapi.db import LazyPgConnection as SharedLazyPgConnection

from semantic_index.acousticbrainz_client import AcousticBrainzClient
from semantic_index.discogs_client import DiscogsClient
from semantic_index.musicbrainz_client import MusicBrainzClient
from semantic_index.wikidata_client import WikidataClient


def test_discogs_client_uses_shared_lazy_pg() -> None:
    client = DiscogsClient(cache_dsn=None, api_base_url=None)
    assert isinstance(client._pg, SharedLazyPgConnection)


def test_musicbrainz_client_uses_shared_lazy_pg() -> None:
    # cache_dsn is annotated `str` but LazyPgConnection accepts None at runtime
    # (graceful-degradation contract); pass None to avoid touching real PG.
    client = MusicBrainzClient(cache_dsn=None)  # type: ignore[arg-type]
    assert isinstance(client._pg, SharedLazyPgConnection)


def test_wikidata_client_uses_shared_lazy_pg() -> None:
    client = WikidataClient(cache_dsn=None)
    assert isinstance(client._pg, SharedLazyPgConnection)


def test_acousticbrainz_client_uses_shared_lazy_pg() -> None:
    client = AcousticBrainzClient(cache_dsn=None)  # type: ignore[arg-type]
    assert isinstance(client._pg, SharedLazyPgConnection)


def test_utils_no_longer_defines_lazy_pg_connection() -> None:
    """utils.py must not re-export a local LazyPgConnection after Phase E."""
    import semantic_index.utils as utils_mod

    assert not hasattr(utils_mod, "LazyPgConnection"), (
        "semantic_index.utils should not define LazyPgConnection after the "
        "Phase E migration to wxyc_fastapi.db.lazy_pg (issue #309)."
    )
