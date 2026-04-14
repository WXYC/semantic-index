# Semantic Index

Builds a semantic artist graph from WXYC DJ transition data. DJs curate transitions between artists during their shows — these adjacency relationships encode latent genre/mood/style similarity that PMI (Pointwise Mutual Information) can surface.

## Architecture

A batch pipeline that parses a tubafrenzy MySQL dump, resolves artist names via catalog and Discogs, extracts adjacency pairs and cross-reference edges, computes PMI, enriches artists with Discogs metadata, computes Discogs-derived edges, and exports a GEXF graph and SQLite database.

```
SQL dump → sql_parser → artist_resolver → adjacency → pmi ──────────────→ graph_export → GEXF
                       → cross_reference ────────────────────────────────→ sqlite_export → SQLite
                       → node_attributes ────────────────────────────────→
         → discogs_client → discogs_enrichment → discogs_edges ─────────→
         → wikidata_client → wikidata_influence ────────────────────────→
         → musicbrainz_client → acousticbrainz (feature loader) ───────→ audio_profile + acoustic_similarity

SQLite ──→ api (FastAPI + aiosqlite) ──→ JSON responses
```

### Modules

| Module | Responsibility |
|--------|---------------|
| `semantic_index/sql_parser.py` | Parse MySQL INSERT statements from SQL dump files. Uses `wxyc_etl.parser` Rust extension for ~1000x faster parsing, with `sql_parser_rs` and pure-Python fallbacks. Set `WXYC_ETL_NO_RUST=1` to force pure-Python. |
| `semantic_index/models.py` | Pydantic data models for all pipeline entities. |
| `semantic_index/artist_resolver.py` | Multi-tier artist name resolution: FK chain, name match, normalized (via `wxyc_etl.text.normalize_artist_name` + local bracket/the/& transforms), fuzzy (Jaro-Winkler), Discogs, raw fallback. Uses `wxyc_etl.text.split_artist_name` for alias splitting. |
| `semantic_index/adjacency.py` | Extract consecutive artist pairs within radio shows. |
| `semantic_index/pmi.py` | Compute Pointwise Mutual Information for artist co-occurrences. |
| `semantic_index/node_attributes.py` | Extract and compute per-artist temporal, DJ, and request statistics. |
| `semantic_index/cross_reference.py` | Extract cross-reference edges from catalog cross-reference tables. |
| `semantic_index/discogs_client.py` | Two-tier Discogs client: discogs-cache PostgreSQL with library-metadata-lookup API fallback. Uses `wxyc_etl.schema` constants for all discogs-cache table names. |
| `semantic_index/wikidata_client.py` | Wikidata SPARQL client: batched lookups by Discogs ID (P1953), influence relationships (P737), label hierarchy (P749/P355), streaming service IDs (P1902 Spotify, P2850 Apple Music, P3283 Bandcamp), and name search via wbsearchentities API. |
| `semantic_index/entity_store.py` | Persistent entity store for reconciled artist identities: schema creation/migration, CRUD, artist upsert, reconciliation log, artist styles, entity deduplication by shared Wikidata QID. Creates the artist table from scratch on a fresh database or migrates an existing one. |
| `semantic_index/lml_identity.py` | Import pre-resolved identities from LML's `entity.identity` PG table into the local SQLite entity store. Used by `--entity-source=lml`. Bridge module for ETL pipeline unification. |
| `semantic_index/reconciliation.py` | Bulk Discogs matching for unreconciled artists via discogs-cache release_artist table, with member/group fallback via artist_member table. |
| `semantic_index/wikidata_influence.py` | Extract directed Wikidata P737 influence edges between reconciled artists. Resolves QIDs to canonical names via entity store. |
| `semantic_index/label_hierarchy.py` | Populate label and label_hierarchy tables from Wikidata P749/P355 relationships via Discogs label ID (P1902) lookups. |
| `semantic_index/discogs_enrichment.py` | Aggregate Discogs metadata (styles, personnel, labels, compilations) per artist. |
| `semantic_index/discogs_edges.py` | Compute Discogs-derived edges: shared personnel, shared style (Jaccard), label family, compilation co-appearance. |
| `semantic_index/acousticbrainz.py` | Load AcousticBrainz high-level features from extracted data dump, aggregate per-artist audio profiles, compute cosine similarity edges. |
| `semantic_index/musicbrainz_client.py` | MusicBrainz cache client: artist name matching and recording MBID resolution via `mb_artist_recording` materialized view. |
| `semantic_index/graph_metrics.py` | Compute and persist Louvain communities, betweenness centrality, PageRank, and discovery scores to the SQLite database. Uses `wxyc_etl.text.is_compilation_artist` to filter compilation entries. Idempotent post-processing step runnable standalone or as a pipeline step. |
| `semantic_index/graph_export.py` | Build NetworkX graph and export GEXF. |
| `semantic_index/sqlite_export.py` | Build and export SQLite graph database with enrichment and edge tables. Supports optional entity store integration for persistent artist identities. |
| `semantic_index/facet_export.py` | Export play-level data and pre-materialized aggregate tables for dynamic faceted PMI computation. Creates dj, play, artist_month_count, artist_dj_count, month_total, and dj_total tables. |
| `semantic_index/api/app.py` | FastAPI application factory. Takes a SQLite database path, returns a configured app. |
| `semantic_index/api/database.py` | Request-scoped SQLite connection dependency for FastAPI. |
| `semantic_index/api/schemas.py` | Pydantic response models for the Graph API (ArtistSummary, ArtistDetail, EntityArtists, SearchResponse, NeighborsResponse, ExplainResponse, FacetsResponse, DjSummary, NarrativeResponse, CommunitiesResponse, DiscoveryResponse, PreviewResponse). |
| `semantic_index/api/routes.py` | Graph API query endpoints: search, artist detail, neighbors by edge type (with optional month/DJ facet filters), explain relationships, entity artist groups, available facets, community metadata, discovery (underplayed sonic fits). |
| `semantic_index/api/narrative.py` | LLM-generated edge narrative endpoint. Calls Claude Haiku to explain artist relationships in plain English. Caches results in a sidecar SQLite database. Facet-aware. |
| `semantic_index/api/preview.py` | Audio preview URL endpoint with multi-source fallback (iTunes lookup, Spotify, Bandcamp, Deezer, iTunes search). Caches results in a sidecar SQLite database. Powers the in-card transition player in the graph explorer. |
| `run_pipeline.py` | CLI entry point wiring the pipeline. |

### Column Mappings (0-indexed from SQL INSERT order)

| Table | Key columns |
|-------|------------|
| FLOWSHEET_ENTRY_PROD | 0=ID, 1=ARTIST_NAME, 3=SONG_TITLE, 4=RELEASE_TITLE, 6=LIBRARY_RELEASE_ID, 8=LABEL_NAME, 10=START_TIME, 12=RADIO_SHOW_ID, 13=SEQUENCE_WITHIN_SHOW, 15=FLOWSHEET_ENTRY_TYPE_CODE_ID, 18=REQUEST_FLAG |
| FLOWSHEET_RADIO_SHOW_PROD | 0=ID, 2=DJ_NAME, 3=DJ_ID |
| LIBRARY_RELEASE | 0=ID, 8=LIBRARY_CODE_ID |
| LIBRARY_CODE | 0=ID, 1=GENRE_ID, 7=PRESENTATION_NAME |
| LIBRARY_CODE_CROSS_REFERENCE | 1=CROSS_REFERENCING_ARTIST_ID (→ LIBRARY_CODE.ID), 2=CROSS_REFERENCED_LIBRARY_CODE_ID, 3=COMMENT |
| RELEASE_CROSS_REFERENCE | 1=CROSS_REFERENCING_ARTIST_ID (→ LIBRARY_CODE.ID), 2=CROSS_REFERENCED_RELEASE_ID, 3=COMMENT |
| GENRE | 0=ID, 1=NAME |

### SQLite Schema

```sql
CREATE TABLE artist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    genre TEXT,
    total_plays INTEGER NOT NULL DEFAULT 0,
    active_first_year INTEGER,
    active_last_year INTEGER,
    dj_count INTEGER NOT NULL DEFAULT 0,
    -- Added by graph_metrics.py (nullable, only set for artists in the transition graph):
    community_id INTEGER,          -- Louvain community assignment
    betweenness REAL,              -- Betweenness centrality
    pagerank REAL,                 -- PageRank score
    discovery_score REAL,          -- acoustic_neighbor_count / (dj_edge_count + 1)
    dj_edge_count INTEGER,         -- Undirected degree in transition graph
    acoustic_neighbor_count INTEGER, -- Acoustic neighbors at similarity >= 0.95
    request_ratio REAL NOT NULL DEFAULT 0.0,
    show_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE dj_transition (
    source_id INTEGER NOT NULL REFERENCES artist(id),
    target_id INTEGER NOT NULL REFERENCES artist(id),
    raw_count INTEGER NOT NULL,
    pmi REAL NOT NULL,
    PRIMARY KEY (source_id, target_id)
);

CREATE TABLE cross_reference (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    comment TEXT,
    source TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id, source)
);

CREATE TABLE wikidata_influence (
    source_id INTEGER NOT NULL REFERENCES artist(id),
    target_id INTEGER NOT NULL REFERENCES artist(id),
    source_qid TEXT NOT NULL,
    target_qid TEXT NOT NULL,
    PRIMARY KEY (source_id, target_id)
);

CREATE TABLE audio_profile (
    artist_id INTEGER PRIMARY KEY REFERENCES artist(id),
    avg_danceability REAL,
    primary_genre TEXT,
    primary_genre_probability REAL,
    voice_instrumental_ratio REAL,
    feature_centroid TEXT,  -- JSON array of 36 floats
    recording_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE acoustic_similarity (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    similarity REAL NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);

-- Graph metrics tables (created by graph_metrics.py)

CREATE TABLE community (
    id INTEGER PRIMARY KEY,
    size INTEGER NOT NULL,
    label TEXT,
    top_genres TEXT,   -- JSON: [["Rock", 150], ["Jazz", 80], ...]
    top_artists TEXT   -- JSON: ["Yo La Tengo", "The Beatles", ...]
);

-- Entity store tables (created by EntityStore.initialize())

CREATE TABLE entity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wikidata_qid TEXT,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL DEFAULT 'artist',
    spotify_artist_id TEXT,
    apple_music_artist_id TEXT,
    bandcamp_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE artist_style (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    style TEXT NOT NULL,
    PRIMARY KEY (artist_id, style)
);

CREATE TABLE reconciliation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    method TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Facet tables (created by facet_export.py for dynamic PMI)

CREATE TABLE dj (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_id TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL
);

CREATE TABLE play (
    id INTEGER PRIMARY KEY,
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    show_id INTEGER NOT NULL,
    dj_id INTEGER REFERENCES dj(id),
    sequence INTEGER NOT NULL,
    month INTEGER NOT NULL,       -- 1-12 (0 = no timestamp)
    request_flag INTEGER NOT NULL DEFAULT 0,
    timestamp INTEGER
);

CREATE TABLE artist_month_count (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    month INTEGER NOT NULL,
    play_count INTEGER NOT NULL,
    PRIMARY KEY (artist_id, month)
);

CREATE TABLE artist_dj_count (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    dj_id INTEGER NOT NULL REFERENCES dj(id),
    play_count INTEGER NOT NULL,
    PRIMARY KEY (artist_id, dj_id)
);

CREATE TABLE month_total (
    month INTEGER PRIMARY KEY,
    total_plays INTEGER NOT NULL,
    total_pairs INTEGER NOT NULL
);

CREATE TABLE dj_total (
    dj_id INTEGER PRIMARY KEY REFERENCES dj(id),
    total_plays INTEGER NOT NULL,
    total_pairs INTEGER NOT NULL
);
```

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                          # unit tests only
pytest -m integration           # needs fixture dump
pytest -m slow                  # needs production dump
```

### Shared Dependencies (wxyc-etl)

The pipeline uses `wxyc-etl` (a Rust/PyO3 package) for shared text normalization, compilation detection, and schema constants:

- **`wxyc_etl.text.normalize_artist_name(name)`** -- NFKD decomposition + diacritics stripping + lowercase + trim. Used as the base layer in `artist_resolver._normalize()`, which adds semantic-index-specific transforms (bracket removal, "the " strip, `&` -> `and`).
- **`wxyc_etl.text.is_compilation_artist(name)`** -- Compilation/VA detection covering "Various Artists", "V/A", "v.a.", "Soundtrack", "Compilation". Replaces the old narrow `is_various_artists()` in `utils.py`.
- **`wxyc_etl.text.split_artist_name(name)`** -- Context-free multi-artist splitting on `, `, ` / `, ` + `. Used in `artist_resolver._normalized_forms()`.
- **`wxyc_etl.schema.*`** -- Table name constants (`RELEASE_TABLE`, `RELEASE_ARTIST_TABLE`, `RELEASE_LABEL_TABLE`, `RELEASE_STYLE_TABLE`, `RELEASE_TRACK_TABLE`, `RELEASE_TRACK_ARTIST_TABLE`) for all discogs-cache SQL queries in `discogs_client.py` and `reconciliation.py`.

Summary tables (`artist_style_summary`, `artist_personnel_summary`, `artist_label_summary`, `artist_compilation_summary`) are materialized views created by discogs-cache and are not part of the wxyc-etl schema constants.

### Code Style

- Python 3.12+
- black (100 char line length)
- ruff (100 char, rules: E, W, F, I, N, UP, B, C4)
- mypy with pydantic plugin
- TDD: write failing test first, then implement

### Testing

- Unit tests in `tests/unit/` — hand-crafted data via factory functions, no SQL files
- Integration tests in `tests/integration/` — run against fixture dump, marked `@pytest.mark.integration`
- Slow tests marked `@pytest.mark.slow` — run against production dump, manual only
- Use WXYC example artists (Autechre, Stereolab, Father John Misty, etc.) in test fixtures

## Usage

```bash
python run_pipeline.py /path/to/wxycmusic.sql [--output-dir output/] [--min-count 2]
```

Output: `output/wxyc_artist_pmi.gexf` (Gephi graph) + `output/wxyc_artist_graph.db` (SQLite database).

Use `--no-sqlite` to skip the SQLite export.

### Entity store mode

Pass `--entity-store-path` to enable the entity store pipeline: artists are managed by the entity store (with persistent reconciliation state) rather than created fresh on each run. The entity store database becomes the SQLite output.

```bash
python run_pipeline.py dump.sql --entity-store-path output/wxyc_artist_graph.db --discogs-cache-dsn postgresql://...
```

- `--entity-store-path PATH` — Path to entity store SQLite database. Creates it if needed.
- `--entity-source {local,lml}` — Identity source for reconciliation. `local` (default) runs local reconciliation via entity_store.py + reconciliation.py. `lml` reads pre-resolved identities from LML's `entity.identity` PG table (requires `--discogs-cache-dsn`). Both paths coexist during the transition to centralized identity resolution. When `lml` is set, local reconciliation steps (reconcile_batch, reconcile_members, reconcile_wikidata) are skipped.
- `--skip-reconciliation` — Skip Discogs reconciliation step (only applies to `--entity-source=local`).
- `--compute-discogs-edges` — Compute Discogs-derived edges (shared personnel, styles, labels, compilations). Off by default.
- `--compute-wikidata-influences` — Query Wikidata P737 (influenced by) and create directed influence edges. Requires `--entity-store-path` with reconciled Wikidata QIDs.
- `--populate-label-hierarchy` — Populate label and label_hierarchy tables from Wikidata P749/P355. Requires `--entity-store-path` and enrichment data.
- `--fetch-streaming-ids` — Fetch Spotify (P1902), Apple Music (P2850), and Bandcamp (P3283) IDs from Wikidata for entities with QIDs. Requires `--entity-store-path`.

## Graph API

A read-only FastAPI service that queries the SQLite database produced by the pipeline. Serves the D3.js graph explorer at the root URL and the JSON API at `/graph/*`.

```bash
python -m semantic_index.api
```

Or programmatically:

```python
from semantic_index.api.app import create_app
app = create_app("data/wxyc_artist_graph.db")
```

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | D3.js graph explorer (interactive visualization). |
| `GET` | `/health` | Health check — returns artist count or 503 if database is unreachable. |
| `GET` | `/graph/artists/search?q=autechre&limit=10` | Case-insensitive LIKE search, ordered by total_plays descending. |
| `GET` | `/graph/artists/{id}` | Full artist detail including external IDs (Discogs, MusicBrainz, Wikidata QID) and streaming service IDs (Spotify, Apple Music, Bandcamp) joined from the entity table. Gracefully degrades on old-schema databases. |
| `GET` | `/graph/artists/{id}/neighbors?type=djTransition&limit=20` | Neighbors by edge type. Types: `djTransition`, `sharedPersonnel`, `sharedStyle`, `labelFamily`, `compilation`, `crossReference`, `wikidataInfluence`. Supports optional `month` (1-12) and `dj_id` facet filters for `djTransition` — computes PMI dynamically from play-level data. `min_raw_count` (default 1) filters DJ transition edges by minimum co-occurrence count; applies to `djTransition` and `affinity` edge types. |
| `GET` | `/graph/artists/{id}/explain/{target_id}` | All relationship types between two artists with weights and details. |
| `GET` | `/graph/entities/{id}/artists` | All artists sharing an entity (alias group). Returns entity metadata and a list of artist summaries. |
| `GET` | `/graph/facets` | Available facet values (months with data, DJ list) for filtering. Gracefully returns empty lists on databases without facet tables. |
| `GET` | `/graph/communities?min_size=5&limit=50` | Louvain community metadata (size, label, top genres, top artists). Gracefully returns empty on databases without the `community` table. |
| `GET` | `/graph/discovery?limit=25&community_id=&genre=` | Underplayed sonic fits: artists with high acoustic similarity but few DJ transitions, ordered by discovery score descending. Optional community/genre filters. Returns empty on databases without graph metrics. |
| `GET` | `/graph/artists/{id}/explain/{target_id}/narrative?month=&dj_id=` | LLM-generated natural-language explanation of the relationship between two artists. Uses Claude Haiku. Cached in sidecar SQLite DB. Returns 501 when `ANTHROPIC_API_KEY` is not set. |
| `GET` | `/graph/artists/{id}/preview` | Audio preview URL for an artist. Multi-source fallback: iTunes lookup (by Apple Music ID) -> Spotify top tracks (by Spotify ID, requires credentials) -> Bandcamp (by bandcamp_id, scrapes track stream) -> Deezer search (by name) -> iTunes search (by name). Cached in sidecar `.preview-cache.db`. |

### Deployment

Deployed on Railway via Dockerfile. The `data/wxyc_artist_graph.db` file is the committed, deployment-ready copy of the pipeline output. After re-running the pipeline, copy the new database from `output/` to `data/` and commit:

```bash
cp output/wxyc_artist_graph.db data/wxyc_artist_graph.db
```

Configuration via environment variables:
- `DB_PATH` — path to SQLite database (default: `data/wxyc_artist_graph.db`)
- `HOST` — bind address (default: `0.0.0.0`)
- `PORT` — port (default: `8000`, set automatically by Railway)
- `ANTHROPIC_API_KEY` — Anthropic API key for narrative generation (optional; narrative endpoint returns 501 when not set)
- `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` — Spotify API credentials for preview URL lookups (optional; Spotify tier in the preview fallback chain is skipped when not set)

## Data

The pipeline parses tubafrenzy MySQL dump files directly (no database required). Production dumps are not committed to git — pass the path as a CLI argument. The fixture dump at `tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql` has minimal data suitable for structural testing only. The `data/` directory contains a committed copy of the latest pipeline output for deployment.
