# Semantic Index

Builds a semantic artist graph from WXYC DJ transition data. DJs curate transitions between artists during their shows — these adjacency relationships encode latent genre/mood/style similarity that PMI (Pointwise Mutual Information) can surface.

## Architecture

A batch pipeline that parses a tubafrenzy MySQL dump, resolves artist names via catalog and Discogs, extracts adjacency pairs and cross-reference edges, computes PMI, enriches artists with Discogs metadata, computes Discogs-derived edges, and exports a GEXF graph and SQLite database.

```
SQL dump → sql_parser → artist_resolver → adjacency → pmi ──────────────→ graph_export → GEXF
                       → cross_reference ────────────────────────────────→ sqlite_export → SQLite
                       → node_attributes ────────────────────────────────→
         → discogs_client → discogs_enrichment → discogs_edges ─────────→

SQLite ──→ api (FastAPI + aiosqlite) ──→ JSON responses
```

### Modules

| Module | Responsibility |
|--------|---------------|
| `semantic_index/sql_parser.py` | Parse MySQL INSERT statements from SQL dump files. Streaming interface for large files. |
| `semantic_index/models.py` | Pydantic data models for all pipeline entities. |
| `semantic_index/artist_resolver.py` | Multi-tier artist name resolution: FK chain, name match, normalized, fuzzy (Jaro-Winkler), Discogs, raw fallback. |
| `semantic_index/adjacency.py` | Extract consecutive artist pairs within radio shows. |
| `semantic_index/pmi.py` | Compute Pointwise Mutual Information for artist co-occurrences. |
| `semantic_index/node_attributes.py` | Extract and compute per-artist temporal, DJ, and request statistics. |
| `semantic_index/cross_reference.py` | Extract cross-reference edges from catalog cross-reference tables. |
| `semantic_index/discogs_client.py` | Two-tier Discogs client: discogs-cache PostgreSQL with library-metadata-lookup API fallback. |
| `semantic_index/wikidata_client.py` | Wikidata SPARQL client: batched lookups by Discogs ID (P1953), influence relationships (P737), label hierarchy (P749/P355), and name search via wbsearchentities API. |
| `semantic_index/entity_store.py` | Persistent entity store for reconciled artist identities: schema creation/migration, CRUD, artist upsert, reconciliation log, artist styles. Creates the artist table from scratch on a fresh database or migrates an existing one. |
| `semantic_index/reconciliation.py` | Bulk Discogs matching for unreconciled artists via discogs-cache release_artist table, with member/group fallback via artist_member table. |
| `semantic_index/discogs_enrichment.py` | Aggregate Discogs metadata (styles, personnel, labels, compilations) per artist. |
| `semantic_index/discogs_edges.py` | Compute Discogs-derived edges: shared personnel, shared style (Jaccard), label family, compilation co-appearance. |
| `semantic_index/graph_export.py` | Build NetworkX graph and export GEXF. |
| `semantic_index/sqlite_export.py` | Build and export SQLite graph database with enrichment and edge tables. Supports optional entity store integration for persistent artist identities. |
| `semantic_index/api/app.py` | FastAPI application factory. Takes a SQLite database path, returns a configured app. |
| `semantic_index/api/database.py` | Request-scoped SQLite connection dependency for FastAPI. |
| `semantic_index/api/schemas.py` | Pydantic response models for the Graph API (ArtistSummary, ArtistDetail, EntityArtists, SearchResponse, NeighborsResponse, ExplainResponse). |
| `semantic_index/api/routes.py` | Graph API query endpoints: search, artist detail, neighbors by edge type, explain relationships, entity artist groups. |
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

-- Entity store tables (created by EntityStore.initialize())

CREATE TABLE entity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wikidata_qid TEXT UNIQUE,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL DEFAULT 'artist',
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
```

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                          # unit tests only
pytest -m integration           # needs fixture dump
pytest -m slow                  # needs production dump
```

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
- `--skip-reconciliation` — Skip Discogs reconciliation step.
- `--compute-discogs-edges` — Compute Discogs-derived edges (shared personnel, styles, labels, compilations). Off by default.

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
| `GET` | `/graph/artists/{id}` | Full artist detail including external IDs (Discogs, MusicBrainz, Wikidata QID) joined from the entity table. Gracefully degrades on old-schema databases. |
| `GET` | `/graph/artists/{id}/neighbors?type=djTransition&limit=20` | Neighbors by edge type. Types: `djTransition`, `sharedPersonnel`, `sharedStyle`, `labelFamily`, `compilation`, `crossReference`. |
| `GET` | `/graph/artists/{id}/explain/{target_id}` | All relationship types between two artists with weights and details. |
| `GET` | `/graph/entities/{id}/artists` | All artists sharing an entity (alias group). Returns entity metadata and a list of artist summaries. |

### Deployment

Deployed on Railway via Dockerfile. The `data/wxyc_artist_graph.db` file is the committed, deployment-ready copy of the pipeline output. After re-running the pipeline, copy the new database from `output/` to `data/` and commit:

```bash
cp output/wxyc_artist_graph.db data/wxyc_artist_graph.db
```

Configuration via environment variables:
- `DB_PATH` — path to SQLite database (default: `data/wxyc_artist_graph.db`)
- `HOST` — bind address (default: `0.0.0.0`)
- `PORT` — port (default: `8000`, set automatically by Railway)

## Data

The pipeline parses tubafrenzy MySQL dump files directly (no database required). Production dumps are not committed to git — pass the path as a CLI argument. The fixture dump at `tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql` has minimal data suitable for structural testing only. The `data/` directory contains a committed copy of the latest pipeline output for deployment.
