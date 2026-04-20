# Semantic Index

Builds a semantic artist graph from WXYC DJ transition data. DJs curate transitions between artists during their shows — these adjacency relationships encode latent genre/mood/style similarity that PMI (Pointwise Mutual Information) can surface.

## Architecture

A batch pipeline that parses a tubafrenzy MySQL dump, resolves artist names via catalog and Discogs, extracts adjacency pairs and cross-reference edges, computes PMI, enriches artists with Discogs metadata, computes Discogs-derived edges, and exports a GEXF graph and SQLite database.

```
SQL dump → sql_parser ──→ artist_resolver → adjacency → pmi ────────────→ graph_export → GEXF
Backend PG → pg_source ─┘ → cross_reference ────────────────────────────→ sqlite_export → SQLite
                           → node_attributes ───────────────────────────→
            → discogs_client → discogs_enrichment → discogs_edges ──────→
            → wikidata_client → wikidata_influence ─────────────────────→
            → musicbrainz_client → acousticbrainz_client (PG) ─────────→ audio_profile + acoustic_similarity
            → musicbrainz_client → acousticbrainz (tar loader, deprecated) → audio_profile + acoustic_similarity

S3 archive → archive_client → archive_essentia (VGGish + classification heads) → audio_profile

SQLite ──→ api (FastAPI + aiosqlite) ──→ JSON responses
```

### Modules

| Module | Responsibility |
|--------|---------------|
| `semantic_index/sql_parser.py` | Parse MySQL INSERT statements from SQL dump files. Uses `wxyc_etl.parser` Rust extension for ~1000x faster parsing, with `sql_parser_rs` and pure-Python fallbacks. Set `WXYC_ETL_NO_RUST=1` to force pure-Python. |
| `semantic_index/models.py` | Pydantic data models for all pipeline entities. |
| `semantic_index/artist_resolver.py` | Multi-tier artist name resolution: compilation track (CTA from SQL dump + Discogs from `compilation_track_artists.json`), FK chain, name match, normalized (via `wxyc_etl.text.normalize_artist_name` + local bracket/the/& transforms), fuzzy (Jaro-Winkler), Discogs search, raw fallback. Uses `wxyc_etl.text.split_artist_name` for alias splitting and `wxyc_etl.text.is_compilation_artist` for VA detection. |
| `semantic_index/adjacency.py` | Extract consecutive artist pairs within radio shows. |
| `semantic_index/pmi.py` | Compute Pointwise Mutual Information for artist co-occurrences. |
| `semantic_index/node_attributes.py` | Extract and compute per-artist temporal, DJ, and request statistics. |
| `semantic_index/cross_reference.py` | Extract cross-reference edges from catalog cross-reference tables. |
| `semantic_index/discogs_client.py` | Two-tier Discogs client: discogs-cache PostgreSQL with library-metadata-lookup API fallback. Uses `wxyc_etl.schema` constants for all discogs-cache table names. |
| `semantic_index/wikidata_client.py` | Wikidata SPARQL client: influence relationships (P737), label hierarchy (P749/P355), and label-to-QID bridging via P1902. Identity resolution methods (Discogs artist ID lookup, name search, streaming IDs) have been moved to LML. |
| `semantic_index/pipeline_db.py` | Pipeline SQLite database manager: schema creation/migration, artist CRUD with COALESCE upsert, bulk stats, style persistence, entity deduplication by shared Wikidata QID. Successor to the deleted `entity_store.py`. |
| `semantic_index/label_store.py` | Label CRUD operations: get_or_create_label, update_label_qid, insert_label_hierarchy. Extracted from the deleted `entity_store.py` for use by `label_hierarchy.py`. |
| `semantic_index/lml_identity.py` | Import pre-resolved identities from LML's `entity.identity` PG table into the local pipeline database. |
| `semantic_index/wikidata_influence.py` | Extract directed Wikidata P737 influence edges between reconciled artists. Resolves QIDs to canonical names via the pipeline database. |
| `semantic_index/label_hierarchy.py` | Populate label and label_hierarchy tables from Wikidata P749/P355 relationships via Discogs label ID (P1902) lookups. |
| `semantic_index/discogs_enrichment.py` | Aggregate Discogs metadata (styles, personnel, labels, compilations) per artist. |
| `semantic_index/discogs_edges.py` | Compute Discogs-derived edges: shared personnel, shared style (Jaccard), label family, compilation co-appearance. |
| `semantic_index/acousticbrainz.py` | Load AcousticBrainz high-level features, aggregate per-artist audio profiles (59-dim feature vector across 18 classifiers), compute cosine similarity edges. Supports both PG and tar-based loading. |
| `semantic_index/acousticbrainz_client.py` | PostgreSQL client for AcousticBrainz features. Queries `ab_recording` in musicbrainz-cache, joining with `mb_artist_recording` for per-artist feature retrieval. Preferred over tar-based loading. |
| `semantic_index/musicbrainz_client.py` | MusicBrainz cache client: recording MBID resolution via `mb_artist_recording` materialized view. Identity resolution methods (lookup_by_name, batch_lookup) have been moved to LML. |
| `semantic_index/graph_metrics.py` | Compute and persist Louvain communities, betweenness centrality, PageRank, and discovery scores to the SQLite database. Uses `wxyc_etl.text.is_compilation_artist` to filter compilation entries. Idempotent post-processing step runnable standalone or as a pipeline step. |
| `semantic_index/graph_export.py` | Build NetworkX graph and export GEXF. |
| `semantic_index/sqlite_export.py` | Build and export SQLite graph database with enrichment and edge tables. Supports optional PipelineDB integration for persistent artist identities. |
| `semantic_index/facet_export.py` | Export play-level data and pre-materialized aggregate tables for dynamic faceted PMI computation. Creates dj, play, artist_month_count, artist_dj_count, month_total, and dj_total tables. |
| `semantic_index/api/app.py` | FastAPI application factory. Takes a SQLite database path, returns a configured app. |
| `semantic_index/api/database.py` | Request-scoped SQLite connection dependency for FastAPI. |
| `semantic_index/api/schemas.py` | Pydantic response models for the Graph API (ArtistSummary, ArtistDetail, EntityArtists, SearchResponse, NeighborsResponse, ExplainResponse, FacetsResponse, DjSummary, NarrativeResponse, CommunitiesResponse, DiscoveryResponse, PreviewResponse). |
| `semantic_index/api/routes.py` | Graph API query endpoints: search, artist detail, neighbors by edge type (with optional month/DJ facet filters), explain relationships, entity artist groups, available facets, community metadata, discovery (underplayed sonic fits). |
| `semantic_index/api/narrative.py` | LLM-generated edge narrative endpoint. Calls Claude Haiku to explain artist relationships in plain English. Caches results in a sidecar SQLite database. Facet-aware. Enriches prompts with audio profile features (genre, mood, danceability) when available. |
| `semantic_index/api/preview.py` | Audio preview URL endpoint with multi-source fallback (iTunes lookup, Spotify, Bandcamp, Deezer, iTunes search). Caches results in a sidecar SQLite database. Powers the in-card transition player in the graph explorer. |
| `semantic_index/pg_source.py` | Query Backend-Service PostgreSQL (`wxyc_schema.*`) for pipeline input data. Returns the same types as `sql_parser.py` (FlowsheetEntry, LibraryCode, LibraryRelease). Used by the nightly sync instead of SQL dump parsing. |
| `semantic_index/nightly_sync.py` | Nightly sync orchestrator: query PG → resolve → PMI → stats → export → facets → graph metrics → atomic DB swap. Preserves enrichment tables from the existing database. |
| `run_pipeline.py` | CLI entry point wiring the full pipeline (SQL dump mode). |
| `scripts/nightly_sync.py` | CLI wrapper for `semantic_index.nightly_sync.main()`. |
| `semantic_index/archive_client.py` | S3 client for WXYC hourly audio archives. Downloads MP3 files from `wxyc-archive` S3 bucket, decodes to PCM WAV via ffmpeg, extracts audio segments at specified offsets. Computes S3 keys from timestamps (`YYYY/MM/DD/YYYYMMDDHH00.mp3`). |
| `semantic_index/archive_essentia.py` | Essentia TF audio classification. Runs VGGish embeddings through 15 classification heads (genre, mood, danceability, voice/instrumental, tonal, gender, MIREX) to produce per-segment features compatible with the 59-dim RecordingFeatures layout. Three AB classifiers lack VGGish heads and are zero-filled (ismir04_rhythm, genre_electronic, timbre). |
| `scripts/process_archive.py` | CLI entry point for archive audio processing. Queries Backend-Service PG for flowsheet entries, groups by archive hour, downloads from S3, classifies segments via Essentia TF, aggregates per-artist profiles, writes to audio_profile table. Per-hour checkpointing, `--date-range`, `--max-hours`, `--aggregate-only`, `--retry-failed`, `--dry-run`. |
| `scripts/import_acousticbrainz.py` | ETL script: import AcousticBrainz high-level features from tar archives into PostgreSQL `ab_recording` table. Per-tar checkpointing, NAS-resilient, idempotent via `ON CONFLICT DO NOTHING`. |

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
| COMPILATION_TRACK_ARTIST | 0=ID, 1=LIBRARY_RELEASE_ID, 2=ARTIST_NAME, 3=TRACK_TITLE (loaded from separate dump via `--compilation-track-artist-dump`) |

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
    feature_centroid TEXT,  -- JSON array of 59 floats
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

### Pipeline DB mode

Pass `--db-path` to enable the pipeline database: artists are managed with persistent identity resolution from LML rather than created fresh on each run. The pipeline database becomes the SQLite output.

```bash
python run_pipeline.py dump.sql --db-path output/wxyc_artist_graph.db --discogs-cache-dsn postgresql://...
```

- `--db-path PATH` — Path to pipeline SQLite database. Creates it if needed. Identity resolution is read from LML's `entity.identity` PG table (requires `--discogs-cache-dsn`).
- `--compilation-track-artist-dump PATH` — Path to a SQL dump containing the `COMPILATION_TRACK_ARTIST` table. When provided, VA/compilation entries are resolved to per-track artists (Tier 0) before the FK chain.
- `--compute-discogs-edges` — Compute Discogs-derived edges (shared personnel, styles, labels, compilations). Off by default.
- `--compute-wikidata-influences` — Query Wikidata P737 (influenced by) and create directed influence edges. Requires `--db-path` with reconciled Wikidata QIDs.
- `--populate-label-hierarchy` — Populate label and label_hierarchy tables from Wikidata P749/P355. Requires `--db-path` and enrichment data.
- `--discogs-track-json PATH` — Path to `compilation_track_artists.json` (from LML `match_compilations.py`). Provides a Discogs-derived fallback (Tier 0b) for VA entries not matched by the CTA table. JSON format: `[{comp_id, discogs_release_id, tracks: [{position, title, artists: [str]}]}]` where `comp_id` = WXYC `LIBRARY_RELEASE_ID`.
- `--musicbrainz-cache-dsn` — When set (without `--acousticbrainz-dir`), uses the PostgreSQL `ab_recording` table for audio features. This is the preferred path — a single JOIN query replaces the two-step MusicBrainzClient + tar loader flow. Requires `import_acousticbrainz.py` to have populated `ab_recording`.
- `--acousticbrainz-dir` — **(Deprecated)** Path to AcousticBrainz tar archives. Requires `--musicbrainz-cache-dsn`. When both `--acousticbrainz-dir` and `--musicbrainz-cache-dsn` are set, the PG path is used and the tar dir is ignored.

### AcousticBrainz import

One-time ETL to populate the `ab_recording` table in the musicbrainz PostgreSQL database from the AcousticBrainz data dump tar archives. The import is resumable — per-tar checkpointing skips completed tars, and `ON CONFLICT DO NOTHING` handles duplicate MBIDs.

```bash
python scripts/import_acousticbrainz.py \
    --tar-dir "/Volumes/Peak Twins/acousticbrainz/" \
    --dsn postgresql://localhost/musicbrainz \
    --checkpoint output/ab_import_progress.db \
    [--retry-failed]
```

The `ab_recording` table stores all 18 AcousticBrainz classifiers as structured columns plus JSONB for probability distributions and metadata tags. The feature vector uses all 18 classifiers for a 59-dimension representation.

### Archive audio classification

Extends audio feature coverage beyond AcousticBrainz (which covers only ~13% of WXYC artists) by classifying WXYC's hourly audio archives directly. Uses flowsheet timestamps to locate each play within the S3 archive, extracts 30-second segments, and runs Essentia TF classifiers (VGGish + 15 classification heads) to produce per-segment features. Results are aggregated per-artist and written to the `audio_profile` table, enriching narrative generation with genre, mood, and danceability data.

```bash
python scripts/process_archive.py \
    --backend-dsn postgresql://... \
    --model-dir /path/to/essentia-models \
    --db-path data/wxyc_artist_graph.db \
    --checkpoint output/archive_progress.db \
    --date-range 2021-06-01:2026-01-01 \
    --max-hours 100 \
    [--segment-duration 30] \
    [--retry-failed] \
    [--dry-run]
```

- `--backend-dsn` / `DATABASE_URL_BACKEND` — Backend-Service PostgreSQL DSN (required). Queries `wxyc_schema.flowsheet` for entry timestamps.
- `--model-dir` / `ESSENTIA_MODEL_DIR` — Directory containing Essentia TF models: `audioset-vggish-3.pb` (275 MB feature extractor) + 15 classification heads (~50 KB each).
- `--db-path` / `DB_PATH` — Pipeline SQLite database for writing aggregated audio profiles (optional; omit to skip aggregation).
- `--checkpoint` / `ARCHIVE_CHECKPOINT` — Path to checkpoint SQLite database (default: `output/archive_progress.db`).
- `--bucket` — S3 bucket name (default: `wxyc-archive`).
- `--date-range` — Date range to process as `START:END` (YYYY-MM-DD:YYYY-MM-DD, required unless `--aggregate-only`).
- `--max-hours` — Maximum archive hours to process (0 = unlimited).
- `--segment-duration` — Duration of each segment in seconds (default: 30).
- `--aggregate-only` — Skip processing; aggregate existing checkpoint data into the DB.
- `--retry-failed` — Re-attempt previously failed archive hours.
- `--dry-run` — Log what would be processed without downloading audio.

System dependencies: `ffmpeg`. Python: `pip install -e ".[archive]"` (essentia-tensorflow requires Python 3.13, not 3.14).

**Essentia model setup:**

```bash
# Download VGGish feature extractor (275 MB)
curl -o models/audioset-vggish-3.pb https://essentia.upf.edu/models/feature-extractors/vggish/audioset-vggish-3.pb

# Download 15 classification heads (~50 KB each)
for cat in danceability genre_dortmund mood_acoustic mood_aggressive mood_electronic \
  mood_happy mood_party mood_relaxed mood_sad moods_mirex tonal_atonal \
  voice_instrumental gender genre_rosamerica genre_tzanetakis; do
  curl -o "models/${cat}-audioset-vggish-1.pb" \
    "https://essentia.upf.edu/models/classification-heads/${cat}/${cat}-audioset-vggish-1.pb"
done
```

**Processing estimate:** 41,578 hourly MP3s (June 2021–present), 330K–620K segments at ~3s each. 8-core EC2: 1.5–3 days, ~$12–22.

### Nightly sync mode

The nightly sync queries Backend-Service PostgreSQL directly instead of parsing a SQL dump. It recomputes the core graph (resolution, PMI, stats, facets, graph metrics) while preserving enrichment data (Discogs, Wikidata, AcousticBrainz) from the existing production database via an atomic copy-and-swap.

```bash
python scripts/nightly_sync.py --dsn postgresql://... --db-path data/wxyc_artist_graph.db
```

Or via environment variables (for Railway cron):

```bash
DATABASE_URL_BACKEND=postgresql://... DB_PATH=data/wxyc_artist_graph.db python scripts/nightly_sync.py
```

- `--dsn` / `DATABASE_URL_BACKEND` — PostgreSQL DSN for Backend-Service (required).
- `--db-path` / `DB_PATH` — Production SQLite database path (default: `data/wxyc_artist_graph.db`).
- `--min-count` / `MIN_COUNT` — Minimum co-occurrence count for DJ transition edges (default: 2).
- `--dry-run` — Run the full pipeline but skip the atomic swap (writes to a temp file instead).
- `--verbose` — Enable debug logging.

**PG schema mappings (`wxyc_schema.*` → pipeline types):**
- `artists` → `LibraryCode` (id, genre_id from `genre_artist_crossreference`, artist_name → presentation_name)
- `library` → `LibraryRelease` (id, artist_id → library_code_id)
- `flowsheet` → `FlowsheetEntry` (filtered to `entry_type = 'track'`, `add_time` → epoch, `request_flag` boolean → int)
- `shows` → show-to-DJ mapping (keyed by `shows.id`, `primary_dj_id` as value)

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

Deployed on EC2 (us-east-1) as a Docker container alongside Backend-Service. The container runs on port 8083, with nginx reverse proxy and Let's Encrypt TLS for `explore.wxyc.org`.

**GitHub Actions** (`.github/workflows/deploy.yml`) auto-deploys on push to main: builds a Docker image, pushes to ECR, SSHs to EC2 to pull and restart the container. Manual deploys via `workflow_dispatch`.

**EC2 container setup:**

```bash
docker run -d \
  --name semantic-index \
  -p 8083:8083 \
  -v /home/ec2-user/semantic-index-data:/data \
  --restart unless-stopped \
  --env-file .env.semantic-index \
  $ECR_URI/semantic-index:$TAG
```

The SQLite database and sidecar caches live in the bind-mounted `/data` directory and persist across deploys.

**Configuration** via environment variables (`.env.semantic-index` on EC2):
- `DB_PATH` — path to SQLite database (default: `/data/wxyc_artist_graph.db`)
- `HOST` — bind address (default: `0.0.0.0`)
- `PORT` — port (default: `8083`)
- `ANTHROPIC_API_KEY` — Anthropic API key for narrative generation (optional; narrative endpoint returns 501 when not set)
- `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` — Spotify API credentials for preview URL lookups (optional; Spotify tier in the preview fallback chain is skipped when not set)
- `SYNC_ENABLED` — enable the in-process nightly sync scheduler (default: `false`)
- `SYNC_HOUR_UTC` — hour (UTC) to run the daily sync (default: `9`, i.e. 5:00 AM ET)
- `DATABASE_URL_BACKEND` — Backend-Service PostgreSQL DSN for nightly sync (required when `SYNC_ENABLED=true`; uses the RDS private endpoint since EC2 and RDS share a VPC)
- `SYNC_MIN_COUNT` — minimum co-occurrence count for DJ transition edges (default: `2`)

**GitHub Actions secrets** (shared with Backend-Service, same GitHub org):
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ECR_URI`
- `EC2_HOST`, `EC2_USER`, `EC2_SSH_KEY`

### Nightly sync scheduler (in-process)

The API service includes a built-in sync scheduler that runs `nightly_sync()` as a background daemon thread. Enable it by setting env vars in `.env.semantic-index`:

- `SYNC_ENABLED=true` — enable the scheduler (default: false)
- `SYNC_HOUR_UTC=9` — hour to run daily sync (default: 9 = 5:00 AM ET)
- `DATABASE_URL_BACKEND=postgresql://...` — Backend-Service PG DSN (required when sync enabled)
- `SYNC_MIN_COUNT=2` — minimum co-occurrence count for DJ transition edges

The scheduler sleeps until the configured hour, runs the full pipeline (PG → resolve → PMI → export → facets → graph metrics), atomically swaps the database, then sleeps until the next day. The API continues serving requests during the rebuild. Runtime is ~5 minutes.

The sync can also be run manually via CLI: `python scripts/nightly_sync.py --dsn postgresql://... --verbose`

## Data

The pipeline parses tubafrenzy MySQL dump files directly (no database required). Production dumps are not committed to git — pass the path as a CLI argument. The fixture dump at `tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql` has minimal data suitable for structural testing only. The `data/` directory contains a committed copy of the latest pipeline output for deployment.
