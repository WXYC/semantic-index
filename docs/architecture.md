# Architecture

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

## Modules

| Module | Responsibility |
|--------|---------------|
| `semantic_index/sql_parser.py` | Parse MySQL INSERT statements from SQL dump files. Uses `wxyc_etl.parser` Rust extension for ~1000x faster parsing, with `sql_parser_rs` and pure-Python fallbacks. Set `WXYC_ETL_NO_RUST=1` to force pure-Python. |
| `semantic_index/models.py` | Pydantic data models for all pipeline entities. |
| `semantic_index/artist_resolver.py` | Multi-tier artist name resolution: compilation track (CTA from SQL dump + Discogs from `compilation_track_artists.json`), FK chain, name match, normalized (via `wxyc_etl.text.to_identity_match_form` plus an `&` → `and` shim applied first), fuzzy (Jaro-Winkler), Discogs search, raw fallback. Uses `wxyc_etl.text.split_artist_name` for alias splitting and `wxyc_etl.text.is_compilation_artist` for VA detection. |
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
| `semantic_index/discogs_edges.py` | Compute Discogs-derived edges: shared personnel, shared style (Jaccard), label family, compilation co-appearance. Per-artist top-K prune for shared_personnel and label_family delegates to `edge_prune`. |
| `semantic_index/edge_prune.py` | Shared top-K-per-artist prune for symmetric `(artist_a_id, artist_b_id)` edge tables. Backs `prune_acoustic_similarity` (in `acousticbrainz.py`), `prune_shared_personnel`, and `prune_label_family` (in `discogs_edges.py`). |
| `semantic_index/acousticbrainz.py` | Load AcousticBrainz high-level features, aggregate per-artist audio profiles (59-dim feature vector across 18 classifiers), compute cosine similarity edges. Supports both PG and tar-based loading. |
| `semantic_index/acousticbrainz_client.py` | PostgreSQL client for AcousticBrainz features. Queries `ab_recording` in musicbrainz-cache, joining with `mb_artist_recording` for per-artist feature retrieval. Preferred over tar-based loading. |
| `semantic_index/musicbrainz_client.py` | MusicBrainz cache client: recording MBID resolution via `mb_artist_recording` materialized view. Identity resolution methods (lookup_by_name, batch_lookup) have been moved to LML. |
| `semantic_index/graph_metrics.py` | Compute and persist Louvain communities, betweenness centrality, and PageRank to the SQLite database. Uses `wxyc_etl.text.is_compilation_artist` to filter compilation entries. Idempotent post-processing step runnable standalone or as a pipeline step. |
| `semantic_index/graph_export.py` | Build NetworkX graph and export GEXF. |
| `semantic_index/sqlite_export.py` | Build and export SQLite graph database with enrichment and edge tables. Supports optional PipelineDB integration for persistent artist identities. |
| `semantic_index/facet_export.py` | Export play-level data and pre-materialized aggregate tables for dynamic faceted PMI computation. Creates dj, play, artist_month_count, artist_dj_count, month_total, and dj_total tables. |
| `semantic_index/api/app.py` | FastAPI application factory. Takes a SQLite database path, returns a configured app. |
| `semantic_index/api/database.py` | Request-scoped SQLite connection dependency for FastAPI. |
| `semantic_index/api/schemas.py` | Pydantic response models for the Graph API (ArtistSummary, ArtistDetail, EntityArtists, SearchResponse, NeighborsResponse, ExplainResponse, FacetsResponse, DjSummary, NarrativeResponse, CommunitiesResponse, DiscoveryResponse, PreviewResponse). |
| `semantic_index/api/routes.py` | Graph API query endpoints: search, artist detail, neighbors by edge type (with optional month/DJ facet filters), explain relationships, entity artist groups, available facets, community metadata, discovery (underplayed sonic fits). |
| `semantic_index/api/narrative.py` | LLM-generated edge narrative endpoint. Calls Claude Haiku to explain artist relationships in plain English. Caches results in a sidecar SQLite database. Facet-aware. Enriches prompts with audio profile features (genre, mood, danceability) when available. |
| `semantic_index/narrative_audit.py` | Periodic claim-ratio audit on cached narratives. Samples N narratives, looks up source/target metadata from the production DB so the verifier sees the same data the live narrative endpoint scored against, runs each through a Haiku verifier prompt that decomposes them into grounded/ungrounded claims, and records ratios to a sidecar audit DB for review. Catches structural-claim hallucinations the always-on token-match gate can miss. |
| `semantic_index/api/narrative_audit_routes.py` | Read-only endpoint exposing the most-recent audit rows from the audit sidecar at `/graph/narrative-audit/recent`. |
| `scripts/audit_narratives.py` | CLI entry point for the claim-ratio audit. Sample-and-score with a configurable threshold; writes to the audit sidecar DB. |
| `semantic_index/labeling_app/` | Standalone FastAPI single-page web UI for labeling narrative eval-set rows. Reads `labeling.jsonl`, persists labels to a SQLite sidecar (`<jsonl>.labels.db`) keyed by labeler name, exports merge_labels-compatible CSV. Run with `python -m semantic_index.labeling_app --jsonl output/eval/labeling.jsonl`. |
| `semantic_index/api/preview.py` | Audio preview URL endpoint with multi-source fallback (iTunes lookup, Spotify, Bandcamp, Deezer, iTunes search). Caches results in a sidecar SQLite database. Powers the in-card transition player in the graph explorer. |
| `semantic_index/pg_source.py` | Query Backend-Service PostgreSQL (`wxyc_schema.*`) for pipeline input data. Returns the same types as `sql_parser.py` (FlowsheetEntry, LibraryCode, LibraryRelease). Used by the nightly sync instead of SQL dump parsing. `load_flowsheet_entries` uses a psycopg3 server-side cursor inside an explicit transaction to bound libpq's row buffer (#338); other loaders stay on the client-side cursor since their result sets are small. |
| `semantic_index/nightly_sync.py` | Nightly sync orchestrator: query PG → resolve → PMI → stats → export → entity dedup → facets → graph metrics → atomic DB swap. Preserves enrichment tables from the existing database. |
| `run_pipeline.py` | CLI entry point wiring the full pipeline (SQL dump mode). |
| `scripts/nightly_sync.py` | CLI wrapper for `semantic_index.nightly_sync.main()`. |
| `semantic_index/archive_client.py` | S3 client for WXYC hourly audio archives. Downloads MP3 files from `wxyc-archive` S3 bucket, decodes to PCM WAV via ffmpeg, extracts audio segments at specified offsets. Computes S3 keys from timestamps (`YYYY/MM/DD/YYYYMMDDHH00.mp3`). |
| `semantic_index/archive_essentia.py` | Essentia TF audio classification. Runs VGGish embeddings through 15 classification heads (genre, mood, danceability, voice/instrumental, tonal, gender, MIREX) to produce per-segment features compatible with the 59-dim RecordingFeatures layout. Three AB classifiers lack VGGish heads and are zero-filled (ismir04_rhythm, genre_electronic, timbre). |
| `semantic_index/archive_match.py` | Resolve archive artist names to production `artist.id` rows. Folds across case, diacritics, HTML entities, nickname quoting, brackets, "the" prefix, `&` ↔ `and`, and multi-artist credits. Skips compilation/VA. Refuses ambiguous normalized forms. Used by the aggregation step in `scripts/process_archive.py`. |
| `scripts/process_archive.py` | CLI entry point for archive audio processing. Queries Backend-Service PG for flowsheet entries, groups by archive hour, downloads from S3, classifies segments via Essentia TF, aggregates per-artist profiles via `ArchiveNameMatcher`, writes to audio_profile table. Per-hour checkpointing, `--date-range`, `--max-hours`, `--aggregate-only`, `--retry-failed`, `--dry-run`. |
| `scripts/import_acousticbrainz.py` | ETL script: import AcousticBrainz high-level features from tar archives into PostgreSQL `ab_recording` table. Per-tar checkpointing, NAS-resilient, idempotent via `ON CONFLICT DO NOTHING`. |
| `scripts/recover_audio_profiles.py` | Recovery ETL: restore audio profiles for artists with MusicBrainz GIDs. Resolves GID -> integer ID via PostgreSQL `mb_artist`, fetches AcousticBrainz features, builds 59-dim profiles, recomputes acoustic similarity. Atomic swap, dry-run support. |

## Column Mappings (0-indexed from SQL INSERT order)

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

## SQLite Schema

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
    style_tag TEXT NOT NULL,
    PRIMARY KEY (artist_id, style_tag)
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

## Data

The pipeline parses tubafrenzy MySQL dump files directly (no database required). Production dumps are not committed to git — pass the path as a CLI argument. The fixture dump at `tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql` has minimal data suitable for structural testing only. The `data/` directory contains a committed copy of the latest pipeline output for deployment.
