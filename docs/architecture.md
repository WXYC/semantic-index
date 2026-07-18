# Architecture

The pipeline is a batch job: data goes in one end (flowsheet history plus external metadata sources), a SQLite graph database comes out the other, and the Graph API serves that database read-only. This doc walks through the stages, then catalogs every module, the input column mappings, and the output schema. Unfamiliar terms are defined in the [glossary](glossary.md).

## The pipeline in stages

1. **Ingest.** Flowsheet entries, catalog rows, and show metadata come from one of two sources: a [tubafrenzy](glossary.md#tubafrenzy) MySQL dump parsed directly from the file (`sql_parser.py` — no database server needed), or the [Backend-Service](glossary.md#backend-service) PostgreSQL queried live (`pg_source.py`, used by the nightly sync). Both produce the same in-memory types, so everything downstream is source-agnostic.
2. **Resolve.** Every flowsheet entry's free-text artist name is mapped to a canonical artist (`artist_resolver.py`), trying the most reliable evidence first and degrading gracefully — see [artist resolution](glossary.md#artist-resolution). Compilation entries ("Various Artists") are resolved to the actual per-track artist where possible.
3. **Core edges.** Consecutive plays within each show become adjacency pairs (`adjacency.py`), scored with [PMI](glossary.md#pmi) (`pmi.py`). Curated catalog links become [cross-reference](glossary.md#cross-reference) edges (`cross_reference.py`). Per-artist statistics — active years, DJ counts, request ratio — are computed alongside (`node_attributes.py`).
4. **Enrich.** External sources add context and more edge types: [Discogs](glossary.md#discogs) metadata per artist (`discogs_enrichment.py`) and Discogs-derived edges — shared personnel, shared styles, label family, compilation co-appearances (`discogs_edges.py`); [Wikidata](glossary.md#wikidata) influence edges (`wikidata_influence.py`) and record-label hierarchy (`label_hierarchy.py`).
5. **Audio.** Two paths fill the `audio_profile` table: precomputed [AcousticBrainz](glossary.md#acousticbrainz) features fetched via the musicbrainz-cache (`acousticbrainz_client.py`), and direct classification of the [WXYC audio archive](glossary.md#wxyc-audio-archive) with Essentia (`archive_essentia.py`). Profile similarity becomes `acoustic_similarity` edges (`acousticbrainz.py`). See [docs/audio-ingest.md](audio-ingest.md).
6. **Graph metrics.** With all edges in place, [Louvain communities](glossary.md#louvain-community), [betweenness centrality](glossary.md#betweenness-centrality), and [PageRank](glossary.md#pagerank) are computed and persisted (`graph_metrics.py`).
7. **Export.** Everything is written to the SQLite [graph database](glossary.md#graph-database) (`sqlite_export.py`), including play-level [facet](glossary.md#facet) tables for on-demand filtered PMI (`facet_export.py`). In SQL-dump mode a [GEXF](glossary.md#gexf) file for Gephi is written as well (`graph_export.py`).
8. **Serve.** The Graph API (`semantic_index/api/`) reads that SQLite database and serves JSON plus the D3.js explorer — see [docs/graph-api.md](graph-api.md).

The same stages as a data-flow sketch:

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

Reference table — every module and script, in roughly pipeline order.

| Module | Responsibility |
|--------|---------------|
| `semantic_index/sql_parser.py` | Parses MySQL INSERT statements straight out of dump files, no database server involved. The heavy lifting is done by the `wxyc_etl.parser` Rust extension (~1000x faster than pure Python); `sql_parser_rs` and pure-Python implementations remain as fallbacks. Set `WXYC_ETL_NO_RUST=1` to force pure Python. |
| `semantic_index/pg_source.py` | The other ingest path: queries Backend-Service PostgreSQL (`wxyc_schema.*`) and returns the same types as `sql_parser.py` (FlowsheetEntry, LibraryCode, LibraryRelease), so the rest of the pipeline doesn't care which source was used. Used by the nightly sync. `load_flowsheet_entries` streams rows through a psycopg3 server-side cursor inside an explicit transaction to bound memory on the ~2M-row flowsheet (#338); the other loaders return small result sets and stay on client-side cursors. |
| `semantic_index/models.py` | Pydantic data models for all pipeline entities. |
| `semantic_index/artist_resolver.py` | Multi-tier [artist resolution](glossary.md#artist-resolution), most reliable evidence first: compilation-track lookup ([CTA](glossary.md#cta) from the SQL dump, then Discogs tracklists from `compilation_track_artists.json`), the [FK chain](glossary.md#fk-chain), exact name match, [normalized](glossary.md#normalization) match (via `wxyc_etl.text.to_identity_match_form`, with an `&` → `and` shim applied first), fuzzy match ([Jaro-Winkler](glossary.md#jaro-winkler)), Discogs search, and a raw fallback so no play is dropped. Uses `wxyc_etl.text.split_artist_name` to split multi-artist credits and `wxyc_etl.text.is_compilation_artist` to detect [Various-Artists entries](glossary.md#compilation-artist-va). |
| `semantic_index/adjacency.py` | Extracts consecutive artist pairs within each radio show. |
| `semantic_index/pmi.py` | Computes [PMI](glossary.md#pmi) for artist co-occurrences. |
| `semantic_index/node_attributes.py` | Computes per-artist statistics: active years, per-DJ play spread, request ratio. |
| `semantic_index/cross_reference.py` | Extracts curated [cross-reference](glossary.md#cross-reference) edges from the catalog's cross-reference tables. |
| `semantic_index/discogs_client.py` | Two-tier Discogs client: reads the discogs-cache PostgreSQL first, falls back to the [LML](glossary.md#lml) API for artists the cache misses. All table names come from `wxyc_etl.schema` constants. |
| `semantic_index/wikidata_client.py` | Wikidata SPARQL client: influence relationships (P737), label hierarchy (P749/P355), and label-to-QID bridging by Discogs label ID. (Identity-resolution lookups — Discogs artist ID, name search, streaming IDs — now live in LML.) |
| `semantic_index/pipeline_db.py` | Manages the persistent pipeline SQLite database: schema creation and migration, artist upserts (COALESCE semantics so re-runs don't clobber known values), bulk stats updates, style persistence, and [entity deduplication](glossary.md#entity-deduplication) by shared Wikidata QID. |
| `semantic_index/label_store.py` | Label CRUD (`get_or_create_label`, `update_label_qid`, `insert_label_hierarchy`) used by `label_hierarchy.py`. |
| `semantic_index/lml_identity.py` | Imports pre-resolved identities from LML's `entity.identity` PostgreSQL table into the local pipeline database (`--entity-source=lml`). |
| `semantic_index/wikidata_influence.py` | Builds directed Wikidata P737 ("influenced by") edges between reconciled artists, resolving QIDs back to canonical names via the pipeline database. |
| `semantic_index/label_hierarchy.py` | Populates the `label` and `label_hierarchy` tables from Wikidata parent-organization/subsidiary relationships (P749/P355), finding each label's QID via its Discogs label ID. |
| `semantic_index/discogs_enrichment.py` | Aggregates Discogs metadata (styles, personnel, labels, compilations) per artist. |
| `semantic_index/discogs_edges.py` | Computes the Discogs-derived edge types: shared personnel, shared style ([Jaccard](glossary.md#jaccard-similarity)), label family, compilation co-appearance. Per-artist top-K pruning for `shared_personnel` and `label_family` delegates to `edge_prune`. |
| `semantic_index/edge_prune.py` | Shared top-K-per-artist prune for symmetric `(artist_a_id, artist_b_id)` edge tables — keeps each artist's strongest K edges so dense edge types don't balloon. Backs `prune_acoustic_similarity` (in `acousticbrainz.py`), `prune_shared_personnel`, and `prune_label_family` (in `discogs_edges.py`). |
| `semantic_index/acousticbrainz.py` | Loads AcousticBrainz high-level features, aggregates them into per-artist [audio profiles](glossary.md#audio-profile) (59-dimension vector across 18 classifiers), and computes cosine-similarity edges. Supports both PostgreSQL and tar-based loading. |
| `semantic_index/acousticbrainz_client.py` | PostgreSQL client for AcousticBrainz features: queries `ab_recording` in musicbrainz-cache, joined with `mb_artist_recording` for per-artist retrieval. Preferred over the tar loader — one JOIN replaces a two-step flow. |
| `semantic_index/musicbrainz_client.py` | musicbrainz-cache client: resolves recording [MBIDs](glossary.md#musicbrainz) via the `mb_artist_recording` materialized view. (Name-based identity lookups now live in LML.) |
| `semantic_index/graph_metrics.py` | Computes and persists [Louvain communities](glossary.md#louvain-community), [betweenness centrality](glossary.md#betweenness-centrality), and [PageRank](glossary.md#pagerank). Filters compilation entries via `is_compilation_artist`. Idempotent post-processing step — runnable standalone or as a pipeline step. |
| `semantic_index/graph_export.py` | Builds the NetworkX graph and exports [GEXF](glossary.md#gexf). |
| `semantic_index/sqlite_export.py` | Builds and exports the SQLite graph database with enrichment and edge tables. Optionally integrates with the pipeline DB so artist identities persist across runs. |
| `semantic_index/facet_export.py` | Exports play-level data plus pre-aggregated tables (`dj`, `play`, `artist_month_count`, `artist_dj_count`, `month_total`, `dj_total`) so the API can compute [faceted](glossary.md#facet) PMI on demand. |
| `semantic_index/api/app.py` | FastAPI application factory: takes a SQLite database path, returns a configured app. |
| `semantic_index/api/database.py` | Request-scoped SQLite connection dependency for FastAPI. |
| `semantic_index/api/schemas.py` | Pydantic response models for the Graph API (ArtistSummary, ArtistDetail, EntityArtists, SearchResponse, NeighborsResponse, ExplainResponse, FacetsResponse, DjSummary, NarrativeResponse, CommunitiesResponse, DiscoveryResponse, PreviewResponse). |
| `semantic_index/api/routes.py` | The query endpoints: search, artist detail, neighbors by edge type (with optional month/DJ facet filters), explain relationships, entity alias groups, available facets, community metadata, discovery (underplayed sonic fits). |
| `semantic_index/api/narrative.py` | The LLM narrative endpoint: calls Claude Haiku to explain an artist relationship in plain English, grounded in the pair's graph data. Caches results in a [sidecar](glossary.md#sidecar-database) SQLite database. Facet-aware; enriches prompts with audio-profile features (genre, mood, danceability) when available. |
| `semantic_index/narrative_audit.py` | Periodic claim-ratio audit of cached narratives: samples N of them, reconstructs each pair's source/target metadata from the production DB (so the verifier sees what the live endpoint scored against), has a Haiku verifier prompt decompose each narrative into grounded vs ungrounded claims, and records the ratios to a sidecar audit DB. Catches structural hallucinations the always-on token-match gate can miss — see [docs/graph-api.md](graph-api.md). |
| `semantic_index/api/narrative_audit_routes.py` | Read-only endpoint exposing the most-recent audit rows at `/graph/narrative-audit/recent`. |
| `scripts/audit_narratives.py` | CLI entry point for the claim-ratio audit: sample-and-score with a configurable threshold, writing to the audit sidecar DB. |
| `semantic_index/labeling_app/` | Standalone FastAPI single-page web UI for hand-labeling narrative eval-set rows. Reads `labeling.jsonl`, persists labels to a SQLite sidecar (`<jsonl>.labels.db`) keyed by labeler name, exports merge_labels-compatible CSV. Run with `python -m semantic_index.labeling_app --jsonl output/eval/labeling.jsonl`. |
| `semantic_index/api/preview.py` | Audio-preview URL endpoint with multi-source fallback (iTunes lookup, Spotify, Bandcamp, Deezer, iTunes search). Caches results in a sidecar SQLite database. Powers the in-card transition player in the graph explorer. |
| `semantic_index/nightly_sync.py` | [Nightly sync](glossary.md#nightly-sync) orchestrator: query PG → resolve → PMI → stats → export → entity dedup → facets → graph metrics → [atomic swap](glossary.md#atomic-swap). Preserves the enrichment tables already in the existing database. |
| `run_pipeline.py` | CLI entry point wiring the full pipeline (SQL-dump mode). |
| `scripts/nightly_sync.py` | CLI wrapper for `semantic_index.nightly_sync.main()`. |
| `semantic_index/archive_client.py` | S3 client for the [WXYC audio archive](glossary.md#wxyc-audio-archive): downloads hourly MP3s from the `wxyc-archive` bucket, decodes to PCM WAV via ffmpeg, extracts segments at given offsets. Computes S3 keys from timestamps (`YYYY/MM/DD/YYYYMMDDHH00.mp3`). |
| `semantic_index/archive_essentia.py` | Essentia TF audio classification: runs [VGGish](glossary.md#vggish) embeddings through 15 [classification heads](glossary.md#classification-head) (genre, mood, danceability, voice/instrumental, tonal, gender, MIREX) to produce per-segment features compatible with the 59-dimension RecordingFeatures layout. Three AcousticBrainz classifiers lack VGGish heads and are zero-filled (ismir04_rhythm, genre_electronic, timbre). |
| `semantic_index/archive_match.py` | Resolves archive artist names to production `artist.id` rows, folding across case, diacritics, HTML entities, nickname quoting, brackets, "the" prefix, `&` ↔ `and`, and multi-artist credits. Skips compilation/VA entries. Refuses ambiguous normalized forms rather than guessing. Used by the aggregation step in `scripts/process_archive.py`. |
| `scripts/process_archive.py` | CLI entry point for archive audio processing: queries Backend-Service PG for flowsheet entries, groups them by archive hour, downloads from S3, classifies segments via Essentia TF, aggregates per-artist profiles via `ArchiveNameMatcher`, writes to the `audio_profile` table. Per-hour [checkpointing](glossary.md#checkpoint); `--date-range`, `--max-hours`, `--aggregate-only`, `--retry-failed`, `--dry-run`. |
| `scripts/import_acousticbrainz.py` | ETL script: imports AcousticBrainz high-level features from tar archives into the PostgreSQL `ab_recording` table. Per-tar checkpointing, resilient to flaky network storage, idempotent via `ON CONFLICT DO NOTHING`. |
| `scripts/recover_audio_profiles.py` | Recovery ETL: restores audio profiles for artists with MusicBrainz GIDs. Resolves GID → integer ID via PostgreSQL `mb_artist`, fetches AcousticBrainz features, builds 59-dimension profiles, recomputes acoustic similarity. Atomic swap, dry-run support. |

## Column Mappings (0-indexed from SQL INSERT order)

Only relevant when working on the SQL-dump ingest path: the parser reads each table's INSERT tuples positionally, so this table records which 0-indexed positions carry the columns the pipeline uses.

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

The schema of the exported (and production-serving) SQLite database. In reading order: the core graph (`artist` plus the edge tables `dj_transition`, `cross_reference`, `wikidata_influence`), the audio tables (`audio_profile`, `acoustic_similarity`), graph-metrics output (`community`, plus the nullable metric columns on `artist`), the entity store (`entity`, `artist_style`, `reconciliation_log`), and the facet tables that power on-demand filtered PMI.

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
    show_count INTEGER NOT NULL DEFAULT 0,
    -- Backend library-artist id (wxyc_schema.artists.id). Populated by nightly
    -- sync (PG mode) for unambiguous catalog names; NULL for raw/CTA-only and
    -- homonym names. Backs the neighbors-by-library-id endpoint (WXYC/semantic-index#358).
    -- A UNIQUE partial index (idx_artist_library_code, WHERE ... IS NOT NULL)
    -- enforces the injective code->artist mapping the endpoint assumes, so a
    -- writer bug fails the rebuild loudly instead of mis-mapping (WXYC/semantic-index#365).
    wxyc_library_code_id INTEGER
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

The pipeline parses tubafrenzy MySQL dump files directly, so no database server is required for SQL-dump mode. Production dumps are not committed to git — pass the path as a CLI argument. The fixture dump at `tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql` has minimal data suitable for structural testing only. The `data/` directory contains a committed copy of the latest pipeline output, used for deployment.
