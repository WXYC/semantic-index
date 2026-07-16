# Pipeline Usage

The pipeline runs in three modes, differing in where input comes from and whether artist identities persist between runs:

1. **SQL-dump mode** — parse a [tubafrenzy](glossary.md#tubafrenzy) MySQL dump and build the graph from scratch. Good for local experiments and full historical rebuilds; needs no database server.
2. **Pipeline-DB mode** — SQL-dump mode plus a persistent SQLite database (`--db-path`), so resolved artist identities carry over between runs instead of being recreated each time.
3. **Nightly sync mode** — query [Backend-Service](glossary.md#backend-service) PostgreSQL directly (no dump), recompute the core graph, and preserve existing enrichment data. This is the production path.

## SQL-dump mode

```bash
python run_pipeline.py /path/to/wxycmusic.sql [--output-dir output/] [--min-count 2]
```

Output: `output/wxyc_artist_pmi.gexf` (a [GEXF](glossary.md#gexf) graph for Gephi) + `output/wxyc_artist_graph.db` (the SQLite database).

Use `--no-sqlite` to skip the SQLite export.

## Pipeline-DB mode

Pass `--db-path` to enable the pipeline database: artists are managed with persistent identity resolution rather than created fresh on each run, and the pipeline database becomes the SQLite output.

```bash
python run_pipeline.py dump.sql --db-path output/wxyc_artist_graph.db --entity-source lml --discogs-cache-dsn postgresql://...
```

- `--db-path PATH` — Path to the pipeline SQLite database. Created if it doesn't exist.
- `--entity-source {local,lml}` — Where artist identity comes from; see the decision guide below.
- `--compilation-track-artist-dump PATH` — Path to a SQL dump containing the [`COMPILATION_TRACK_ARTIST`](glossary.md#cta) table. When provided, [Various-Artists entries](glossary.md#compilation-artist-va) are resolved to per-track artists (resolution Tier 0) before the FK chain is tried.
- `--discogs-track-json PATH` — Path to `compilation_track_artists.json` (produced by LML's `match_compilations.py`). A Discogs-tracklist fallback (Tier 0b) for Various-Artists entries the CTA table doesn't cover. JSON format: `[{comp_id, discogs_release_id, tracks: [{position, title, artists: [str]}]}]` where `comp_id` = WXYC `LIBRARY_RELEASE_ID`.
- `--compute-discogs-edges` — Compute the Discogs-derived edge types (shared personnel, styles, labels, compilations). Off by default.
- `--compute-wikidata-influences` — Query Wikidata for P737 ("influenced by") relationships and create directed influence edges. Requires `--db-path` with reconciled Wikidata QIDs.
- `--populate-label-hierarchy` — Populate the `label` and `label_hierarchy` tables from Wikidata parent-organization/subsidiary relationships (P749/P355). Requires `--db-path` and enrichment data.
- `--musicbrainz-cache-dsn` — When set (without `--acousticbrainz-dir`), audio features come from the PostgreSQL `ab_recording` table: a single JOIN query, the preferred path. Requires `import_acousticbrainz.py` to have populated `ab_recording` first (see [docs/audio-ingest.md](audio-ingest.md)).
- `--acousticbrainz-dir` — **(Deprecated)** Path to AcousticBrainz tar archives. Requires `--musicbrainz-cache-dsn`. When both are set, the PostgreSQL path wins and the tar directory is ignored.

### Choosing `--entity-source`

The flag decides who resolves artist identities:

- **`local`** — the pipeline resolves identities itself using only its own SQLite database. No LML involvement. This is the default in most flag combinations, and the way to proceed when LML's PostgreSQL is down.
- **`lml`** — import pre-resolved identities from [LML](glossary.md#lml)'s `entity.identity` PostgreSQL table before the run. Requires **both** `--discogs-cache-dsn` (the PostgreSQL connection that table lives in) and `--db-path` (the destination for the imported identities). An early validator raises `LmlEntitySourceError` if either is missing or the PostgreSQL is unreachable, so a broken import can't fail silently.
- **When both `--db-path` and `--discogs-cache-dsn` are set, `--entity-source` is required** — the pipeline refuses to start without an explicit choice. (Why: that flag combination used to pick a behavior implicitly, and the implicit behavior flipped across PR [#184](https://github.com/WXYC/semantic-index/pull/184) — from silently importing LML identities to silently skipping them — so operators must now say which they mean: `lml` preserves the old behavior, `local` skips LML.)

## Nightly sync mode

The nightly sync queries Backend-Service PostgreSQL directly instead of parsing a SQL dump. It recomputes the core graph (resolution, PMI, stats, facets, graph metrics) while preserving enrichment data (Discogs, Wikidata, AcousticBrainz) from the existing production database, finishing with an [atomic](glossary.md#atomic-swap) copy-and-swap. How this runs in production — on a nightly Fargate task rather than in the API process — is covered in [docs/deployment.md](deployment.md).

```bash
python scripts/nightly_sync.py --dsn postgresql://... --db-path data/wxyc_artist_graph.db
```

Or via environment variables:

```bash
DATABASE_URL_BACKEND=postgresql://... DB_PATH=data/wxyc_artist_graph.db python scripts/nightly_sync.py
```

- `--dsn` / `DATABASE_URL_BACKEND` — PostgreSQL [DSN](glossary.md#dsn) for Backend-Service (required).
- `--db-path` / `DB_PATH` — Production SQLite database path (default: `data/wxyc_artist_graph.db`).
- `--min-count` / `MIN_COUNT` — Minimum co-occurrence count for DJ transition edges (default: 2).
- `--dry-run` — Run the full pipeline but skip the atomic swap (writes to a temp file instead).
- `--verbose` — Enable debug logging.

**PG schema mappings.** The Backend-Service tables map onto the same pipeline types the dump parser produces (`wxyc_schema.*` → pipeline types):

- `artists` → `LibraryCode` (id, genre_id from `genre_artist_crossreference`, artist_name → presentation_name)
- `library` → `LibraryRelease` (id, artist_id → library_code_id)
- `flowsheet` → `FlowsheetEntry` (filtered to `entry_type = 'track'`, `add_time` → epoch, `request_flag` boolean → int)
- `shows` → show-to-DJ mapping (keyed by `shows.id`, `primary_dj_id` as value)
