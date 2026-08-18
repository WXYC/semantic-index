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
- `compilation_track_artist` → the Tier 0a CTA index (`library_id`, `artist_name`, `track_title`)

### Compilation-track resolution and the two id spaces (nightly-sync only)

Tier 0a resolves a Various-Artists entry to the artist actually credited for that track, and it only fires when the index key and the probe value are in the same id space. On this path both come from Backend: `compilation_track_artist.library_id` and `flowsheet.album_id` are each a foreign key to `library.id`, so the index is built straight from PG with no translation (WXYC/semantic-index#375).

That equivalence is worth stating because `library` carries a *second* identifier — `legacy_release_id`, the tubafrenzy `LIBRARY_RELEASE.ID` — whose values overlap the serials numerically without meaning the same thing. An index keyed in one space and probed from the other does not raise. Mostly it misses, and the entry quietly falls through to name resolution as "Various Artists"; in the tail where a numeric collision meets an identical normalized track title, it attributes the play to the wrong artist outright. Both outcomes are silent, which is why the build logs how much of the index overlaps the catalog's id space, and why the invariant is pinned by tests rather than left to convention.

**Tier 0b is not wired here yet.** Its only source is `compilation_track_artists.json`, whose `comp_id` is a legacy `LIBRARY_RELEASE_ID`, and this entry point takes no input *data* files — only a DSN and the SQLite path. Wiring it needs two things: a file input, and a legacy→serial bridge through `library.legacy_release_id` (NOT NULL and unique, one column away from the query that already loads `library`). Neither is hard; neither is done. Until then Tier 0b runs only in the SQL-dump pipeline, which is internally consistent in the legacy space — and whose input disappears when tubafrenzy is decommissioned, so this is a gap with a deadline rather than a permanent split.

### Library-code mapping post-pass (nightly-sync only)

After entity dedup, nightly sync populates `artist.wxyc_library_code_id` — the graph-artist ↔ Backend library-artist-id mapping the neighbors-by-library-id endpoint reads (WXYC/semantic-index#354, On Tour R3b). It is a **name-equality post-pass**, not resolver plumbing: catalog-resolved canonical names are verbatim `LIBRARY_CODE` presentation names, so an exact-name join over the loaded `codes` reproduces the resolver's collapse without touching `ResolvedEntry` or the SQL-dump path. `build_library_code_map` builds the `{presentation_name → id}` dict and `PipelineDB.map_library_code_ids` applies it, recomputing the column from scratch each run (a name that became ambiguous since the last sync drops back to NULL rather than retaining a stale id from the copied-forward DB). The mapping is injective — each code claimed by at most one graph artist — and the partial index `idx_artist_library_code` is **UNIQUE**, so a regression that assigned one code to two artists fails the rebuild loudly at write time rather than silently mis-mapping the neighbors-by-library-id endpoint (WXYC/semantic-index#365).

Two rules make the mapping trustworthy:

- **Case-sensitive exact match** on `canonical_name = artists.artist_name` (no lower/trim). Exact equality captures every catalog-resolved artist by construction and keeps the mapping **injective** (`canonical_name` is UNIQUE, so no two graph artists claim one code id). A lowered join would let a Discogs-cased `BOLA` node and catalog `Bola` collide onto one code.
- **Ambiguity exclusion.** A name borne by ≥2 catalog codes stays **NULL**. These are usually [homonyms](glossary.md#homonym) — distinct artists sharing a name (e.g. several bands filed as "Lake") — that the resolver's first-wins name index already conflates into one graph node, so no code id is correct. This is deliberate, not a bug to "fix" into a pick-one rule; splitting the conflated nodes is a separate track (WXYC/semantic-index#360). Measured impact: ~1% of catalog names, but ~22K artists still map, covering 974 of the top 1,000 by plays.

SQL-dump mode leaves the column NULL (present, unpopulated): tubafrenzy `LIBRARY_CODE.ID`s are probably the same id-space but that invariant is unverified, and dump builds don't serve production. The pre-swap [coverage gate](deployment.md#pre-swap-validation) fail-closes if a build's mapped-artist count collapses.
