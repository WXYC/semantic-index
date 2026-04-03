# Pipeline Performance Optimizations

The semantic-index pipeline processes 22 years of WXYC flowsheet data (~2.6M entries, 408MB SQL dump) and enriches ~144K unique artists with Discogs metadata. This document describes the performance bottlenecks encountered and the optimizations applied.

## Summary

| Component | Before | After | Speedup | Optimization |
|-----------|--------|-------|---------|-------------|
| SQL parsing | 40 min | 2 sec | 1,200x | Rust PyO3 parser |
| Artist resolution | 38 min | 2 min | 19x | Batch C scoring + result cache |
| Discogs reconciliation | ~24 hrs | 39 sec | 2,215x | Bulk SQL queries + materialized summary tables + indexes |
| Discogs enrichment | 25 min | ~3 min | 8x | Summary table enrichment (no release_id joins) |
| Cached reruns (parse+resolve) | 4 min | 7 sec | 34x | Pickle cache keyed by dump file size+mtime |
| **Total pipeline (first run)** | **~25 hrs** | **~14 min** | **~107x** | |
| **Total pipeline (cached rerun)** | **~25 hrs** | **~12 min** | **~130x** | |

## 1. Rust SQL Parser

**Bottleneck**: The pure-Python SQL parser (`sql_parser.py`) used a character-by-character state machine to parse MySQL INSERT statements. Each of the ~400 INSERT lines in the production dump is approximately 1MB of text. Walking every character in Python — 408MB total — took 40 minutes.

**Optimization**: Replaced the inner parsing loop with a Rust extension module (`rust/sql-parser/`) compiled via PyO3. The Rust parser memory-maps the entire dump file (zero-copy access via `memmap2`), scans for table-specific INSERT lines using `memchr`, and parses VALUES tuples with a Rust state machine. Results are returned as Python tuples via PyO3's type conversion.

**Why it's fast**: Memory-mapping eliminates file I/O overhead. The Rust character-by-character parser runs ~100x faster than Python for the same algorithm due to native code execution. Combined with zero-copy access (no intermediate string allocation), the total speedup is ~1,200x.

**Fallback**: The pure-Python parser remains in `sql_parser.py` and is used automatically when the Rust extension isn't built (e.g., in CI or on systems without a Rust toolchain). The Python module detects the Rust extension at import time.

**Files**: `rust/sql-parser/src/lib.rs`, `semantic_index/sql_parser.py`

## 2. Batch Fuzzy Scoring + Result Cache

**Bottleneck**: The artist resolver's fuzzy matching (Jaro-Winkler similarity) scored each unresolved artist name against all 24,356 catalog candidates. While `rapidfuzz` uses C-accelerated scoring, the Python loop calling it 24K times per entry was slow. With ~1.2M unresolved entries (after FK chain and exact match), this meant ~29 billion scoring operations done one at a time from Python.

**Optimization**: Two changes:

1. **`rapidfuzz.process.extract`**: Replaced the manual Python loop with rapidfuzz's batch API. `process.extract(query, choices, scorer=JaroWinkler.similarity)` runs the entire 24K-candidate scoring in C, returning only the top matches. This eliminates ~24K Python→C round-trips per entry.

2. **Result cache**: Added a dict mapping `(query_string, threshold) → result` to `_fuzzy_match()`. Since 2.6M flowsheet entries share only ~144K unique artist names, the same fuzzy query is repeated an average of 18 times. The cache ensures each unique name is scored only once.

**Why it's fast**: The batch API eliminates Python loop overhead (C processes all candidates in a single call). The cache reduces 2.6M fuzzy operations to 144K. Together: 19x speedup.

**Files**: `semantic_index/artist_resolver.py`

## 3. Bulk Enrichment Queries + Materialized Summary Tables

**Bottleneck**: The original Discogs enrichment called `DiscogsClient.get_release(id)` for each of an artist's releases, running 5 SQL queries per release (release header, artists, labels, tracks, track artists). For an artist like Autechre with 698 releases, that's 3,490 individual PostgreSQL queries. Across 144K artists, this produced hundreds of thousands of queries taking ~24 hours.

**Optimization**: Three changes:

1. **Bulk `ANY()` queries**: Replaced per-artist queries with batched queries using PostgreSQL's `ANY()` operator. Instead of 144K individual lookups, the pipeline processes 1000 artist names per batch with a single `SELECT ... WHERE lower(artist_name) = ANY($1)` query. 144 batches × 5 queries = 720 total queries (vs 864K before).

2. **Materialized summary tables**: Precomputed the expensive joins into flat lookup tables in PostgreSQL:
   - `artist_style_summary`: Pre-joined `release_artist × release_style` (6.8M rows) — eliminates a 78M × 28M row join at query time
   - `artist_discogs_id`: Pre-aggregated `release_artist` to a simple name→ID mapping (2.8M rows)
   - `artist_label_summary`, `artist_personnel_summary`: Same pattern for labels and credits

   With the summary tables, style lookups dropped from 5.8 seconds to 0.01 seconds per batch (580x).

3. **PostgreSQL indexes**: Added `CREATE INDEX CONCURRENTLY` on `lower(artist_name)` for `release_artist` and `release_track_artist`. Without the index, each batch query did a sequential scan of 78M+ rows (~3.7 seconds). With the index: ~5ms.

**Why it's fast**: Bulk queries amortize connection overhead across 1000 names. Materialized tables eliminate runtime joins. Indexes eliminate sequential scans. The combination reduces enrichment from ~24 hours to ~32 minutes.

**Files**: `semantic_index/discogs_client.py`, `semantic_index/discogs_enrichment.py`, `semantic_index/reconciliation.py`

## 4. Resolved Entries Cache

**Bottleneck**: Even with the Rust parser (2 seconds) and batch fuzzy scoring (2 minutes), the parse+resolve phase takes ~4 minutes. During iterative development (testing different enrichment settings, thresholds, export options), re-parsing the same dump file every run wastes time.

**Optimization**: Added `--cache-dir` flag to `run_pipeline.py`. After the first run, the resolved entries (plus all supporting data: genre names, codes, releases, show-to-DJ mapping, entry counts) are serialized to a pickle file keyed by the dump file's size and modification time. Subsequent runs load the cache in <1 second, skipping the entire parse+resolve phase.

**Why it's fast**: Pickle deserialization of pre-resolved Python objects is orders of magnitude faster than re-parsing SQL text + re-running fuzzy matching. The cache key (file size + mtime) ensures stale caches are automatically invalidated when the dump file changes.

**Files**: `run_pipeline.py`

## 5. Materialized Summary Tables

**Bottleneck**: Even with bulk `ANY()` queries, the Discogs reconciliation spent most of its time on joins. Each batch of 1000 names triggered a join between `release_artist` (78M rows) and `release_style` (28M rows) to fetch styles. At 5.8 seconds per batch × 144 batches = 14 minutes just for style queries.

**Optimization**: Precomputed the expensive joins into flat lookup tables in PostgreSQL:

```sql
CREATE TABLE artist_style_summary AS
SELECT DISTINCT lower(ra.artist_name) AS artist_name, rs.style
FROM release_artist ra JOIN release_style rs ON ra.release_id = rs.release_id
WHERE ra.extra = 0;
-- 6.8M rows, ~10 minutes to build once

CREATE TABLE artist_discogs_id AS
SELECT DISTINCT lower(artist_name) AS artist_name, artist_id AS discogs_artist_id
FROM release_artist WHERE extra = 0;
-- 2.8M rows
```

The reconciler queries these flat tables with `WHERE artist_name = ANY(...)` — no join at all. Style lookups: 5.8s → 0.01s per batch (580x). Total reconciliation: 32 minutes → 49 seconds.

**Why it's fast**: The summary tables are precomputed once (during initial Discogs cache setup) and eliminate all runtime joins. The indexed `ANY()` lookups on a 6.8M-row flat table are orders of magnitude faster than joining 78M × 28M rows.

**Files**: PostgreSQL materialized tables (created via `psql`), `semantic_index/reconciliation.py` (falls back to join if summary tables don't exist)

## 6. PostgreSQL Indexes

**Bottleneck**: The discogs-cache PostgreSQL tables (`release_artist`, `release_track_artist`, `artist_alias`, `artist_member`) had no indexes on the columns used for lookups. Each `ANY()` query did a sequential scan of tens of millions of rows.

**Optimization**: Created expression indexes matching the exact query predicates:

```sql
CREATE INDEX CONCURRENTLY idx_ra_lower_name ON release_artist (lower(artist_name)) WHERE extra = 0;
CREATE INDEX CONCURRENTLY idx_rta_lower_name ON release_track_artist (lower(artist_name));
CREATE INDEX CONCURRENTLY idx_artist_alias_name ON artist_alias (lower(alias_name));
CREATE INDEX CONCURRENTLY idx_artist_member_name ON artist_member (lower(member_name));
CREATE INDEX CONCURRENTLY idx_artist_member_group ON artist_member (lower(group_name));
```

`CONCURRENTLY` avoids blocking running queries. Each index takes 1-5 minutes to build on tens of millions of rows but only needs to be created once.

**Why it's fast**: Indexed lookups are O(log n) instead of O(n). For `release_artist` (78M rows): 3.7 seconds → 5ms per query (740x).

**Files**: PostgreSQL indexes (created via `psql`)

## 7. Discogs XML Converter Extensions

**Bottleneck**: The Discogs cache had no `release_style`, `release_genre`, or `release_company` tables. Styles (critical for the semantic index) could only be fetched from the library-metadata-lookup API at 50 req/min — making style enrichment take hours.

**Optimization**: Extended the Rust discogs-xml-converter to parse `<genres>`, `<styles>`, and `<companies>` elements from the Discogs XML dump and stream them directly into PostgreSQL via COPY. Added three new tables with ~25M, ~29M, and ~34M rows respectively.

**Why it's fast**: The Rust converter processes 57GB of XML in ~100 minutes using streaming XML parsing (quick-xml) and PostgreSQL COPY for batch insertion. Once the data is in PostgreSQL, style lookups are instant via the materialized summary tables.

**Files**: `discogs-xml-converter/src/model.rs`, `discogs-xml-converter/src/parser.rs`, `discogs-xml-converter/src/pg_output.rs`

## 8. Wikidata Rate Limit Handling

**Bottleneck**: The Wikidata SPARQL endpoint at `query.wikidata.org` returns HTTP 403 ("Too many requests") when queries arrive faster than ~1 per second. The per-artist name search made one SPARQL filter call per candidate batch, triggering rate limits and losing matches.

**Optimization**: Two changes:

1. **Exponential backoff**: SPARQL queries retry up to 3 times with 2s, 4s, 8s delays on 403/429 responses. After max retries, returns empty result instead of raising.

2. **Batch musician filter**: Added `search_musicians_batch()` that collects all candidates from multiple name searches first, then runs a single SPARQL filter query for all candidates instead of one per name. Reduces SPARQL calls from N (one per name search result) to 1.

**Files**: `semantic_index/wikidata_client.py`
