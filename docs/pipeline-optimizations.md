# Pipeline Performance Optimizations

The semantic-index pipeline processes 22 years of WXYC flowsheet data (~2.6M entries, 408MB SQL dump) and enriches ~144K unique artists with Discogs metadata. This document describes the performance bottlenecks encountered and the optimizations applied.

## Summary

| Component | Before | After | Speedup | Optimization |
|-----------|--------|-------|---------|-------------|
| SQL parsing | 40 min | 2 sec | 1,200x | Rust PyO3 parser |
| Artist resolution | 38 min | 2 min | 19x | Batch C scoring + result cache |
| Discogs reconciliation | ~24 hrs | 32 min | 45x | Bulk SQL queries + materialized tables |
| **Total pipeline** | **~25 hrs** | **~37 min** | **~40x** | |

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
