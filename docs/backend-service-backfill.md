# Backend-Service PG Data Gaps for Nightly Sync

The nightly sync pipeline (`semantic_index/nightly_sync.py`) queries Backend-Service PostgreSQL (`wxyc_schema.*`) instead of parsing a SQL dump. Three columns/tables are empty despite the data existing in the original tubafrenzy MySQL source. These gaps degrade the quality of the artist graph.

## 1. `flowsheet.album_id` — FK to library releases

**Impact:** Without this FK, the resolver can't use Tier 1 (FK chain) resolution: `flowsheet.album_id → library.id → library.artist_id → artists.id → canonical name`. All 1.95M entries fall through to name-based matching (Tiers 2-4), which produces more fragmented artist identities. The production graph drops from 81K to 69K DJ transition edges because name variants that the FK chain would have canonicalized instead become separate "raw" entries.

**Current state:** `flowsheet.album_id` is NULL for all 1,954,255 track entries. The `library` table has 64,123 releases with valid `artist_id` FKs. The tubafrenzy source has this relationship as `FLOWSHEET_ENTRY_PROD.LIBRARY_RELEASE_ID` (column index 6).

**What needs to happen:** For each flowsheet entry, match it to the correct `library.id` row. The tubafrenzy bulk load created both tables but didn't establish the FK. The most reliable approach is to match on the original tubafrenzy IDs — the `flowsheet.legacy_entry_id` column maps to the original `FLOWSHEET_ENTRY_PROD.ID`, and the `library` table would need a similar legacy ID mapping (currently `library.legacy_release_id` exists but is NULL for all rows).

If legacy IDs aren't available for a direct mapping, a heuristic join on `(flowsheet.artist_name, flowsheet.album_title) → (artists.artist_name via library.artist_id, library.album_title)` could work, but may have ambiguity for common album names.

**Permanent fix:** Beyond the one-time backfill, the Backend-Service application code should set `album_id` when DJs log tracks on the flowsheet. If a DJ selects a release from the library catalog, that FK should be written to the flowsheet row.

## 2. `shows.primary_dj_id` — DJ assignment per show

**Impact:** Without DJ data, the nightly sync produces a graph with no DJ facets. Users can't filter transitions by DJ or see DJ play counts. The facet tables (`dj`, `artist_dj_count`, `dj_total`) are empty.

**Current state:** `shows.primary_dj_id` is NULL for all 71,629 shows. The `show_djs` junction table (for multi-DJ shows) is also empty. There is no `djs` table in the schema. The tubafrenzy source has `FLOWSHEET_RADIO_SHOW_PROD.DJ_ID` (column 3) and `DJ_NAME` (column 2) per show.

**What needs to happen:** The Backend-Service needs a `djs` table (or equivalent) and the show-to-DJ relationship populated. The tubafrenzy data has integer DJ IDs and string DJ names. The `shows.primary_dj_id` column is varchar, suggesting it may reference a user/auth ID rather than a legacy integer ID — the mapping strategy depends on how the Backend-Service models DJs (as `djs` table rows vs. auth users).

**Permanent fix:** When a DJ starts a show via the flowsheet UI, their user/DJ ID should be written to `shows.primary_dj_id` (and `show_djs` for multi-DJ shows). The Backend-Service auth system (better-auth) already knows who's logged in.

## 3. `artist_crossreference` and `artist_library_crossreference` — catalog cross-references

**Impact:** The production graph has 135 cross-reference edges (e.g., "see also: Yo La Tengo" on a catalog entry for Ira Kaplan). These are curated by music directors and encode relationships that PMI can't discover. With the PG tables empty, the nightly sync output has 0 cross-reference edges.

**Current state:** Both `wxyc_schema.artist_crossreference` (0 rows) and `wxyc_schema.artist_library_crossreference` (0 rows) are empty. The tubafrenzy source has `LIBRARY_CODE_CROSS_REFERENCE` and `RELEASE_CROSS_REFERENCE` tables with this data.

**What needs to happen:** Import the cross-reference rows from the tubafrenzy dump into the PG tables. The mapping is:
- `LIBRARY_CODE_CROSS_REFERENCE(CROSS_REFERENCING_ARTIST_ID, CROSS_REFERENCED_LIBRARY_CODE_ID, COMMENT)` → `artist_crossreference(source_artist_id, target_artist_id, comment)` — both FK to `artists.id`
- `RELEASE_CROSS_REFERENCE(CROSS_REFERENCING_ARTIST_ID, CROSS_REFERENCED_RELEASE_ID, COMMENT)` → `artist_library_crossreference(artist_id, library_id, comment)` — artist FK to `artists.id`, library FK to `library.id`

**Permanent fix:** The dj-site card catalog UI should support adding/editing cross-references, writing them to these PG tables. This is a feature that music directors use when cataloging new additions.

## PG Schema Reference

```sql
-- wxyc_schema.flowsheet (relevant columns)
album_id        INTEGER  -- FK to library.id, currently NULL for all rows
legacy_entry_id INTEGER  -- maps to tubafrenzy FLOWSHEET_ENTRY_PROD.ID

-- wxyc_schema.library (relevant columns)
id              INTEGER  -- PK
artist_id       INTEGER  -- FK to artists.id
legacy_release_id INTEGER -- maps to tubafrenzy LIBRARY_RELEASE.ID, currently NULL

-- wxyc_schema.shows (relevant columns)
id              INTEGER  -- PK
primary_dj_id   VARCHAR  -- FK to djs or auth user, currently NULL
legacy_show_id  INTEGER  -- maps to tubafrenzy FLOWSHEET_RADIO_SHOW_PROD.ID

-- wxyc_schema.artist_crossreference (0 rows)
source_artist_id INTEGER -- FK to artists.id
target_artist_id INTEGER -- FK to artists.id
comment          VARCHAR

-- wxyc_schema.artist_library_crossreference (0 rows)
artist_id        INTEGER -- FK to artists.id
library_id       INTEGER -- FK to library.id
comment          VARCHAR
```
