# Graph API

A read-only FastAPI service over the SQLite [graph database](glossary.md#graph-database) the pipeline produces. It serves two things: the D3.js graph explorer (the interactive visualization) at the root URL, and the JSON API under `/graph/*` that the explorer — and anything else — queries. In production this is what runs at [explore.wxyc.org](https://explore.wxyc.org); see [docs/deployment.md](deployment.md).

One pattern to know up front: the API never writes to the graph database, but several endpoints cache expensive results (LLM narratives, audio-preview URLs, audit history) in [sidecar databases](glossary.md#sidecar-database) — small SQLite files next to the main one, so a nightly graph rebuild doesn't destroy them.

```bash
python -m semantic_index.api
```

Or programmatically:

```python
from semantic_index.api.app import create_app
app = create_app("data/wxyc_artist_graph.db")
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | D3.js graph explorer (interactive visualization). |
| `GET` | `/health` | Health check — returns `status`, `artist_count`, and `graph_db_age_seconds` (age in seconds of the serving DB file's mtime, or `null` if the file is briefly absent mid [atomic swap](glossary.md#atomic-swap)), or 503 if the database is unreachable. `graph_db_age_seconds` is the freshness signal the WXYC synthetic-DJ canary reads to catch SIGKILL-class silent nightly-sync failures (WXYC/semantic-index#348). |
| `GET` | `/graph/artists/search?q=autechre&limit=10` | Case-insensitive LIKE search, ordered by total_plays descending. |
| `GET` | `/graph/artists/{id}` | Full artist detail including external IDs (Discogs, MusicBrainz, Wikidata QID) and streaming service IDs (Spotify, Apple Music, Bandcamp) joined from the [entity](glossary.md#entity) table, plus `wxyc_library_code_id` (the [library artist id](glossary.md#library-artist-id), or `null` when unmapped). Gracefully degrades on old-schema databases. |
| `GET` | `/graph/artists/{id}/neighbors?type=djTransition&limit=20` | Neighbors by [edge type](glossary.md#edge-type). Types: `djTransition`, `sharedPersonnel`, `sharedStyle`, `labelFamily`, `compilation`, `crossReference`, `wikidataInfluence`. Supports optional `month` (1-12) and `dj_id` [facet](glossary.md#facet) filters for `djTransition` — computes PMI dynamically from play-level data. `min_raw_count` (default 1) filters DJ transition edges by minimum co-occurrence count; applies to `djTransition` and `affinity` edge types. |
| `POST` | `/graph/library-artists/neighbors/batch` | Batch affinity neighbors keyed by [library artist id](glossary.md#library-artist-id) rather than graph id — for the Backend concerts "On Tour" enrichment. See [below](#library-artist-neighbors-on-tour). |
| `GET` | `/graph/artists/{id}/explain/{target_id}` | All relationship types between two artists with weights and details. |
| `GET` | `/graph/entities/{id}/artists` | All artists sharing an entity (alias group). Returns entity metadata and a list of artist summaries. |
| `GET` | `/graph/facets` | Available facet values (months with data, DJ list) for filtering. Gracefully returns empty lists on databases without facet tables. |
| `GET` | `/graph/communities?min_size=5&limit=50` | [Louvain community](glossary.md#louvain-community) metadata (size, label, top genres, top artists). Gracefully returns empty on databases without the `community` table. |
| `GET` | `/graph/artists/{id}/explain/{target_id}/narrative?month=&dj_id=` | LLM-generated natural-language explanation of the relationship between two artists (see below). Uses Claude Haiku. Cached in a sidecar SQLite DB. Returns 501 when `ANTHROPIC_API_KEY` is not set. |
| `GET` | `/graph/artists/{id}/preview` | Audio preview URL for an artist. Multi-source fallback: iTunes lookup (by Apple Music ID) → Spotify top tracks (by Spotify ID, requires credentials) → Bandcamp (by bandcamp_id, scrapes track stream) → Deezer search (by name) → iTunes search (by name). Cached in sidecar `.preview-cache.db`. |
| `GET` | `/graph/narrative-audit/recent?limit=50&flagged_only=false` | Most-recent narrative-audit rows from the audit sidecar (`<db>.narrative-audit-cache.db`). Returns an empty list when no audits have run yet. |

## Library-artist neighbors (On Tour)

`POST /graph/library-artists/neighbors/batch` answers "artists similar to X" in the [library artist id](glossary.md#library-artist-id) keyspace instead of internal graph ids, so the Backend-Service concerts enrichment (WXYC/Backend-Service#1626) can look up affinity neighbors for concert headliners without ever translating id spaces. It reads the `artist.wxyc_library_code_id` mapping the [nightly sync](glossary.md#nightly-sync) populates (WXYC/semantic-index#358).

```json
POST /graph/library-artists/neighbors/batch
{ "library_artist_ids": [4210, 887], "limit": 20, "heat": 0.5 }

{ "results": {
    "4210": [ {"artist_id": 5121, "weight": 4.83} ],
    "887":  [] },
  "source_plays": { "4210": 312 } }
```

- **Shape** — each requested id maps to a list of `SimilarArtist` items (`{artist_id, weight}`, the wxyc-shared DTO verbatim), weight-descending. Every requested id is present in `results`; an unknown, unmapped, or [homonym](glossary.md#homonym) id maps to an empty list rather than being omitted, so the caller can tell "asked, none" from "never asked". Duplicate ids collapse.
- **`source_plays`** (WXYC/Backend-Service#1702) — each mapped source artist's own all-time WXYC play count (`artist.total_plays`), keyed by the same requested library-artist-id strings as `results`. Present **only** for ids that mapped to a graph artist; an unknown, unmapped, or [homonym](glossary.md#homonym) id is absent from the map (distinct from `results`, which carries it with an empty list). Additive and backward-compatible — consumers reading only `results` are unaffected. Feeds the Backend concerts "On Tour" station play-affinity shelf (WXYC/Backend-Service#1626).
- **`heat`** (optional, 0.0–1.0, default 0.5) — the same discovery dial as the neighbors endpoints: cool ranks by well-worn co-occurrence, hot by surprising enrichment edges. One heat per request keeps a response's weights mutually comparable. The production consumer omits it; the iOS debug dial (WXYC/wxyc-ios-64#534) passes it for live preview.
- **`weight`** is the raw affinity composite and is **list-relative** — type-max normalized per source artist, so weights are comparable *within* one list, not *across* lists. Rank and cap relative to a list's own maximum; never compare a weight from one list against a weight from another.
- **Cap and overflow** — at most 100 ids per request (`limit` 1–100, default 20); 101+ ids returns a structured `422`, never a silent truncation, because the nightly caller is unattended and a dropped id would be silent data loss. Empty input returns `200` with empty `results`.
- **Top-K refill** — neighbors are ranked once at 2× `limit`, translated to library ids, unmapped neighbors dropped, then cut to `limit`. The over-fetch keeps a list full when its top-ranked neighbors happen to be unmapped (probe: a plain top-`limit` left a fifth of lists short; 2× saturated all of them).
- **No auth** — public, like the rest of the graph API. The worst case is a bounded local SQLite read (100 sources × 2× `limit`), never a fan-out to a rate-limited external service, and the secondary consumer is the keyless iOS debug screen.

The endpoint returns empty lists for every id until #358 has deployed **and** a nightly rebuild has run; `GET /health`'s `mapped_artist_count` (~22K once populated) is the integration-day signal that separates "mapping not yet rebuilt" from "endpoint broken".

## Narratives and their quality gates

The narrative endpoint asks Claude Haiku to explain, in two or three sentences, why two artists are connected — grounded in the pair's actual graph data (genres, Discogs styles, audio-profile features, shared neighbors, transition counts). LLMs confabulate, so two automated checks guard the output:

- **Token-match gate** — always-on and mechanical: generated narratives are checked against the data that was in the prompt, so a narrative that names things it wasn't told about gets caught at generation time.
- **Claim-ratio audit** — periodic and offline: an LLM verifier decomposes cached narratives into individual claims and scores how many are grounded in the source data. This catches the structural hallucinations the token-match gate can miss (claims built from real tokens that assert something the data doesn't support).

## Narrative claim-ratio audit

The audit samples N cached narratives, opens a read-only connection to the production database to reconstruct each pair's source/target metadata (the same shape the live narrative endpoint scored against), runs each narrative through a Haiku verifier prompt that decomposes it into grounded vs ungrounded claims, and records the resulting ratio to `<db_path>.narrative-audit-cache.db`. Narratives with `ungrounded / total > threshold` are flagged for review or regeneration.

```bash
ANTHROPIC_API_KEY=sk-... python scripts/audit_narratives.py \
    --db-path data/wxyc_artist_graph.db \
    [--n 100] [--threshold 0.2]
```

- `--db-path` / `DB_PATH` — production SQLite database (the narrative cache lives at `<db-path>.narrative-cache.db`).
- `--n` — sample size (default `100`).
- `--threshold` / `NARRATIVE_AUDIT_CLAIM_THRESHOLD` — claim-ratio above which a narrative is flagged (default `0.2`, strict `>` boundary).

The audit DB is a separate sidecar from the narrative cache so audit history survives cache-version bumps. Recent rows are exposed via `GET /graph/narrative-audit/recent`. Scheduling (nightly or periodic invocation) is a follow-up; for now the script is run manually or by external cron.
