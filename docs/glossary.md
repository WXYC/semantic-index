# Glossary

Plain-English definitions for the domain terms, math, data sources, and infrastructure vocabulary used throughout this repo's documentation. The other docs link here on first use of a term, so each term below has its own heading (stable anchor). Entries cross-link with each other where one term builds on another.

If you are brand new to the project, read the [README](../README.md) first — it explains what the project is for; this file explains the words.

## WXYC domain

### Flowsheet

The station's play-by-play log. During every show, the DJ records each track they play — artist, song, release, label — in order. WXYC has kept a digital flowsheet since 2003, producing roughly 2.6 million entries across 22+ years. The flowsheet is the pipeline's primary input: the order of entries within a show is what encodes DJ curation.

### Radio show

One DJ's contiguous on-air shift. The pipeline only counts two artists as adjacent when they were played back-to-back *within the same show* — adjacency across a show boundary would connect two different DJs' choices, which isn't a curatorial decision anyone made.

### DJ transition

Two artists played consecutively within one show. This is the core signal of the whole project: each transition is one DJ's implicit statement that these two artists belong next to each other. Aggregated over millions of plays and scored with [PMI](#pmi), transitions become the `dj_transition` edges of the graph.

### tubafrenzy

WXYC's legacy web system (Java/Tomcat/MySQL, running at wxyc.info since 2003) that operates the flowsheet and the card catalog. Its MySQL database is the historical source of record. The pipeline's original mode parses a tubafrenzy MySQL dump file directly, without needing a running database.

### Backend-Service

The modern WXYC API service (Express + PostgreSQL) that mirrors flowsheet and catalog data from tubafrenzy in near-real-time. The [nightly sync](#nightly-sync) reads from its PostgreSQL schema (`wxyc_schema.*`) instead of parsing SQL dumps.

### Library catalog

WXYC's card catalog of ~72,000 physical releases. Two tables matter here: a **library code** is an artist's filing entry in the catalog (carrying the artist's presentation name and shelving genre), and a **library release** is one release filed under a library code. When a flowsheet entry links to a library release, the pipeline can follow database keys back to an authoritative artist name — see [FK chain](#fk-chain).

### Genre (shelving code)

WXYC's genre labels (Rock, Jazz, Electronic, OCS, Africa, Asia, Latin, Blues, Reggae, Classical, Hiphop) are physical shelving codes for the record library, not descriptions of sound — "Rock" contains both Bob Dylan and Black Dice. The docs and code treat genre as a filing label useful for display and community summaries, never as a measure of musical similarity.

### Request

A flowsheet entry can be flagged as a listener request. The pipeline aggregates this into a per-artist `request_ratio` (what fraction of an artist's plays were requested), one of the node statistics.

### Cross-reference

A "see also" link between catalog entries, curated by WXYC music directors — for example, a solo artist's card pointing to their band. These are hand-made relationship edges that no statistical method could infer, extracted from tubafrenzy's cross-reference tables into `cross_reference` edges.

## Graph concepts

### Semantic artist graph

The project's output artifact: a graph whose nodes are canonical artists and whose edges are typed relationships between them — DJ transitions, shared personnel, shared styles, label families, compilation co-appearances, cross-references, Wikidata influences, acoustic similarity. It is exported as a SQLite database (plus a [GEXF](#gexf) file in SQL-dump mode), served by the Graph API, and drawn by the explorer at [explore.wxyc.org](https://explore.wxyc.org).

### PMI

Pointwise Mutual Information — the statistic that turns raw transition counts into a meaningful edge weight. Formula: `log2( P(a,b) / (P(a) × P(b)) )`, where `P(a,b)` is the probability of artists a and b being played adjacently and `P(a)`, `P(b)` are each artist's overall play probability. The intuition: heavily played artists will co-occur with everything just by volume, so raw counts mostly measure popularity. PMI asks "do these two appear together *more than chance would predict* given how much each is played?" — a high score means the pairing is a deliberate curatorial pattern, not an accident of rotation.

### Edge type

The graph is multi-relational: each edge carries a type saying *why* two artists are connected. Current types: `djTransition` (back-to-back plays, weighted by [PMI](#pmi)), `crossReference` (curated catalog links), `sharedPersonnel` (same musician credited on both artists' releases, from Discogs), `sharedStyle` (overlapping Discogs style tags, scored by [Jaccard](#jaccard-similarity)), `labelFamily` (same or related record label), `compilation` (appear on the same compilation), `wikidataInfluence` (directed "influenced by" claims from Wikidata), acoustic similarity (audio-feature closeness), and `affinity` (a composite that blends the other types into one weighted score for the explorer's default view).

### Facet

A filter dimension for recomputing transition edges on demand. The exported database keeps play-level data plus aggregate tables so the API can compute [PMI](#pmi) *within a slice* — a single calendar month (season effects) or a single DJ (one person's taste) — instead of only serving the global precomputed graph.

### Louvain community

A cluster of artists found by the Louvain algorithm, which groups nodes so that edges are dense inside groups and sparse between them. In this graph, communities tend to correspond to scenes or sonic neighborhoods. Each artist gets a `community_id`, and the `community` table stores per-cluster size, top genres, and top artists.

### Betweenness centrality

A per-node score measuring how often an artist sits on the shortest path between other artists. High betweenness marks bridge artists — the ones DJs use to travel between otherwise-distant scenes.

### PageRank

A per-node importance score (the algorithm behind early Google search): an artist ranks highly when many well-connected artists link to it. Complements raw play counts as a measure of an artist's structural weight in the graph.

### Adamic-Adar

A shared-neighbor score used when ranking how related two artists are through mutual connections. Instead of just counting mutual neighbors, it down-weights ubiquitous ones — sharing a neighbor that connects to everything (a Miles Davis-sized hub) proves little, while sharing a rarely-connected neighbor is strong evidence. Used to select which shared neighbors are worth mentioning in narratives.

### Jaccard similarity

Overlap between two sets, `|A ∩ B| / |A ∪ B|` — 1.0 when identical, 0 when disjoint. Used to score shared Discogs style tags for `sharedStyle` edges.

### Jaro-Winkler

A string-similarity measure (0 to 1) tolerant of typos and small spelling differences, with extra weight on matching prefixes. Used in the fuzzy tier of [artist resolution](#artist-resolution) to match misspelled flowsheet names against catalog names.

### GEXF

Graph Exchange XML Format — a file format for graphs, readable by [Gephi](https://gephi.org/), a free desktop application for exploring and laying out networks visually. The pipeline can export one alongside the SQLite database.

## Artist identity

### Artist resolution

The process of mapping the free-text artist names DJs typed into the flowsheet ("Stereolab", "stereo lab", "Stereolab feat. ...") onto canonical artists. The resolver tries a sequence of tiers, taking the first that matches: compilation-track lookup ([CTA](#cta), then Discogs tracklists), the [FK chain](#fk-chain), exact name match, [normalized](#normalization) match, fuzzy match ([Jaro-Winkler](#jaro-winkler)), Discogs search, and finally a raw fallback that keeps the name as-is so no play is dropped.

### Canonical name

The single authoritative display name chosen for an artist, under which all of that artist's name variants are merged. Each canonical artist is one node in the graph (one row in the `artist` table).

### Library artist id

A Backend-Service catalog artist id — `wxyc_schema.artists.id`, the keyspace Backend and the iOS app identify artists by, and the id space of the shared `SimilarArtist.artist_id` and `Concert.headlining_artist_id` DTOs. The graph keys its own nodes by [canonical name](#canonical-name) with internal ids, so the [nightly sync](#nightly-sync) records each unambiguous artist's library artist id in the `artist.wxyc_library_code_id` column (WXYC/semantic-index#358). The batch endpoint `POST /graph/library-artists/neighbors/batch` reads that mapping in both directions to answer "artists similar to X" entirely in library-artist-id space, so the Backend concerts enrichment never translates ids itself. A [homonym](#homonym) name carries more than one catalog id and is left unmapped.

### Homonym

Two or more *distinct* artists that share a name — e.g. several unrelated bands all filed as "Lake" in the [library catalog](#library-catalog) under different library codes. Because the resolver keys artists by [canonical name](#canonical-name), its first-wins name index conflates homonyms into a single graph node whose neighborhood mixes all of them. No single library code is correct for such a node, so the graph↔Backend-library-id mapping (see [docs/pipeline.md](pipeline.md)) deliberately leaves ambiguous names **unmapped** rather than guessing. Splitting conflated homonym nodes is a separate, open track (WXYC/semantic-index#360).

### FK chain

Foreign-key chain — the most reliable resolution tier. When a DJ picked a release from the catalog while logging, the flowsheet entry carries a database link to that release, and the pipeline follows it: flowsheet entry → library release → library code → artist presentation name. No string matching involved, so it can't be fooled by spelling.

### Normalization

Deterministically simplifying a name before comparing it, so trivial variations ("The Sea and Cake" vs "sea and cake") don't defeat a match. The shared `wxyc-etl` package provides two standard forms: `to_match_form` (a general-purpose form: strip diacritics and invisible characters, lowercase, collapse whitespace) and `to_identity_match_form` (the stricter org-wide **cross-cache identity** form that all WXYC caches use so they agree on which names are "the same"; it also strips enclosing brackets and leading articles). See [docs/development.md](development.md) for exact definitions.

### Compilation artist (VA)

A release credited to no single artist — "Various Artists", "V/A", soundtracks. These must not become graph nodes ("Various Artists" would otherwise be the best-connected artist at the station). The resolver detects them and resolves compilation plays to the actual per-track artist instead, via [CTA](#cta) and Discogs tracklists.

### CTA

`COMPILATION_TRACK_ARTIST` — a tubafrenzy table mapping each track on a compilation in the WXYC library to its actual artist. Loaded from a separate dump, it lets the resolver replace a "Various Artists" play with the artist who actually performed the logged song (resolution Tier 0).

### Entity

A record of an artist's resolved real-world identity, stored in the `entity` table: Wikidata [QID](#wikidata), streaming-service IDs (Spotify, Apple Music, Bandcamp). Multiple artist rows can point at one entity, which is how alias groups (the same act under different names) are represented.

### Entity deduplication

A pipeline step that merges artist rows discovered to be the same act — typically because resolution assigned them the same Wikidata QID — re-keying their edges onto the surviving row.

### Cross-cache identity

The org-wide program to make every WXYC data cache (Discogs, MusicBrainz, Wikidata) resolve artist names to identities the same way, using shared hook tables and the `to_identity_match_form` normalizer. This repo's per-cache rollout switches are documented in [docs/deployment.md](deployment.md).

### LML

[library-metadata-lookup](https://github.com/WXYC/library-metadata-lookup) — the WXYC service that owns library search and identity resolution for the live request pipeline. Its `entity.identity` PostgreSQL table stores pre-resolved artist identities, which this pipeline can import instead of resolving from scratch (`--entity-source=lml`).

## External data sources

### Discogs

[Discogs](https://www.discogs.com/) is a crowd-sourced database of music releases — credits, styles, labels, tracklists. The pipeline reads from **discogs-cache**, a local PostgreSQL copy (port 5433) built from Discogs' public data dumps by the [discogs-etl](https://github.com/WXYC/discogs-etl) repo, with the [LML](#lml) API as a fallback for artists not in the cache. Discogs data powers the enrichment edges: shared personnel, shared styles, label families, compilation co-appearances.

### MusicBrainz

[MusicBrainz](https://musicbrainz.org/) is an open music encyclopedia. An **MBID** is a MusicBrainz identifier — a UUID permanently naming one artist, recording, or release. The pipeline reads from **musicbrainz-cache**, a local PostgreSQL copy (port 5434) filtered to WXYC library artists, mainly to map artists to the recording MBIDs that key [AcousticBrainz](#acousticbrainz) features.

### Wikidata

[Wikidata](https://www.wikidata.org/) is the structured-data sibling of Wikipedia: entities identified by **QIDs** (like `Q42`) connected by numbered properties. The pipeline queries it over [SPARQL](#sparql) for three relationships: P737 ("influenced by") between artists, and P749 ("parent organization") / P355 ("subsidiary") between record labels; label QIDs are found by matching Discogs label IDs (the bridge queried in `wikidata_client.py`). QIDs also serve as the strongest identity key for [entity deduplication](#entity-deduplication).

### AcousticBrainz

A discontinued research project that computed audio features (danceability, mood, genre classifiers) for millions of recordings, keyed by MusicBrainz recording [MBID](#musicbrainz). Its final data dumps are imported into the `ab_recording` table of musicbrainz-cache. Coverage is the catch: only ~13% of WXYC artists have AcousticBrainz features, which is why the [archive classification](#wxyc-audio-archive) path exists.

### SPARQL

The query language for graph-shaped knowledge bases, used here to query Wikidata's public endpoint. Rate-limited (~1 query/second), which the client handles with batching and exponential backoff.

## Audio features

### Essentia

An open-source audio-analysis library from the Music Technology Group at Universitat Pompeu Fabra. "Essentia TF" refers to its TensorFlow-model support, which this repo uses to run pretrained music classifiers over WXYC's own broadcast recordings.

### VGGish

A general-purpose audio embedding model (published by Google, trained on YouTube's AudioSet). It converts a second of audio into a compact numeric vector capturing its sonic character. VGGish does no classifying itself — it produces the representation that [classification heads](#classification-head) consume.

### Classification head

A small model that takes [VGGish](#vggish) embeddings and predicts one musical attribute — danceability, `mood_happy`, `voice_instrumental`, a genre taxonomy, and so on. The archive path runs 15 heads per audio segment; each is a ~50 KB file downloaded alongside the 275 MB VGGish extractor.

### Audio profile

A per-artist summary of what the artist sounds like, stored in the `audio_profile` table. Its core is the **feature centroid**: a 59-dimension vector concatenating the outputs of the 18 AcousticBrainz classifiers, averaged over all of an artist's analyzed recordings. Cosine similarity between two artists' centroids produces `acoustic_similarity` edges. Archive-derived profiles map the 15 Essentia heads onto the same 59-dimension layout (the 3 classifiers without VGGish equivalents are zero-filled) so both sources are comparable.

### WXYC audio archive

Hourly MP3 recordings of the actual WXYC broadcast since June 2021, stored in the `wxyc-archive` S3 bucket keyed by timestamp (`YYYY/MM/DD/YYYYMMDDHH00.mp3`). Because flowsheet entries are timestamped, the pipeline can locate a specific play inside a specific archive hour, extract a segment, and classify what the artist actually sounds like on air — extending audio coverage far beyond [AcousticBrainz](#acousticbrainz)'s 13%.

## Storage and infrastructure

### DSN

Data Source Name — a database connection string, e.g. `postgresql://user:password@host:5432/dbname`. Flags and env vars ending in `-dsn` / `_DSN` or named `DATABASE_URL_*` expect one.

### Graph database

The SQLite file (`wxyc_artist_graph.db`) that is both the pipeline's output and the Graph API's serving store. In pipeline-DB mode the same file also persists artist identities between runs, so re-running the pipeline updates artists in place instead of rebuilding them from scratch.

### Sidecar database

A small SQLite file that lives next to the main graph database and holds auxiliary state — the narrative cache, the preview-URL cache, the narrative-audit history. Keeping these separate means a graph rebuild (or cache-version bump) doesn't destroy them, and vice versa.

### Atomic swap

Replacing the serving database file in one filesystem operation (`os.replace`), so any reader sees either the complete old file or the complete new one — never a half-written database. This is how the nightly rebuild ships a new graph under a live API without a restart.

### Checkpoint

A small SQLite progress record kept by the long-running ETL scripts (archive processing, AcousticBrainz import). Work is recorded per unit (per archive hour, per tar file) as it completes, so an interrupted job resumes where it stopped instead of starting over — and `--retry-failed` can re-attempt only the units that failed.

### Summary table

A precomputed join, stored as a real table in the discogs-cache PostgreSQL (e.g. `artist_style_summary`). The raw Discogs tables are tens of millions of rows; joining them per-query made enrichment take hours. The summary tables flatten those joins once so lookups are single-table reads. (Also called materialized summary tables; they are rebuilt after each monthly Discogs cache refresh.)

### Nightly sync

The daily rebuild of the graph's core (plays, resolution, [PMI](#pmi), stats, facets, graph metrics) from [Backend-Service](#backend-service) PostgreSQL, preserving the enrichment tables (Discogs, Wikidata, audio) already in the production database. It runs out-of-process on [Fargate](#ecr--ecs--fargate) and ends with an [atomic swap](#atomic-swap); see [docs/deployment.md](deployment.md).

### ECR / ECS / Fargate

Three AWS container services that run the deployment: **ECR** (Elastic Container Registry) stores the built Docker images; **ECS** (Elastic Container Service) runs containers from those images; **Fargate** is ECS's serverless mode — you specify CPU/memory for a task and AWS finds the hardware, so there's no build server to maintain for the nightly rebuild.

### Conductor

`scripts/ec2-build-conductor.sh` — the script on the EC2 serving host that drives the nightly rebuild round-trip: snapshot the live database, upload it as the [seed](#seed), launch the Fargate build task, download and validate the result, and [atomically swap](#atomic-swap) it into place. Fired daily by a systemd timer.

### Seed

The consistent snapshot of the current production database that the nightly build task starts from. The build *must* seed from current production rather than starting empty, because the nightly sync is incremental: the enrichment tables (Discogs, Wikidata, audio) only exist in the current database and are carried forward, not recomputed.
