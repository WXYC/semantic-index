# Test fixtures

## `wxycmusic-fixture.sql`

A truncated `mysqldump` of the tubafrenzy `wxycmusic` MySQL database (23 tables, 20 of them with rows; 164 `FLOWSHEET_ENTRY` and 1000 `FLOWSHEET_ENTRY_PROD` rows, top-1000-by-ID `LIBRARY_CODE` / `LIBRARY_RELEASE`). The pipeline resolves 551 artists and 721 DJ-transition edges from it. It is the substrate for the pipeline tests that run the real extractor against real tubafrenzy table shapes:

- [`tests/integration/test_pipeline.py`](../integration/test_pipeline.py)
- [`tests/integration/test_entity_source_fallback.py`](../integration/test_entity_source_fallback.py)
- [`tests/e2e/test_full_pipeline.py`](../e2e/test_full_pipeline.py)

Those tests resolve it from this directory by default. Set `TUBAFRENZY_FIXTURE` to an absolute path to point them at a different dump instead.

### Provenance

| | |
|---|---|
| Upstream repo | [`WXYC/tubafrenzy`](https://github.com/WXYC/tubafrenzy) |
| Upstream path | `scripts/dev/fixtures/wxycmusic-fixture.sql` |
| Source commit | `6e1a27e9fb7ec13ddabe218f93de8bdb725a8850` (tubafrenzy `main`, 2026-09-17) |
| Git blob SHA | `e4b7909e798fa5f61b2460f94eb7fa7d6bb7b8fb` |
| SHA-256 | `5d1f4cb7c7cc22d58fe0cf41912f997287619a1181ee2e8d1f08a75770baa7e2` |
| Size | 697,662 bytes |

Copied in under [WXYC/semantic-index#377](https://github.com/WXYC/semantic-index/issues/377), Phase 5 step 6 of the [tubafrenzy decommissioning plan](https://github.com/WXYC/wiki/blob/main/plans/tubafrenzy-decommissioning.md) ([wiki#92](https://github.com/WXYC/wiki/issues/92)). Before that, the three tests above loaded the file out of a sibling `tubafrenzy/` checkout and silently skipped when one was not on disk.

**There is no live source to re-pull from.** `WXYC/tubafrenzy` is being archived under [wiki#100](https://github.com/WXYC/wiki/issues/100) — it stays readable at the commit above, but becomes read-only. The Kattare-hosted MySQL server the dump was originally taken from goes away when that hosting ends on 2026-09-22. This file is the maintained copy.

### Do not regenerate

Copy, do not re-dump. A checksum was taken against this exact byte sequence during Phase 0 of the decommissioning, and the e2e cross-reference assertions depend on a hand-tuned truncation window — `tubafrenzy/scripts/dev/generate-fixture-dump.sh` supplemented the top-1000-by-ID cut with extra `--no-create-info` invocations so that both the `library_code` and `release` cross-reference extraction paths have rows inside the window. Re-dumping loses that. See `test_cross_reference_edges_from_both_sources` in `tests/e2e/test_full_pipeline.py`, [WXYC/semantic-index#185](https://github.com/WXYC/semantic-index/issues/185), and [WXYC/tubafrenzy#486](https://github.com/WXYC/tubafrenzy/issues/486).

### Not the production dump

This is a small, structurally-representative test fixture. It is **not** the final tubafrenzy production dump, which is PII-bearing, lives at `s3://wxyc-archive/legacy/tubafrenzy/2026-09-16/`, and belongs in no repository.
