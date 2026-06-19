#!/usr/bin/env python3
"""Fargate entrypoint for the out-of-process nightly graph rebuild.

Runs as the container command of the VPC build task (see
``plans/si-out-of-process-rebuild`` and ``infra/build-job.yaml``). It round-trips
the rebuild through S3 so the heavy ``nightly_sync`` work happens off the API
serving host, with an adequate memory budget:

  1. Download the *seed* (current production graph) from ``SEED_S3_URI`` to
     ``DB_PATH``. ``nightly_sync`` is incremental — it ``shutil.copy2``-es this
     file forward and layers fresh PG-derived tables on top, so seeding from prod
     is what preserves the Discogs/Wikidata/AcousticBrainz enrichment tables.
  2. Run ``nightly_sync`` against Backend-Service PG (reachable here because the
     task runs in the VPC). It reads ~2M flowsheet rows, resolves artists,
     computes PMI + graph metrics, checkpoints, and atomically swaps the result
     into ``DB_PATH`` locally.
  3. Upload the rebuilt ``DB_PATH`` to ``BUILD_S3_URI``.

A failure in any step propagates and the process exits non-zero **without**
uploading, so the EC2 conductor's exit-code check is meaningful and a failed
build never ships. The conductor validates the artifact (``validate_graph_db``)
before swapping it into the live serving path.

Environment:
    DATABASE_URL_BACKEND   Backend-Service PG DSN (RDS private endpoint). Required.
    SEED_S3_URI            s3:// URI of the seed (current prod) DB. Required.
    BUILD_S3_URI           s3:// URI to upload the rebuilt DB to. Required.
    DB_PATH                Local working path (default /data/wxyc_artist_graph.db).
    SYNC_MIN_COUNT         Min co-occurrence for DJ-transition edges (default 2).
    ENRICHMENT_TOP_K       Per-artist neighbor cap for shared_personnel/label_family
                           (default 50, 0 disables).
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from wxyc_etl.logger import init_logger

from semantic_index.nightly_sync import nightly_sync

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = "/data/wxyc_artist_graph.db"
DEFAULT_MIN_COUNT = 2
DEFAULT_ENRICHMENT_TOP_K = 50


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into ``(bucket, key)``.

    Raises ``ValueError`` for anything that is not a well-formed S3 URI with a
    non-empty bucket and key.
    """
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError(f"not a valid s3:// URI: {uri!r}")
    return parsed.netloc, key


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"required environment variable {name} is not set")
    return value


def run(
    *,
    db_path: str,
    dsn: str,
    seed_uri: str,
    build_uri: str,
    min_count: int,
    enrichment_top_k: int,
    s3_client: Any,
) -> None:
    """Download seed → run ``nightly_sync`` → upload result. Raises on failure."""
    seed_bucket, seed_key = _parse_s3_uri(seed_uri)
    build_bucket, build_key = _parse_s3_uri(build_uri)

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading seed %s -> %s", seed_uri, db_path)
    s3_client.download_file(seed_bucket, seed_key, db_path)

    logger.info("Running nightly_sync against Backend-Service PG...")
    # Mirror sync_scheduler._run_sync: invoke the orchestrator directly with a
    # Namespace rather than re-parsing argv. dry_run=False so the pipeline does
    # its local checkpoint + atomic swap into db_path; we upload that file.
    args = argparse.Namespace(
        db_path=db_path,
        dsn=dsn,
        min_count=min_count,
        enrichment_top_k=enrichment_top_k,
        dry_run=False,
        verbose=True,
    )
    nightly_sync(args)

    logger.info("Uploading rebuilt graph %s -> %s", db_path, build_uri)
    s3_client.upload_file(db_path, build_bucket, build_key)
    logger.info("Build job complete.")


def main(argv: list[str] | None = None) -> None:
    """Container entrypoint: wire env → :func:`run`. Non-zero exit on failure."""
    init_logger(repo="semantic-index", tool="semantic-index build-job", level=logging.INFO)

    import boto3  # imported here so the module imports without boto3 in tests

    run(
        db_path=os.environ.get("DB_PATH", DEFAULT_DB_PATH),
        dsn=_require_env("DATABASE_URL_BACKEND"),
        seed_uri=_require_env("SEED_S3_URI"),
        build_uri=_require_env("BUILD_S3_URI"),
        min_count=int(os.environ.get("SYNC_MIN_COUNT", str(DEFAULT_MIN_COUNT))),
        enrichment_top_k=int(os.environ.get("ENRICHMENT_TOP_K", str(DEFAULT_ENRICHMENT_TOP_K))),
        s3_client=boto3.client("s3"),
    )


if __name__ == "__main__":
    main()
