"""Tests for the Fargate build-job entrypoint (out-of-process nightly rebuild).

``scripts/run_build_job.py`` is the container command for the VPC build task:
download the seed (current production graph) from S3, run ``nightly_sync``, and
upload the rebuilt graph back to S3. The build itself (``nightly_sync``) is
stubbed here — these tests pin the S3 round-trip wiring and the "don't upload a
failed build" contract.
"""

import boto3
import pytest
from moto import mock_aws

import scripts.run_build_job as rbj

BUCKET = "wxyc-semantic-index-build"


@pytest.fixture(autouse=True)
def _aws_creds(monkeypatch):
    """Dummy credentials/region so moto's intercept doesn't trip boto3's
    credential lookup."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@mock_aws
def test_run_round_trips_seed_then_build_through_s3(tmp_path, monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(Bucket=BUCKET, Key="seed/graph.db", Body=b"SEED-DB-BYTES")

    db_path = tmp_path / "data" / "wxyc_artist_graph.db"
    captured = {}

    def fake_nightly_sync(args):
        # The seed must already be downloaded to db_path before the build runs.
        assert db_path.read_bytes() == b"SEED-DB-BYTES"
        captured["args"] = args
        db_path.write_bytes(b"BUILT-DB-BYTES")  # simulate the rebuild output

    monkeypatch.setattr(rbj, "nightly_sync", fake_nightly_sync)

    rbj.run(
        db_path=str(db_path),
        dsn="postgresql://u@host/db",
        seed_uri=f"s3://{BUCKET}/seed/graph.db",
        build_uri=f"s3://{BUCKET}/build/graph.db",
        min_count=2,
        enrichment_top_k=50,
        s3_client=s3,
    )

    # nightly_sync invoked with the expected Namespace (the sync_scheduler shape).
    args = captured["args"]
    assert args.dsn == "postgresql://u@host/db"
    assert args.db_path == str(db_path)
    assert args.min_count == 2
    assert args.enrichment_top_k == 50
    assert args.dry_run is False
    # verbose is read by nightly_sync; assert it so a dropped field is caught here.
    assert args.verbose is True

    # The rebuilt graph (not the seed) is what gets uploaded.
    body = s3.get_object(Bucket=BUCKET, Key="build/graph.db")["Body"].read()
    assert body == b"BUILT-DB-BYTES"


@mock_aws
def test_run_does_not_upload_when_build_fails(tmp_path, monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(Bucket=BUCKET, Key="seed/graph.db", Body=b"SEED")

    db_path = tmp_path / "graph.db"

    def boom(args):
        raise RuntimeError("pipeline OOM")

    monkeypatch.setattr(rbj, "nightly_sync", boom)

    with pytest.raises(RuntimeError, match="pipeline OOM"):
        rbj.run(
            db_path=str(db_path),
            dsn="x",
            seed_uri=f"s3://{BUCKET}/seed/graph.db",
            build_uri=f"s3://{BUCKET}/build/graph.db",
            min_count=2,
            enrichment_top_k=50,
            s3_client=s3,
        )

    # A failed build must never ship: no object at the build key.
    with pytest.raises(s3.exceptions.NoSuchKey):
        s3.get_object(Bucket=BUCKET, Key="build/graph.db")


def test_parse_s3_uri_ok():
    assert rbj._parse_s3_uri("s3://bucket/a/b/c.db") == ("bucket", "a/b/c.db")


@pytest.mark.parametrize("bad", ["http://x/y", "s3://bucket", "s3:///key", "not-a-uri", ""])
def test_parse_s3_uri_rejects_bad(bad):
    with pytest.raises(ValueError):
        rbj._parse_s3_uri(bad)


def test_require_env_raises_when_missing(monkeypatch):
    monkeypatch.delenv("DEFINITELY_NOT_SET", raising=False)
    with pytest.raises(SystemExit):
        rbj._require_env("DEFINITELY_NOT_SET")


def test_require_env_returns_value(monkeypatch):
    monkeypatch.setenv("PRESENT", "value")
    assert rbj._require_env("PRESENT") == "value"
