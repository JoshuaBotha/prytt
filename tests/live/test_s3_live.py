"""Opt-in live S3 check: passthrough reads real AWS, the write lock blocks writes.

Run with ``mise run test:s3-live``, never in CI. Skipped unless
``PRYTT_TEST_S3_URI`` points at existing data (an ``s3://`` table directory).
Credentials come from the shell: ``PRYTT_S3_PROFILE`` or the usual ``AWS_*``
env vars / default chain.

Optional:

- ``PRYTT_TEST_S3_FORMAT``: the data's format (default ``parquet``).
- ``PRYTT_TEST_S3_PARTITIONS``: comma-separated partition columns the table
  must expose, if the data is partitioned (``key=value/`` directories).

Read-only: the one write goes through the S3 write lock, which raises before
Sail. This directory is kept apart from ``tests/unit`` and
``tests/acceptance`` (own pytest process, no shared conftest) because those
tests point AWS at a dead endpoint with fake keys, and Sail loads AWS config
once per process.
"""

import os

import pytest

from prytt import PryttS3WriteBlocked, config

URI = os.environ.get("PRYTT_TEST_S3_URI", "")
FORMAT = os.environ.get("PRYTT_TEST_S3_FORMAT", "parquet")
PARTITIONS = [
    c.strip() for c in os.environ.get("PRYTT_TEST_S3_PARTITIONS", "").split(",") if c.strip()
]

pytestmark = [
    pytest.mark.s3_live,
    pytest.mark.skipif(not URI, reason="PRYTT_TEST_S3_URI is not set"),
]


@pytest.fixture
def live_session(spark, tmp_path, monkeypatch):
    """A fresh session with ``live_db.data`` mapped as a passthrough table at
    ``PRYTT_TEST_S3_URI``; the S3 write lock is on."""
    from pyspark.sql import SparkSession

    assert URI.startswith("s3://"), f"PRYTT_TEST_S3_URI must be an s3:// URI, got {URI!r}"
    monkeypatch.delenv("PRYTT_ALLOW_S3_WRITES", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "prytt.toml").write_text(
        f'[catalog."live_db.data"]\npath = "{URI.rstrip("/")}/"\n'
        f'format = "{FORMAT}"\nmode = "passthrough"\n'
    )
    config.reset()
    session = SparkSession.builder.create()
    yield session
    session.stop()
    config.reset()


def test_passthrough_table_reads_real_s3_data(live_session):
    df = live_session.sql("SELECT * FROM live_db.data")

    assert df.schema.fields, "the passthrough table has no columns"
    assert set(PARTITIONS) <= set(df.columns)
    assert df.limit(1).count() == 1, f"no rows at {URI}"


def test_write_to_the_same_prefix_is_blocked_by_the_lock(live_session):
    assert "PRYTT_ALLOW_S3_WRITES" not in os.environ
    df = live_session.createDataFrame([(1,)], "id int")
    target = f"{URI.rstrip('/')}/_prytt_live_write_check/"

    with pytest.raises(PryttS3WriteBlocked):
        df.write.mode("overwrite").parquet(target)
