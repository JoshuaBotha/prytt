"""The S3 write lock for writer APIs that take a table name, on a live Sail
session: ``saveAsTable``, ``insertInto`` and ``DataFrameWriterV2``.

Blocked writes raise before Sail runs them, so no AWS is needed. The ``spark``
fixture is one session for the whole test run, so tables a test creates get
names no other test uses.
"""

import pytest
from pyspark.sql import functions as F

from prytt import PryttS3WriteBlocked, config

UNLOCK = "PRYTT_ALLOW_S3_WRITES"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, s3_root):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL", "PRYTT_S3_MODE", UNLOCK):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")  # passthrough's region check
    config.reset()
    yield
    config.reset()


def set_env(monkeypatch, **values):
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    config.reset()


@pytest.fixture
def df(spark):
    return spark.createDataFrame([(1, "a")], ["id", "v"])


@pytest.fixture
def passthrough_table(monkeypatch, tmp_path):
    """``pt_db.orders``, a mapped table read from real S3 (global passthrough)."""
    (tmp_path / "prytt.toml").write_text(
        '[catalog."pt_db.orders"]\npath = "s3://real/orders/"\nformat = "parquet"\n'
    )
    monkeypatch.chdir(tmp_path)
    set_env(monkeypatch, PRYTT_S3_MODE="passthrough")
    return "pt_db.orders"


TABLE_WRITES = {
    "saveAsTable": lambda df, name: df.write.mode("append").saveAsTable(name),
    "insertInto": lambda df, name: df.write.insertInto(name),
    "writeTo.append": lambda df, name: df.writeTo(name).append(),
    "writeTo.create": lambda df, name: df.writeTo(name).create(),
    "writeTo.replace": lambda df, name: df.writeTo(name).replace(),
    "writeTo.createOrReplace": lambda df, name: df.writeTo(name).createOrReplace(),
    "writeTo.overwrite": lambda df, name: df.writeTo(name).overwrite(F.lit(True)),
    "writeTo.overwritePartitions": lambda df, name: df.writeTo(name).overwritePartitions(),
}


@pytest.mark.parametrize("write", TABLE_WRITES.values(), ids=TABLE_WRITES.keys())
def test_table_writes_to_a_passthrough_table_are_blocked(df, passthrough_table, write):
    with pytest.raises(PryttS3WriteBlocked, match=r"pt_db\.orders.*passthrough"):
        write(df, passthrough_table)


@pytest.fixture
def no_real_aws(monkeypatch):
    """Anything that does reach Sail's S3 client fails fast on a closed local
    port with fake keys instead of reaching AWS."""
    for name in ("AWS_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prytt-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prytt-test")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


@pytest.fixture
def s3_located_table(spark, no_real_aws):
    """``tbl_s3_made``, created in this session at an ``s3://`` location."""
    spark.sql("CREATE TABLE tbl_s3_made (id INT, v STRING) USING parquet LOCATION 's3://b/tbl/'")
    yield "tbl_s3_made"
    spark.sql("DROP TABLE IF EXISTS tbl_s3_made")


S3_LOCATED_WRITES = {k: TABLE_WRITES[k] for k in ("saveAsTable", "insertInto", "writeTo.append")}


@pytest.mark.parametrize("write", S3_LOCATED_WRITES.values(), ids=S3_LOCATED_WRITES.keys())
def test_table_writes_to_a_table_created_at_s3_are_blocked(df, s3_located_table, write):
    with pytest.raises(PryttS3WriteBlocked, match=r"tbl_s3_made.*s3://b/tbl/"):
        write(df, s3_located_table)


def test_save_as_table_with_an_s3_path_option_is_blocked(df):
    with pytest.raises(PryttS3WriteBlocked, match="s3a://b/ext"):
        df.write.format("parquet").option("path", "s3a://b/ext").saveAsTable("tbl_ext")
    with pytest.raises(PryttS3WriteBlocked, match="s3://b/ext2"):
        df.write.saveAsTable("tbl_ext", format="parquet", path="s3://b/ext2")


def test_write_to_with_an_s3_location_is_blocked(df):
    with pytest.raises(PryttS3WriteBlocked, match="s3://b/v2"):
        df.writeTo("tbl_ext").using("parquet").option("path", "s3://b/v2").create()
    with pytest.raises(PryttS3WriteBlocked, match="s3n://b/v2loc"):
        df.writeTo("tbl_ext").tableProperty("location", "s3n://b/v2loc").createOrReplace()


@pytest.mark.parametrize("name", ["PT_DB.Orders", "`pt_db`.`ORDERS`", "spark_catalog.pt_db.orders"])
def test_table_names_match_case_insensitively(df, passthrough_table, name):
    with pytest.raises(PryttS3WriteBlocked):
        df.write.insertInto(name)
    with pytest.raises(PryttS3WriteBlocked):
        df.writeTo(name).append()


@pytest.fixture
def use_database(spark):
    """``use_database(name)`` creates ``name`` if needed and makes it the
    current database until the test ends."""

    def use(name):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")
        spark.sql(f"USE DATABASE {name}")

    yield use
    spark.sql("USE DATABASE default")


@pytest.mark.parametrize("write", S3_LOCATED_WRITES.values(), ids=S3_LOCATED_WRITES.keys())
def test_unqualified_names_resolve_against_the_current_database(
    df, passthrough_table, use_database, write
):
    use_database("pt_db")

    with pytest.raises(PryttS3WriteBlocked, match=r"pt_db\.orders.*passthrough"):
        write(df, "orders")


def test_writer_wrappers_are_installed_independently(monkeypatch, spark):
    from pyspark.sql.connect.readwriter import DataFrameWriter, DataFrameWriterV2

    from prytt import s3lock

    methods = [(DataFrameWriter, name) for name in ("save", "saveAsTable", "insertInto")]
    methods += [(DataFrameWriterV2, name) for name in s3lock._V2_METHODS]
    for cls, name in methods:
        with monkeypatch.context() as patch:
            original = getattr(cls, name).__wrapped__
            patch.setattr(cls, name, original)  # as if only this one were unwrapped
            others = {other: getattr(c, other) for c, other in methods if other != name}

            s3lock._wrap_writers()

            assert getattr(cls, name).__wrapped__ is original, name
            assert {other: getattr(c, other) for c, other in methods if other != name} == others


def test_mixed_case_name_of_a_table_created_at_s3_is_blocked(df, s3_located_table):
    with pytest.raises(PryttS3WriteBlocked):
        df.write.mode("append").saveAsTable("default.TBL_S3_Made")


def test_table_writes_to_a_local_table_are_not_blocked(spark, df, tmp_path):
    location = (tmp_path / "tbl_local").as_posix()
    spark.sql("DROP TABLE IF EXISTS tbl_local")
    spark.sql(f"CREATE TABLE tbl_local (id BIGINT, v STRING) USING parquet LOCATION '{location}'")
    try:
        df.write.mode("append").saveAsTable("tbl_local")
        df.write.insertInto("tbl_local")
        df.writeTo("tbl_local").append()

        assert spark.table("tbl_local").count() == 3
    finally:
        spark.sql("DROP TABLE IF EXISTS tbl_local")


def test_table_writes_to_a_session_managed_table_are_not_blocked(spark, df):
    spark.sql("DROP TABLE IF EXISTS tbl_managed")
    try:
        df.write.saveAsTable("tbl_managed")
        df.write.insertInto("tbl_managed")
        df.writeTo("tbl_managed").append()

        assert spark.table("tbl_managed").count() == 3
    finally:
        spark.sql("DROP TABLE IF EXISTS tbl_managed")


def not_blocked(call):
    """Run ``call``; it may fail in Sail (no real S3, no such database) but not
    from the lock."""
    try:
        call()
    except PryttS3WriteBlocked:
        raise
    except Exception:
        pass


def test_unlock_lets_the_same_table_writes_through(
    monkeypatch, df, passthrough_table, s3_located_table
):
    set_env(monkeypatch, PRYTT_ALLOW_S3_WRITES="1")

    for write in TABLE_WRITES.values():
        not_blocked(lambda write=write: write(df, passthrough_table))
    for write in S3_LOCATED_WRITES.values():
        not_blocked(lambda write=write: write(df, s3_located_table))
