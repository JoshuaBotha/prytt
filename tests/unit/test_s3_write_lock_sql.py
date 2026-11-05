"""The S3 write lock for ``spark.sql`` writes, on a live Sail session.

Blocked statements raise before Sail runs them, so no AWS is needed. The
``spark`` fixture is one session for the whole test run, so tables a test
creates get names no other test uses.
"""

import pytest

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
def src(spark):
    """A local temp view to select from."""
    spark.createDataFrame([(1, "a")], ["id", "v"]).createOrReplaceTempView("lock_src")
    return "lock_src"


@pytest.fixture
def passthrough_table(monkeypatch, tmp_path):
    """``pt_db.orders``, a mapped table read from real S3 (global passthrough)."""
    (tmp_path / "prytt.toml").write_text(
        '[catalog."pt_db.orders"]\npath = "s3://real/orders/"\nformat = "parquet"\n'
    )
    monkeypatch.chdir(tmp_path)
    set_env(monkeypatch, PRYTT_S3_MODE="passthrough")
    return "pt_db.orders"


S3_LOCATION_WRITES = [
    "INSERT INTO delta.`s3://b/k` SELECT * FROM lock_src",
    "INSERT OVERWRITE delta.`s3://b/k` SELECT * FROM lock_src",
    "INSERT OVERWRITE DIRECTORY 's3://b/k' USING parquet SELECT * FROM lock_src",
    "CREATE TABLE ctas_s3 USING parquet LOCATION 's3://b/k' AS SELECT * FROM lock_src",
    "REPLACE TABLE ctas_s3 USING delta LOCATION 's3://b/k' AS SELECT * FROM lock_src",
    "CREATE OR REPLACE TABLE ctas_s3 USING delta LOCATION 's3a://b/k' AS SELECT * FROM lock_src",
    "CREATE TABLE ctas_s3 USING parquet OPTIONS (path 's3://b/k') SELECT * FROM lock_src",
    "INSERT OVERWRITE DIRECTORY USING json OPTIONS ('path' = 's3://b/k') SELECT * FROM lock_src",
    "MERGE INTO delta.`s3://b/k` t USING lock_src s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *",
    "UPDATE delta.`s3n://b/k` SET v = 'x'",
    "DELETE FROM delta.`s3://b/k` WHERE id = 1",
    "TRUNCATE TABLE delta.`s3://b/k`",
]


@pytest.mark.parametrize("statement", S3_LOCATION_WRITES)
def test_sql_writes_to_an_s3_location_are_blocked(spark, src, statement):
    with pytest.raises(PryttS3WriteBlocked) as info:
        spark.sql(statement)

    assert "://b/k" in str(info.value)
    assert "PRYTT_ALLOW_S3_WRITES=1" in str(info.value)


PASSTHROUGH_WRITES = [
    "INSERT INTO pt_db.orders SELECT * FROM lock_src",
    "INSERT OVERWRITE TABLE pt_db.orders SELECT * FROM lock_src",
    "CREATE TABLE IF NOT EXISTS pt_db.orders AS SELECT * FROM lock_src",
    "REPLACE TABLE pt_db.orders USING parquet AS SELECT * FROM lock_src",
    "MERGE INTO PT_DB.Orders t USING lock_src s ON t.id = s.id WHEN MATCHED THEN DELETE",
    "UPDATE `pt_db`.`orders` SET v = 'x'",
    "DELETE FROM pt_db.orders WHERE id = 1",
    "TRUNCATE TABLE pt_db.orders",
    "WITH s AS (SELECT * FROM lock_src) INSERT INTO pt_db.orders SELECT * FROM s",
]


@pytest.mark.parametrize("statement", PASSTHROUGH_WRITES)
def test_sql_writes_to_a_passthrough_table_are_blocked(spark, src, passthrough_table, statement):
    with pytest.raises(PryttS3WriteBlocked, match=r"(?i)pt_db\.orders"):
        spark.sql(statement)


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


def test_bare_create_table_at_s3_is_allowed_and_later_writes_to_it_are_blocked(
    spark, src, no_real_aws
):
    spark.sql("CREATE TABLE s3_made (id INT, v STRING) USING parquet LOCATION 's3://b/made/'")

    with pytest.raises(PryttS3WriteBlocked, match=r"s3_made.*s3://b/made/"):
        spark.sql("INSERT INTO s3_made SELECT * FROM lock_src")
    with pytest.raises(PryttS3WriteBlocked):
        spark.sql("insert into default.S3_MADE select * from lock_src")


@pytest.fixture
def local_table(spark, src, tmp_path):
    """``lock_local``, a table on local disk with one row."""
    location = (tmp_path / "lock_local").as_posix()
    spark.sql("DROP TABLE IF EXISTS lock_local")
    spark.sql(f"CREATE TABLE lock_local USING parquet LOCATION '{location}' AS SELECT * FROM {src}")
    yield "lock_local"
    spark.sql("DROP TABLE IF EXISTS lock_local")


def test_sql_writes_to_local_tables_are_not_blocked(spark, local_table):
    spark.sql("INSERT INTO lock_local SELECT * FROM lock_src")
    spark.sql("INSERT INTO lock_local VALUES (3, 'c')")

    assert spark.sql("SELECT * FROM lock_local").count() == 3


def test_reads_and_drop_table_are_not_blocked(spark, src, passthrough_table, no_real_aws):
    spark.sql("SELECT 1").collect()
    spark.sql("DROP TABLE IF EXISTS pt_db.orders")
    spark.sql("CREATE TABLE s3_dropped (id INT) USING parquet LOCATION 's3://b/dropped/'")
    spark.sql("DROP TABLE s3_dropped")


def test_dropped_s3_table_name_can_be_reused_locally(spark, src, tmp_path, no_real_aws):
    spark.sql("CREATE TABLE s3_reused (id INT, v STRING) USING parquet LOCATION 's3://b/re/'")
    spark.sql("DROP TABLE s3_reused")

    location = (tmp_path / "re").as_posix()
    spark.sql(f"CREATE TABLE s3_reused USING parquet LOCATION '{location}' AS SELECT * FROM {src}")
    try:
        spark.sql(f"INSERT INTO s3_reused SELECT * FROM {src}")
    finally:
        spark.sql("DROP TABLE s3_reused")


@pytest.mark.parametrize(
    "statement",
    [
        "-- INSERT INTO delta.`s3://b/k`\nINSERT INTO lock_local SELECT 1, 's3://b/k'",
        "/* TRUNCATE TABLE delta.`s3://b/k` */ INSERT INTO lock_local SELECT 2, 'x'",
        "INSERT INTO lock_local SELECT 3, 'LOCATION ''s3://b/k'''",
        "SELECT 's3://b/k' AS v, 'INSERT INTO delta.`s3://b/k`' AS w",
    ],
)
def test_s3_in_comments_and_value_literals_does_not_trigger_the_lock(spark, local_table, statement):
    spark.sql(statement)  # commands run here; a blocked one raises here


def test_s3_value_literals_in_a_local_ctas_do_not_trigger_the_lock(spark, tmp_path):
    location = (tmp_path / "ctas_lit").as_posix()

    spark.sql(
        f"CREATE TABLE lock_ctas_lit USING json LOCATION '{location}' "
        "AS SELECT 'path' AS k, 's3://b/k' AS v, 'LOCATION' AS w, 's3://b/w' AS x"
    )
    try:
        assert spark.table("lock_ctas_lit").first().v == "s3://b/k"
    finally:
        spark.sql("DROP TABLE lock_ctas_lit")


def not_blocked(call):
    """Run ``call``; it may fail in Sail (no real S3) but not from the lock."""
    try:
        call()
    except PryttS3WriteBlocked:
        raise
    except Exception:  # Sail's error from the dead S3 endpoint, or a bad plan
        pass


def test_unlock_lets_the_same_sql_writes_through(
    monkeypatch, spark, src, passthrough_table, no_real_aws
):
    set_env(monkeypatch, PRYTT_ALLOW_S3_WRITES="1")

    for statement in S3_LOCATION_WRITES + PASSTHROUGH_WRITES:
        not_blocked(lambda statement=statement: spark.sql(statement))
    spark.sql("CREATE TABLE s3_unlocked (id INT, v STRING) USING parquet LOCATION 's3://b/u/'")
    not_blocked(lambda: spark.sql("INSERT INTO s3_unlocked SELECT * FROM lock_src"))


@pytest.fixture
def mapped_session(monkeypatch, tmp_path, s3_root):
    """A new session over a local mapped table ``sales.Orders`` with one row,
    and an Iceberg warehouse under ``tmp_path``; stopped afterwards."""
    from pyspark.sql import SparkSession

    (tmp_path / "prytt.toml").write_text(
        '[catalog."sales.Orders"]\npath = "s3://raw/orders/"\nformat = "json"\n'
    )
    (s3_root / "raw" / "orders").mkdir(parents=True)
    (s3_root / "raw" / "orders" / "part-1.json").write_text('{"id": 1, "v": "a"}\n')
    monkeypatch.chdir(tmp_path)
    set_env(monkeypatch, PRYTT_ICEBERG_WAREHOUSE=str(tmp_path / "wh"))
    session = SparkSession.builder.create()  # runs the on_new_session hooks
    yield session
    session.stop()


def test_table_name_and_iceberg_rewrites_still_work_with_the_lock(mapped_session, tmp_path, capsys):
    spark = mapped_session

    assert spark.sql("SELECT id FROM SALES.orders").first().id == 1  # case rewrite
    spark.sql("INSERT INTO sales.ORDERS SELECT 2 AS id, 'b' AS v")  # local table: not blocked
    assert spark.sql("SELECT id FROM sales.orders").count() == 2

    capsys.readouterr()
    spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.lock_db")
    wh = (tmp_path / "wh").resolve().as_posix()
    assert f"MAP  glue_catalog database lock_db -> {wh}/lock_db" in capsys.readouterr().err

    with pytest.raises(PryttS3WriteBlocked):
        spark.sql("INSERT INTO delta.`s3://b/k` SELECT id FROM sales.orders")
