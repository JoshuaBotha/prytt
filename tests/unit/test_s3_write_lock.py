"""The S3 write lock on a live Sail session (pytest plugin fixtures).

Every blocked write raises before Sail is asked to do anything, so no AWS is
needed.
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
def df(spark):
    return spark.createDataFrame([(1, "a")], ["id", "v"])


@pytest.mark.parametrize("mode", ["local", "passthrough"])
def test_plain_spark_write_to_s3_is_blocked_in_every_mode(monkeypatch, df, mode):
    set_env(monkeypatch, PRYTT_S3_MODE=mode)

    with pytest.raises(PryttS3WriteBlocked) as info:
        df.write.parquet("s3://b/k")

    assert isinstance(info.value, PermissionError)
    assert "s3://b/k" in str(info.value)
    assert "PRYTT_ALLOW_S3_WRITES=1" in str(info.value)


@pytest.mark.parametrize("uri", ["s3a://b/k", "s3n://b/k", "S3://b/k", "S3A://b/k", "s3N://b/k"])
def test_s3a_and_s3n_paths_are_blocked_too(df, uri):
    with pytest.raises(PryttS3WriteBlocked, match=uri):
        df.write.mode("overwrite").save(uri, format="json")


def test_path_given_as_an_option_is_blocked(df):
    with pytest.raises(PryttS3WriteBlocked, match="s3://b/opt"):
        df.write.format("parquet").option("path", "s3://b/opt").save()


def test_plain_spark_write_in_local_mode_suggests_a_local_path_or_glue_writer(df):
    with pytest.raises(PryttS3WriteBlocked) as info:
        df.write.csv("s3://b/k")

    assert "local path" in str(info.value)
    assert "write_dynamic_frame" in str(info.value)


def test_blocked_write_raises_with_strict_mode_off(monkeypatch, df):
    set_env(monkeypatch, PRYTT_STRICT="0")

    with pytest.raises(PryttS3WriteBlocked):
        df.write.json("s3://b/k")


def test_writes_to_local_paths_are_unaffected(df, tmp_path, spark):
    out = (tmp_path / "out").as_posix()

    df.write.parquet(out)

    assert spark.read.parquet(out).count() == 1


def test_reads_from_s3_are_never_blocked(spark, glue_context, s3_root):
    (s3_root / "b" / "in").mkdir(parents=True)
    (s3_root / "b" / "in" / "a.json").write_text('{"id": 1}\n')

    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3", connection_options={"paths": ["s3://b/in/"]}, format="json"
    )

    assert dyf.toDF().count() == 1
    # Plain Spark reads aren't mapped. Building one must not trip the lock (it
    # is lazy, so nothing reaches S3 here).
    spark.read.format("json").load("s3://b/in/")


def glue_write_options(glue_context, df):
    from awsglue.dynamicframe import DynamicFrame

    glue_context.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(df, glue_context, "out"),
        connection_type="s3",
        connection_options={"path": "s3://out/orders/"},
        format="json",
    )


def glue_write_catalog(glue_context, df):
    from awsglue.dynamicframe import DynamicFrame

    glue_context.write_dynamic_frame.from_catalog(
        frame=DynamicFrame.fromDF(df, glue_context, "out"),
        database="out_db",
        table_name="orders",
    )


GLUE_WRITERS = [glue_write_options, glue_write_catalog]


@pytest.fixture
def out_table(monkeypatch, tmp_path):
    (tmp_path / "prytt.toml").write_text(
        '[catalog."out_db.orders"]\npath = "s3://out/orders/"\nformat = "json"\n'
    )
    monkeypatch.chdir(tmp_path)
    config.reset()


@pytest.mark.parametrize("write", GLUE_WRITERS)
def test_glue_writers_in_local_mode_write_under_s3_root(
    glue_context, df, s3_root, out_table, write
):
    write(glue_context, df)

    assert list((s3_root / "out" / "orders").glob("*.json"))


@pytest.mark.parametrize("write", GLUE_WRITERS)
def test_glue_writers_in_passthrough_mode_are_blocked(
    monkeypatch, glue_context, df, out_table, write
):
    set_env(monkeypatch, PRYTT_S3_MODE="passthrough")

    with pytest.raises(PryttS3WriteBlocked) as info:
        write(glue_context, df)

    assert "s3://out/orders/" in str(info.value)
    assert "write_dynamic_frame" not in str(info.value)  # the tip is for plain Spark writes


@pytest.fixture
def no_real_aws(monkeypatch):
    """Unlocked writes go on to Sail: point it at a closed local port with fake
    keys so it fails fast instead of reaching AWS."""
    for name in ("AWS_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prytt-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prytt-test")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


@pytest.fixture
def fresh_session(spark):
    """A new session (its unlock WARN not logged yet), stopped afterwards."""
    from pyspark.sql import SparkSession

    session = SparkSession.builder.create()
    yield session
    session.stop()


def not_blocked(call):
    """Run ``call``; it may fail in Sail (no real S3) but not from the lock."""
    try:
        call()
    except PryttS3WriteBlocked:
        raise
    except Exception:  # Sail's S3 error from the dead endpoint
        pass


@pytest.mark.parametrize("value", ["1", "true", "TRUE"])
def test_unlock_lets_blocked_writes_through_with_one_warn_per_session(
    monkeypatch, fresh_session, glue_context, out_table, no_real_aws, capsys, value
):
    set_env(monkeypatch, PRYTT_S3_MODE="passthrough", PRYTT_ALLOW_S3_WRITES=value)
    df = fresh_session.createDataFrame([(1, "a")], ["id", "v"])
    capsys.readouterr()

    not_blocked(lambda: df.write.parquet("s3://b/k"))
    not_blocked(lambda: df.write.json("s3a://b/k"))
    for write in GLUE_WRITERS:
        not_blocked(lambda write=write: write(glue_context, df))

    assert capsys.readouterr().err.count("S3 writes unlocked") == 1


@pytest.mark.parametrize("value", ["0", "yes", "on", ""])
def test_any_other_unlock_value_stays_locked(monkeypatch, df, value):
    set_env(monkeypatch, PRYTT_ALLOW_S3_WRITES=value)

    with pytest.raises(PryttS3WriteBlocked):
        df.write.parquet("s3://b/k")


def test_iceberg_warehouse_on_s3_is_a_config_error_while_locked(monkeypatch, spark):
    from pyspark.sql import SparkSession

    set_env(monkeypatch, PRYTT_ICEBERG_WAREHOUSE="s3://b/warehouse")

    with pytest.raises(ValueError, match=r"iceberg\.warehouse.*s3://b/warehouse"):
        SparkSession.builder.getOrCreate()


def test_iceberg_warehouse_on_s3_is_allowed_when_unlocked(monkeypatch, spark, capsys):
    from pyspark.sql import SparkSession

    set_env(monkeypatch, PRYTT_ICEBERG_WAREHOUSE="s3://b/warehouse", PRYTT_ALLOW_S3_WRITES="1")

    session = SparkSession.builder.create()
    try:
        assert session.conf.get("spark.sql.warehouse.dir") == "s3://b/warehouse"
    finally:
        session.stop()
    assert "S3 writes unlocked" in capsys.readouterr().err
