"""Per-table S3 mode on a live Sail session (pytest plugin fixtures).

A mapped table's ``mode`` (``local`` | ``passthrough``) beats the global
``s3.mode``. Passthrough tables point Sail at a dead local endpoint with fake
keys (``no_real_aws``), so nothing here reaches AWS.
"""

import json

import pytest

from prytt import PryttS3WriteBlocked, config


@pytest.fixture(autouse=True)
def project(monkeypatch, tmp_path, s3_root):
    """cwd is a project dir; s3 maps under ``s3_root``; Sail can't reach AWS."""
    for name in (
        "CI",
        "PRYTT_STRICT",
        "PRYTT_LOG_LEVEL",
        "PRYTT_S3_MODE",
        "PRYTT_ALLOW_S3_WRITES",
        "AWS_PROFILE",
        "AWS_SESSION_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")  # passthrough's region check
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prytt-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prytt-test")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.chdir(tmp_path)
    config.reset()
    yield tmp_path
    config.reset()


def configure(project, monkeypatch, toml, **env):
    (project / "prytt.toml").write_text(toml)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    config.reset()


# raw_db.orders follows the global mode; ref_db.prices is pinned to passthrough.
MIXED = """
[s3]
mode = "local"

[catalog."raw_db.orders"]
path = "s3://raw/orders/"
format = "json"

[catalog."ref_db.prices"]
path = "s3://ref/prices/"
format = "json"
mode = "passthrough"
"""

# raw_db.orders is pinned to local; the global mode comes from the caller.
LOCAL_TABLE = """
[catalog."raw_db.orders"]
path = "s3://raw/orders/"
format = "json"
mode = "local"
"""


def put_orders(s3_root, *ids):
    out = s3_root / "raw" / "orders"
    out.mkdir(parents=True, exist_ok=True)
    (out / "part-1.json").write_text("\n".join(json.dumps({"id": i}) for i in ids))


def rows(df):
    return sorted(r["id"] for r in df.collect())


@pytest.fixture
def new_session(spark):
    """Build a fresh session (runs the registration hooks); stopped afterwards."""
    from pyspark.sql import SparkSession

    sessions = []

    def build():
        sessions.append(SparkSession.builder.create())
        return sessions[-1]

    yield build
    for session in sessions:
        session.stop()


def test_passthrough_table_keeps_its_s3_location_under_global_local(
    project, monkeypatch, s3_root, new_session, capsys
):
    configure(project, monkeypatch, MIXED)
    put_orders(s3_root, 1)

    new_session()

    err = capsys.readouterr().err
    orders = (s3_root / "raw" / "orders").as_posix()
    assert f"MAP  raw_db.orders -> {orders}/ " in err
    # Not mapped and not checked for local data: Sail is asked for s3://ref/prices/
    # (and fails on the dead endpoint).
    assert "catalog table ref_db.prices" in err
    assert "s3://ref/prices/" in err
    assert (s3_root / "ref").as_posix() not in err
    assert "no data at" not in err


@pytest.mark.parametrize("source", ["file", "env"])
def test_local_table_reads_local_rows_under_global_passthrough(
    project, monkeypatch, s3_root, new_session, glue_context, source
):
    if source == "file":
        configure(project, monkeypatch, '[s3]\nmode = "passthrough"\n' + LOCAL_TABLE)
    else:
        configure(project, monkeypatch, LOCAL_TABLE, PRYTT_S3_MODE="passthrough")
    put_orders(s3_root, 1, 2)

    spark = new_session()

    assert rows(spark.sql("SELECT id FROM raw_db.orders")) == [1, 2]
    assert rows(spark.table("raw_db.orders")) == [1, 2]
    dyf = glue_context.create_dynamic_frame.from_catalog(database="raw_db", table_name="orders")
    assert rows(dyf.toDF()) == [1, 2]


def test_map_line_shows_each_tables_mode_and_its_source(
    project, monkeypatch, s3_root, new_session, capsys
):
    configure(
        project,
        monkeypatch,
        MIXED + '\n[catalog."raw_db.items"]\npath = "s3://raw/items/"\nformat = "json"\n'
        'mode = "local"\n',
    )
    put_orders(s3_root, 1)
    (s3_root / "raw" / "items").mkdir(parents=True)
    (s3_root / "raw" / "items" / "a.json").write_text('{"id": 1}\n')

    new_session()

    err = capsys.readouterr().err
    assert "(table raw_db.orders, view raw_db_orders, mode=local (global))" in err
    assert "(table raw_db.items, view raw_db_items, mode=local (table))" in err


@pytest.fixture
def fake_s3_registration(monkeypatch):
    """Let ``s3://`` tables register without S3: their read becomes a one-row
    local frame and their ``CREATE TABLE`` is skipped. Local tables are untouched."""
    from prytt import io, paths

    real_read, real_create = io.read_df, paths._create_table

    def read_df(spark, uris, format, format_options=None, mode=None):
        if str(uris).startswith("s3://"):
            return spark.createDataFrame([(1,)], "id int")
        return real_read(spark, uris, format, format_options, mode=mode)

    def create_table(spark, database, table_name, fmt, options, location):
        if not location.startswith("s3://"):
            real_create(spark, database, table_name, fmt, options, location)

    monkeypatch.setattr(io, "read_df", read_df)
    monkeypatch.setattr(paths, "_create_table", create_table)


@pytest.mark.parametrize(
    ("toml", "source"),
    [
        (MIXED, "table"),
        (
            '[s3]\nmode = "passthrough"\n[catalog."ref_db.prices"]\n'
            'path = "s3://ref/prices/"\nformat = "json"\n',
            "global",
        ),
    ],
    ids=["table", "global"],
)
def test_map_line_for_a_passthrough_table_keeps_s3_and_shows_its_source(
    project, monkeypatch, new_session, fake_s3_registration, capsys, toml, source
):
    configure(project, monkeypatch, toml)

    new_session()

    assert (
        "MAP  ref_db.prices -> s3://ref/prices/ (table ref_db.prices, view ref_db_prices, "
        f"mode=passthrough ({source}))" in capsys.readouterr().err
    )


def test_explicit_table_mode_wins_over_prytt_s3_mode(
    project, monkeypatch, s3_root, new_session, capsys
):
    configure(project, monkeypatch, LOCAL_TABLE, PRYTT_S3_MODE="passthrough")
    put_orders(s3_root, 1)

    new_session()

    orders = (s3_root / "raw" / "orders").as_posix()
    assert (
        f"MAP  raw_db.orders -> {orders}/ (table raw_db.orders, view raw_db_orders, "
        "mode=local (table))" in capsys.readouterr().err
    )


def write_catalog(glue_context, spark, database, table_name):
    from awsglue.dynamicframe import DynamicFrame

    df = spark.createDataFrame([(7,)], "id int")
    glue_context.write_dynamic_frame.from_catalog(
        frame=DynamicFrame.fromDF(df, glue_context, "out"),
        database=database,
        table_name=table_name,
    )


def test_glue_catalog_write_to_passthrough_table_under_global_local_is_blocked(
    project, monkeypatch, spark, glue_context
):
    configure(project, monkeypatch, MIXED)

    with pytest.raises(PryttS3WriteBlocked, match="s3://ref/prices/"):
        write_catalog(glue_context, spark, "ref_db", "prices")


def test_glue_catalog_write_to_local_table_under_global_passthrough_writes_under_s3_root(
    project, monkeypatch, s3_root, spark, glue_context
):
    configure(project, monkeypatch, LOCAL_TABLE, PRYTT_S3_MODE="passthrough")

    write_catalog(glue_context, spark, "raw_db", "orders")

    assert rows(spark.read.json((s3_root / "raw" / "orders").as_posix())) == [7]


@pytest.mark.xfail(reason="needs tickets 02/03", strict=False)
def test_save_as_table_into_passthrough_table_is_blocked(project, monkeypatch, spark):
    configure(project, monkeypatch, MIXED)
    df = spark.createDataFrame([(7,)], "id int")

    with pytest.raises(PryttS3WriteBlocked):
        df.write.mode("append").saveAsTable("ref_db.prices")


@pytest.mark.xfail(reason="needs tickets 02/03", strict=False)
def test_sql_insert_into_passthrough_table_is_blocked(project, monkeypatch, new_session):
    configure(project, monkeypatch, MIXED)
    spark = new_session()

    with pytest.raises(PryttS3WriteBlocked):
        spark.sql("INSERT INTO ref_db.prices VALUES (7)").collect()


def test_from_options_follows_the_global_mode_not_the_tables(
    project, monkeypatch, s3_root, spark, glue_context
):
    from awsglue.dynamicframe import DynamicFrame

    # ref_db.prices is pinned to passthrough, but from_options on its path is local.
    configure(project, monkeypatch, MIXED)
    df = spark.createDataFrame([(3,)], "id int")
    glue_context.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(df, glue_context, "out"),
        connection_type="s3",
        connection_options={"path": "s3://ref/prices/"},
        format="json",
    )
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3", connection_options={"paths": ["s3://ref/prices/"]}, format="json"
    )
    assert rows(dyf.toDF()) == [3]

    # raw_db.orders is pinned to local, but from_options on its path is passthrough.
    configure(project, monkeypatch, LOCAL_TABLE, PRYTT_S3_MODE="passthrough")
    with pytest.raises(PryttS3WriteBlocked, match="s3://raw/orders/"):
        glue_context.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(df, glue_context, "out"),
            connection_type="s3",
            connection_options={"path": "s3://raw/orders/"},
            format="json",
        )
