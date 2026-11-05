"""Catalog mapping through GlueContext and temp views.

Live Sail session; one GlueContext for the module.
"""

import json

import pytest

from prytt import config, paths


@pytest.fixture(scope="module")
def glue_context():
    from awsglue.context import GlueContext

    return GlueContext()


@pytest.fixture(autouse=True)
def project(monkeypatch, tmp_path):
    """cwd is a project dir with ``prytt.toml``; s3 maps under ``tmp/s3``."""
    for name in (
        "CI",
        "PRYTT_STRICT",
        "PRYTT_LOG_LEVEL",
        "PRYTT_S3_MODE",
        "PRYTT_S3_ROOT",
        "PRYTT_CATALOG_ON_MISSING",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "prytt.toml").write_text(
        """
[s3]
root = "s3"

[catalog."raw_db.orders"]
path = "s3://raw/orders/"
format = "json"

[catalog."curated_db.orders"]
path = "s3://curated/orders/"
format = "parquet"
"""
    )
    config.reset()
    yield tmp_path
    config.reset()


def put_orders(project, *ids):
    out = project / "s3" / "raw" / "orders"
    out.mkdir(parents=True, exist_ok=True)
    (out / "part-1.json").write_text("\n".join(json.dumps({"id": i, "item": f"x{i}"}) for i in ids))


def rows(df, *cols):
    return sorted(tuple(r[c] for c in cols) for r in df.collect())


def test_mapped_table_reads_as_dynamic_frame(glue_context, project):
    put_orders(project, 1, 2)

    dyf = glue_context.create_dynamic_frame.from_catalog(database="raw_db", table_name="orders")

    assert rows(dyf.toDF(), "id", "item") == [(1, "x1"), (2, "x2")]


def test_mapped_table_reads_as_data_frame_via_flat_spelling(glue_context, project):
    put_orders(project, 3)

    df = glue_context.create_data_frame_from_catalog("raw_db", "orders")

    assert rows(df, "id") == [(3,)]


def test_write_to_mapped_table_then_read_back(glue_context, project):
    spark = glue_context.spark_session
    df = spark.createDataFrame([(1, "za")], "id int, country string")

    glue_context.write_data_frame.from_catalog(frame=df, database="curated_db", table_name="orders")

    back = spark.read.parquet(str(project / "s3" / "curated" / "orders"))
    assert rows(back, "id", "country") == [(1, "za")]


def test_missing_table_raises_with_the_toml_to_add(glue_context):
    with pytest.raises(
        paths.CatalogTableNotFound, match=r'\[tool\.prytt\.catalog\."raw_db\.missing"\]'
    ):
        glue_context.create_dynamic_frame.from_catalog(database="raw_db", table_name="missing")


def test_missing_table_reads_empty_when_on_missing_is_empty(glue_context, monkeypatch, capsys):
    monkeypatch.setenv("PRYTT_CATALOG_ON_MISSING", "empty")
    config.reset()

    dyf = glue_context.create_dynamic_frame.from_catalog(database="raw_db", table_name="missing")

    assert dyf.count() == 0
    assert "raw_db.missing" in capsys.readouterr().err


def test_missing_table_write_still_raises_when_on_missing_is_empty(glue_context, monkeypatch):
    monkeypatch.setenv("PRYTT_CATALOG_ON_MISSING", "empty")
    config.reset()
    df = glue_context.spark_session.createDataFrame([(1,)], "id int")

    with pytest.raises(paths.CatalogTableNotFound):
        glue_context.write_data_frame.from_catalog(
            frame=df, database="raw_db", table_name="missing"
        )


def test_unknown_on_missing_value_is_rejected(glue_context, monkeypatch):
    monkeypatch.setenv("PRYTT_CATALOG_ON_MISSING", "ignore")
    config.reset()

    with pytest.raises(ValueError, match="on_missing"):
        glue_context.create_dynamic_frame.from_catalog(database="raw_db", table_name="missing")


def test_mapped_tables_are_temp_views_on_each_new_session(glue_context, project, capsys):
    from pyspark.sql import SparkSession

    put_orders(project, 5, 6)

    spark = SparkSession.builder.create()  # a fresh session runs the on_new_session hooks
    try:
        assert rows(spark.sql("select id from raw_db_orders"), "id") == [(5,), (6,)]
    finally:
        spark.stop()  # don't leave it for the GC to collect mid-query in a later test
    err = capsys.readouterr().err
    assert "(table raw_db.orders, view raw_db_orders, mode=local (global))" in err  # MAP
    assert "WARN catalog table curated_db.orders skipped" in err  # no data yet: not a crash
