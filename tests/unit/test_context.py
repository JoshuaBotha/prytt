"""awsglue.context.GlueContext on a live Sail session.

One Spark session for the whole module: it is slow to start.
"""

import json

import pytest

from prytt import config
from prytt.stub import GlueLocalUnsupported


@pytest.fixture(scope="module")
def glue_context():
    from pyspark.context import SparkContext

    from awsglue.context import GlueContext

    return GlueContext(SparkContext.getOrCreate())


@pytest.fixture(autouse=True)
def s3_root(monkeypatch, tmp_path):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL", "PRYTT_S3_MODE"):
        monkeypatch.delenv(name, raising=False)
    root = tmp_path / "s3"
    monkeypatch.setenv("PRYTT_S3_ROOT", str(root))
    config.reset()
    yield root
    config.reset()


def put(root, key, text):
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def rows(df, *cols):
    return sorted(tuple(r[c] for c in cols) for r in df.collect())


def test_read_json_from_mapped_s3_path(glue_context, s3_root):
    put(s3_root, "raw/orders/part-1.json", "\n".join(json.dumps({"id": i, "item": f"x{i}"}) for i in (1, 2)))

    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://raw/orders/"]},
        format="json",
    )

    assert rows(dyf.toDF(), "id", "item") == [(1, "x1"), (2, "x2")]


def test_read_csv_with_glue_header_and_separator_options(glue_context, s3_root):
    put(s3_root, "raw/people/p.csv", "name|age\nann|31\nbob|42\n")

    df = glue_context.create_data_frame.from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://raw/people/p.csv"]},
        format="csv",
        format_options={"withHeader": True, "separator": "|"},
    )

    assert rows(df, "name", "age") == [("ann", 31), ("bob", 42)]


def write_orders(glue_context, frame):
    glue_context.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        connection_options={"path": "s3://curated/orders/", "partitionKeys": ["country"]},
        format="glueparquet",
    )


def test_parquet_write_appends_and_partitions(glue_context, s3_root):
    from awsglue.dynamicframe import DynamicFrame

    spark = glue_context.spark_session
    df = spark.createDataFrame([(1, "za"), (2, "uk")], "id int, country string")
    frame = DynamicFrame.fromDF(df, glue_context, "orders")

    write_orders(glue_context, frame)
    write_orders(glue_context, frame)

    out = s3_root / "curated" / "orders"
    assert sorted(p.name for p in out.iterdir() if p.is_dir()) == ["country=uk", "country=za"]
    back = spark.read.parquet(str(out))
    assert rows(back, "id", "country") == [(1, "za"), (1, "za"), (2, "uk"), (2, "uk")]


def test_write_data_frame_takes_a_dataframe(glue_context, s3_root):
    spark = glue_context.spark_session
    df = spark.createDataFrame([(1, "a")], "id int, v string")

    glue_context.write_data_frame.from_options(
        frame=df,
        connection_type="s3",
        connection_options={"path": "s3://out/t/"},
        format="json",
    )

    assert rows(spark.read.json(str(s3_root / "out" / "t")), "id", "v") == [(1, "a")]


def test_orc_read_raises_unsupported(glue_context):
    with pytest.raises(GlueLocalUnsupported, match="ORC"):
        glue_context.create_dynamic_frame.from_options(
            connection_type="s3", connection_options={"paths": ["s3://b/k"]}, format="orc"
        )


def test_non_s3_connection_raises_unsupported(glue_context):
    with pytest.raises(GlueLocalUnsupported, match="jdbc"):
        glue_context.create_dynamic_frame.from_options(connection_type="jdbc", connection_options={})


def test_flat_spelling_reads_too(glue_context, s3_root):
    put(s3_root, "raw/flat/a.json", '{"id": 7}\n')

    dyf = glue_context.create_dynamic_frame_from_options(
        "s3", {"paths": ["s3://raw/flat/"]}, format="json"
    )

    assert dyf.count() == 1


def test_spark_session_attributes_are_delegated(glue_context):
    assert glue_context.sql("select 1 as one").collect()[0]["one"] == 1
    assert glue_context.spark_session is glue_context.sparkSession


def test_unknown_methods_are_logged_stubs(glue_context, capsys):
    assert glue_context.purge_s3_path("s3://b/k", {"retentionPeriod": 0}) is glue_context

    assert "STUB GlueContext.purge_s3_path('s3://b/k'" in capsys.readouterr().err


def test_get_logger_has_info_warn_error(glue_context):
    logger = glue_context.get_logger()

    for name in ("info", "warn", "error"):
        assert callable(getattr(logger, name))
    logger.warn("hello")


def test_from_catalog_goes_through_the_catalog_seam(glue_context, monkeypatch, s3_root):
    from prytt import paths

    put(s3_root, "lake/orders/o.json", '{"id": 1}\n')
    seen = []

    def resolve(database, table_name):
        seen.append((database, table_name))
        return paths.CatalogTable("s3://lake/orders/", "json", {})

    monkeypatch.setattr(paths, "resolve_table", resolve)

    dyf = glue_context.create_dynamic_frame.from_catalog(database="raw_db", table_name="orders")

    assert seen == [("raw_db", "orders")]
    assert rows(dyf.toDF(), "id") == [(1,)]
