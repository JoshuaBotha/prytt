"""prytt.formats: Glue format names and format_options -> Spark (spec §9)."""

import pytest

from prytt import config, formats
from prytt.stub import GlueLocalUnsupported


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    config.reset()
    yield
    config.reset()


@pytest.mark.parametrize(
    "glue, spark",
    [
        ("glueparquet", "parquet"),
        ("parquet", "parquet"),
        ("csv", "csv"),
        ("json", "json"),
        ("delta", "delta"),
        ("iceberg", "iceberg"),
        ("JSON", "json"),
    ],
)
def test_format_names_map_to_spark(glue, spark):
    assert formats.spark_format(glue) == spark


def test_orc_warns_and_raises_unsupported(capsys):
    with pytest.raises(GlueLocalUnsupported, match="ORC"):
        formats.spark_format("orc")

    assert "WARN" in capsys.readouterr().err


def test_missing_format_is_an_error():
    with pytest.raises(ValueError, match="format"):
        formats.spark_format(None)


def test_unknown_format_is_passed_to_sail_with_a_warning(capsys):
    assert formats.spark_format("avro") == "avro"
    assert "WARN" in capsys.readouterr().err


def test_common_glue_options_are_renamed():
    opts = formats.spark_options("csv", {"withHeader": True, "separator": "|", "quoteChar": "'", "escaper": "\\"})

    assert opts == {"header": True, "sep": "|", "quote": "'", "escape": "\\", "inferSchema": True}


def test_json_multiline_lowercase_maps_to_multiLine():
    assert formats.spark_options("json", {"multiline": True}) == {"multiLine": True}


def test_csv_reads_infer_schema_unless_set():
    assert formats.spark_options("csv", {}) == {"inferSchema": True}
    assert formats.spark_options("csv", {"inferSchema": False}) == {"inferSchema": False}
    assert formats.spark_options("csv", {}, write=True) == {}


def test_write_header_maps_to_header_on_write():
    assert formats.spark_options("csv", {"writeHeader": True}, write=True) == {"header": True}


def test_unmapped_options_pass_through_and_are_logged(capsys):
    assert formats.spark_options("parquet", {"compression": "snappy"}) == {"compression": "snappy"}
    assert "compression" in capsys.readouterr().err


def test_glue_only_options_are_dropped_with_a_warning(capsys):
    assert formats.spark_options("parquet", {"useGlueParquetWriter": True, "blockSize": 1}) == {}
    err = capsys.readouterr().err
    assert "useGlueParquetWriter" in err and "WARN" in err


def test_none_options_are_empty():
    assert formats.spark_options("json", None) == {}
