"""awsglue package layout: imports Glue scripts use must resolve."""

import subprocess
import sys

import pytest

from prytt import config


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    config.reset()
    yield
    config.reset()


def test_package_exports_dynamic_frame():
    import awsglue
    from awsglue.dynamicframe import DynamicFrame

    assert awsglue.DynamicFrame is DynamicFrame


def test_importing_awsglue_does_not_import_pyspark():
    code = "import sys, awsglue, awsglue.context, awsglue.dynamicframe; print('pyspark' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)

    assert out.stdout.strip() == "False"


def test_gluetypes_has_common_type_names():
    from awsglue import gluetypes

    for name in ("StringType", "IntegerType", "LongType", "DoubleType", "BooleanType", "StructType", "ArrayType", "Field"):
        assert hasattr(gluetypes, name), name

    field = gluetypes.Field("id", gluetypes.IntegerType())
    assert field.name == "id"


def test_stub_modules_import_and_log(capsys):
    from awsglue.data_sink import DataSink
    from awsglue.data_source import DataSource

    sink = DataSink(None, None)
    assert sink.setFormat("json") is sink
    assert DataSource(None, None).getFrame() is not None
    assert "STUB DataSink.setFormat('json')" in capsys.readouterr().err
