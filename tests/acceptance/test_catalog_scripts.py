"""T5: catalog mapping from pyproject.toml, in a fresh interpreter (real ``prytt.pth``)."""

import os
import subprocess
import sys
import textwrap

TIMEOUT = 180  # first Sail start after install can take ~30 s

PYPROJECT = """
[project]
name = "some-glue-job"
version = "0"

[tool.prytt.s3]
root = "s3"

[tool.prytt.catalog."raw_db.orders"]
path = "s3://raw/orders/"
format = "json"
"""


def run_script(tmp_path, code):
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    orders = tmp_path / "s3" / "raw" / "orders"
    orders.mkdir(parents=True)
    (orders / "a.json").write_text('{"id": 1}\n{"id": 2}\n')
    script = tmp_path / "job.py"
    script.write_text(textwrap.dedent(code))
    env = {k: v for k, v in os.environ.items() if not k.startswith("PRYTT_")}
    for name in ("SPARK_REMOTE", "CI", "RUST_LOG"):
        env.pop(name, None)
    proc = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, env=env, cwd=tmp_path, timeout=TIMEOUT
    )
    assert proc.returncode == 0, proc.stderr
    return proc


def test_spark_only_script_sees_mapped_tables_as_temp_views(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        print(sorted(r.id for r in spark.sql("select id from raw_db_orders").collect()))
        """,
    )

    assert proc.stdout.strip().splitlines()[-1] == "[1, 2]"
    assert "MAP  catalog raw_db.orders -> temp view raw_db_orders" in proc.stderr


def test_glue_script_reads_from_catalog(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext
        glueContext = GlueContext(SparkContext())
        dyf = glueContext.create_dynamic_frame.from_catalog(database="raw_db", table_name="orders")
        print(sorted(r.id for r in dyf.toDF().collect()))
        """,
    )

    assert proc.stdout.strip().splitlines()[-1] == "[1, 2]"
