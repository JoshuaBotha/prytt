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


CUSTOMERS = """
[tool.prytt.catalog."raw_db.customers"]
path = "s3://raw/customers/"
format = "csv"
options = { withHeader = true }
"""


def run_script(tmp_path, code, extra_toml=""):
    (tmp_path / "pyproject.toml").write_text(PYPROJECT + extra_toml)
    orders = tmp_path / "s3" / "raw" / "orders"
    orders.mkdir(parents=True)
    (orders / "a.json").write_text('{"id": 1, "customer": 10}\n{"id": 2, "customer": 20}\n')
    customers = tmp_path / "s3" / "raw" / "customers"
    customers.mkdir(parents=True)
    (customers / "a.csv").write_text("customer,name\n10,ann\n20,bob\n")
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
    orders = (tmp_path / "s3" / "raw" / "orders").resolve()
    assert f"MAP  raw_db.orders -> {orders}/ (table raw_db.orders, view raw_db_orders)" in proc.stderr


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


def test_mapped_table_reads_by_glue_name_through_sql_and_table(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        print(sorted(r.id for r in spark.sql("select id from raw_db.orders").collect()))
        print(sorted(r.id for r in spark.table("raw_db.orders").collect()))
        """,
    )

    assert proc.stdout.strip().splitlines()[-2:] == ["[1, 2]", "[1, 2]"]


def test_join_across_two_mapped_tables(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        rows = spark.sql(
            "select o.id, c.name from raw_db.orders o join raw_db.customers c on o.customer = c.customer"
        ).collect()
        print(sorted((r.id, r.name) for r in rows))
        """,
        extra_toml=CUSTOMERS,
    )

    assert proc.stdout.strip().splitlines()[-1] == "[(1, 'ann'), (2, 'bob')]"


def test_mixed_case_glue_name_resolves_like_lowercase(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        print(sorted(r.id for r in spark.sql("select id from Raw_DB.Orders").collect()))
        print(sorted(r.id for r in spark.table("RAW_DB.orders").collect()))
        """,
    )

    assert proc.stdout.strip().splitlines()[-2:] == ["[1, 2]", "[1, 2]"]


def test_mapped_table_with_no_data_is_skipped_with_warn(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        print(sorted(r.id for r in spark.table("raw_db.orders").collect()))
        """,
        extra_toml="""
[tool.prytt.catalog."curated_db.daily"]
path = "s3://curated/daily/"
format = "parquet"
""",
    )

    assert proc.stdout.strip().splitlines()[-1] == "[1, 2]"
    assert "WARN catalog table curated_db.daily skipped:" in proc.stderr
    assert "MAP  curated_db.daily" not in proc.stderr


def test_non_spark_script_loads_nothing_from_spark_or_the_catalog(tmp_path):
    proc = run_script(
        tmp_path,
        """
        import json, sys
        print(sorted(m for m in ("pyspark", "pysail", "prytt.paths", "prytt.config") if m in sys.modules))
        """,
    )

    assert proc.stdout.strip() == "[]"
    assert proc.stderr == ""
