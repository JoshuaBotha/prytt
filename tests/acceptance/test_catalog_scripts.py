"""T5: catalog mapping from pyproject.toml, in a fresh interpreter (real ``prytt.pth``)."""

import os
import subprocess
import sys
import textwrap

import pytest

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


def run_script(tmp_path, code, extra_toml="", remote=None, ok=True):
    (tmp_path / "pyproject.toml").write_text(PYPROJECT + extra_toml)
    orders = tmp_path / "s3" / "raw" / "orders"
    orders.mkdir(parents=True, exist_ok=True)
    (orders / "a.json").write_text('{"id": 1, "customer": 10}\n{"id": 2, "customer": 20}\n')
    customers = tmp_path / "s3" / "raw" / "customers"
    customers.mkdir(parents=True, exist_ok=True)
    (customers / "a.csv").write_text("customer,name\n10,ann\n20,bob\n")
    script = tmp_path / "job.py"
    script.write_text(textwrap.dedent(code))
    env = {k: v for k, v in os.environ.items() if not k.startswith("PRYTT_")}
    for name in ("SPARK_REMOTE", "CI", "RUST_LOG"):
        env.pop(name, None)
    if remote:
        env["SPARK_REMOTE"] = remote
    proc = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        env=env,
        cwd=tmp_path,
        timeout=TIMEOUT,
    )
    if ok:
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
    orders = (tmp_path / "s3" / "raw" / "orders").resolve().as_posix()
    assert (
        f"MAP  raw_db.orders -> {orders}/ (table raw_db.orders, view raw_db_orders)" in proc.stderr
    )


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


@pytest.fixture(scope="module")
def external_server():
    """A Sail server prytt did not start, as a script with ``SPARK_REMOTE`` set sees it."""
    from pysail.spark import SparkConnectServer

    sail = SparkConnectServer(ip="127.0.0.1", port=0)
    sail.start(background=True)
    host, port = sail.listening_address
    yield f"sc://{host}:{port}"
    sail.stop()


READ_BY_GLUE_NAME = """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    print(sorted(r.id for r in spark.sql("select id from raw_db.orders").collect()))
    """


def test_mapped_table_reads_by_glue_name_on_external_server(tmp_path, external_server):
    proc = run_script(tmp_path, READ_BY_GLUE_NAME, remote=external_server)

    assert f"using external server SPARK_REMOTE={external_server}" in proc.stderr
    assert "sail at" not in proc.stderr
    assert proc.stdout.strip().splitlines()[-1] == "[1, 2]"


def test_registration_repeats_without_error_on_external_server(tmp_path, external_server):
    first = run_script(tmp_path, READ_BY_GLUE_NAME, remote=external_server)
    second = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        other = SparkSession.builder.create()
        print(sorted(r.id for r in other.table("raw_db.orders").collect()))
        other.stop()
        print(sorted(r.id for r in spark.sql("select id from raw_db.orders").collect()))
        """,
        remote=external_server,
    )

    assert first.stdout.strip().splitlines()[-1] == "[1, 2]"
    assert second.stdout.strip().splitlines()[-2:] == ["[1, 2]", "[1, 2]"]
    assert "WARN catalog table raw_db.orders skipped" not in first.stderr + second.stderr


HINT = '[tool.prytt.catalog."sales_db.refunds"]'


def test_unmapped_table_in_sql_exits_with_toml_hint(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        spark.sql("select * from sales_db.refunds").collect()
        """,
        ok=False,
    )

    assert proc.returncode != 0
    assert "TABLE_OR_VIEW_NOT_FOUND" in proc.stderr
    assert HINT in proc.stderr
    assert 'path = "s3://<bucket>/<prefix>/refunds/"' in proc.stderr


def test_unmapped_table_in_spark_table_exits_with_toml_hint(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        spark.table("sales_db.refunds").show()
        """,
        ok=False,
    )

    assert proc.returncode != 0
    assert "TABLE_OR_VIEW_NOT_FOUND" in proc.stderr
    assert HINT in proc.stderr


def test_script_created_table_errors_pass_through_without_hint(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        from pyspark.errors import AnalysisException
        spark = SparkSession.builder.getOrCreate()
        spark.sql("create database scratch_db")
        spark.sql("create table scratch_db.x (a int) using parquet")
        for query in ("select no_such_column from scratch_db.x", "select * from scratch_db.x join no_such_view"):
            try:
                spark.sql(query).collect()
            except AnalysisException as exc:
                print("hint" if "prytt" in str(exc) + str(getattr(exc, "__notes__", "")) else "no hint")
        """,
    )

    assert proc.stdout.strip().splitlines()[-2:] == ["no hint", "no hint"]


def test_on_missing_empty_does_not_apply_to_sql(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        spark.sql("select * from sales_db.refunds").collect()
        """,
        extra_toml='\n[tool.prytt.catalog]\non_missing = "empty"\n',
        ok=False,
    )

    assert proc.returncode != 0
    assert HINT in proc.stderr
    assert 'on_missing = "empty" to read it' not in proc.stderr


def test_column_reference_shaped_like_a_mapped_name_is_left_alone(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        print(spark.sql("select RAW_DB.ORDERS from (select named_struct('ORDERS', 1) as RAW_DB)").columns)
        """,
    )

    assert proc.stdout.strip().splitlines()[-1] == "['ORDERS']"


def test_missing_view_with_qualified_column_gets_no_hint(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        spark.sql("select t.x from x t").collect()
        """,
        ok=False,
    )

    assert "TABLE_OR_VIEW_NOT_FOUND" in proc.stderr
    assert "prytt: catalog table" not in proc.stderr


def test_two_unmapped_tables_with_one_name_get_no_misleading_hint(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        spark.sql("select * from a_db.orders join b_db.orders").collect()
        """,
        ok=False,
    )

    assert "TABLE_OR_VIEW_NOT_FOUND" in proc.stderr
    # Spark's error names only ``orders``, so naming one database could be wrong.
    assert ('catalog."a_db.orders"' in proc.stderr) == ('catalog."b_db.orders"' in proc.stderr)


def test_backtick_quoted_unmapped_name_in_spark_table_gets_toml_hint(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        print(sorted(r.id for r in spark.table("`raw_db`.`orders`").collect()))
        spark.table("`sales_db`.`refunds`").show()
        """,
        ok=False,
    )

    assert proc.stdout.strip().splitlines()[-1] == "[1, 2]"
    assert HINT in proc.stderr


def test_table_whose_registration_fails_leaves_no_view_behind(tmp_path):
    proc = run_script(
        tmp_path,
        """
        from pyspark.errors import AnalysisException
        from pyspark.sql.connect.session import SparkSession as Connect

        real_sql = Connect.sql

        def failing_create_table(self, query, *args, **kwargs):
            if query.lstrip().upper().startswith("CREATE TABLE"):
                raise AnalysisException(message="create table refused")
            return real_sql(self, query, *args, **kwargs)

        Connect.sql = failing_create_table  # the server refuses the catalog DDL

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        try:
            spark.sql("select * from raw_db_orders").collect()
            print("view registered")
        except AnalysisException:
            print("no view")
        """,
    )

    assert "WARN catalog table raw_db.orders skipped: create table refused" in proc.stderr
    assert proc.stdout.strip().splitlines()[-1] == "no view"
