"""T10: Iceberg ``glue_catalog``, in fresh interpreters (real ``prytt.pth``).

Sail's catalogs live in memory, so each case runs scripts as subprocesses and
checks what a *later* process (or session) can still see.
"""

import os
import subprocess
import sys
import textwrap

TIMEOUT = 180  # first Sail start after install can take ~30 s

GLUE_BOILERPLATE = """
from awsglue.context import GlueContext
from pyspark.context import SparkContext
spark = GlueContext(SparkContext.getOrCreate()).spark_session
"""


def run_script(cwd, code, extra_env=None):
    script = cwd / "job.py"
    script.write_text(textwrap.dedent(code))
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PRYTT_", "SAIL_"))}
    for name in ("SPARK_REMOTE", "CI", "RUST_LOG"):
        env.pop(name, None)
    env.update(extra_env or {})
    proc = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, env=env, cwd=cwd, timeout=TIMEOUT
    )
    assert proc.returncode == 0, proc.stderr
    return proc


def last_line(proc):
    return proc.stdout.strip().splitlines()[-1]


def test_glue_catalog_table_survives_into_a_new_process(tmp_path):
    run_script(
        tmp_path,
        GLUE_BOILERPLATE
        + """
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.db")
spark.sql("CREATE TABLE glue_catalog.db.tbl (id int, v string) USING iceberg")
spark.sql("INSERT INTO glue_catalog.db.tbl VALUES (1, 'a'), (2, 'b')")
""",
    )

    proc = run_script(
        tmp_path,
        GLUE_BOILERPLATE
        + """
spark.createDataFrame([(2, "B"), (3, "c")], "id int, v string").createOrReplaceTempView("upd")
spark.sql('''MERGE INTO glue_catalog.db.tbl t USING upd s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *''')
print(sorted(tuple(r) for r in spark.table("glue_catalog.db.tbl").collect()))
""",
    )

    assert last_line(proc) == "[(1, 'a'), (2, 'B'), (3, 'c')]"
    table_dir = tmp_path / ".prytt" / "warehouse" / "db" / "tbl"
    assert (table_dir / "metadata").is_dir()
    assert f"MAP  glue_catalog.db.tbl -> {table_dir}" in proc.stderr


def test_every_new_session_gets_the_warehouse_and_the_tables(tmp_path):
    proc = run_script(
        tmp_path,
        """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("CREATE TABLE glue_catalog.default.t USING iceberg AS SELECT 1 AS x")
spark.range(2).write.format("delta").saveAsTable("d")
other = SparkSession.builder.create()
print(other.conf.get("spark.sql.warehouse.dir"))
print(other.table("glue_catalog.default.t").count())
other.stop()
""",
    )

    warehouse = tmp_path / ".prytt" / "warehouse"
    assert proc.stdout.strip().splitlines() == [str(warehouse), "1"]
    assert (warehouse / "default" / "t" / "metadata").is_dir()
    assert not (tmp_path / "spark-warehouse").exists()


def test_tables_of_the_default_spark_catalog_are_not_glue_databases(tmp_path):
    write = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.range(2).write.format("delta").saveAsTable("d")
spark.range(2).write.format("iceberg").saveAsTable("i")
"""
    run_script(tmp_path, write)
    proc = run_script(
        tmp_path,
        """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(sorted(r.name for r in spark.sql("SHOW DATABASES IN glue_catalog").collect()))
""",
    )

    assert last_line(proc) == "['default']"
    assert "WARN" not in proc.stderr


def test_pytest_spark_fixture_has_glue_catalog(pytester, monkeypatch):
    for name in ("SPARK_REMOTE", "CI", "PRYTT_STRICT", "SAIL_CATALOG__LIST", "SAIL_CATALOG__DEFAULT_CATALOG"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PRYTT_ICEBERG_WAREHOUSE", str(pytester.path / "wh"))
    pytester.makepyfile(
        """
        def test_merge(spark):
            spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.db")
            spark.sql("CREATE TABLE glue_catalog.db.t (id int) USING iceberg")
            spark.sql("INSERT INTO glue_catalog.db.t VALUES (1)")
            assert spark.table("glue_catalog.db.t").count() == 1
        """
    )
    result = pytester.runpytest_subprocess("-p", "no:cacheprovider", timeout=TIMEOUT)

    result.assert_outcomes(passed=1)
    assert (pytester.path / "wh" / "db" / "t" / "metadata").is_dir()


def test_glue_catalog_exists_when_the_server_starts_before_pyspark_is_imported(tmp_path):
    proc = run_script(
        tmp_path,
        """
import sys
from prytt import server
server.ensure_running()
assert "pyspark.sql.session" not in sys.modules
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(sorted(c.name for c in spark.catalog.listCatalogs()))
""",
    )

    assert "glue_catalog" in last_line(proc)


def test_a_user_catalog_list_without_glue_catalog_is_kept(tmp_path):
    proc = run_script(
        tmp_path,
        """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(sorted(c.name for c in spark.catalog.listCatalogs()))
""",
        extra_env={
            "SAIL_CATALOG__LIST": '[{name="mine", type="memory", initial_database=["default"]}]',
            "SAIL_CATALOG__DEFAULT_CATALOG": "mine",
        },
    )

    assert last_line(proc) == "['mine', 'system']"
    assert "WARN glue_catalog not available on this server" in proc.stderr
