"""prytt.iceberg: the ``spark.sql`` CREATE DATABASE rewrite.

A fake session records the SQL that reaches the server, so no Sail is needed.
"""

from types import SimpleNamespace

import pytest

from prytt import config, iceberg


class FakeSpark:
    def __init__(self):
        self.queries = []
        self.conf = SimpleNamespace(set=lambda key, value: None)
        self.catalog = SimpleNamespace(
            listCatalogs=lambda: [
                SimpleNamespace(name="sail"),
                SimpleNamespace(name="glue_catalog"),
            ]
        )

    def sql(self, sqlQuery, *args, **kwargs):
        self.queries.append(sqlQuery)


@pytest.fixture
def warehouse(monkeypatch, tmp_path):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PRYTT_ICEBERG_WAREHOUSE", str(tmp_path / "wh"))
    config.reset()
    yield (tmp_path / "wh").resolve()
    config.reset()


@pytest.fixture
def spark(warehouse, capsys):
    session = FakeSpark()
    iceberg.setup_session(session)
    session.queries.clear()
    capsys.readouterr()
    return session


def sent(spark, query):
    spark.sql(query)
    return spark.queries[-1]


def test_create_database_without_location_gets_the_warehouse_location(spark, warehouse, capsys):
    assert sent(spark, "CREATE DATABASE glue_catalog.sales") == (
        f"CREATE DATABASE glue_catalog.sales LOCATION '{warehouse.as_posix()}/sales'"
    )
    assert (
        capsys.readouterr().err
        == f"[prytt] MAP  glue_catalog database sales -> {warehouse.as_posix()}/sales\n"
    )


def test_leading_comments_and_trailing_semicolon_are_handled(spark, warehouse):
    query = "-- make the db\n/* once */ create schema if not exists glue_catalog.`raw db`;\n"

    assert sent(spark, query) == (
        f"-- make the db\n/* once */ create schema if not exists glue_catalog.`raw db` LOCATION '{warehouse.as_posix()}/raw db'"
    )


def test_location_goes_before_a_trailing_comment(spark, warehouse):
    assert sent(spark, "CREATE DATABASE glue_catalog.sales -- owned by data eng") == (
        f"CREATE DATABASE glue_catalog.sales LOCATION '{warehouse.as_posix()}/sales' -- owned by data eng"
    )


def test_location_and_semicolons_inside_quotes_do_not_count(spark, warehouse):
    query = "CREATE DATABASE glue_catalog.sales COMMENT 'LOCATION tbd; ask ops'"

    assert sent(spark, query) == f"{query} LOCATION '{warehouse.as_posix()}/sales'"


def test_an_explicit_location_is_kept_with_a_warning(spark, capsys):
    query = "CREATE DATABASE glue_catalog.sales LOCATION '/data/sales'"

    assert sent(spark, query) == query
    err = capsys.readouterr().err
    assert err.startswith("[prytt] WARN glue_catalog database sales has its own LOCATION")


def test_the_warehouse_location_itself_is_kept_silently(spark, warehouse, capsys):
    query = f"CREATE DATABASE IF NOT EXISTS glue_catalog.`sales` LOCATION '{warehouse.as_posix()}/sales'"

    assert sent(spark, query) == query
    assert capsys.readouterr().err == ""


def test_databases_on_disk_are_recreated_without_warnings(warehouse, capsys):
    (warehouse / "sales").mkdir(parents=True)
    session = FakeSpark()

    iceberg.setup_session(session)

    assert (
        f"CREATE DATABASE IF NOT EXISTS glue_catalog.`sales` LOCATION '{warehouse.as_posix()}/sales'"
        in session.queries
    )
    assert "WARN" not in capsys.readouterr().err


def test_multi_statement_sql_is_not_rewritten_and_warns(spark, capsys):
    query = "CREATE DATABASE glue_catalog.a; CREATE TABLE glue_catalog.a.t (id int) USING iceberg"

    assert sent(spark, query) == query
    assert "[prytt] WARN glue_catalog CREATE DATABASE not rewritten" in capsys.readouterr().err


def test_other_sql_is_untouched_and_not_logged(spark, capsys):
    for query in (
        "CREATE DATABASE other_db",
        "SELECT 'CREATE DATABASE glue_catalog.x' AS s",
        "-- CREATE DATABASE glue_catalog.x\nSELECT 1",
    ):
        assert sent(spark, query) == query
    assert capsys.readouterr().err == ""
