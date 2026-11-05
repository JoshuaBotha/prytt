"""Iceberg ``glue_catalog``. Sail's catalogs are in memory, so prytt
rebuilds ``glue_catalog`` from the warehouse on disk for every new session.

Warehouse layout: ``<iceberg.warehouse>/<db>/<tbl>``. Each directory under the
warehouse is a ``glue_catalog`` database; each table directory in it that holds
Iceberg metadata is a table.

Interface (wired in ``prytt.shims.pyspark_hooks.register_session_setup``):

- ``set_sail_catalog_env()``: ``server.on_before_start`` callback. Declares the
  ``glue_catalog`` memory catalog next to Sail's default ``sail`` catalog via
  ``SAIL_CATALOG__LIST`` / ``SAIL_CATALOG__DEFAULT_CATALOG``. Values the user
  already set are kept. Not called for an external ``SPARK_REMOTE`` server.
- ``setup_session(spark)``: ``server.on_new_session`` callback. Sets
  ``spark.sql.warehouse.dir`` to the absolute warehouse, creates each
  ``glue_catalog`` database with ``LOCATION '<warehouse>/<db>'`` (``default``
  always exists) and re-registers each table (``CREATE TABLE IF NOT EXISTS ...
  USING iceberg LOCATION``, logged ``MAP``). Wraps ``spark.sql`` so a script's
  ``CREATE DATABASE glue_catalog.<db>`` without ``LOCATION`` gets
  ``LOCATION '<warehouse>/<db>'``; otherwise Sail puts that database's tables
  flat in the warehouse as ``<tbl>-<uuid>`` and a later run could not tell
  which database they belong to. Sail's memory catalog has no default
  database location setting, so this is a rewrite: it skips comments and
  quoted text, drops a trailing ``;``, and only touches a single statement
  that starts with that ``CREATE`` and has no ``LOCATION`` clause. A
  multi-statement string, or an explicit ``LOCATION`` other than
  ``<warehouse>/<db>``, is sent unchanged with a ``WARN``. Table dirs directly under the warehouse
  (tables of the default ``sail`` catalog) are not databases and are skipped.
  If the server has no ``glue_catalog`` (external server with its own
  catalog list), logs one ``WARN`` and only the warehouse dir is set.
"""

import functools
import os
import re
from pathlib import Path

from prytt import config
from prytt.sqltext import ident, literal, mask
from prytt.stub import first_line, log

CATALOG = "glue_catalog"
SAIL_CATALOG_LIST = (
    '[{name="sail", type="memory", initial_database=["default"]},'
    ' {name="glue_catalog", type="memory", initial_database=["default"]}]'
)

_CREATE_DB = re.compile(
    r"\bCREATE\s+(?:DATABASE|SCHEMA|NAMESPACE)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"`?glue_catalog`?\s*\.\s*(?:`([^`]+)`|(\w+))",
    re.IGNORECASE,
)
_UUID_SUFFIX = re.compile(r"-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_LOCATION = re.compile(r"\bLOCATION\b", re.IGNORECASE)


def set_sail_catalog_env() -> None:
    os.environ.setdefault("SAIL_CATALOG__LIST", SAIL_CATALOG_LIST)
    os.environ.setdefault("SAIL_CATALOG__DEFAULT_CATALOG", "sail")


def _is_iceberg_table(path: Path) -> bool:
    metadata = path / "metadata"
    return metadata.is_dir() and any(metadata.glob("*.metadata.json"))


def _is_table_not_database(path: Path) -> bool:
    """True if a dir directly under the warehouse is a table, so not a database.

    Iceberg or Delta table dirs of the default ``sail`` catalog, and Sail's
    flat ``<tbl>-<uuid>`` tables of a database created without a location.
    """
    return (
        (path / "metadata").is_dir()
        or (path / "_delta_log").is_dir()
        or _UUID_SUFFIX.search(path.name) is not None
    )


def _subdirs(path: Path) -> list:
    if not path.is_dir():
        return []
    return sorted(p for p in path.iterdir() if p.is_dir() and not p.name.startswith((".", "_")))


def _create_database(spark, warehouse: Path, name: str) -> None:
    location = literal((warehouse / name).as_posix())
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{ident(name)} LOCATION {location}")


def _rewrite_create_database(query: str, warehouse: Path) -> str:
    """Add ``LOCATION '<warehouse>/<db>'`` to a ``CREATE DATABASE glue_catalog.<db>``.

    Only a single statement whose first token is that ``CREATE`` and that has
    no ``LOCATION`` clause of its own is rewritten. Comments, quoted text and
    trailing ``;`` are handled. Anything else that creates a ``glue_catalog``
    database is sent as is with a ``WARN``.
    """
    masked = mask(query)
    seen = _CREATE_DB.search(masked)
    if seen is None:
        return query
    group = 1 if seen.group(1) is not None else 2
    db = query[seen.start(group) : seen.end(group)].replace("``", "`")
    location = (warehouse / db).as_posix()  # as paths.map_path: no backslashes in SQL

    end = len(masked)
    while end and (masked[end - 1].isspace() or masked[end - 1] == ";"):
        end -= 1
    at_start = masked[: seen.start()].strip() == ""
    if not at_start or ";" in masked[:end]:
        log(
            "WARN",
            f"{CATALOG} CREATE DATABASE not rewritten (multi-statement SQL or not at the start): "
            f"add LOCATION {literal(location)} yourself so later runs find database {db}'s tables",
        )
        return query
    if _LOCATION.search(masked, seen.end(), end):
        if literal(location) in query[seen.end() : end]:
            return query  # already where prytt looks for it (also prytt's own re-registration)
        log(
            "WARN",
            f"{CATALOG} database {db} has its own LOCATION; kept as is "
            f"(later runs re-register its tables only if it is {location})",
        )
        return query

    tail = "".join(c for c, m in zip(query[end:], masked[end:]) if m != ";").rstrip()
    log("MAP", f"{CATALOG} database {db} -> {location}")
    return f"{query[:end]} LOCATION {literal(location)}{tail}"


def _wrap_sql(spark, warehouse: Path) -> None:
    sql = spark.sql
    if getattr(sql, "__prytt_wrapped__", False):
        return

    @functools.wraps(sql)
    def wrapper(sqlQuery, *args, **kwargs):
        if isinstance(sqlQuery, str):
            sqlQuery = _rewrite_create_database(sqlQuery, warehouse)
        return sql(sqlQuery, *args, **kwargs)

    wrapper.__prytt_wrapped__ = True
    spark.sql = wrapper


def setup_session(spark) -> None:
    warehouse = config.get().iceberg_warehouse
    spark.conf.set("spark.sql.warehouse.dir", warehouse.as_posix())

    # Sail reads an unknown ``glue_catalog.x`` as a nested namespace, so ask.
    if CATALOG not in {c.name for c in spark.catalog.listCatalogs()}:
        log(
            "WARN",
            f"{CATALOG} not available on this server (SAIL_CATALOG__LIST), Iceberg tables not registered",
        )
        return
    # The built-in ``default`` has no location (its tables would land flat).
    try:
        spark.sql(f"DROP DATABASE IF EXISTS {CATALOG}.`default`")
        _create_database(spark, warehouse, "default")
    except Exception as exc:
        log(
            "WARN", f"{CATALOG}.default keeps no location (its tables land flat): {first_line(exc)}"
        )
    _wrap_sql(spark, warehouse)

    for db_dir in _subdirs(warehouse):
        if _is_table_not_database(db_dir):  # a table of the ``sail`` catalog, not a database
            continue
        db = db_dir.name
        try:
            _create_database(spark, warehouse, db)
        except Exception as exc:
            log("WARN", f"{CATALOG} database {db} skipped: {first_line(exc)}")
            continue
        for table_dir in _subdirs(db_dir):
            if not _is_iceberg_table(table_dir):
                continue
            name = f"{CATALOG}.{db}.{table_dir.name}"
            try:
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {CATALOG}.{ident(db)}.{ident(table_dir.name)} "
                    f"USING iceberg LOCATION {literal(table_dir.as_posix())}"
                )
            except Exception as exc:
                log("WARN", f"{name} skipped: {first_line(exc)}")
                continue
            log("MAP", f"{name} -> {table_dir.as_posix()}")
