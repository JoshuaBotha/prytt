"""s3:// and catalog -> local mapping.

Interface:

- ``map_path(uri, for_write=False) -> str``: map one URI. ``s3://``,
  ``s3a://`` and ``s3n://`` become ``<s3.root>/bucket/key`` (absolute; a
  trailing ``/`` is kept) and each rewrite logs ``MAP``. Anything else is
  returned unchanged. ``for_write=True`` also creates the local directory.
  ``s3.root`` is read from ``config.get()`` on every call, so changing
  ``PRYTT_S3_ROOT`` + ``config.reset()`` redirects later calls (pytest plugin).
  In ``s3.mode = "passthrough"`` URIs are left alone for Sail to read from S3:
  the first call checks a region is set (``AWS_REGION``,
  ``AWS_DEFAULT_REGION`` or the profile's ``region`` in the AWS config file)
  and raises ``RuntimeError`` if not, and logs one best-effort ``WARN``.
- ``map_paths(uris) -> list[str]``: ``map_path`` over a list (or one string).
- ``CatalogTable(path, format, options)``: what a catalog lookup returns.
  ``path`` is the configured (unmapped) location; callers run it through
  ``map_path``. ``options`` are Spark/Glue format options.
- ``resolve_table(database, table_name) -> CatalogTable``: look up
  ``catalog."database.table"`` in config (exact key first, then
  case-insensitive). Unmapped tables raise ``CatalogTableNotFound`` (a
  ``LookupError``) whose message is the exact TOML to add. A bad entry (no
  ``path`` / ``format``, unknown keys, non-table ``options``) raises
  ``ValueError``. ``resolve_table`` never applies ``catalog.on_missing``:
  readers check ``catalog_on_missing()`` (``"error"`` | ``"empty"``; config
  rejects anything else when it loads) and turn the miss into an empty frame; writers
  always raise.
- ``mapped_tables() -> {(database, table): CatalogTable}``: every mapped table.
- ``view_name(database, table_name) -> str``: ``db_table`` (non-word chars
  become ``_``).
- ``register_mapped_tables(spark)``: ``server.on_new_session`` callback
  (wired in ``prytt.shims.pyspark_hooks.on_session``) that registers each
  mapped table as table ``db.table`` in the session's default catalog and as
  temp view ``db_table``. Tables Spark cannot read (no data yet) log ``WARN``
  and are skipped; registered ones log one ``MAP`` line. Config errors (ORC, bad
  ``s3.mode``, passthrough without a region) raise. It also wraps
  ``spark.sql`` / ``spark.table``: mapped names resolve case-insensitively,
  and Spark's not-found error for an unmapped ``db.table`` gets the
  ``CatalogTableNotFound`` TOML as an exception note (``on_missing`` does not
  apply). Other errors pass through unchanged.
"""

import functools
import os
import re
from pathlib import Path
from typing import NamedTuple

from prytt import config
from prytt.stub import first_line, log

_S3_SCHEMES = ("s3://", "s3a://", "s3n://")
_passthrough_checked_for = None  # the Config passthrough was last checked under


class CatalogTable(NamedTuple):
    path: str
    format: str
    options: dict


def _aws_profile_region(env) -> str | None:
    import configparser

    config_file = Path(env.get("AWS_CONFIG_FILE") or Path.home() / ".aws" / "config")
    profile = env.get("AWS_PROFILE") or "default"
    parser = configparser.ConfigParser()
    try:
        parser.read(config_file)
    except configparser.Error:
        return None
    section = "default" if profile == "default" else f"profile {profile}"
    return parser.get(section, "region", fallback=None)


def _check_passthrough(cfg) -> None:
    global _passthrough_checked_for
    if _passthrough_checked_for is cfg:
        return
    env = os.environ
    region = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or _aws_profile_region(env)
    if not region:
        raise RuntimeError(
            "prytt s3.mode = 'passthrough' needs an AWS region: set AWS_REGION "
            "(or AWS_DEFAULT_REGION, or region in your AWS profile)"
        )
    log("WARN", f"s3.mode = 'passthrough' (region {region}): Sail reads S3 directly; best-effort, unverified")
    _passthrough_checked_for = cfg


def _s3_key(uri: str) -> str | None:
    for scheme in _S3_SCHEMES:
        if uri.startswith(scheme):
            return uri[len(scheme) :]
    return None


def map_path(uri: str, for_write: bool = False) -> str:
    key = _s3_key(uri)
    if key is None:
        return uri
    cfg = config.get()
    if cfg.s3_mode == "passthrough":
        _check_passthrough(cfg)
        return uri
    local_path = Path(cfg.s3_root).resolve() / key
    local = str(local_path)
    if key.endswith("/"):
        local += "/"
    log("MAP", f"{uri} -> {local}")
    if for_write:
        local_path.mkdir(parents=True, exist_ok=True)
    return local


def map_paths(uris, for_write: bool = False) -> list:
    if isinstance(uris, str):
        uris = [uris]
    return [map_path(uri, for_write=for_write) for uri in uris]


class CatalogTableNotFound(LookupError):
    """A catalog table with no ``catalog."db.table"`` mapping. ``sql=True``
    leaves out the ``on_missing = "empty"`` tip (SQL reads can't honour it)."""

    def __init__(self, database: str, table_name: str, sql: bool = False):
        self.database = database
        self.table_name = table_name
        key = f"{database}.{table_name}"
        super().__init__(
            f"prytt: catalog table {key} is not mapped. Add this to pyproject.toml "
            f"(or the same without 'tool.prytt.' to prytt.toml):\n\n"
            f'[tool.prytt.catalog."{key}"]\n'
            f'path = "s3://<bucket>/<prefix>/{table_name}/"\n'
            f'format = "parquet"\n'
            f"options = {{}}"
            + ("" if sql else f'\n\nor set catalog.on_missing = "empty" to read it as an empty frame')
        )

    def __str__(self):
        return self.args[0]


_ENTRY_KEYS = {"path", "format", "options"}


def mapped_tables() -> dict:
    """``{(database, table): CatalogTable}`` for every configured table."""
    tables = {}
    for key, entry in config.get().catalog.items():
        database, sep, table_name = key.partition(".")
        if not sep or not database or not table_name:
            raise ValueError(f'prytt config: catalog key {key!r} must be "database.table"')
        tables[(database, table_name)] = _catalog_table(key, entry)
    return tables


def _catalog_table(key: str, entry: dict) -> CatalogTable:
    unknown = sorted(set(entry) - _ENTRY_KEYS)
    if unknown:
        raise ValueError(f"prytt config: catalog.{key!r} has unknown key(s) {', '.join(unknown)}")
    missing = [name for name in ("path", "format") if not entry.get(name)]
    if missing:
        raise ValueError(f"prytt config: catalog.{key!r} needs {' and '.join(missing)}")
    options = entry.get("options") or {}
    if not isinstance(options, dict):
        raise ValueError(f"prytt config: catalog.{key!r} options must be a table")
    return CatalogTable(str(entry["path"]), str(entry["format"]), dict(options))


def view_name(database: str, table_name: str) -> str:
    return re.sub(r"\W", "_", f"{database}_{table_name}")


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _literal(value) -> str:
    if isinstance(value, bool):
        value = str(value).lower()
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def register_mapped_tables(spark) -> None:
    """``on_new_session`` hook: each mapped table becomes registered table
    ``db.table`` (default catalog) and temp view ``db_table``.

    A table whose data Spark cannot read yet (nothing written) is skipped
    with a ``WARN`` so session creation never fails because of it. Config
    errors (unsupported format such as ORC, bad ``s3.mode``, passthrough
    without a region) are resolved before Spark is touched and raise.
    ``IF NOT EXISTS`` keeps registration safe on a persistent external catalog.
    """
    from pyspark.errors import PySparkException

    from prytt import formats

    registered = {}
    for (database, table_name), table in mapped_tables().items():
        name = f"{database}.{table_name}"
        view = view_name(database, table_name)
        fmt = formats.spark_format(table.format)  # config errors raise here
        location = map_path(table.path)
        options = formats.spark_options(fmt, table.options)
        # Sail registers a missing parquet/json/csv location without error, as
        # an empty table with no columns, so check for data ourselves.
        if not _has_data(location):
            log("WARN", f"catalog table {name} skipped: no data at {location} yet")
            continue
        try:
            spark.read.format(fmt).options(**options).load([location]).createOrReplaceTempView(view)
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {_ident(database)}")
            opts = ", ".join(f"{_literal(k)} {_literal(v)}" for k, v in options.items())
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {_ident(database)}.{_ident(table_name)} USING {fmt}"
                + (f" OPTIONS ({opts})" if opts else "")
                + f" LOCATION {_literal(location)}"
            )
        except PySparkException as exc:
            log("WARN", f"catalog table {name} skipped: {first_line(exc)}")
            continue
        log("MAP", f"{name} -> {location} (table {name}, view {view})")
        registered[(database.lower(), table_name.lower())] = name
    _wrap_table_names(spark, registered)


def _has_data(location: str) -> bool:
    if _s3_key(location) is not None:  # passthrough: Spark decides
        return True
    path = Path(location)
    if path.is_file():
        return True
    return path.is_dir() and any(not p.name.startswith(".") for p in path.iterdir())


# ``db.table`` not preceded by ``.`` (so not part of ``catalog.db.table``).
_DOTTED_NAME = re.compile(r"(?<![\w.`])(\w+)\s*\.\s*(\w+)(?![\w`])")


def _wrap_table_names(spark, registered: dict) -> None:
    """Wrap ``spark.sql`` and ``spark.table`` for mapped ``db.table`` names.

    Sail 0.7.3 resolves table names case-sensitively (Spark and Glue do not),
    so mapped names are rewritten to their configured spelling. Comments,
    string literals and backtick-quoted names are left alone.

    A query or name with a dotted name that is not registered is analysed up
    front (Spark Connect is otherwise lazy, as classic Spark is not). If that
    fails with Spark's not-found error for such a name, the error is
    re-raised with the TOML to add as a note. Any other error is dropped
    here and surfaces at the action as before.
    """
    from prytt.iceberg import _mask

    def canonical(match_text: str, db: str, table: str) -> str:
        return registered.get((db.lower(), table.lower()), match_text)

    def rewrite_sql(query: str) -> tuple:
        masked = _mask(query)
        out, last, unregistered = [], 0, []
        for m in _DOTTED_NAME.finditer(masked):
            original = query[m.start() : m.end()]
            db, tbl = m.groups()
            if (db.lower(), tbl.lower()) not in registered:
                unregistered.append((db, tbl))
            out += [query[last : m.start()], canonical(original, db, tbl)]
            last = m.end()
        return "".join(out) + query[last:], unregistered

    sql, table = spark.sql, spark.table
    if getattr(sql, "__prytt_names_wrapped__", False):
        return

    @functools.wraps(sql)
    def sql_wrapper(sqlQuery, *args, **kwargs):
        if not isinstance(sqlQuery, str):
            return sql(sqlQuery, *args, **kwargs)
        sqlQuery, unregistered = rewrite_sql(sqlQuery)
        return _hint_unmapped(lambda: sql(sqlQuery, *args, **kwargs), unregistered)

    @functools.wraps(table)
    def table_wrapper(tableName, *args, **kwargs):
        if not isinstance(tableName, str) or tableName.count(".") != 1:
            return table(tableName, *args, **kwargs)
        db, tbl = tableName.split(".")
        tableName = canonical(tableName, db, tbl)
        unregistered = [] if (db.lower(), tbl.lower()) in registered else [(db, tbl)]
        return _hint_unmapped(lambda: table(tableName, *args, **kwargs), unregistered)

    # Not ``__prytt_wrapped__``: functools.wraps copies the flag, and iceberg
    # checks that one before wrapping this wrapper in turn.
    sql_wrapper.__prytt_names_wrapped__ = True
    spark.sql, spark.table = sql_wrapper, table_wrapper


_NOT_FOUND = re.compile(r"\[TABLE_OR_VIEW_NOT_FOUND\][^:]*:\s*`?([^\s`]+)")


def _hint_unmapped(call, unregistered: list):
    """Run ``call`` (a ``spark.sql`` / ``spark.table``); if it, or analysing
    its result, fails with not-found for one of ``unregistered``
    ``(db, table)`` names, add the TOML hint to Spark's error and re-raise."""
    if not unregistered:
        return call()
    from pyspark.errors import AnalysisException

    def hinted(exc):
        match = _NOT_FOUND.search(str(exc))
        if match:
            missing = match.group(1).rsplit(".", 1)[-1].lower()
            for db, tbl in unregistered:
                if tbl.lower() == missing:
                    exc.add_note(str(CatalogTableNotFound(db, tbl, sql=True)))
                    return True
        return False

    try:
        df = call()
    except AnalysisException as exc:
        hinted(exc)
        raise
    try:
        df.schema  # noqa: B018 (forces analysis)
    except AnalysisException as exc:
        if hinted(exc):
            raise
    return df


def catalog_on_missing() -> str:
    return config.get().catalog_on_missing


def resolve_table(database: str, table_name: str) -> CatalogTable:
    key = f"{database}.{table_name}"
    catalog = config.get().catalog
    entry = catalog.get(key)
    if entry is None:  # Glue names are case-insensitive
        entry = next((v for k, v in catalog.items() if k.lower() == key.lower()), None)
    if entry is None:
        raise CatalogTableNotFound(database, table_name)
    return _catalog_table(key, entry)
