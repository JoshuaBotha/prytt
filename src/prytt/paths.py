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
- ``register_temp_views(spark)``: ``server.on_new_session`` callback (wired
  in ``prytt.shims.pyspark_hooks.on_session``) that registers each mapped
  table as a temp view. Tables Spark cannot read (no data yet) log ``WARN``
  and are skipped; registered ones log ``MAP``. Config errors (ORC, bad
  ``s3.mode``, passthrough without a region) raise.
"""

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
    """A ``from_catalog`` table with no ``catalog."db.table"`` mapping."""

    def __init__(self, database: str, table_name: str):
        self.database = database
        self.table_name = table_name
        key = f"{database}.{table_name}"
        super().__init__(
            f"prytt: catalog table {key} is not mapped. Add this to pyproject.toml "
            f"(or the same without 'tool.prytt.' to prytt.toml):\n\n"
            f'[tool.prytt.catalog."{key}"]\n'
            f'path = "s3://<bucket>/<prefix>/{table_name}/"\n'
            f'format = "parquet"\n'
            f"options = {{}}\n\n"
            f'or set catalog.on_missing = "empty" to read it as an empty frame'
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


def register_temp_views(spark) -> None:
    """``on_new_session`` hook: each mapped table becomes temp view ``db_table``.

    A table whose data Spark cannot read yet (nothing written) is skipped
    with a ``WARN`` so session creation never fails because of it. Config
    errors (unsupported format such as ORC, bad ``s3.mode``, passthrough
    without a region) are resolved before Spark is touched and raise.
    """
    from pyspark.errors import PySparkException

    from prytt.io import read_df

    for (database, table_name), table in mapped_tables().items():
        view = view_name(database, table_name)
        df = read_df(spark, [table.path], table.format, table.options)  # lazy; config errors raise here
        try:
            df.createOrReplaceTempView(view)
        except PySparkException as exc:
            log("WARN", f"temp view {view} for {database}.{table_name} skipped: {first_line(exc)}")
            continue
        log("MAP", f"catalog {database}.{table_name} -> temp view {view}")


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
