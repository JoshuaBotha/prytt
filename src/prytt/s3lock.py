"""The S3 write lock: prytt refuses every write to real S3 it can detect.

On by default, in every ``s3.mode``. The only unlock is the environment
variable ``PRYTT_ALLOW_S3_WRITES`` set to ``1`` or ``true`` (any case); there
is deliberately no config key, so committed config can never unlock writes.
Blocked writes always raise, strict mode or not. Reads are never blocked.

Interface:

- ``PryttS3WriteBlocked``: the error a blocked write raises (a
  ``PermissionError``; also ``prytt.PryttS3WriteBlocked``).
- ``unlocked(env=None) -> bool``: whether ``PRYTT_ALLOW_S3_WRITES`` unlocks
  writes (read on every call, default ``os.environ``).
- ``is_s3_uri(value) -> bool``: whether ``value`` is an ``s3://``, ``s3a://``
  or ``s3n://`` URI (scheme in any case, as Spark and Hadoop accept).
- ``is_real_s3_write(location) -> bool``: whether writing to ``location`` would
  reach real S3. ``location`` is the target *after* prytt's path and mode
  resolution (``paths.map_path``): a local path means prytt mapped it under
  ``s3.root``; an S3 URI means it goes to S3 as is (plain Spark paths in any
  mode, prytt-mediated paths in passthrough).
- ``check_write(location, spark=None, plain_spark=False)``: raise
  ``PryttS3WriteBlocked`` if ``location`` is a real-S3 write and the lock is
  on. If it is a real-S3 write and writes are unlocked, log the per-session
  ``WARN`` (see ``warn_if_unlocked``). ``plain_spark=True`` (a Spark writer
  call, not a Glue writer) adds, in ``s3.mode = "local"``, the tip to write
  to a local path or through a Glue writer, since prytt doesn't map those.
- ``warn_if_unlocked(spark)``: log one ``WARN`` per Spark session (by
  ``session_id``) when writes are unlocked. No-op while locked.
- ``check_config(cfg=None)``: config that would let writes reach S3 while
  locked raises ``ValueError``: an ``iceberg.warehouse`` that is an ``s3://``
  URI. Called by the session builder hook before the session is built.
- ``is_passthrough_table(database, table_name) -> bool``: whether prytt reads
  this table from real S3, so a write to it would reach S3: a mapped table
  whose effective mode (``paths.table_mode``: its own ``mode``, else the
  global ``s3.mode``) is ``"passthrough"``. So a table with
  ``mode = "passthrough"`` is blocked under global ``local`` mode, and one
  with ``mode = "local"`` is not blocked under global ``passthrough``. The one
  place that decides which tables are passthrough.
- ``check_table_write(name, spark=None)``: raise ``PryttS3WriteBlocked`` if
  ``name`` (a string such as ``db.t`` or a list of parts) is a passthrough
  table or one of ``spark``'s S3-located tables and the lock is on
  (per-session ``WARN`` when unlocked). Names match case-insensitively, a
  leading ``spark_catalog`` is ignored and a one-part name is in ``spark``'s
  current database (``default`` without a session).
- ``setup_session(spark)``: ``server.on_new_session`` callback. Installs the
  enforcement wrappers (once per process: ``DataFrameWriter.save``, which the
  format shortcuts such as ``parquet`` / ``json`` / ``csv`` route through,
  ``DataFrameWriter.saveAsTable`` / ``insertInto`` and the
  ``DataFrameWriterV2`` terminal methods; per session: ``spark.sql``) and
  logs the unlocked ``WARN``.

The table-name writers call ``check_table_write`` on their table.
``saveAsTable`` also checks a ``path`` option, and ``DataFrameWriterV2`` a
``path`` option or ``location`` table property, with ``check_write``: those
create a table at that location.

``spark.sql`` is checked by a best-effort classifier on the SQL text
(``sqltext.statements``), not a parser. Write statements (``INSERT``,
``CREATE TABLE ... AS``, ``REPLACE TABLE ... AS``, ``MERGE``, ``UPDATE``,
``DELETE``, ``TRUNCATE``) are blocked when their target is an S3 location
literal (``LOCATION``, an ``OPTIONS`` ``path``, ``INSERT OVERWRITE
DIRECTORY``, or a path table such as ``delta.`s3://...```), a passthrough
table or an S3-located table. A ``CREATE TABLE`` with an S3 ``LOCATION`` and
no query, and ``ALTER TABLE ... SET LOCATION`` to S3, are allowed and record
the table as S3-located, in a private per-session registry (by
``session_id``); a later non-S3 location or ``DROP TABLE`` forgets it. A
``CREATE TABLE IF NOT EXISTS`` of a table that already exists records
nothing. The ``spark.sql`` wrapper is installed before the table-name and
Iceberg wrappers, so it sees their rewritten SQL.

Glue writers (``awsglue.context``) call ``check_write`` through
``prytt.io.write_df`` after mapping, so writes mapped to local disk pass.
"""

import functools
import itertools
import os
import threading
from typing import NamedTuple

from prytt import config, paths, sqltext
from prytt.stub import log

ENV = "PRYTT_ALLOW_S3_WRITES"
S3_SCHEMES = ("s3://", "s3a://", "s3n://")

_lock = threading.Lock()
_warned_sessions = set()
_s3_tables = {}  # session key -> {(database, table), lower-cased: location}


class PryttS3WriteBlocked(PermissionError):
    """A write to real S3 refused by prytt's S3 write lock."""

    def __init__(self, target: str, tip: str = ""):
        self.target = target
        super().__init__(
            f"prytt: write to {target} blocked by the S3 write lock (it would write to real S3). "
            f"Set {ENV}=1 in the environment to allow writes to S3." + (f" {tip}" if tip else "")
        )

    def __str__(self):
        return self.args[0]


def unlocked(env=None) -> bool:
    env = os.environ if env is None else env
    return env.get(ENV, "").lower() in ("1", "true")


def is_s3_uri(value) -> bool:
    return isinstance(value, str) and value.lower().startswith(S3_SCHEMES)


def is_real_s3_write(location) -> bool:
    return is_s3_uri(location)


_LOCAL_MODE_TIP = (
    "prytt does not map plain Spark paths to s3.root in s3.mode = 'local': write to a local "
    "path, or use glueContext.write_dynamic_frame.from_options to write under s3.root."
)


def check_write(location, spark=None, plain_spark: bool = False) -> None:
    if not is_real_s3_write(location):
        return
    if not unlocked():
        local_mode = config.get().s3_mode == "local"
        raise PryttS3WriteBlocked(location, _LOCAL_MODE_TIP if plain_spark and local_mode else "")
    warn_if_unlocked(spark)


def _session_key(spark):
    return getattr(spark, "session_id", None) or id(spark)


def warn_if_unlocked(spark) -> None:
    if not unlocked():
        return
    key = _session_key(spark)
    with _lock:
        if key in _warned_sessions:
            return
        _warned_sessions.add(key)
    log("WARN", f"S3 writes unlocked ({ENV} is set): this session can write to real S3")


def check_config(cfg=None) -> None:
    cfg = config.get() if cfg is None else cfg
    warehouse = cfg.iceberg_warehouse
    if is_s3_uri(warehouse) and not unlocked():
        raise ValueError(
            f"prytt config: iceberg.warehouse = {warehouse!r} is on S3 and the S3 write lock "
            f"is on. Use a local directory, or set {ENV}=1 to allow writes to S3."
        )


def is_passthrough_table(database: str, table_name: str) -> bool:
    return _passthrough_table(database, table_name) is not None


def _passthrough_table(database: str, table_name: str):
    """The mapped table (``paths.CatalogTable``) if prytt reads it from real
    S3, else ``None``."""
    try:
        table = paths.resolve_table(database, table_name)
    except paths.CatalogTableNotFound:
        return None
    mode, _source = paths.table_mode(table)
    return table if paths.is_passthrough(mode) else None


def _table(name, spark) -> tuple | None:
    """``(database, table)`` as written for a table name (string or parts):
    a leading ``spark_catalog`` dropped, a one-part name in ``spark``'s
    current database. ``None`` for any other shape (another catalog, ...)."""
    parts = (sqltext.parse_name(name) if isinstance(name, str) else list(name or ())) or []
    if len(parts) == 3 and parts[0].lower() == "spark_catalog":
        parts = parts[1:]
    if len(parts) == 1:
        database = spark.catalog.currentDatabase() if spark is not None else "default"
        parts = [database, *parts]
    return tuple(parts) if len(parts) == 2 else None


# The registry of S3-located tables, per session; ``table`` is a ``_table``.


def _registry_key(table) -> tuple:
    return tuple(part.lower() for part in table)


def _record_s3_table(spark, table, location: str) -> None:
    with _lock:
        _s3_tables.setdefault(_session_key(spark), {})[_registry_key(table)] = location


def _forget_table(spark, table) -> None:
    with _lock:
        _s3_tables.get(_session_key(spark), {}).pop(_registry_key(table), None)


def _s3_table_location(spark, table) -> str | None:
    with _lock:
        return _s3_tables.get(_session_key(spark), {}).get(_registry_key(table))


def check_table_write(name, spark=None) -> None:
    table = _table(name, spark)
    if table is None:
        return
    mapped = _passthrough_table(*table)
    if mapped is not None:
        reason = f"a passthrough table, read from {mapped.path}"
    else:
        location = _s3_table_location(spark, table)
        if location is None:
            return
        reason = f"located at {location}"
    if not unlocked():
        raise PryttS3WriteBlocked(f"table {'.'.join(table)} ({reason})")
    warn_if_unlocked(spark)


def setup_session(spark) -> None:
    _wrap_writers()
    _wrap_sql(spark)
    warn_if_unlocked(spark)


# spark.sql: a best-effort write classifier on sqltext.statements tokens.
# Its actions, one type per kind:


class WriteLocation(NamedTuple):
    """A write to the location ``uri``."""

    uri: str


class WriteTable(NamedTuple):
    """A write to the table named ``parts``."""

    parts: list


class SetLocation(NamedTuple):
    """The table named ``parts`` is at ``location`` once the statement ran,
    unless ``if_not_exists`` and it already existed."""

    parts: list
    location: str
    if_not_exists: bool = False


class DropTable(NamedTuple):
    """The table named ``parts`` is dropped."""

    parts: list


_QUERY_WORDS = {"AS", "SELECT"}
_TABLE_PREFIX = {"TABLE", "INTO", "OVERWRITE", "IF", "NOT", "EXISTS", "FROM"}
_CREATE_MODIFIERS = {"OR", "REPLACE", "EXTERNAL", "TEMPORARY", "TEMP", "GLOBAL"}


def _skip(tokens, i, words) -> int:
    while i < len(tokens) and tokens[i].word in words:
        i += 1
    return i


def _name_after(tokens, i):
    """Name parts of the first token from ``i`` past ``_TABLE_PREFIX`` keywords."""
    i = _skip(tokens, i, _TABLE_PREFIX)
    if i < len(tokens) and tokens[i].kind == "name":
        return tokens[i].value
    return None


def _target(parts):
    """A ``WriteLocation`` for a path table such as ``delta.`s3://b/k```, else
    a ``WriteTable``."""
    return WriteLocation(parts[-1]) if is_s3_uri(parts[-1]) else WriteTable(parts)


def _top_level(tokens, start=0):
    """``(index, token)`` for each token from ``start`` on outside parentheses."""
    depth = 0
    for i in range(start, len(tokens)):
        text = tokens[i].text
        if text == "(":
            depth += 1
        elif text == ")":
            depth = max(depth - 1, 0)
        elif not depth:
            yield i, tokens[i]


def _location(tokens, start):
    """The string after a top-level ``LOCATION`` from ``start`` on, else an
    ``OPTIONS`` ``path`` value, else ``None``."""
    for i, token in _top_level(tokens, start):
        if token.word == "LOCATION" and i + 1 < len(tokens) and tokens[i + 1].kind == "string":
            return tokens[i + 1].value
    for i, token in _top_level(tokens, start):
        if token.word == "OPTIONS" and i + 1 < len(tokens) and tokens[i + 1].text == "(":
            group = _group(tokens, i + 1)
            for key, value in itertools.pairwise(group):
                is_path_key = key.word == "PATH" or (key.kind == "string" and key.value == "path")
                if is_path_key and value.kind == "string":
                    return value.value
    return None


def _group(tokens, open_index) -> list:
    """The tokens inside the parentheses opened at ``open_index``."""
    depth = 0
    for i in range(open_index, len(tokens)):
        if tokens[i].text == "(":
            depth += 1
        elif tokens[i].text == ")":
            depth -= 1
            if not depth:
                return tokens[open_index + 1 : i]
    return tokens[open_index + 1 :]


def _insert(tokens) -> list:
    """Actions of the ``INSERT`` that ``tokens`` starts with."""
    i = 1
    if i < len(tokens) and tokens[i].word == "OVERWRITE":
        j = _skip(tokens, i + 1, {"LOCAL"})
        if j < len(tokens) and tokens[j].word == "DIRECTORY":
            if j + 1 < len(tokens) and tokens[j + 1].kind == "string":
                return [WriteLocation(tokens[j + 1].value)]
            location = _location(tokens, j + 1)
            return [WriteLocation(location)] if location else []
    parts = _name_after(tokens, i)
    return [_target(parts)] if parts else []


def _create(tokens) -> list:
    """Actions of a ``CREATE`` / ``REPLACE`` statement (only tables count)."""
    i = _skip(tokens, 1 if tokens[0].word == "CREATE" else 0, _CREATE_MODIFIERS)
    if i >= len(tokens) or tokens[i].word != "TABLE":
        return []  # a view, database, function, ...
    name = _skip(tokens, i + 1, _TABLE_PREFIX)
    if name >= len(tokens) or tokens[name].kind != "name":
        return []
    parts = tokens[name].value
    if_not_exists = any(t.word == "EXISTS" for t in tokens[i + 1 : name])
    location = _location(tokens, name + 1)
    is_query = any(t.word in _QUERY_WORDS for _, t in _top_level(tokens, name + 1)) or any(
        t.word == "SELECT" for t in tokens[name + 1 :]
    )
    actions = []
    if is_query:
        actions.append(_target(parts))
        if location:
            actions.append(WriteLocation(location))
    if location:
        actions.append(SetLocation(parts, location, if_not_exists))
    return actions


def _alter(tokens) -> list:
    """Actions of an ``ALTER TABLE ... [PARTITION (...)] SET LOCATION``."""
    if len(tokens) < 3 or tokens[1].word != "TABLE" or tokens[2].kind != "name":
        return []
    rest = list(_top_level(tokens, 3))
    for (_, set_), (_, location_), (_, value) in zip(rest, rest[1:], rest[2:], strict=False):
        if set_.word == "SET" and location_.word == "LOCATION" and value.kind == "string":
            partition = any(t.word == "PARTITION" for _, t in rest)
            if partition and not is_s3_uri(value.value):
                return []  # one partition back on local disk: others may be on S3
            return [SetLocation(tokens[2].value, value.value)]
    return []


def _sql_actions(query: str) -> list:
    """What ``query``'s statements do, as far as the lock cares: a list of
    ``WriteLocation``, ``WriteTable``, ``SetLocation`` and ``DropTable``."""
    actions = []
    for tokens in sqltext.statements(query):
        first = tokens[0].word
        if first in ("WITH", "FROM"):  # a CTE or multi-insert before INSERT
            for i, token in _top_level(tokens, 1):
                if token.word == "INSERT":
                    actions += _insert(tokens[i:])
        elif first == "INSERT":
            actions += _insert(tokens)
        elif first in ("CREATE", "REPLACE"):
            actions += _create(tokens)
        elif first == "ALTER":
            actions += _alter(tokens)
        elif first in ("MERGE", "UPDATE", "DELETE", "TRUNCATE"):
            parts = _name_after(tokens, 1)
            if parts:
                actions.append(_target(parts))
        elif first == "DROP" and len(tokens) > 1 and tokens[1].word == "TABLE":
            parts = _name_after(tokens, 2)
            if parts:
                actions.append(DropTable(parts))
    return actions


def _sql_name(table) -> str:
    return ".".join(sqltext.ident(part) for part in table)


def _wrap_sql(spark) -> None:
    sql = spark.sql
    if getattr(sql, "__prytt_s3lock_sql_wrapped__", False):
        return

    @functools.wraps(sql)
    def sql_wrapper(sqlQuery, *args, **kwargs):
        if not isinstance(sqlQuery, str):
            return sql(sqlQuery, *args, **kwargs)
        located, dropped = [], []  # registry changes, made once the statement ran
        for action in _sql_actions(sqlQuery):
            if isinstance(action, WriteLocation):
                check_write(action.uri, spark)
            elif isinstance(action, WriteTable):
                check_table_write(action.parts, spark)
            elif (table := _table(action.parts, spark)) is None:
                continue
            elif isinstance(action, DropTable):
                dropped.append(table)
            elif not (action.if_not_exists and spark.catalog.tableExists(_sql_name(table))):
                located.append((table, action.location))
        result = sql(sqlQuery, *args, **kwargs)
        for table, location in located:
            if is_s3_uri(location):
                _record_s3_table(spark, table, location)
            else:
                _forget_table(spark, table)
        for table in dropped:
            _forget_table(spark, table)
        return result

    # A flag of its own: functools.wraps copies the wrapped function's
    # __dict__, and the table-name and Iceberg wrappers wrap this one in turn.
    sql_wrapper.__prytt_s3lock_sql_wrapped__ = True
    spark.sql = sql_wrapper


def _option(options, key):
    """``options[key]`` matched case-insensitively, as Spark does, else ``None``."""
    return next((v for k, v in options.items() if k.lower() == key), None)


def _wrap_writers() -> None:
    from pyspark.sql.connect.readwriter import DataFrameWriter, DataFrameWriterV2

    _wrap(DataFrameWriter, "save", _save_wrapper)
    _wrap(DataFrameWriter, "saveAsTable", _save_as_table_wrapper)
    _wrap(DataFrameWriter, "insertInto", _insert_into_wrapper)
    for method in _V2_METHODS:
        _wrap(DataFrameWriterV2, method, _v2_wrapper)


def _wrap(cls, name, make_wrapper) -> None:
    """Replace ``cls.name`` with ``make_wrapper(cls.name)`` unless done. Each
    method gets a flag of its own (``__prytt_s3lock_<name>_wrapped__``):
    ``functools.wraps`` copies the wrapped function's ``__dict__``."""
    flag = f"__prytt_s3lock_{name}_wrapped__"
    method = getattr(cls, name)
    if getattr(method, flag, False):
        return
    wrapper = make_wrapper(method)
    setattr(wrapper, flag, True)
    setattr(cls, name, wrapper)


def _save_wrapper(save):
    @functools.wraps(save)
    def wrapper(self, path=None, *args, **kwargs):
        target = path if path is not None else _option(self._write.options, "path")
        check_write(target, self._spark, plain_spark=True)
        return save(self, path, *args, **kwargs)

    return wrapper


def _save_as_table_wrapper(save_as_table):
    @functools.wraps(save_as_table)
    def wrapper(self, name, *args, **kwargs):
        check_table_write(name, self._spark)
        # .option("path", ...) or path=...: an external table at that location
        check_write(_option({**self._write.options, **kwargs}, "path"), self._spark)
        return save_as_table(self, name, *args, **kwargs)

    return wrapper


def _insert_into_wrapper(insert_into):
    @functools.wraps(insert_into)
    def wrapper(self, tableName, *args, **kwargs):
        check_table_write(tableName, self._spark)
        return insert_into(self, tableName, *args, **kwargs)

    return wrapper


_V2_METHODS = ("append", "create", "replace", "createOrReplace", "overwrite", "overwritePartitions")


def _v2_wrapper(method):
    """A ``DataFrameWriterV2`` terminal method: the table is the one given to
    ``writeTo``; an ``option("path", ...)`` or ``tableProperty("location", ...)``
    is where a new table goes."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        check_table_write(self._table_name, self._spark)
        write = self._write
        location = _option(write.options, "path") or _option(write.table_properties, "location")
        check_write(location, self._spark)
        return method(self, *args, **kwargs)

    return wrapper
