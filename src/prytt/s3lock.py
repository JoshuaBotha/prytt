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
- ``is_real_s3_write(location) -> bool``: whether writing to ``location`` would
  reach real S3. ``location`` is the target *after* prytt's path and mode
  resolution (``paths.map_path``): a local path means prytt mapped it under
  ``s3.root``; an ``s3://`` / ``s3a://`` / ``s3n://`` URI means it goes to S3
  as is (plain Spark paths in any mode, prytt-mediated paths in passthrough).
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
  this table from real S3, so a write to it would reach S3: today a mapped
  table while the global ``s3.mode`` is ``"passthrough"``. The one place that
  decides which tables are passthrough.
- ``record_s3_table(spark, name, location)`` / ``forget_table(spark, name)``
  / ``s3_table_location(spark, name) -> str | None``: the per-session
  registry (by ``session_id``) of tables created at an ``s3://`` location.
  ``name`` is a string such as ``db.t`` or a list of parts; names match
  case-insensitively, a leading ``spark_catalog`` is ignored and a one-part
  name means database ``default``.
- ``check_table_write(name, spark=None)``: raise ``PryttS3WriteBlocked`` if
  ``name`` is a passthrough table or one of ``spark``'s S3-located tables and
  the lock is on (per-session ``WARN`` when unlocked).
- ``setup_session(spark)``: ``server.on_new_session`` callback. Installs the
  enforcement wrappers (once per process: ``DataFrameWriter.save``, which the
  format shortcuts such as ``parquet`` / ``json`` / ``csv`` route through;
  per session: ``spark.sql``) and logs the unlocked ``WARN``.

``spark.sql`` is checked by a best-effort classifier on the SQL text
(``sqltext.statements``), not a parser. Write statements (``INSERT``,
``CREATE TABLE ... AS``, ``REPLACE TABLE ... AS``, ``MERGE``, ``UPDATE``,
``DELETE``, ``TRUNCATE``) are blocked when their target is an S3 location
literal (``LOCATION``, an ``OPTIONS`` ``path``, ``INSERT OVERWRITE
DIRECTORY``, or a path table such as ``delta.`s3://...```), a passthrough
table or an S3-located table. A ``CREATE TABLE`` with an S3 ``LOCATION`` and
no query is allowed and recorded as S3-located; ``DROP TABLE`` forgets it.
The ``spark.sql`` wrapper is installed before the table-name and Iceberg
wrappers, so it sees their rewritten SQL.

Glue writers (``awsglue.context``) call ``check_write`` through
``prytt.io.write_df`` after mapping, so writes mapped to local disk pass.
"""

import functools
import itertools
import os
import threading

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
    return isinstance(value, str) and value.startswith(S3_SCHEMES)


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


def warn_if_unlocked(spark) -> None:
    if not unlocked():
        return
    key = getattr(spark, "session_id", None) or id(spark)
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


def _session_key(spark):
    return getattr(spark, "session_id", None) or id(spark)


def _table_key(name) -> tuple | None:
    parts = sqltext.parse_name(name) if isinstance(name, str) else list(name or ())
    if not parts:
        return None
    parts = [p.lower() for p in parts]
    if len(parts) == 3 and parts[0] == "spark_catalog":
        parts = parts[1:]
    if len(parts) == 1:
        parts = ["default", *parts]
    return tuple(parts) if len(parts) == 2 else None


def is_passthrough_table(database: str, table_name: str) -> bool:
    try:
        paths.resolve_table(database, table_name)
    except paths.CatalogTableNotFound:
        return False
    return config.get().s3_mode == "passthrough"


def record_s3_table(spark, name, location: str) -> None:
    key = _table_key(name)
    if key is not None:
        with _lock:
            _s3_tables.setdefault(_session_key(spark), {})[key] = location


def forget_table(spark, name) -> None:
    key = _table_key(name)
    with _lock:
        _s3_tables.get(_session_key(spark), {}).pop(key, None)


def s3_table_location(spark, name) -> str | None:
    key = _table_key(name)
    with _lock:
        return _s3_tables.get(_session_key(spark), {}).get(key)


def check_table_write(name, spark=None) -> None:
    key = _table_key(name)
    if key is None:
        return
    if is_passthrough_table(*key):
        reason = f"a passthrough table, read from {paths.resolve_table(*key).path}"
    else:
        location = s3_table_location(spark, name)
        if location is None:
            return
        reason = f"created at {location}"
    if not unlocked():
        display = name if isinstance(name, str) else ".".join(name)
        raise PryttS3WriteBlocked(f"table {display} ({reason})")
    warn_if_unlocked(spark)


def setup_session(spark) -> None:
    _wrap_writers()
    _wrap_sql(spark)
    warn_if_unlocked(spark)


# spark.sql: a best-effort write classifier on sqltext.statements tokens.

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
    """``("location", uri)`` for a path table such as ``delta.`s3://b/k```,
    else ``("table", parts)``."""
    if is_s3_uri(parts[-1]):
        return ("location", parts[-1])
    return ("table", parts)


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
    """Write targets of the ``INSERT`` that ``tokens`` starts with."""
    i = 1
    if i < len(tokens) and tokens[i].word == "OVERWRITE":
        j = _skip(tokens, i + 1, {"LOCAL"})
        if j < len(tokens) and tokens[j].word == "DIRECTORY":
            if j + 1 < len(tokens) and tokens[j + 1].kind == "string":
                return [("location", tokens[j + 1].value)]
            location = _location(tokens, j + 1)
            return [("location", location)] if location else []
    parts = _name_after(tokens, i)
    return [_target(parts)] if parts else []


def _create(tokens) -> list:
    """Actions of a ``CREATE`` / ``REPLACE`` statement (only tables count)."""
    i = _skip(tokens, 1 if tokens[0].word == "CREATE" else 0, _CREATE_MODIFIERS)
    if i >= len(tokens) or tokens[i].word != "TABLE":
        return []  # a view, database, function, ...
    i = _skip(tokens, i + 1, _TABLE_PREFIX)
    if i >= len(tokens) or tokens[i].kind != "name":
        return []
    parts = tokens[i].value
    location = _location(tokens, i + 1)
    rest = tokens[i + 1 :]
    is_query = any(t.word in _QUERY_WORDS for _, t in _top_level(tokens, i + 1)) or any(
        t.word == "SELECT" for t in rest
    )
    if is_query:
        actions = [("write", _target(parts))]
        if location:
            actions.append(("write", ("location", location)))
        return actions
    if is_s3_uri(location):
        return [("created_s3", parts, location)]
    return []


def _sql_actions(query: str) -> list:
    """What ``query``'s statements do, as far as the lock cares:
    ``("write", target)`` with ``target`` ``("location", uri)`` or
    ``("table", parts)``, ``("created_s3", parts, location)`` and
    ``("dropped", parts)``."""
    actions = []
    for tokens in sqltext.statements(query):
        first = tokens[0].word
        if first in ("WITH", "FROM"):  # a CTE or multi-insert before INSERT
            for i, token in _top_level(tokens, 1):
                if token.word == "INSERT":
                    actions += [("write", t) for t in _insert(tokens[i:])]
        elif first == "INSERT":
            actions += [("write", t) for t in _insert(tokens)]
        elif first in ("CREATE", "REPLACE"):
            actions += _create(tokens)
        elif first in ("MERGE", "UPDATE", "DELETE", "TRUNCATE"):
            parts = _name_after(tokens, 1)
            if parts:
                actions.append(("write", _target(parts)))
        elif first == "DROP" and len(tokens) > 1 and tokens[1].word == "TABLE":
            parts = _name_after(tokens, 2)
            if parts:
                actions.append(("dropped", parts))
    return actions


def _wrap_sql(spark) -> None:
    sql = spark.sql
    if getattr(sql, "__prytt_s3lock_sql_wrapped__", False):
        return

    @functools.wraps(sql)
    def sql_wrapper(sqlQuery, *args, **kwargs):
        if not isinstance(sqlQuery, str):
            return sql(sqlQuery, *args, **kwargs)
        actions = _sql_actions(sqlQuery)
        for action in actions:
            if action[0] == "write":
                kind, target = action[1]
                if kind == "location":
                    check_write(target, spark)
                else:
                    check_table_write(target, spark)
        result = sql(sqlQuery, *args, **kwargs)
        for action in actions:  # only once the statement ran
            if action[0] == "created_s3":
                record_s3_table(spark, action[1], action[2])
            elif action[0] == "dropped":
                forget_table(spark, action[1])
        return result

    # A flag of its own: functools.wraps copies the wrapped function's
    # __dict__, and the table-name and Iceberg wrappers wrap this one in turn.
    sql_wrapper.__prytt_s3lock_sql_wrapped__ = True
    spark.sql = sql_wrapper


def _wrap_writers() -> None:
    from pyspark.sql.connect.readwriter import DataFrameWriter

    save = DataFrameWriter.save
    if getattr(save, "__prytt_s3lock_wrapped__", False):
        return

    @functools.wraps(save)
    def save_wrapper(self, path=None, *args, **kwargs):
        target = path
        if target is None:  # .option("path", ...).save()
            options = self._write.options
            target = next((v for k, v in options.items() if k.lower() == "path"), None)
        check_write(target, self._spark, plain_spark=True)
        return save(self, path, *args, **kwargs)

    # A flag of its own: functools.wraps copies the wrapped function's __dict__.
    save_wrapper.__prytt_s3lock_wrapped__ = True
    DataFrameWriter.save = save_wrapper
