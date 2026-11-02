"""``awsglue.context`` shim.

Interface:

- ``GlueContext(sc=None, **options)``: ensures the Sail server is up. ``sc``
  is kept but not needed.
- ``spark_session`` (alias ``sparkSession``): the Sail-backed ``SparkSession``.
- ``create_dynamic_frame`` / ``create_data_frame``: readers with
  ``from_options(connection_type, connection_options, format,
  format_options, transformation_ctx)`` and ``from_catalog(database,
  table_name, ...)``. Return a ``DynamicFrame`` / ``DataFrame``.
- ``write_dynamic_frame`` / ``write_data_frame``: writers with
  ``from_options(frame, connection_type, connection_options, format,
  format_options, transformation_ctx)`` and ``from_catalog(frame, database,
  table_name, ...)``. Write mode is append (Glue's default);
  ``connection_options["partitionKeys"]`` becomes ``partitionBy``.
- Flat Glue spellings work too: ``create_dynamic_frame_from_options``,
  ``create_dynamic_frame_from_catalog``, ``create_data_frame_from_options``,
  ``create_data_frame_from_catalog``, ``write_dynamic_frame_from_options``,
  ``write_dynamic_frame_from_catalog``, ``write_data_frame_from_catalog``.
- Only ``connection_type="s3"`` is supported; reads and writes go through
  ``prytt.io.read_df`` / ``write_df`` (path mapping + format/option
  translation). Other connection types log ``WARN`` and raise
  ``GlueLocalUnsupported``.
- ``from_catalog`` calls ``prytt.paths.resolve_table(database, table_name)``
  (the T5 seam) and then reads/writes like ``from_options`` with the table's
  path, format and options; ``additional_options`` are merged over the
  table's options. An unmapped table raises ``paths.CatalogTableNotFound``,
  except on read with ``catalog.on_missing = "empty"``: ``WARN`` and an
  empty frame (no columns).
- ``get_logger()``: a ``logging.Logger`` (``prytt.glue``) with ``info`` /
  ``warn`` / ``error``.
- Any other attribute: delegated to the ``SparkSession`` if it has it
  (``sql``, ``read``, ``table``, ...), else the ``LogAny`` catch-all
  (``purge_s3_path``, ``getSink``, ... log ``STUB``).
"""

import logging

from prytt import paths, server
from prytt.io import read_df, write_df
from prytt.stub import GlueLocalUnsupported, LogAny, log

__all__ = ["GlueContext"]

_PATH_OPTIONS = {"paths", "path", "partitionKeys"}


def _require_s3(connection_type, what):
    if (connection_type or "").lower() != "s3":
        log("WARN", f"{what}: connection_type={connection_type!r} is not supported locally")
        raise GlueLocalUnsupported(
            f"{what}: connection_type={connection_type!r} is not supported by prytt (only 's3')"
        )


def _warn_ignored(what, connection_options):
    for key in sorted(set(connection_options) - _PATH_OPTIONS):
        log("WARN", f"{what}: connection option {key}={connection_options[key]!r} ignored locally")


def _as_df(frame):
    from awsglue.dynamicframe import DynamicFrame

    return frame.toDF() if isinstance(frame, DynamicFrame) else frame


class _Reader(LogAny):
    def __init__(self, glue_ctx, wrap):
        self._ctx = glue_ctx
        self._wrap = wrap

    def _result(self, df, name):
        if not self._wrap:
            return df
        from awsglue.dynamicframe import DynamicFrame

        return DynamicFrame(df, self._ctx, name)

    def from_options(
        self,
        connection_type,
        connection_options=None,
        format=None,
        format_options=None,
        transformation_ctx="",
        **kwargs,
    ):
        _require_s3(connection_type, "from_options")
        opts = dict(connection_options or {})
        _warn_ignored("from_options", opts)
        uris = opts.get("paths") or opts.get("path") or []
        df = read_df(self._ctx.spark_session, uris, format, format_options)
        return self._result(df, transformation_ctx)

    def from_catalog(
        self,
        database=None,
        table_name=None,
        redshift_tmp_dir="",
        transformation_ctx="",
        push_down_predicate="",
        additional_options=None,
        catalog_id=None,
        **kwargs,
    ):
        name = kwargs.pop("name_space", None) or database
        try:
            table = paths.resolve_table(name, table_name)
        except paths.CatalogTableNotFound as missing:
            if paths.catalog_on_missing() != "empty":
                raise
            log("WARN", f"catalog table {name}.{table_name} is not mapped; reading an empty frame")
            from pyspark.sql.types import StructType

            df = self._ctx.spark_session.createDataFrame([], StructType([]))
            return self._result(df, transformation_ctx or missing.table_name)
        options = {**(table.options or {}), **(additional_options or {})}
        df = read_df(self._ctx.spark_session, [table.path], table.format, options)
        return self._result(df, transformation_ctx)


class _Writer(LogAny):
    def __init__(self, glue_ctx):
        self._ctx = glue_ctx

    def from_options(
        self,
        frame,
        connection_type,
        connection_options=None,
        format=None,
        format_options=None,
        transformation_ctx="",
        **kwargs,
    ):
        _require_s3(connection_type, "write from_options")
        opts = dict(connection_options or {})
        _warn_ignored("write from_options", opts)
        write_df(_as_df(frame), opts.get("path"), format, format_options, opts.get("partitionKeys"))
        return frame

    def from_catalog(
        self,
        frame,
        database=None,
        table_name=None,
        redshift_tmp_dir="",
        transformation_ctx="",
        additional_options=None,
        catalog_id=None,
        **kwargs,
    ):
        name = kwargs.pop("name_space", None) or database
        table = paths.resolve_table(name, table_name)
        options = {**(table.options or {}), **(additional_options or {})}
        partition_keys = options.pop("partitionKeys", None)
        write_df(_as_df(frame), table.path, table.format, options, partition_keys)
        return frame


class GlueContext(LogAny):
    def __init__(self, sparkContext=None, **options):
        server.ensure_running()
        self._sc = sparkContext
        self._options = options
        self.create_dynamic_frame = _Reader(self, wrap=True)
        self.create_data_frame = _Reader(self, wrap=False)
        self.write_dynamic_frame = _Writer(self)
        self.write_data_frame = _Writer(self)

    @property
    def spark_session(self):
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()

    sparkSession = spark_session

    # Flat spellings of the reader/writer methods.
    def create_dynamic_frame_from_options(self, *args, **kwargs):
        return self.create_dynamic_frame.from_options(*args, **kwargs)

    def create_dynamic_frame_from_catalog(self, *args, **kwargs):
        return self.create_dynamic_frame.from_catalog(*args, **kwargs)

    def create_data_frame_from_options(self, *args, **kwargs):
        return self.create_data_frame.from_options(*args, **kwargs)

    def create_data_frame_from_catalog(self, *args, **kwargs):
        return self.create_data_frame.from_catalog(*args, **kwargs)

    def write_dynamic_frame_from_options(self, *args, **kwargs):
        return self.write_dynamic_frame.from_options(*args, **kwargs)

    def write_dynamic_frame_from_catalog(self, *args, **kwargs):
        return self.write_dynamic_frame.from_catalog(*args, **kwargs)

    def write_data_frame_from_catalog(self, *args, **kwargs):
        return self.write_data_frame.from_catalog(*args, **kwargs)

    def get_logger(self):
        from prytt.stub import get_logger

        get_logger()  # make sure the prytt handler is configured
        logger = logging.getLogger("prytt.glue")
        logger.warn = logger.warning  # Logger.warn is deprecated; Glue scripts use it
        return logger

    def __getattr__(self, name):
        if not name.startswith("_") and name not in ("spark_session", "sparkSession"):
            spark = self.spark_session
            if hasattr(type(spark), name) or name in getattr(spark, "__dict__", {}):
                return getattr(spark, name)
        return super().__getattr__(name)
