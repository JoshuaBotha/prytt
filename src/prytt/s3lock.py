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
- ``setup_session(spark)``: ``server.on_new_session`` callback. Installs the
  enforcement wrappers (once per process: ``DataFrameWriter.save``, which the
  format shortcuts such as ``parquet`` / ``json`` / ``csv`` route through) and
  logs the unlocked ``WARN``.

Glue writers (``awsglue.context``) call ``check_write`` through
``prytt.io.write_df`` after mapping, so writes mapped to local disk pass.
"""

import functools
import os
import threading

from prytt import config
from prytt.stub import log

ENV = "PRYTT_ALLOW_S3_WRITES"
S3_SCHEMES = ("s3://", "s3a://", "s3n://")

_lock = threading.Lock()
_warned_sessions = set()


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


def setup_session(spark) -> None:
    _wrap_writers()
    warn_if_unlocked(spark)


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
