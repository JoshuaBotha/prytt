"""Post-import hooks fired by ``prytt._hooks``. Order-independent.

- ``on_pyspark(module)``: set ``pyspark.SparkContext`` to the shim.
- ``on_session(module)``: wrap ``SparkSession.Builder.getOrCreate`` / ``create``
  so they call ``server.ensure_running()`` first and
  ``server.session_built(spark)`` after. Also calls ``register_session_setup``,
  so the setup applies to spark-only scripts that never import ``awsglue``.
- ``register_session_setup()``: register the server callbacks (idempotent):
  ``iceberg.set_sail_catalog_env`` before start; ``paths.register_mapped_tables``
  (T5) and ``iceberg.setup_session`` (T10) per new session.
  ``server.ensure_running`` also calls it, so every start path (SparkContext
  shim, builder, ``GlueContext``, pytest plugin) gets it even if
  ``pyspark.sql.session`` was not imported yet.
- ``on_connect_session(module)``: replace ``SparkSession.__del__`` so a
  garbage-collected session closes only its own channel. pyspark-client's
  ``__del__`` calls ``client.close()``, which also shuts down the
  process-wide ReleaseExecute thread pool: if the cyclic GC runs it inside
  that pool's ``submit()`` (lock held), the shutdown deadlocks on the same
  lock, and otherwise it pulls the pool from under any session still in use.
  ``spark.stop()`` is unchanged.
"""

import functools


def on_pyspark(module):
    import pyspark.context  # served by the finder

    module.SparkContext = pyspark.context.SparkContext


def _wrap(build):
    if getattr(build, "__prytt_wrapped__", False):
        return build

    @functools.wraps(build)
    def wrapper(self, *args, **kwargs):
        from prytt import server

        server.ensure_running()
        spark = build(self, *args, **kwargs)
        server.session_built(spark)
        return spark

    wrapper.__prytt_wrapped__ = True
    return wrapper


def register_session_setup():
    from prytt import iceberg, paths, server

    server.on_before_start(iceberg.set_sail_catalog_env)  # glue_catalog (T10)
    server.on_new_session(
        paths.register_mapped_tables
    )  # catalog tables as db.table + db_table views
    server.on_new_session(iceberg.setup_session)  # warehouse dir + glue_catalog tables (T10)


def on_session(module):
    register_session_setup()
    builder = module.SparkSession.Builder
    for name in ("getOrCreate", "create"):
        if hasattr(builder, name):
            setattr(builder, name, _wrap(getattr(builder, name)))


def _collected_session_del(self):
    try:
        if hasattr(self, "_sqm"):
            self._sqm.close()
        client = self.client
        if not client.is_closed:
            client._channel.close()
            client._closed = True
    except Exception:
        pass  # best effort, like pyspark-client's own __del__


def on_connect_session(module):
    module.SparkSession.__del__ = _collected_session_del
