"""Post-import hooks fired by ``prytt._hooks`` (spec §6 table). Order-independent.

- ``on_pyspark(module)``: set ``pyspark.SparkContext`` to the shim.
- ``on_session(module)``: wrap ``SparkSession.Builder.getOrCreate`` / ``create``
  so they call ``server.ensure_running()`` first and
  ``server.session_built(spark)`` after. Also calls ``register_session_setup``,
  so the setup applies to spark-only scripts that never import ``awsglue``.
- ``register_session_setup()``: register the server callbacks (idempotent):
  ``iceberg.set_sail_catalog_env`` before start; ``paths.register_temp_views``
  (T5) and ``iceberg.setup_session`` (T10) per new session.
  ``server.ensure_running`` also calls it, so every start path (SparkContext
  shim, builder, ``GlueContext``, pytest plugin) gets it even if
  ``pyspark.sql.session`` was not imported yet.
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
    server.on_new_session(paths.register_temp_views)  # catalog tables as db_table views (T5)
    server.on_new_session(iceberg.setup_session)  # warehouse dir + glue_catalog tables (T10)


def on_session(module):
    register_session_setup()
    builder = module.SparkSession.Builder
    for name in ("getOrCreate", "create"):
        if hasattr(builder, name):
            setattr(builder, name, _wrap(getattr(builder, name)))
