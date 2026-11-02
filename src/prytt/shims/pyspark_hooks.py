"""Post-import hooks fired by ``prytt._hooks`` (spec §6 table). Order-independent.

- ``on_pyspark(module)``: set ``pyspark.SparkContext`` to the shim.
- ``on_session(module)``: wrap ``SparkSession.Builder.getOrCreate`` / ``create``
  so they call ``server.ensure_running()`` first and
  ``server.session_built(spark)`` after.
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


def on_session(module):
    builder = module.SparkSession.Builder
    for name in ("getOrCreate", "create"):
        if hasattr(builder, name):
            setattr(builder, name, _wrap(getattr(builder, name)))
