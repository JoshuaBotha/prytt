"""``pyspark.context`` replacement (spec §8). pyspark-client has no SparkContext.

Served as ``pyspark.context`` by ``prytt._hooks``; also ``pyspark.SparkContext``.

- ``SparkContext(...)`` / ``SparkContext.getOrCreate(conf=None)``: ensure the
  server, return the process-wide singleton, log the args.
- ``appName``, ``master``, ``version`` (from the session), ``sparkSession``
  (the ``SparkSession``, built on demand; prytt extra used by ``GlueContext``).
- ``_jsc``, ``_jvm``, ``_conf``: ``LogAny`` objects, so Java-style chains such
  as ``sc._jsc.hadoopConfiguration().set(k, v)`` log and keep going.
- Any other method (``setLogLevel``, ``addPyFile``, ...): catch-all logged
  no-op (raises in strict mode).
- RDD entry points (``parallelize``, ``textFile``, ...): raise
  ``GlueLocalUnsupported``.
- ``stop()``: stop the session and drop the singleton.
"""

from prytt import server
from prytt.stub import GlueLocalUnsupported, LogAny, log

__all__ = ["SparkContext"]

_RDD_METHODS = (
    "parallelize",
    "textFile",
    "wholeTextFiles",
    "binaryFiles",
    "emptyRDD",
    "range",
    "union",
    "broadcast",
    "accumulator",
    "pickleFile",
    "sequenceFile",
    "newAPIHadoopFile",
    "hadoopFile",
)


def _Java(label):
    """Stand-in for a py4j object named ``label``; any call logs and returns itself."""
    return type(label, (LogAny,), {"__module__": __name__})()


def _unsupported(name):
    def method(self, *args, **kwargs):
        raise GlueLocalUnsupported(
            f"SparkContext.{name}() needs RDDs, which do not exist in Spark Connect "
            "(prytt runs on Sail). Use the DataFrame API, e.g. spark.createDataFrame "
            "or spark.read."
        )

    method.__name__ = name
    return method


def _args_text(args, kwargs):
    parts = [repr(a) for a in args] + [f"{k}={v!r}" for k, v in kwargs.items()]
    return ", ".join(parts)


class SparkContext(LogAny):
    _active = None

    def __new__(cls, *args, **kwargs):
        if cls._active is None:
            server.ensure_running()
            log("INFO", f"SparkContext({_args_text(args, kwargs)})")
            inst = super().__new__(cls)
            inst._app_name = kwargs.get("appName") or (args[1] if len(args) > 1 else None)
            inst._master = kwargs.get("master") or (args[0] if args else None)
            inst._jsc = _Java("JavaSparkContext")
            inst._jvm = _Java("JVM")
            inst._conf = _Java("SparkConf")
            cls._active = inst
        return cls._active

    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def getOrCreate(cls, conf=None):
        return cls() if conf is None else cls(conf=conf)

    @property
    def sparkSession(self):
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()

    @property
    def appName(self):
        return self._app_name or "prytt"

    @property
    def master(self):
        return self._master or "local[*]"

    @property
    def version(self):
        return self.sparkSession.version

    def stop(self):
        server.stop_session()
        type(self)._active = None


for _name in _RDD_METHODS:
    setattr(SparkContext, _name, _unsupported(_name))
del _name
