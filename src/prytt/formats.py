"""Glue format names and ``format_options`` -> Spark.

Interface:

- ``spark_format(name) -> str``: ``glueparquet`` -> ``parquet``; ``parquet``,
  ``csv``, ``json``, ``delta``, ``iceberg`` pass through (case-insensitive).
  ``orc`` logs ``WARN`` and raises ``GlueLocalUnsupported`` (Sail has no ORC).
  ``None`` raises ``ValueError``. Anything else logs ``WARN`` and is handed
  to Sail as-is.
- ``spark_options(fmt, format_options, write=False) -> dict``: rename Glue
  options to Spark ones (``withHeader``/``writeHeader`` -> ``header``,
  ``separator`` -> ``sep``, ``quoteChar`` -> ``quote``, ``escaper`` ->
  ``escape``, ``multiline`` -> ``multiLine``). Glue-only options with no Spark
  meaning are dropped with a ``WARN``; other keys pass through and log
  ``INFO``. CSV reads default ``inferSchema`` to true (Glue infers CSV types).
"""

from prytt.stub import GlueLocalUnsupported, log

_KNOWN = ("parquet", "csv", "json", "delta", "iceberg")
_ALIASES = {"glueparquet": "parquet"}

_RENAME = {
    "withheader": "header",
    "writeheader": "header",
    "separator": "sep",
    "quotechar": "quote",
    "escaper": "escape",
    "multiline": "multiLine",
}
# Spark option names that are fine as given (no log).
_SPARK_NAMES = {
    "header",
    "sep",
    "quote",
    "escape",
    "multiLine",
    "inferSchema",
    "delimiter",
    "encoding",
    "schema",
}
_GLUE_ONLY = {
    "useglueparquetwriter",
    "blocksize",
    "pagesize",
    "jsonpath",
    "optimizeperformance",
    "strictcheckforquoting",
    "skipfirst",
    "groupfiles",
    "groupsize",
}


def spark_format(name) -> str:
    if not name:
        raise ValueError("prytt: a format is required (e.g. format='json')")
    fmt = str(name).lower()
    fmt = _ALIASES.get(fmt, fmt)
    if fmt == "orc":
        log("WARN", "format 'orc' requested; Sail has no ORC data source")
        raise GlueLocalUnsupported(
            "ORC is not supported locally: Sail has no ORC data source. Use parquet."
        )
    if fmt not in _KNOWN:
        log("WARN", f"format {name!r} is not one prytt knows; handing it to Sail as-is")
    return fmt


def spark_options(fmt: str, format_options, write: bool = False) -> dict:
    out = {}
    for key, value in (format_options or {}).items():
        lower = key.lower()
        if lower in _GLUE_ONLY:
            log("WARN", f"format option {key}={value!r} has no local equivalent; ignored")
            continue
        target = _RENAME.get(lower, key)
        if target not in _SPARK_NAMES and key not in _RENAME:
            log("INFO", f"format option {key}={value!r} passed to Spark unchanged")
        out[target] = value
    if fmt == "csv" and not write:
        out.setdefault("inferSchema", True)
    return out
