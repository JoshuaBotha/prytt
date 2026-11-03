"""Read and write DataFrames at Glue-style locations.

Shared by ``awsglue.context`` (readers/writers) and
``prytt.paths.register_temp_views`` (catalog temp views), so ``prytt`` never
imports ``awsglue``.

Interface:

- ``read_df(spark, uris, format, format_options=None) -> DataFrame``: map
  ``uris`` (one string or a list) through ``paths.map_paths``, the format and
  options through ``prytt.formats``, and return
  ``spark.read.format(fmt).options(...).load(local)``. Config errors (ORC,
  bad ``s3.mode``, passthrough without a region, no paths) raise before
  ``spark`` is touched. The read itself is lazy (Spark Connect), so a missing
  path only fails when the frame is used.
- ``write_df(df, uri, format, format_options=None, partition_keys=None)``:
  append ``df`` at the mapped ``uri`` (creating the local dir), partitioned by
  ``partition_keys`` if given.
"""

from prytt import formats, paths


def read_df(spark, uris, format, format_options=None):
    fmt = formats.spark_format(format)
    local = paths.map_paths(uris)
    if not local:
        raise ValueError("prytt: connection_options needs 'paths' (a list of s3:// URIs)")
    options = formats.spark_options(fmt, format_options)
    return spark.read.format(fmt).options(**options).load(local)


def write_df(df, uri, format, format_options=None, partition_keys=None):
    if not uri:
        raise ValueError("prytt: connection_options needs 'path' (an s3:// URI)")
    fmt = formats.spark_format(format)
    local = paths.map_path(uri, for_write=True)
    writer = df.write.format(fmt).mode("append").options(**formats.spark_options(fmt, format_options, write=True))
    if partition_keys:
        writer = writer.partitionBy(*partition_keys)
    writer.save(local)
