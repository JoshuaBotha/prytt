"""S5: can Sail read s3:// directly (passthrough mode)? Reads a small public object."""
import os, sys, time
from pysail.spark import SparkConnectServer
srv = SparkConnectServer(ip="127.0.0.1", port=0); srv.start(background=True)
h, p = srv.listening_address; os.environ["SPARK_REMOTE"] = f"sc://{h}:{p}"
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
url = sys.argv[1]
t = time.perf_counter()
try:
    df = spark.read.parquet(url)
    print("schema:", df.columns[:6]); print("rows(limit 3):", df.limit(3).collect()[:1])
    print("OK %.1fs" % (time.perf_counter() - t))
except Exception as e:
    print("FAIL %.1fs %s: %s" % (time.perf_counter() - t, type(e).__name__, str(e)[:600]))
spark.stop(); srv.stop()
