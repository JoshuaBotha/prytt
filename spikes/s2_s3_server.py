"""S2 + S3: in-process Sail server lifecycle and pyspark-client 4.1 compatibility.

Usage: python s2_s3_server.py [nostop]
  nostop: skip explicit stop() to see whether a background server blocks exit.
"""
import os, sys, time
t0 = time.perf_counter()
from pysail.spark import SparkConnectServer
srv = SparkConnectServer(ip="127.0.0.1", port=0)
srv.start(background=True)
addr = srv.listening_address
print("start took %.3fs, address=%r running=%r" % (time.perf_counter() - t0, addr, srv.running))
os.environ["SPARK_REMOTE"] = f"sc://{addr[0]}:{addr[1]}"

from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
print("spark.version:", spark.version)
df = spark.createDataFrame([(1, "a"), (2, "b"), (3, None)], "id int, v string")
df.withColumn("u", F.upper("v")).filter("id > 1").show()
print("sql:", spark.sql("select 1 + 1 as x").collect())
# round-trip parquet/json/csv
import tempfile
d = tempfile.mkdtemp()
for fmt in ["parquet", "json", "csv", "orc"]:
    try:
        df.write.format(fmt).mode("append").partitionBy("id").save(f"{d}/{fmt}")
        n = spark.read.format(fmt).load(f"{d}/{fmt}").count()
        print(f"{fmt}: write+read ok, rows={n}")
    except Exception as e:
        print(f"{fmt}: FAIL {type(e).__name__}: {str(e)[:200]}")
df.createOrReplaceTempView("raw_db_orders")
print("tempview:", spark.sql("select count(*) c from raw_db_orders").collect())

if "nostop" not in sys.argv:
    spark.stop()
    srv.stop()
    print("after stop running=%r addr=%r" % (srv.running, srv.listening_address))
print("end of script")
