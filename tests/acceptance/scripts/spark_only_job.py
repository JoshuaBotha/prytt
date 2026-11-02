from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("spark-only").getOrCreate()

totals = (
    spark.table("raw_db_orders")
    .groupBy("country")
    .agg(F.sum(F.col("amount").cast("double")).alias("total"))
    .orderBy(F.desc("total"))
)
print(" ".join(f"{r.country}={r.total}" for r in totals.collect()))
