import sys

from pyspark.context import SparkContext

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# GlueContext method prytt does not implement.
glueContext.purge_s3_path("s3://curated/stale/", options={"retentionPeriod": 0})

orders = glueContext.create_dynamic_frame.from_catalog(database="raw_db", table_name="orders")

# DynamicFrame method prytt does not implement.
orders.relationalize("root", "s3://tmp/relationalize/")

print(f"rows={orders.count()}")
job.commit()
