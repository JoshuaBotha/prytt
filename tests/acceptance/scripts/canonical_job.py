import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path"])
sc = SparkContext()
sc.setLogLevel("WARN")
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
assert job.isInitialized()

orders = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["source_path"]]},
    format="json",
    transformation_ctx="orders",
)

mapped = ApplyMapping.apply(
    frame=orders,
    mappings=[
        ("id", "long", "order_id", "long"),
        ("amount", "string", "amount", "double"),
        ("country", "string", "country", "string"),
    ],
    transformation_ctx="mapped",
)

glueContext.write_dynamic_frame.from_options(
    frame=mapped,
    connection_type="s3",
    connection_options={"path": "s3://curated/orders/", "partitionKeys": ["country"]},
    format="glueparquet",
    format_options={"compression": "snappy"},
    transformation_ctx="sink",
)

job.commit()
