"""Self-contained AWS Glue 6 self-test job.

Needs nothing but this file: it generates its own data, writes it out through
Glue's S3 sinks, reads it back through Glue's sources, runs it through the
Glue transforms and a broad slice of Spark SQL / DataFrame features, and
checks the results. Every check is isolated, so one failure does not hide the
rest. A summary table prints at the end and the job exits 1 if any EXACT
check failed.

Check tiers:
  EXACT  result must be identical on real Glue and locally (prytt + Sail).
  SMOKE  must run without raising; output is printed, not asserted, because
         prytt passes these through unchanged (Filter, Map, Relationalize,
         SplitFields, ...) while real Glue applies them.

Run locally (prytt installed):
  python glue_selftest_job.py --JOB_NAME selftest
  python glue_selftest_job.py --JOB_NAME selftest \
      --base_path s3://my-bucket/selftest --with_iceberg true
  python glue_selftest_job.py   # JOB_NAME is None (logged WARN); or set
                                # PRYTT_ARG_JOB_NAME / [args] in prytt.toml

Run on AWS Glue 6:
  Job parameters:
    --base_path     s3://<your-bucket>/glue-selftest   (required on Glue; the
                    default bucket below does not exist)
    --with_iceberg  true   (optional) and then also:
    --datalake-formats iceberg
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
           --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
           --conf spark.sql.catalog.glue_catalog.catalog-impl=\
                  org.apache.iceberg.aws.glue.GlueCatalog
           --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
           --conf spark.sql.catalog.glue_catalog.warehouse=s3://<your-bucket>/glue-selftest/warehouse
  Every run writes under <base_path>/<run_id>/, so reruns never collide.
  The Iceberg section creates database `prytt_selftest` and drops its table
  at the end.
"""

import sys
import traceback
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

from pyspark.context import SparkContext
from pyspark.sql import Row, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality

# --------------------------------------------------------------------------
# Boilerplate (Glue Studio shape) plus optional arguments
# --------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
optional = {}
for name, default in (
    ("base_path", "s3://prytt-selftest-bucket/glue-selftest"),
    ("with_iceberg", "false"),
):
    optional[name] = (
        getResolvedOptions(sys.argv, [name])[name] if f"--{name}" in sys.argv else default
    )

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

RUN_ID = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
BASE = optional["base_path"].rstrip("/") + "/" + RUN_ID
WITH_ICEBERG = optional["with_iceberg"].lower() in ("1", "true", "yes")
logger.info(f"selftest run_id={RUN_ID} base={BASE} iceberg={WITH_ICEBERG}")

# --------------------------------------------------------------------------
# Tiny check harness
# --------------------------------------------------------------------------

RESULTS = []  # (tier, name, status, detail)


def check(name, tier="EXACT"):
    def decorator(fn):
        try:
            detail = fn()
            RESULTS.append((tier, name, "PASS", "" if detail is None else str(detail)))
        except Exception as exc:  # broad on purpose: report every failure
            RESULTS.append(
                (tier, name, "FAIL", f"{type(exc).__name__}: {exc}".splitlines()[0][:160])
            )
            print(f"\n--- {tier} {name} failed ---\n{traceback.format_exc()}", file=sys.stderr)
        return fn

    return decorator


def eq(actual, expected, what="value"):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


def rows(df, *cols, order=None):
    """Collect as sorted list of tuples (order-independent comparison)."""
    data = df.select(*cols) if cols else df
    out = [tuple(r) for r in data.collect()]
    return (
        sorted(out, key=lambda t: tuple((v is None, str(v)) for v in t)) if order is None else out
    )


def path(*parts):
    return "/".join([BASE, *parts]) + "/"


# --------------------------------------------------------------------------
# Seed data (all generated here)
# --------------------------------------------------------------------------

customer_schema = StructType(
    [
        StructField("cust_id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("tier", StringType(), True),
        StructField(
            "address",
            StructType(
                [
                    StructField("city", StringType(), True),
                    StructField("country", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("tags", ArrayType(StringType()), True),
        StructField("attrs", MapType(StringType(), StringType()), True),
        StructField("signup_date", DateType(), True),
    ]
)
customers_data = [
    (1, "Ada", "gold", ("London", "GB"), ["vip", "early"], {"src": "web"}, date(2024, 1, 5)),
    (2, "Linus", "silver", ("Helsinki", "FI"), ["oss"], {"src": "app"}, date(2024, 3, 9)),
    (3, "Grace", "gold", ("New York", "US"), [], {}, date(2023, 11, 30)),
    (4, "Edsger", None, ("Austin", "US"), None, None, None),
    (5, "Barbara", "bronze", (None, "US"), ["new"], {"src": "web", "ref": "x"}, date(2025, 6, 1)),
]
customers_df = spark.createDataFrame(customers_data, customer_schema)

order_schema = StructType(
    [
        StructField("order_id", LongType(), False),
        StructField("customer_id", LongType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("amount_str", StringType(), True),
        StructField("status", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField(
            "lines",
            ArrayType(
                StructType(
                    [
                        StructField("sku", StringType(), True),
                        StructField("qty", LongType(), True),
                        StructField("price", DoubleType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)


def ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


orders_data = [
    (101, 1, Decimal("10.50"), "10.50", "PAID", ts("2025-01-01 09:00:00"), [("A", 1, 10.5)]),
    (
        102,
        1,
        Decimal("99.99"),
        "99.99",
        "PAID",
        ts("2025-01-03 12:30:00"),
        [("B", 3, 20.0), ("C", 1, 39.99)],
    ),
    (103, 2, Decimal("5.00"), "5", "REFUNDED", ts("2025-01-04 08:15:00"), [("A", 1, 5.0)]),
    (104, 3, Decimal("250.00"), "250.00", "PAID", ts("2025-02-10 18:45:00"), [("D", 5, 50.0)]),
    (105, 3, None, None, "PENDING", ts("2025-02-11 07:00:00"), []),
    (106, 9, Decimal("1.00"), "1.0", "PAID", ts("2025-02-12 23:59:59"), None),  # orphan customer
    (107, 5, Decimal("42.42"), "42.42", "PAID", ts("2025-03-01 00:00:00"), [("E", 2, 21.21)]),
]
orders_df = spark.createDataFrame(orders_data, order_schema).withColumn(
    "order_month", F.date_format("ts", "yyyy-MM")
)

customers_dyf = DynamicFrame.fromDF(customers_df, glueContext, "customers_dyf")
orders_dyf = DynamicFrame.fromDF(orders_df, glueContext, "orders_dyf")

# --------------------------------------------------------------------------
# awsglue.utils / Job
# --------------------------------------------------------------------------


@check("getResolvedOptions: --k v and --k=v forms, extras ignored")
def _():
    got = getResolvedOptions(["prog", "--A", "1", "--B=two", "--extra", "x"], ["A", "B"])
    eq(got["A"], "1", "A")
    eq(got["B"], "two", "B")


@check(
    "getResolvedOptions: missing option (Glue raises; prytt returns None unless strict)",
    tier="SMOKE",
)
def _():
    try:
        got = getResolvedOptions(["prog", "--A", "1"], ["A", "MISSING"])
    except BaseException as exc:  # argparse raises SystemExit on Glue
        return f"raised {type(exc).__name__}"
    eq(got["MISSING"], None, "MISSING")
    return "returned None"


@check("Job.isInitialized after init")
def _():
    eq(job.isInitialized(), True)


# --------------------------------------------------------------------------
# DynamicFrame basics
# --------------------------------------------------------------------------


@check("DynamicFrame.fromDF / toDF / count")
def _():
    eq(customers_dyf.count(), 5, "customers")
    eq(orders_dyf.count(), 7, "orders")
    eq(set(orders_dyf.toDF().columns) >= {"order_id", "lines", "order_month"}, True, "columns")


@check("DynamicFrame.printSchema / show / schema", tier="SMOKE")
def _():
    customers_dyf.printSchema()
    customers_dyf.show(2)
    return type(customers_dyf.schema()).__name__


@check("DynamicFrame.select_fields / drop_fields")
def _():
    eq(sorted(customers_dyf.select_fields(["cust_id", "name"]).toDF().columns), ["cust_id", "name"])
    eq("tags" in customers_dyf.drop_fields(["tags", "attrs"]).toDF().columns, False, "tags dropped")


@check("DynamicFrame.rename_field (top-level)")
def _():
    df = customers_dyf.rename_field("name", "full_name").toDF()
    eq("full_name" in df.columns and "name" not in df.columns, True)


@check("DynamicFrame.apply_mapping flattens nested struct")
def _():
    df = customers_dyf.apply_mapping(
        [("cust_id", "long", "id", "long"), ("address.city", "string", "city", "string")]
    ).toDF()
    eq(sorted(df.columns), ["city", "id"])
    eq(rows(df.filter("id = 2"), "id", "city"), [(2, "Helsinki")])


@check("DynamicFrame.resolveChoice cast spec")
def _():
    df = orders_dyf.resolveChoice(specs=[("amount_str", "cast:double")]).toDF()
    eq(dict(df.dtypes)["amount_str"], "double")
    eq(rows(df.filter("order_id = 103"), "amount_str"), [(5.0,)])


@check("DynamicFrame.repartition / coalesce keep rows")
def _():
    eq(orders_dyf.repartition(4).count(), 7)
    eq(orders_dyf.coalesce(1).count(), 7)


@check("DynamicFrame.errorsCount / stageErrorsCount")
def _():
    eq(orders_dyf.errorsCount(), 0)
    eq(orders_dyf.stageErrorsCount(), 0)


@check("DynamicFrameCollection keys / select / values")
def _():
    dfc = DynamicFrameCollection({"c": customers_dyf, "o": orders_dyf}, glueContext)
    eq(sorted(dfc.keys()), ["c", "o"])
    eq(dfc.select("o").count(), 7)
    eq(SelectFromCollection.apply(dfc=dfc, key="c").count(), 5)
    eq(len(list(dfc.values())), 2)


# --------------------------------------------------------------------------
# awsglue.transforms (real on both)
# --------------------------------------------------------------------------


@check("ApplyMapping: rename, cast string->double, nested path, drop unmapped")
def _():
    mapped = ApplyMapping.apply(
        frame=orders_dyf,
        mappings=[
            ("order_id", "long", "id", "long"),
            ("customer_id", "long", "customer_id", "long"),
            ("amount_str", "string", "amount", "double"),
            ("status", "string", "status", "string"),
        ],
        transformation_ctx="mapped_orders",
    ).toDF()
    eq(sorted(mapped.columns), ["amount", "customer_id", "id", "status"])
    eq(dict(mapped.dtypes)["amount"], "double")
    eq(rows(mapped.filter("id in (101, 105)"), "id", "amount"), [(101, 10.5), (105, None)])


@check("SelectFields / DropFields / RenameField")
def _():
    sel = SelectFields.apply(
        frame=customers_dyf, paths=["cust_id", "tier"], transformation_ctx="sel"
    ).toDF()
    eq(sorted(sel.columns), ["cust_id", "tier"])
    drp = DropFields.apply(frame=customers_dyf, paths=["attrs"], transformation_ctx="drp").toDF()
    eq("attrs" in drp.columns, False)
    ren = RenameField.apply(
        frame=customers_dyf, old_name="tier", new_name="level", transformation_ctx="ren"
    ).toDF()
    eq("level" in ren.columns and "tier" not in ren.columns, True)


@check("Join on differently named keys (inner)")
def _():
    joined = Join.apply(
        frame1=orders_dyf,
        frame2=customers_dyf,
        keys1=["customer_id"],
        keys2=["cust_id"],
        transformation_ctx="j",
    ).toDF()
    eq(joined.count(), 6, "orphan order 106 dropped")
    eq({"order_id", "cust_id", "customer_id", "name"} <= set(joined.columns), True, "columns")
    eq(rows(joined.filter("order_id = 104"), "name"), [("Grace",)])


@check("DropNullFields drops NullType fields (top-level and nested), by type not value")
def _():
    # Glue's DropNullFields walks the schema and drops fields typed NullType.
    # It never inspects values, so an all-null column with a real type stays.
    src = (
        customers_df.withColumn("always_null", F.lit(None))
        .withColumn("typed_null", F.lit(None).cast("string"))
        .withColumn("nested", F.struct(F.lit(1).alias("keep"), F.lit(None).alias("gone")))
    )
    out = DropNullFields.apply(
        frame=DynamicFrame.fromDF(src, glueContext, "wn"), transformation_ctx="dnf"
    ).toDF()
    eq("always_null" in out.columns, False, "NullType column dropped")
    eq("typed_null" in out.columns, True, "typed all-null column kept")
    eq("tier" in out.columns, True, "partly-null column kept")
    eq(out.select("nested.*").columns, ["keep"], "nested NullType field dropped")


@check("ResolveChoice transform cast spec")
def _():
    out = ResolveChoice.apply(
        frame=orders_dyf, specs=[("amount_str", "cast:double")], transformation_ctx="rc"
    ).toDF()
    eq(dict(out.dtypes)["amount_str"], "double")


@check("ErrorsAsDynamicFrame returns a frame", tier="SMOKE")
def _():
    return f"error rows={ErrorsAsDynamicFrame.apply(orders_dyf).count()}"


# --------------------------------------------------------------------------
# awsglue.transforms that prytt passes through (SMOKE)
# --------------------------------------------------------------------------


@check("Filter.apply (prytt: pass-through)", tier="SMOKE")
def _():
    out = Filter.apply(
        frame=orders_dyf, f=lambda r: r["status"] == "PAID", transformation_ctx="flt"
    )
    return f"rows={out.count()} (Glue expects 5, prytt v1 returns 7)"


@check("Map.apply (prytt: pass-through)", tier="SMOKE")
def _():
    def add_flag(rec):
        rec["is_big"] = (rec["amount_str"] or "0") > "50"
        return rec

    out = Map.apply(frame=orders_dyf, f=add_flag, transformation_ctx="mp")
    return f"has is_big={'is_big' in out.toDF().columns}"


@check("SplitFields / SplitRows return collections", tier="SMOKE")
def _():
    sf = SplitFields.apply(
        frame=customers_dyf, paths=["cust_id", "name"], name1="left", name2="right"
    )
    sr = SplitRows.apply(
        frame=orders_dyf, comparison_dict={"customer_id": {">": 2}}, name1="hi", name2="lo"
    )
    return f"split_fields={sorted(sf.keys())} split_rows={sorted(sr.keys())}"


@check("Relationalize / UnnestFrame / Spigot", tier="SMOKE")
def _():
    rel = Relationalize.apply(
        frame=orders_dyf, staging_path=path("tmp", "relationalize"), name="root"
    )
    un = UnnestFrame.apply(frame=customers_dyf)
    sp = Spigot.apply(frame=orders_dyf, path=path("spigot"), options={"topk": 2})
    return (
        f"relationalize keys={sorted(rel.keys())} unnest_cols={len(un.toDF().columns)} "
        f"spigot_rows={sp.count()}"
    )


@check("EvaluateDataQuality.apply / process_rows", tier="SMOKE")
def _():
    ruleset = """Rules = [ ColumnCount > 0, IsComplete "order_id", RowCount between 1 and 100 ]"""
    EvaluateDataQuality.apply(
        frame=orders_dyf,
        ruleset=ruleset,
        publishing_options={"dataQualityEvaluationContext": "selftest_dq"},
    )
    dfc = EvaluateDataQuality().process_rows(
        frame=orders_dyf,
        ruleset=ruleset,
        publishing_options={"dataQualityEvaluationContext": "selftest_dq_rows"},
        additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT"},
    )
    eq(SelectFromCollection.apply(dfc=dfc, key="originalData").count(), 7, "originalData rows")
    return f"keys={sorted(dfc.keys())}"


# --------------------------------------------------------------------------
# S3 sinks and sources (write then read back)
# --------------------------------------------------------------------------

flat_orders = ApplyMapping.apply(
    frame=orders_dyf,
    mappings=[
        ("order_id", "long", "order_id", "long"),
        ("customer_id", "long", "customer_id", "long"),
        ("amount_str", "string", "amount", "double"),
        ("status", "string", "status", "string"),
        ("order_month", "string", "order_month", "string"),
    ],
    transformation_ctx="flat_orders",
)


@check("write+read glueparquet (snappy)")
def _():
    glueContext.write_dynamic_frame.from_options(
        frame=flat_orders,
        connection_type="s3",
        connection_options={"path": path("parquet", "orders")},
        format="glueparquet",
        format_options={"compression": "snappy"},
        transformation_ctx="w_parquet",
    )
    back = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path("parquet", "orders")]},
        format="parquet",
        transformation_ctx="r_parquet",
    ).toDF()
    eq(back.count(), 7)
    eq(rows(back.filter("order_id = 107"), "amount", "status"), [(42.42, "PAID")])


@check("write+read parquet with partitionKeys")
def _():
    glueContext.write_dynamic_frame.from_options(
        frame=flat_orders,
        connection_type="s3",
        connection_options={
            "path": path("parquet", "orders_by_month"),
            "partitionKeys": ["order_month"],
        },
        format="parquet",
        transformation_ctx="w_part",
    )
    back = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path("parquet", "orders_by_month")], "recurse": True},
        format="parquet",
        transformation_ctx="r_part",
    ).toDF()
    eq(back.count(), 7)
    return f"columns={sorted(back.columns)}"


@check("write+read CSV with header and custom separator")
def _():
    glueContext.write_dynamic_frame.from_options(
        frame=flat_orders,
        connection_type="s3",
        connection_options={"path": path("csv", "orders")},
        format="csv",
        format_options={"writeHeader": True, "separator": "|"},
        transformation_ctx="w_csv",
    )
    back = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path("csv", "orders")]},
        format="csv",
        format_options={"withHeader": True, "separator": "|"},
        transformation_ctx="r_csv",
    ).toDF()
    eq(back.count(), 7)
    eq(sorted(back.columns), ["amount", "customer_id", "order_id", "order_month", "status"])
    # Type inference differs between engines; compare as strings.
    got = rows(back.filter(F.col("order_id").cast("long") == 102), F.col("status").cast("string"))
    eq(got, [("PAID",)])


@check("write+read JSON lines, nested types survive")
def _():
    glueContext.write_dynamic_frame.from_options(
        frame=customers_dyf,
        connection_type="s3",
        connection_options={"path": path("json", "customers")},
        format="json",
        transformation_ctx="w_json",
    )
    back = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path("json", "customers")]},
        format="json",
        transformation_ctx="r_json",
    ).toDF()
    eq(back.count(), 5)
    eq(rows(back.filter("cust_id = 1"), F.col("address.city"), F.size("tags")), [("London", 2)])


@check("create_data_frame.from_options on S3 (Glue: streaming sources only)", tier="SMOKE")
def _():
    # Real Glue routes this through getStreamingSource (Kafka/Kinesis), so an
    # S3 batch read is expected to fail there; prytt reads it as a batch.
    df = glueContext.create_data_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path("parquet", "orders")]},
        format="parquet",
        transformation_ctx="cdf",
    )
    eq(type(df).__name__, "DataFrame")
    eq(df.count(), 7)


@check("Glue write mode is append (second write doubles rows)")
def _():
    target = path("parquet", "append_test")
    for i in range(2):
        glueContext.write_dynamic_frame.from_options(
            frame=flat_orders,
            connection_type="s3",
            connection_options={"path": target},
            format="parquet",
            transformation_ctx=f"w_append_{i}",
        )
    back = glueContext.create_dynamic_frame.from_options(
        connection_type="s3", connection_options={"paths": [target]}, format="parquet"
    )
    eq(back.count(), 14)


@check("orc format (prytt: unsupported, raises)", tier="SMOKE")
def _():
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=flat_orders,
            connection_type="s3",
            connection_options={"path": path("orc", "orders")},
            format="orc",
        )
        return "orc write ok"
    except Exception as exc:
        return f"raised {type(exc).__name__}"


# --------------------------------------------------------------------------
# Spark SQL / DataFrame coverage through the Glue session
# --------------------------------------------------------------------------

orders_df.createOrReplaceTempView("orders")
customers_df.createOrReplaceTempView("customers")


@check("glueContext.sql delegates to Spark (temp views)")
def _():
    eq(glueContext.sql("SELECT count(*) AS n FROM orders").collect()[0]["n"], 7)


@check("SQL join + group by + having + order by")
def _():
    df = spark.sql(
        """
        SELECT c.name, count(*) AS n, sum(o.amount) AS total
        FROM orders o JOIN customers c ON o.customer_id = c.cust_id
        WHERE o.status = 'PAID'
        GROUP BY c.name HAVING count(*) >= 1
        ORDER BY total DESC
        """
    )
    eq(
        [tuple(r) for r in df.collect()],
        [
            ("Grace", 1, Decimal("250.00")),
            ("Ada", 2, Decimal("110.49")),
            ("Barbara", 1, Decimal("42.42")),
        ],
    )


@check("SQL CTE + window functions (row_number, running sum, lag)")
def _():
    df = spark.sql(
        """
        WITH paid AS (SELECT * FROM orders WHERE amount IS NOT NULL)
        SELECT order_id,
               row_number() OVER (PARTITION BY customer_id ORDER BY ts) AS rn,
               sum(amount)  OVER (PARTITION BY customer_id ORDER BY ts) AS running,
               lag(order_id) OVER (PARTITION BY customer_id ORDER BY ts) AS prev
        FROM paid WHERE customer_id = 1
        """
    )
    eq(rows(df, order=True), [(101, 1, Decimal("10.50"), None), (102, 2, Decimal("110.49"), 101)])


@check("DataFrame window API: rank + dense_rank")
def _():
    w = Window.partitionBy("tier").orderBy(F.col("signup_date").asc_nulls_last())
    df = (
        customers_df.filter("tier = 'gold'")
        .withColumn("r", F.rank().over(w))
        .withColumn("d", F.dense_rank().over(w))
    )
    eq(rows(df, "name", "r", "d"), [("Ada", 2, 2), ("Grace", 1, 1)])


@check("explode array of structs + aggregate")
def _():
    lines = orders_df.select("order_id", F.explode("lines").alias("l")).select(
        "order_id", "l.sku", (F.col("l.qty") * F.col("l.price")).alias("line_total")
    )
    eq(lines.count(), 6)
    total = (
        lines.filter("order_id = 102")
        .agg(F.round(F.sum("line_total"), 2).alias("t"))
        .collect()[0]["t"]
    )
    eq(total, 99.99)


@check("explode_outer keeps empty/null arrays")
def _():
    eq(
        orders_df.select("order_id", F.explode_outer("lines")).count(), 8
    )  # 6 lines + 1 empty + 1 null


@check("map and array functions")
def _():
    df = customers_df.select(
        "cust_id",
        F.map_keys("attrs").alias("k"),
        F.array_contains("tags", "vip").alias("vip"),
        F.coalesce(F.size("tags"), F.lit(-1)).alias("ntags"),
        F.element_at("attrs", "src").alias("src"),
    )
    got = {r["cust_id"]: (r["vip"], r["src"]) for r in df.collect()}
    eq(got[1], (True, "web"))
    eq(got[2], (False, "app"))


@check("struct construction + to_json / from_json round trip")
def _():
    js = customers_df.select("cust_id", F.to_json(F.struct("name", "address")).alias("j"))
    parsed = js.select(
        "cust_id",
        F.from_json("j", "name STRING, address STRUCT<city: STRING, country: STRING>").alias("p"),
    )
    eq(rows(parsed.filter("cust_id = 3"), "p.name", "p.address.country"), [("Grace", "US")])


@check("date/time functions")
def _():
    r = (
        orders_df.filter("order_id = 104")
        .select(
            F.to_date("ts").alias("d"),
            F.year("ts").alias("y"),
            F.dayofweek("ts").alias("dow"),
            F.date_add(F.to_date("ts"), 30).alias("plus30"),
            F.datediff(F.lit("2025-03-01").cast("date"), F.to_date("ts")).alias("diff"),
            F.date_trunc("month", "ts").alias("m"),
        )
        .collect()[0]
    )
    eq(
        (r["d"], r["y"], r["dow"], r["plus30"], r["diff"]),
        (date(2025, 2, 10), 2025, 2, date(2025, 3, 12), 19),
    )
    eq(r["m"], datetime(2025, 2, 1))


@check("string functions + regex")
def _():
    r = (
        customers_df.filter("cust_id = 2")
        .select(
            F.upper("name").alias("u"),
            F.concat_ws("-", "name", "tier").alias("c"),
            F.regexp_replace("name", "[aeiou]", "_").alias("rx"),
            F.regexp_extract(F.lit("SKU-0042"), r"(\d+)", 1).alias("num"),
            F.lpad(F.col("cust_id").cast("string"), 4, "0").alias("pad"),
        )
        .collect()[0]
    )
    eq(tuple(r), ("LINUS", "Linus-silver", "L_n_s", "0042", "0002"))


@check("decimal arithmetic precision")
def _():
    r = orders_df.agg(F.sum("amount").alias("s"), F.avg("amount").alias("a")).collect()[0]
    eq(r["s"], Decimal("408.91"))
    eq(round(float(r["a"]), 4), round(408.91 / 6, 4))


@check("null semantics: coalesce, nvl, null-safe equal, count(col)")
def _():
    r = spark.sql(
        """
        SELECT count(*) AS n, count(amount) AS n_amount,
               sum(CASE WHEN amount <=> NULL THEN 1 ELSE 0 END) AS n_null,
               max(coalesce(amount_str, 'none')) AS mx
        FROM orders
        """
    ).collect()[0]
    eq((r["n"], r["n_amount"], r["n_null"], r["mx"]), (7, 6, 1, "none"))


@check("pivot")
def _():
    p = orders_df.groupBy("customer_id").pivot("status", ["PAID", "REFUNDED"]).count()
    eq(rows(p.filter("customer_id = 1"), "customer_id", "PAID", "REFUNDED"), [(1, 2, None)])


@check("union / unionByName(allowMissingColumns) / distinct / dropDuplicates")
def _():
    a = customers_df.select("cust_id", "name")
    b = customers_df.select("name", "cust_id")
    eq(a.unionByName(b).distinct().count(), 5)
    eq(a.union(a).dropDuplicates(["cust_id"]).count(), 5)
    c = customers_df.select("cust_id")
    eq(a.unionByName(c, allowMissingColumns=True).filter("name IS NULL").count(), 5)


@check("anti / semi / left joins with broadcast hint")
def _():
    anti = orders_df.join(
        F.broadcast(customers_df), orders_df.customer_id == customers_df.cust_id, "left_anti"
    )
    eq(rows(anti, "order_id"), [(106,)])
    semi = customers_df.join(orders_df, customers_df.cust_id == orders_df.customer_id, "left_semi")
    eq(rows(semi, "cust_id"), [(1,), (2,), (3,), (5,)])
    left = customers_df.join(orders_df, customers_df.cust_id == orders_df.customer_id, "left")
    eq(left.filter("order_id IS NULL").count(), 1, "Edsger has no orders")


@check("collect_list / collect_set / sort_array / approx_count_distinct")
def _():
    r = (
        orders_df.groupBy("customer_id")
        .agg(
            F.sort_array(F.collect_list("order_id")).alias("ids"),
            F.size(F.collect_set("status")).alias("nstatus"),
        )
        .filter("customer_id = 3")
        .collect()[0]
    )
    eq((r["ids"], r["nstatus"]), ([104, 105], 2))
    eq(orders_df.agg(F.approx_count_distinct("customer_id")).collect()[0][0], 5)


@check("cache / persist / unpersist")
def _():
    cached = orders_df.cache()
    eq(cached.count(), 7)
    cached.unpersist()


@check("Python UDF")
def _():
    @F.udf(returnType=StringType())
    def bucket(amount):
        if amount is None:
            return "none"
        return "big" if amount >= 100 else "small"

    df = orders_df.select("order_id", bucket("amount").alias("b"))
    eq(dict((r["order_id"], r["b"]) for r in df.collect())[104], "big")
    eq(dict((r["order_id"], r["b"]) for r in df.collect())[105], "none")


@check("pandas_udf (scalar)")
def _():
    import pandas as pd

    @F.pandas_udf(DoubleType())
    def double_it(s: pd.Series) -> pd.Series:
        return s * 2

    df = orders_df.select("order_id", double_it(F.col("amount").cast("double")).alias("x"))
    eq(rows(df.filter("order_id = 101"), "x"), [(21.0,)])


@check("mapInPandas with nulls in string column")
def _():
    def fn(batches):
        for pdf in batches:
            pdf = pdf.astype(object).where(pdf.notna(), None)
            pdf["tier_len"] = pdf["tier"].map(lambda t: len(t) if t is not None else -1)
            yield pdf[["cust_id", "tier_len"]]

    out = customers_df.select("cust_id", "tier").mapInPandas(fn, "cust_id long, tier_len long")
    eq(dict((r["cust_id"], r["tier_len"]) for r in out.collect())[4], -1)


@check("groupBy().applyInPandas")
def _():
    def summarise(pdf):
        import pandas as pd

        return pd.DataFrame({"customer_id": [pdf["customer_id"].iloc[0]], "n": [len(pdf)]})

    out = (
        orders_df.select("customer_id", "order_id")
        .groupBy("customer_id")
        .applyInPandas(summarise, "customer_id long, n long")
    )
    eq(rows(out), [(1, 2), (2, 1), (3, 2), (5, 1), (9, 1)])


@check("createDataFrame from Row objects + schema DDL string")
def _():
    df = spark.createDataFrame([Row(a=1, b="x"), Row(a=2, b=None)])
    eq(df.count(), 2)
    df2 = spark.createDataFrame([(1, "x")], "a INT, b STRING")
    eq(df2.dtypes, [("a", "int"), ("b", "string")])


@check("spark.range + selectExpr: sum + percentile_approx")
def _():
    r = (
        spark.range(1, 101)
        .selectExpr("sum(id) AS s", "percentile_approx(id, 0.5) AS med")
        .collect()[0]
    )
    eq(r["s"], 5050)
    # Spark returns an actual input value (bigint 50); Sail 0.7.3 interpolates (50.375).
    eq(r["med"], 50, "percentile_approx median")


@check("SQL temp view of a DynamicFrame (Glue Studio sparkSqlQuery pattern)")
def _():
    def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(query)
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

    out = sparkSqlQuery(
        glueContext,
        query="SELECT status, count(*) AS n FROM src GROUP BY status",
        mapping={"src": flat_orders},
        transformation_ctx="sql_node",
    )
    eq(rows(out.toDF()), [("PAID", 5), ("PENDING", 1), ("REFUNDED", 1)])


# --------------------------------------------------------------------------
# Iceberg via glue_catalog (opt-in: --with_iceberg true)
# --------------------------------------------------------------------------

if WITH_ICEBERG:
    ICE_DB = "prytt_selftest"
    ICE_TBL = f"glue_catalog.{ICE_DB}.orders_{RUN_ID.replace('-', '_').lower()}"

    @check("Iceberg: CREATE DATABASE / CREATE TABLE")
    def _():
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{ICE_DB}")
        spark.sql(
            f"""
            CREATE TABLE {ICE_TBL} (
              order_id BIGINT, customer_id BIGINT, amount DOUBLE, status STRING, order_month STRING
            ) USING iceberg PARTITIONED BY (order_month)
            """
        )

    @check("Iceberg: INSERT via writeTo().append()")
    def _():
        flat_orders.toDF().select(
            "order_id", "customer_id", "amount", "status", "order_month"
        ).writeTo(ICE_TBL).append()
        eq(spark.table(ICE_TBL).count(), 7)

    @check("Iceberg: MERGE INTO (update + insert)")
    def _():
        spark.createDataFrame(
            [(105, 3, 77.0, "PAID", "2025-02"), (108, 2, 8.0, "PAID", "2025-03")],
            "order_id BIGINT, customer_id BIGINT, amount DOUBLE, status STRING, order_month STRING",
        ).createOrReplaceTempView("ice_updates")
        spark.sql(
            f"""
            MERGE INTO {ICE_TBL} t USING ice_updates s ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        t = spark.table(ICE_TBL)
        eq(t.count(), 8)
        eq(rows(t.filter("order_id = 105"), "status", "amount"), [("PAID", 77.0)])

    @check("Iceberg: INSERT INTO ... SELECT + aggregate read")
    def _():
        spark.sql(f"INSERT INTO {ICE_TBL} VALUES (109, 1, 1.5, 'PAID', '2025-03')")
        r = spark.sql(
            f"SELECT count(*) AS n, round(sum(amount), 2) AS s FROM {ICE_TBL} WHERE status = 'PAID'"
        ).collect()[0]
        eq((r["n"], r["s"]), (8, round(10.5 + 99.99 + 250.0 + 1.0 + 42.42 + 77.0 + 8.0 + 1.5, 2)))

    @check("Iceberg: DELETE FROM", tier="SMOKE")
    def _():
        spark.sql(f"DELETE FROM {ICE_TBL} WHERE status = 'REFUNDED'")
        return f"rows after delete={spark.table(ICE_TBL).count()}"

    @check("Iceberg: DROP TABLE", tier="SMOKE")
    def _():
        spark.sql(f"DROP TABLE IF EXISTS {ICE_TBL}")


# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------

job.commit()

width = max(len(name) for _, name, _, _ in RESULTS)
print("\n" + "=" * (width + 30))
print(f"Glue self-test {RUN_ID}  base={BASE}")
print("=" * (width + 30))
for tier, name, status, detail in RESULTS:
    print(f"{status:4}  {tier:5}  {name:<{width}}  {detail}")
failed = [r for r in RESULTS if r[2] == "FAIL" and r[0] == "EXACT"]
smoke_failed = [r for r in RESULTS if r[2] == "FAIL" and r[0] == "SMOKE"]
passed = sum(1 for r in RESULTS if r[2] == "PASS")
print(
    f"\n{passed}/{len(RESULTS)} passed; "
    f"EXACT failures: {len(failed)}; SMOKE failures: {len(smoke_failed)}"
)
logger.info(f"selftest finished: {passed}/{len(RESULTS)} passed")

if failed or smoke_failed:
    sys.exit(1)
