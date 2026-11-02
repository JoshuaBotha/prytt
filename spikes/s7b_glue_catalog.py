"""S7b: expose a `glue_catalog` catalog via SAIL_CATALOG__LIST and run Iceberg MERGE through 3-part names."""
import os, tempfile
wh = tempfile.mkdtemp()
os.environ["SAIL_CATALOG__LIST"] = ('[{name="sail", type="memory", initial_database=["default"]},'
                                    ' {name="glue_catalog", type="memory", initial_database=["default"]}]')
os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "sail"
from pysail.spark import SparkConnectServer
srv = SparkConnectServer(ip="127.0.0.1", port=0); srv.start(background=True)
h, p = srv.listening_address; os.environ["SPARK_REMOTE"] = f"sc://{h}:{p}"
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.sql.warehouse.dir", wh).getOrCreate()

def case(name, fn):
    try:
        print(f"OK   {name}: {fn()}")
    except Exception as e:
        print(f"FAIL {name}: {type(e).__name__}: {(str(e).strip().splitlines() or [''])[0][:250]}")

case("listCatalogs", lambda: [c.name for c in spark.catalog.listCatalogs()])
case("CREATE DATABASE glue_catalog.db", lambda: spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.db").collect())
case("CREATE iceberg glue_catalog.db.tbl", lambda: spark.sql(f"CREATE TABLE glue_catalog.db.tbl (id int, v string) USING iceberg LOCATION '{wh}/db/tbl'").collect())
case("INSERT", lambda: spark.sql("INSERT INTO glue_catalog.db.tbl VALUES (1,'a'),(2,'b')").collect() or "ok")
spark.createDataFrame([(2, "B"), (3, "c")], "id int, v string").createOrReplaceTempView("upd")
case("MERGE INTO glue_catalog.db.tbl", lambda: spark.sql("""MERGE INTO glue_catalog.db.tbl t USING upd s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""").collect() or "ok")
case("read back", lambda: sorted(spark.table("glue_catalog.db.tbl").collect()))
case("CTAS no LOCATION (warehouse)", lambda: spark.sql("CREATE TABLE glue_catalog.db.ctas USING iceberg AS SELECT * FROM upd").collect() or spark.table("glue_catalog.db.ctas").count())
case("saveAsTable iceberg w/ warehouse", lambda: spark.table("upd").write.format("iceberg").saveAsTable("glue_catalog.db.sat") or spark.table("glue_catalog.db.sat").count())
case("writeTo().append()", lambda: spark.table("upd").writeTo("glue_catalog.db.tbl").append() or spark.table("glue_catalog.db.tbl").count())
case("table persists in new session? (same server)", lambda: SparkSession.builder.create().table("glue_catalog.db.tbl").count())
spark.stop(); srv.stop()
print("warehouse contents:", sorted(os.listdir(wh)))
