"""S7: Sail lakehouse table support (Delta / Iceberg), MERGE INTO, and catalog DDL (also covers S-SQL1)."""
import os, tempfile
from pysail.spark import SparkConnectServer
srv = SparkConnectServer(ip="127.0.0.1", port=0); srv.start(background=True)
h, p = srv.listening_address; os.environ["SPARK_REMOTE"] = f"sc://{h}:{p}"
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
d = tempfile.mkdtemp()
df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string")
upd = spark.createDataFrame([(2, "B"), (3, "c")], "id int, v string"); upd.createOrReplaceTempView("upd")

def case(name, fn):
    try:
        r = fn(); print(f"OK   {name}: {r}")
    except Exception as e:
        print(f"FAIL {name}: {type(e).__name__}: {str(e).strip().splitlines()[0][:250] if str(e).strip() else ''}")

for fmt in ["delta", "iceberg"]:
    path = f"{d}/{fmt}_t"
    case(f"{fmt} df write", lambda: df.write.format(fmt).mode("overwrite").save(path) or "written")
    case(f"{fmt} df append", lambda: df.write.format(fmt).mode("append").save(path) or "appended")
    case(f"{fmt} df read", lambda: spark.read.format(fmt).load(path).count())
    case(f"{fmt} CREATE TABLE LOCATION", lambda: spark.sql(f"CREATE TABLE {fmt}_tbl (id int, v string) USING {fmt} LOCATION '{d}/{fmt}_sql'").collect() or "created")
    case(f"{fmt} INSERT", lambda: spark.sql(f"INSERT INTO {fmt}_tbl VALUES (1,'a'),(2,'b')").collect() or "inserted")
    case(f"{fmt} MERGE INTO", lambda: spark.sql(f"""MERGE INTO {fmt}_tbl t USING upd s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.v = s.v WHEN NOT MATCHED THEN INSERT *""").collect() or sorted(spark.table(f"{fmt}_tbl").collect()))
    case(f"{fmt} saveAsTable", lambda: df.write.format(fmt).saveAsTable(f"{fmt}_sat") or spark.table(f"{fmt}_sat").count())

# catalog / DDL (S-SQL1)
case("CREATE DATABASE", lambda: spark.sql("CREATE DATABASE IF NOT EXISTS raw_db").collect() or spark.catalog.listDatabases())
df.write.mode("overwrite").json(f"{d}/orders_json")
case("CREATE external json table", lambda: spark.sql(f"CREATE TABLE raw_db.orders USING json LOCATION '{d}/orders_json'").collect() or "created")
case("spark.sql db.table", lambda: spark.sql("SELECT count(*) c FROM raw_db.orders").collect())
case("spark.table db.table", lambda: spark.table("raw_db.orders").count())
case("3-part glue_catalog.db.tbl", lambda: spark.sql("SELECT * FROM glue_catalog.raw_db.orders").count())
case("listCatalogs", lambda: spark.catalog.listCatalogs())
case("setCurrentCatalog glue_catalog", lambda: spark.catalog.setCurrentCatalog("glue_catalog") or "ok")
spark.stop(); srv.stop()
