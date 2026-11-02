"""S6: Sail support for Python UDFs / mapInPandas / applyInPandas (gates DynamicFrame filter/map v2)."""
import os
from pysail.spark import SparkConnectServer
srv = SparkConnectServer(ip="127.0.0.1", port=0); srv.start(background=True)
h, p = srv.listening_address; os.environ["SPARK_REMOTE"] = f"sc://{h}:{p}"
from pyspark.sql import SparkSession, functions as F, types as T
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a", {"k": 1}), (2, "b", {"k": 2}), (3, None, {"k": 3})],
                           "id int, v string, m map<string,int>")

def case(name, fn):
    try:
        print(f"OK   {name}: {fn()}")
    except Exception as e:
        print(f"FAIL {name}: {type(e).__name__}: {str(e)[:300]}")

@F.udf("string")
def up(s): return s.upper() if s else s
case("scalar udf", lambda: df.select(up("v")).collect())

@F.pandas_udf("long")
def plus1(s): return s + 1
case("pandas_udf", lambda: df.select(plus1("id")).collect())

# DynamicFrame.filter(f) shape: f takes a dict-like record, returns bool
def filt(it):
    for pdf in it:
        recs = pdf.to_dict("records")
        keep = [r for r in recs if r["id"] % 2 == 1]
        import pandas as pd
        yield pd.DataFrame(keep, columns=pdf.columns)
case("mapInPandas filter", lambda: df.mapInPandas(filt, df.schema).collect())

# DynamicFrame.map(f) shape: dict in, dict out (same schema here)
def mapper(it):
    import pandas as pd
    for pdf in it:
        out = [{**r, "v": (r["v"] or "") + "!"} for r in pdf.to_dict("records")]
        yield pd.DataFrame(out, columns=pdf.columns)
case("mapInPandas map", lambda: df.mapInPandas(mapper, df.schema).collect())

def arrow_id(it):
    for b in it: yield b
case("mapInArrow", lambda: df.mapInArrow(arrow_id, df.schema).count())
case("applyInPandas", lambda: df.groupBy("id").applyInPandas(lambda pdf: pdf, df.schema).count())
spark.stop(); srv.stop()
