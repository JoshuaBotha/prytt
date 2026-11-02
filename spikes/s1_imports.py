"""S1: what classic-only entry points do under pyspark-client alone."""
import importlib, traceback

def probe(label, stmt):
    try:
        ns = {}
        exec(stmt, ns)
        print(f"OK   {label}")
        return ns
    except BaseException as e:
        print(f"FAIL {label}: {type(e).__name__}: {e}")
        return None

probe("import pyspark", "import pyspark; print('     version', pyspark.__version__); print('     has SparkContext attr:', hasattr(pyspark,'SparkContext'))")
probe("from pyspark import SparkContext", "from pyspark import SparkContext")
probe("import pyspark.context", "import pyspark.context")
probe("from pyspark.context import SparkContext", "from pyspark.context import SparkContext")
ns = probe("from pyspark.conf import SparkConf", "from pyspark.conf import SparkConf; c=SparkConf(); c.set('a','b'); print('     SparkConf get', c.get('a'))")
probe("from pyspark.sql import SQLContext", "from pyspark.sql import SQLContext")
probe("from pyspark.sql import SparkSession", "from pyspark.sql import SparkSession; print('     ', SparkSession)")
probe("import py4j", "import py4j")
probe("SparkContext()", "from pyspark import SparkContext; SparkContext()")
probe("SparkContext.getOrCreate()", "from pyspark import SparkContext; SparkContext.getOrCreate()")
probe("from pyspark.sql import functions as F", "from pyspark.sql import functions as F")
probe("from pyspark.sql.types import *", "from pyspark.sql.types import *")
import sys
print("pyspark mods loaded:", sorted(m for m in sys.modules if m.startswith('pyspark'))[:40])
