"""T4: a Glue-shaped script reads s3 JSON, maps it and writes partitioned parquet.

Runs in a fresh interpreter so the installed ``prytt.pth`` path is real.
"""

import os
import subprocess
import sys
import textwrap

TIMEOUT = 180  # first Sail start after install can take ~30 s

SCRIPT = """
import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

src = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://raw/orders/"]},
    format="json",
)
mapped = src.apply_mapping([
    ("id", "long", "order_id", "long"),
    ("amount", "string", "amount", "double"),
    ("country", "string", "country", "string"),
])
glueContext.write_dynamic_frame.from_options(
    frame=mapped,
    connection_type="s3",
    connection_options={"path": "s3://curated/orders/", "partitionKeys": ["country"]},
    format="glueparquet",
)
job.commit()
back = spark.read.parquet(sys.argv[-1])
print(sorted((r.order_id, r.amount, r.country) for r in back.collect()))
"""


def test_glue_script_reads_maps_and_writes(tmp_path):
    root = tmp_path / "s3"
    (root / "raw" / "orders").mkdir(parents=True)
    (root / "raw" / "orders" / "a.json").write_text(
        '{"id": 1, "amount": "9.5", "country": "za"}\n{"id": 2, "amount": "3", "country": "uk"}\n'
    )
    script = tmp_path / "job.py"
    script.write_text(textwrap.dedent(SCRIPT))
    env = {k: v for k, v in os.environ.items() if not k.startswith("PRYTT_")}
    for name in ("SPARK_REMOTE", "CI", "RUST_LOG"):
        env.pop(name, None)
    env["PRYTT_S3_ROOT"] = str(root)

    proc = subprocess.run(
        [sys.executable, str(script), "--JOB_NAME", "local", str(root / "curated" / "orders")],
        capture_output=True,
        text=True,
        env=env,
        cwd=tmp_path,
        timeout=TIMEOUT,
    )

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip().splitlines()[-1] == "[(1, 9.5, 'za'), (2, 3.0, 'uk')]"
    assert sorted(p.name for p in (root / "curated" / "orders").iterdir() if p.is_dir()) == [
        "country=uk",
        "country=za",
    ]
    assert "MAP  s3://raw/orders/ ->" in proc.stderr
