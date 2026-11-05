"""The S3 write lock for an unmodified Glue script, run as a subprocess so the
installed ``prytt.pth`` path is real."""

import os
import subprocess
import sys
import textwrap

TIMEOUT = 180  # first Sail start after install can take ~30 s

SCRIPT = """
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext())
spark = glueContext.spark_session
df = spark.createDataFrame([(1, "a")], ["id", "v"])
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(df, glueContext, "out"),
    connection_type="s3",
    connection_options={"path": "s3://curated/orders/"},
    format="parquet",
)
"""


def run(tmp_path, **extra):
    script = tmp_path / "job.py"
    script.write_text(textwrap.dedent(SCRIPT))
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PRYTT_", "AWS_"))}
    for name in ("SPARK_REMOTE", "CI", "RUST_LOG"):
        env.pop(name, None)
    env.update(
        PRYTT_S3_MODE="passthrough",
        AWS_REGION="eu-west-1",
        # Past the lock, Sail must fail fast instead of reaching AWS.
        AWS_ACCESS_KEY_ID="prytt-test",
        AWS_SECRET_ACCESS_KEY="prytt-test",
        AWS_ENDPOINT_URL="http://127.0.0.1:9",
        AWS_EC2_METADATA_DISABLED="true",
        **extra,
    )
    return subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        env=env,
        cwd=tmp_path,
        timeout=TIMEOUT,
    )


def test_glue_script_writing_to_s3_in_passthrough_is_blocked_unless_unlocked(tmp_path):
    locked = run(tmp_path)

    assert locked.returncode != 0
    assert "PryttS3WriteBlocked" in locked.stderr
    assert "s3://curated/orders/" in locked.stderr
    assert "PRYTT_ALLOW_S3_WRITES=1" in locked.stderr

    unlocked = run(tmp_path, PRYTT_ALLOW_S3_WRITES="1")

    assert "PryttS3WriteBlocked" not in unlocked.stderr
    assert "S3 writes unlocked" in unlocked.stderr
