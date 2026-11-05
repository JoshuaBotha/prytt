# Test Glue jobs with pytest

prytt registers a pytest plugin automatically (through the `pytest11` entry
point), so tests in a project that has prytt installed get three fixtures.
Loading the plugin costs nothing until a test uses one of them.

| Fixture | Scope | What |
|---|---|---|
| `spark` | session | A Sail-backed `SparkSession`. One server for the whole test session, stopped at the end if this fixture started it. |
| `glue_context` | session | An `awsglue.context.GlueContext` on that session. |
| `s3_root` | per test | A fresh `tmp_path / "s3"`. `s3://bucket/key` maps to `s3_root / "bucket" / "key"` for the duration of the test. |

Fixtures with the same names in your own `conftest.py` take precedence.

## Test a transform

```python
def test_cleans_orders(glue_context, s3_root):
    (s3_root / "raw" / "orders").mkdir(parents=True)
    (s3_root / "raw" / "orders" / "a.json").write_text('{"id": 1}\n')

    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://raw/orders/"]},
        format="json",
    )

    assert dyf.count() == 1
```

Factor job logic into functions that take and return `DynamicFrame`s or
`DataFrame`s, and test those with the fixtures. The `spark` fixture is
enough for pure Spark code:

```python
from jobs.orders import add_totals


def test_add_totals(spark):
    df = spark.createDataFrame([(1, 2.0, 3)], "id long, price double, qty int")
    assert add_totals(df).collect()[0]["total"] == 6.0
```

## Test a whole job

Run the script as a subprocess with `sys.executable`, so the job starts the
way it does from the command line, with prytt's import hook and its own Sail
server. Point `PRYTT_S3_ROOT` at a temp dir and check the output files:

```python
import os
import subprocess
import sys


def test_job_end_to_end(tmp_path):
    s3 = tmp_path / "s3"
    (s3 / "raw-bucket" / "orders").mkdir(parents=True)
    (s3 / "raw-bucket" / "orders" / "a.json").write_text('{"id": 1, "amount": "2.5"}\n')

    env = {**os.environ, "PRYTT_S3_ROOT": str(s3), "PRYTT_STRICT": "1"}
    env.pop("SPARK_REMOTE", None)  # let the job start its own Sail
    result = subprocess.run(
        [
            sys.executable,
            "jobs/bronze_to_silver.py",
            "--JOB_NAME",
            "test",
            "--source_path",
            "s3://raw-bucket/orders/",
            "--target_path",
            "s3://curated-bucket/orders/",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stderr
    assert any((s3 / "curated-bucket" / "orders").glob("*.parquet"))
```

That is how prytt's own acceptance tests work (`tests/acceptance/`). Each
subprocess starts its own server, which takes under a second once Sail is
warm.

Also set `PRYTT_ICEBERG_WAREHOUSE` to a temp dir if the job uses
`glue_catalog`, or tests share Iceberg tables through `./.prytt/warehouse`.

## Catalog tables in tests

Catalog mappings come from config, which is read once per process. In a
subprocess test, write a `prytt.toml` into the subprocess's working
directory (`cwd=tmp_path`) with the `[catalog."db.table"]` entries it needs.
In-process, the fixtures pick up the project's `pyproject.toml` /
`prytt.toml` as found from pytest's working directory.

Mapped tables are registered when a session is created. The session-scoped
`spark` fixture is created once, so a table whose data a test writes later
is not registered in that session (see
[Compatibility and limits](compatibility.md#catalog-tables-in-sql)). Read
such tables through `glue_context.create_dynamic_frame.from_catalog`, which
looks the path up on every call.

## Tests that read real S3

Tests run with the S3 write lock on: the fixtures remove
`PRYTT_ALLOW_S3_WRITES`. To read passthrough tables in tests, pick the AWS
profile once per test run (`PRYTT_S3_PROFILE=team-sso pytest`), not per
test: Sail keeps one AWS identity per process. See
[S3 and AWS: testing with prytt](s3-and-aws.md#testing-with-prytt).

## Extra `SparkSession`s

If a test makes a second session with `SparkSession.builder.create()`, call
`stop()` on it when the test is done:

```python
other = SparkSession.builder.create()
try:
    ...
finally:
    other.stop()
```

Background: in pyspark-client, a session that is dropped without `stop()` is
closed by its `__del__` once garbage collection reaches it. That close also
shuts down a thread pool that all sessions share. If the collection happens
while another session is mid-query, the process deadlocks: the run hangs with
no error and 0% CPU, in whichever test is running at the time. prytt patches
that `__del__` so a collected session closes only its own connection, which
avoids the hang. Stopping extra sessions yourself is still cleaner and
doesn't depend on the patch.

## Catch hangs

prytt's own test config dumps every thread's stack after 60 s and fails the
run after 180 s, so a hang shows where it's stuck instead of stalling CI.
Copy it if your suite needs it (it uses the `pytest-timeout` plugin):

```toml
[tool.pytest.ini_options]
faulthandler_timeout = 60
timeout = 180
timeout_method = "thread"
```

`timeout_method = "thread"`, because a signal can't interrupt a blocked gRPC
call.

## Run tests in CI

- Leave `SPARK_REMOTE` unset.
- `CI=true` (set by GitHub Actions and most CI systems) turns on
  [strict mode](running-jobs.md#strict-mode): unimplemented calls and
  missing job arguments raise. Set `PRYTT_STRICT=0` if you want CI to
  tolerate them.
- The first test that touches Spark on a fresh runner can take ~30 s while
  Sail loads its native library. Give the first test a timeout that allows
  for it, or warm Sail in an earlier step:

  ```bash
  python -c "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().range(1).collect()"
  ```
