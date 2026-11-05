"""T9: the acceptance scripts, run unmodified as subprocesses.

Each script in ``scripts/`` is a plain Glue-style job. It runs in a fresh
interpreter (so the installed ``prytt.pth`` path is real) with ``cwd`` set to a
tmp project dir holding a ``pyproject.toml`` and seeded ``s3/`` data.
"""

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).parent / "scripts"
TIMEOUT = 180  # first Sail start after install can take ~30 s

PYPROJECT = """
[project]
name = "some-glue-job"
version = "0"

[tool.prytt.s3]
root = "s3"

[tool.prytt.catalog."raw_db.orders"]
path = "s3://raw/orders/"
format = "json"

[tool.prytt.catalog."raw_db.customers"]
path = "s3://raw/customers/"
format = "csv"
options = { header = true }
"""

ORDERS = (
    '{"id": 1, "amount": "9.5", "country": "za", "customer_id": 10}\n'
    '{"id": 2, "amount": "3", "country": "uk", "customer_id": 20}\n'
    '{"id": 3, "amount": "4.25", "country": "za", "customer_id": 10}\n'
)
CUSTOMERS = "customer_id,name\n10,Thandi\n20,Oliver\n"


@pytest.fixture
def project(tmp_path):
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    s3 = tmp_path / "s3"
    (s3 / "raw" / "orders").mkdir(parents=True)
    (s3 / "raw" / "orders" / "part-0.json").write_text(ORDERS)
    (s3 / "raw" / "customers").mkdir(parents=True)
    (s3 / "raw" / "customers" / "part-0.csv").write_text(CUSTOMERS)
    return tmp_path


def _env(**overrides):
    env = {k: v for k, v in os.environ.items() if not k.startswith("PRYTT_")}
    for name in ("SPARK_REMOTE", "CI", "RUST_LOG"):
        env.pop(name, None)
    env.update(overrides)
    return env


def run(project, script, *args, **env_overrides):
    return subprocess.run(
        [sys.executable, str(SCRIPTS / script), *args],
        capture_output=True,
        text=True,
        env=_env(**env_overrides),
        cwd=project,
        timeout=TIMEOUT,
    )


READ_PARQUET = """
import sys
from pyspark.sql import SparkSession
rows = SparkSession.builder.getOrCreate().read.parquet(sys.argv[1]).collect()
print(repr(sorted(tuple(r[c] for c in sys.argv[2:]) for r in rows)))
"""


def read_parquet(path, *columns):
    """Sorted row tuples of ``columns``, read in a separate interpreter.

    Not via the ``spark`` fixture: a gRPC channel in the pytest process makes
    every later ``subprocess`` fork print gRPC fork warnings to stderr, which
    breaks other tests that assert on a clean stderr.
    """
    proc = subprocess.run(
        [sys.executable, "-c", READ_PARQUET, str(path), *columns],
        capture_output=True,
        text=True,
        env=_env(),
        cwd=path.parent,
        timeout=TIMEOUT,
    )
    assert proc.returncode == 0, proc.stderr
    return ast.literal_eval(proc.stdout.strip().splitlines()[-1])


def test_canonical_boilerplate_job_writes_partitioned_parquet(project):
    proc = run(project, "canonical_job.py", "--JOB_NAME", "t", "--source_path", "s3://raw/orders/")

    assert proc.returncode == 0, proc.stderr
    out = project / "s3" / "curated" / "orders"
    assert sorted(p.name for p in out.iterdir() if p.is_dir()) == ["country=uk", "country=za"]
    assert read_parquet(out, "order_id", "amount", "country") == [
        (1, 9.5, "za"),
        (2, 3.0, "uk"),
        (3, 4.25, "za"),
    ]
    assert "MAP  s3://raw/orders/ ->" in proc.stderr
    assert "INFO Job.commit('t')" in proc.stderr


def test_glue_studio_job_reads_catalog_runs_sql_and_skips_data_quality(project):
    proc = run(project, "glue_studio_job.py", "--JOB_NAME", "studio")

    assert proc.returncode == 0, proc.stderr
    assert read_parquet(project / "s3" / "curated" / "totals", "name", "total") == [
        ("Oliver", 3.0),
        ("Thandi", 13.75),
    ]
    assert (
        "WARN EvaluateDataQuality skipped: ruleset 'orders_dq' not evaluated locally" in proc.stderr
    )


@pytest.mark.parametrize(
    "script, args",
    [
        ("canonical_job.py", ["--JOB_NAME", "t", "--source_path", "s3://raw/orders/"]),
        ("glue_studio_job.py", ["--JOB_NAME", "studio"]),
    ],
)
def test_boilerplate_jobs_pass_in_strict_mode(project, script, args):
    proc = run(project, script, *args, PRYTT_STRICT="1")

    assert proc.returncode == 0, proc.stderr
    assert "GlueLocalUnsupported" not in proc.stderr


def test_spark_only_job_gets_a_sail_session_from_the_builder(project):
    proc = run(project, "spark_only_job.py")

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip().splitlines()[-1] == "za=13.75 uk=3.0"
    assert "; sail at sc://127.0.0.1:" in proc.stderr


def test_gap_job_logs_stubs_and_finishes_when_not_strict(project):
    proc = run(project, "gap_job.py", "--JOB_NAME", "gap", PRYTT_STRICT="0")

    assert proc.returncode == 0, proc.stderr
    assert (
        "STUB GlueContext.purge_s3_path('s3://curated/stale/', options={'retentionPeriod': 0})"
        in proc.stderr
    )
    assert "STUB DynamicFrame.relationalize('root', 's3://tmp/relationalize/')" in proc.stderr
    assert proc.stdout.strip().splitlines()[-1] == "rows=3"


def test_gap_job_fails_on_first_unsupported_call_when_strict(project):
    proc = run(project, "gap_job.py", "--JOB_NAME", "gap", PRYTT_STRICT="1")

    assert proc.returncode != 0
    assert "GlueLocalUnsupported: GlueContext.purge_s3_path(" in proc.stderr
    assert "rows=" not in proc.stdout
