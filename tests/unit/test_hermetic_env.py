"""The suite ignores PRYTT_* / AWS_* from the developer's shell or mise.local.toml."""

import os
import subprocess
import sys
from pathlib import Path

UNIT = Path(__file__).parent

# Each of these, inherited from the shell, used to fail unit tests.
LEAKED = {
    "PRYTT_S3_PROFILE": "some-sso-profile",  # conflicts with the tests' fake AWS keys
    "PRYTT_ICEBERG_WAREHOUSE": "/nonexistent/warehouse",
    "PRYTT_DISABLE": "1",  # prytt.pth skips the import hook in the pytest process
    "AWS_PROFILE": "some-sso-profile",
    "PRYTT_ALLOW_S3_WRITES": "1",
}


def test_leaked_developer_env_does_not_change_results():
    # Not SPARK_REMOTE: it points at this process's own Sail server once one runs.
    env = {k: v for k, v in os.environ.items() if k != "SPARK_REMOTE"} | LEAKED
    files = ["test_per_table_s3_mode.py", "test_s3_write_lock_tables.py"]
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *files],
        cwd=UNIT,
        env=env,
        capture_output=True,
        text=True,
        timeout=170,
    )
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]
