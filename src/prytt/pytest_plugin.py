"""pytest plugin (spec §4 Test), auto-registered through the ``pytest11`` entry point.

Fixtures:

- ``s3_root`` (function): a fresh ``tmp_path / "s3"`` directory wired into
  ``s3.root`` (``PRYTT_S3_ROOT`` + ``config.reset()``), so ``s3://bucket/key``
  maps to ``s3_root / "bucket" / "key"`` for the duration of the test.
- ``spark`` (session): the Sail-backed ``SparkSession``. One Sail server per
  test session (or the external ``SPARK_REMOTE`` server if set); stopped at
  session end if this fixture started it.
- ``glue_context`` (session): an ``awsglue.context.GlueContext`` on ``spark``.
  Combine with ``s3_root`` to read/write ``s3://`` paths under a tmp dir.

Loading the plugin is cheap: nothing here imports pyspark until a fixture is
used, so projects that never request these fixtures are unaffected. Local
fixtures with the same names (e.g. in a ``conftest.py``) override these.
"""

import pytest


@pytest.fixture
def s3_root(tmp_path, monkeypatch):
    from prytt import config

    root = tmp_path / "s3"
    root.mkdir()
    monkeypatch.setenv("PRYTT_S3_ROOT", str(root))
    config.reset()
    yield root
    monkeypatch.undo()
    config.reset()


@pytest.fixture(scope="session")
def spark():
    from prytt import server

    started = not server.running()
    server.ensure_running()

    from pyspark.sql import SparkSession

    session = SparkSession.builder.getOrCreate()
    yield session
    if started:
        server.stop()


@pytest.fixture(scope="session")
def glue_context(spark):
    from awsglue.context import GlueContext

    return GlueContext()  # uses the active session built by `spark`
