"""T8: the auto-registered pytest plugin.

Each case runs pytest in a subprocess (pytester) so the ``pytest11`` entry
point and a fresh Sail server are exercised for real.
"""

import pytest

TIMEOUT = 300  # first Sail start after install can take ~30 s


@pytest.fixture
def run(pytester, monkeypatch):
    for name in ("SPARK_REMOTE", "CI", "PRYTT_STRICT", "PRYTT_S3_ROOT", "PRYTT_LOG_LEVEL", "PRYTT_S3_MODE"):
        monkeypatch.delenv(name, raising=False)

    def run(source, *args):
        pytester.makepyfile(source)
        return pytester.runpytest_subprocess("-p", "no:cacheprovider", *args, timeout=TIMEOUT)

    return run


def test_s3_root_is_a_tmp_dir_wired_into_s3_root_config(run):
    result = run(
        """
        from prytt import config, paths

        def test_root(s3_root, tmp_path):
            assert s3_root.is_dir()
            assert str(s3_root).startswith(str(tmp_path))
            assert paths.map_path("s3://bucket/key.json") == str(s3_root.resolve() / "bucket" / "key.json")

        def test_each_test_gets_its_own_root(s3_root):
            assert config.get().s3_root == str(s3_root)
        """
    )
    result.assert_outcomes(passed=2)


def test_projects_not_using_the_fixtures_are_unaffected(run):
    result = run(
        """
        import sys

        def test_plain():
            assert "pyspark" not in sys.modules
        """,
        "-W",
        "error::pytest.PytestAssertRewriteWarning",
    )
    result.assert_outcomes(passed=1)
    assert "sail at" not in result.stderr.str()
    assert "already imported" not in result.stdout.str() + result.stderr.str()


def test_glue_context_reads_and_writes_s3_paths_under_s3_root(run):
    result = run(
        """
        import json
        import os

        REMOTES = []

        def test_read_write(glue_context, spark, s3_root):
            assert glue_context.spark_session is spark
            REMOTES.append(os.environ["SPARK_REMOTE"])
            src = s3_root / "raw" / "orders" / "p1.json"
            src.parent.mkdir(parents=True)
            src.write_text("\\n".join(json.dumps({"id": i}) for i in (1, 2)))

            dyf = glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={"paths": ["s3://raw/orders/"]},
                format="json",
            )
            glue_context.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="s3",
                connection_options={"path": "s3://curated/orders/"},
                format="parquet",
            )
            out = spark.read.parquet(str(s3_root / "curated" / "orders"))
            assert sorted(r.id for r in out.collect()) == [1, 2]

        def test_second_test_shares_the_session(spark, s3_root):
            REMOTES.append(os.environ["SPARK_REMOTE"])
            assert REMOTES[0] == REMOTES[1]
            assert spark.range(3).count() == 3
            assert not any(s3_root.iterdir())
        """,
        "-s",
    )
    result.assert_outcomes(passed=2)
    assert result.stderr.str().count("sail at") == 1
