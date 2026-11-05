"""T2: Sail lifecycle, import hooks, SparkContext shim.

Every test runs a fresh interpreter so the installed ``prytt.pth`` path is real.
"""

import os
import subprocess
import sys
import textwrap

TIMEOUT = 180  # first Sail start after install can take ~30 s


def run(code: str, **env_overrides: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if not k.startswith("PRYTT_")}
    for name in ("SPARK_REMOTE", "CI", "RUST_LOG"):
        env.pop(name, None)
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        env=env,
        timeout=TIMEOUT,
    )


def ok(code: str, **env) -> subprocess.CompletedProcess:
    proc = run(code, **env)
    assert proc.returncode == 0, (
        f"exit {proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    return proc


def test_glue_boilerplate_sparkcontext_then_session():
    out = ok(
        """
        from pyspark.context import SparkContext
        from pyspark.sql import SparkSession
        sc = SparkContext()
        spark = SparkSession.builder.getOrCreate()
        print(spark.range(3).count())
        """
    )
    assert out.stdout.strip().splitlines()[-1] == "3"


def test_builder_alone_starts_sail():
    out = ok(
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        print(spark.range(3).count())
        """
    )
    assert out.stdout.strip().splitlines()[-1] == "3"


def test_every_import_route_gives_the_same_singleton_shim():
    out = ok(
        """
        import pyspark
        from pyspark import SparkContext as A
        from pyspark.context import SparkContext as B
        assert A is B is pyspark.SparkContext
        assert B() is B.getOrCreate()
        print("ok")
        """
    )
    assert out.stdout.strip() == "ok"


def test_rdd_methods_raise_glue_local_unsupported():
    out = ok(
        """
        from pyspark.context import SparkContext
        from prytt.stub import GlueLocalUnsupported
        sc = SparkContext()
        for name in ("parallelize", "textFile", "wholeTextFiles", "emptyRDD", "broadcast", "accumulator"):
            try:
                getattr(sc, name)([1])
            except GlueLocalUnsupported as exc:
                assert "RDD" in str(exc), exc
            else:
                raise AssertionError(name)
        print("ok")
        """
    )
    assert out.stdout.strip() == "ok"


def test_documented_no_ops_and_java_chains_log_and_continue_even_when_strict():
    out = ok(
        """
        from pyspark.context import SparkContext
        sc = SparkContext()
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "x")
        sc._conf.get("spark.app.name")
        assert sc.setLogLevel("WARN") is None
        assert sc.addPyFile("deps.zip") is None
        assert sc.setCheckpointDir("/tmp/ckpt") is None
        print(sc.appName, sc.master, sc.version)
        """,
        PRYTT_STRICT="1",
    )
    assert "INFO SparkContext.setLogLevel('WARN'), no-op locally" in out.stderr
    assert "INFO SparkContext.addPyFile('deps.zip'), no-op locally" in out.stderr
    assert "INFO SparkContext.setCheckpointDir('/tmp/ckpt'), no-op locally" in out.stderr
    assert "STUB JavaSparkContext.hadoopConfiguration()" in out.stderr
    app, master, version = out.stdout.split()
    assert version.startswith("4.")


def test_strict_mode_raises_on_catch_all_hit():
    proc = run(
        """
        from pyspark.context import SparkContext
        SparkContext().setJobGroup("g", "d")
        """,
        PRYTT_STRICT="1",
    )
    assert proc.returncode != 0
    assert "GlueLocalUnsupported" in proc.stderr


def test_external_spark_remote_is_used_and_sail_not_started():
    out = ok(
        """
        import sys
        from prytt import server
        print(server.ensure_running(), server.running(), "pysail" in sys.modules)
        """,
        SPARK_REMOTE="sc://example.invalid:15002",
    )
    assert out.stdout.split() == ["sc://example.invalid:15002", "False", "False"]
    assert "using external server SPARK_REMOTE=sc://example.invalid:15002" in out.stderr


def test_first_line_logs_versions_and_address_and_rust_logs_are_quiet():
    out = ok(
        """
        from pyspark.sql import SparkSession
        SparkSession.builder.getOrCreate().range(1).count()
        """
    )
    first = out.stderr.splitlines()[0]
    assert first.startswith("[prytt] INFO")
    assert "pysail 0." in first and "pyspark-client 4.1" in first
    assert "sail at sc://127.0.0.1:" in first
    assert "sail_" not in out.stderr, out.stderr


def test_debug_log_level_lets_sail_logs_through():
    out = ok(
        """
        from pyspark.sql import SparkSession
        SparkSession.builder.getOrCreate().range(1).count()
        """,
        PRYTT_LOG_LEVEL="DEBUG",
    )
    assert "sail_" in out.stderr


def test_stop_then_restart_and_clean_exit():
    out = ok(
        """
        import os
        from pyspark.sql import SparkSession
        from prytt import server
        first = server.ensure_running()
        assert SparkSession.builder.getOrCreate().range(2).count() == 2
        server.stop()
        server.stop()
        assert not server.running() and "SPARK_REMOTE" not in os.environ
        second = server.ensure_running()
        assert server.running() and os.environ["SPARK_REMOTE"] == second
        assert SparkSession.builder.getOrCreate().range(2).count() == 2
        print("ok")
        """
    )
    assert out.stdout.strip() == "ok"
    assert "Traceback" not in out.stderr


def test_session_seam_hooks():
    out = ok(
        """
        import os
        from pyspark.sql import SparkSession
        from prytt import server
        calls = []
        server.on_before_start(lambda: calls.append(("before", "SPARK_REMOTE" in os.environ)))
        def setup(spark):
            calls.append(("session", spark.range(1).count()))
            SparkSession.builder.getOrCreate()  # re-entrant, no recursion
        server.on_new_session(setup)
        server.on_new_session(setup)  # duplicate is a no-op
        a = SparkSession.builder.getOrCreate()
        b = SparkSession.builder.getOrCreate()
        assert a is b
        a.stop()
        SparkSession.builder.getOrCreate()
        print(calls)
        """
    )
    assert out.stdout.strip() == str([("before", False), ("session", 1), ("session", 1)])


def test_importing_pyspark_does_not_start_sail():
    out = ok(
        """
        import os, sys
        import pyspark, pyspark.context
        from pyspark.sql import SparkSession
        print("pysail" in sys.modules, "SPARK_REMOTE" in os.environ)
        """
    )
    assert out.stdout.split() == ["False", "False"]
    assert out.stderr == ""
