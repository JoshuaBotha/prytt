"""A garbage-collected SparkSession must not deadlock or break the live one.

pyspark-client's ``SparkSession.__del__`` closes the client, which shuts down
the process-wide ReleaseExecute thread pool. If the cyclic GC runs it inside
that pool's ``submit()`` (lock held), the shutdown waits on the same lock
forever. Runs in a fresh interpreter with a timeout, so a regression fails
instead of hanging the suite.
"""

import os
import subprocess
import sys
import textwrap

SCRIPT = textwrap.dedent(
    """
    import gc

    from pyspark.sql import SparkSession
    from pyspark.sql.connect.client.reattach import ExecutePlanResponseReattachableIterator

    live = SparkSession.builder.getOrCreate()
    other = SparkSession.builder.create()
    other.range(1).collect()
    other_client = other.client

    pool = ExecutePlanResponseReattachableIterator._get_or_create_release_thread_pool()
    with pool._shutdown_lock:  # as if the GC fired inside pool.submit()
        del other
        gc.collect()

    assert other_client.is_closed, "collected session's channel was not closed"
    assert not pool._shutdown, "collected session shut down the shared pool"
    print("live count", live.range(3).count())
    """
)


def test_collected_session_closes_only_its_own_channel():
    env = {k: v for k, v in os.environ.items() if not k.startswith("PRYTT_")}
    env.pop("SPARK_REMOTE", None)

    try:
        result = subprocess.run(
            [sys.executable, "-c", SCRIPT], capture_output=True, text=True, env=env, timeout=60
        )
    except subprocess.TimeoutExpired:
        raise AssertionError("deadlocked: collected SparkSession.__del__ waited on the pool lock") from None

    assert result.returncode == 0, result.stderr
    assert "live count 3" in result.stdout
