"""Sail server lifecycle and the session-setup seam for later features.

Interface:

- ``ensure_running() -> str``: idempotent. Returns the Spark Connect URL. If
  ``SPARK_REMOTE`` is already set (and prytt did not set it), that server is
  used and Sail is not started. Otherwise starts Sail in-process on
  ``127.0.0.1:<sail.port>`` (0 = ephemeral), sets ``SPARK_REMOTE`` and
  registers an ``atexit`` handler that stops the active session, then Sail.
  Logs one ``INFO`` line with prytt / pysail / pyspark-client versions and the
  address. Quiets Sail's Rust logs (``RUST_LOG=error``) unless ``RUST_LOG`` is
  set or ``log_level`` is ``DEBUG``.
- ``stop()``: stop the active session and the Sail server prytt started (if
  any) and clear the ``SPARK_REMOTE`` it set. Safe to call twice. Also the
  ``atexit`` handler.
- ``stop_session()``: stop the active ``SparkSession`` only (server stays up).
- ``running() -> bool``: whether prytt's own Sail server is up.

Extension seam for later tickets (keep it this small):

- ``on_before_start(fn)``: ``fn()`` runs just before Sail starts, after
  ``RUST_LOG`` is set. Use it to set Sail env such as ``SAIL_CATALOG__LIST`` /
  ``SAIL_CATALOG__DEFAULT_CATALOG`` (T10). Not called when ``SPARK_REMOTE``
  points at an external server. Register in
  ``prytt.shims.pyspark_hooks.register_session_setup`` (``ensure_running``
  calls it first); a callback added after the server started never runs.
- ``on_new_session(fn)``: ``fn(spark)`` runs once per new ``SparkSession``
  (by ``session_id``), right after ``SparkSession.builder.getOrCreate()`` /
  ``.create()`` builds it and before the caller gets it. Use it for temp views
  (T5), ``spark.sql.warehouse.dir`` and catalog re-registration (T10): Sail
  catalogs are in-memory per session. Runs for external servers too.
  Callbacks run in registration order; registering the same function twice is
  a no-op. Calling ``getOrCreate()`` inside a callback is safe (no recursion).
"""

import atexit
import os
import threading

from prytt import config
from prytt.stub import log

_lock = threading.RLock()
_server = None  # pysail SparkConnectServer started by prytt
_our_remote = None  # the SPARK_REMOTE value prytt set
_before_start = []
_new_session = []
_seen_sessions = set()
_atexit_registered = False
_external_logged = None  # external SPARK_REMOTE already announced


def on_before_start(fn):
    if fn not in _before_start:
        _before_start.append(fn)
    return fn


def on_new_session(fn):
    if fn not in _new_session:
        _new_session.append(fn)
    return fn


def running() -> bool:
    return _server is not None


def _version(dist: str) -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(dist)
    except PackageNotFoundError:
        return "?"


def _versions() -> str:
    return f"prytt {_version('prytt')}, pysail {_version('pysail')}, pyspark-client {_version('pyspark-client')}"


def ensure_running() -> str:
    global _server, _our_remote, _atexit_registered, _external_logged
    from prytt.shims.pyspark_hooks import register_session_setup

    register_session_setup()  # whichever start path got here first (T10)
    with _lock:
        if _server is not None:
            return os.environ.setdefault("SPARK_REMOTE", _our_remote)
        remote = os.environ.get("SPARK_REMOTE")
        if remote:
            if remote != _external_logged:
                _external_logged = remote
                log("INFO", f"{_versions()}; using external server SPARK_REMOTE={remote}")
            return remote

        cfg = config.get()
        if cfg.log_level != "DEBUG":
            os.environ.setdefault("RUST_LOG", "error")  # "warn" shows benign shutdown noise
        for fn in list(_before_start):
            fn()

        from pysail.spark import SparkConnectServer

        server = SparkConnectServer(ip="127.0.0.1", port=cfg.sail_port)
        server.start(background=True)
        host, port = server.listening_address
        _server = server
        _our_remote = f"sc://{host}:{port}"
        os.environ["SPARK_REMOTE"] = _our_remote
        if not _atexit_registered:
            atexit.register(stop)
            _atexit_registered = True
        log("INFO", f"{_versions()}; sail at {_our_remote}")
        return _our_remote


def stop_session() -> None:
    import sys

    if "pyspark.sql.session" not in sys.modules:
        return
    from pyspark.sql import SparkSession

    try:
        session = SparkSession.getActiveSession()
    except Exception:
        session = None
    if session is not None:
        try:
            session.stop()
        except Exception as exc:  # server may already be gone
            log("WARN", f"stopping Spark session failed: {exc!r}")


def stop() -> None:
    global _server, _our_remote
    with _lock:
        stop_session()
        _seen_sessions.clear()
        if _server is None:
            return
        server, _server = _server, None
        try:
            server.stop()
        finally:
            if os.environ.get("SPARK_REMOTE") == _our_remote:
                del os.environ["SPARK_REMOTE"]
            _our_remote = None


def session_built(spark) -> None:
    """Run ``on_new_session`` callbacks if ``spark`` is new. Called by the builder hook."""
    key = getattr(spark, "session_id", None) or id(spark)
    with _lock:
        if key in _seen_sessions:
            return
        _seen_sessions.add(key)
    for fn in list(_new_session):
        fn(spark)
