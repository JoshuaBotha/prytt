import os, atexit
_srv = None

def ensure_running():
    global _srv
    if _srv is not None or os.environ.get("SPARK_REMOTE"):
        return
    from pysail.spark import SparkConnectServer
    _srv = SparkConnectServer(ip="127.0.0.1", port=0)
    _srv.start(background=True)
    host, port = _srv.listening_address
    os.environ["SPARK_REMOTE"] = f"sc://{host}:{port}"
    atexit.register(_srv.stop)
    print(f"[prytt] INFO sail at {os.environ['SPARK_REMOTE']}")
