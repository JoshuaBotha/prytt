# T2 server.py + import hooks + SparkContext shim + builder hook

Status: ready-for-agent
Blocked by: 01

Spec: §6 (hooks table), §7, §8, §12 (first-line versions/address log), §13 Non-functional (clean exit).

## Scope
- `prytt/server.py`: `ensure_running()` idempotent; respect SPARK_REMOTE; in-process Sail port 0 (or `sail.port`); RUST_LOG quieting; atexit ordered stop. Also expose a hook for later tickets to set Sail catalog env before start (T10) and to run per-new-session setup (T5 temp views, T10 re-register, warehouse dir) — keep it a small interface.
- `prytt/_hooks.py`: watch `pyspark`, `pyspark.context`, `pyspark.sql.session` (any order).
- `prytt/shims/pyspark_context.py`: SparkContext shim per §8.
- Builder hook: wrap `SparkSession.Builder.getOrCreate` → `ensure_running()` first.

## Acceptance
- Subprocess tests: `from pyspark.context import SparkContext; SparkContext()` works; `SparkSession.builder.getOrCreate()` alone works; `spark.range(3).count()==3`; process exits cleanly; RDD methods raise GlueLocalUnsupported.

