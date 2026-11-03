# 0001: Run Sail in-process, started lazily on first Spark use

**Status:** accepted (2026-10-30, spikes S2/S3)

## Context

Glue scripts need a Spark engine. pyspark-client (the Spark Connect-only
PySpark, no JVM) needs a Spark Connect server to talk to. Sail provides one
as a Rust library in the `pysail` wheel. The server could run as a separate
long-lived process, as a subprocess per job, or inside the job's own Python
process.

## Decision

prytt starts `pysail.spark.SparkConnectServer(ip="127.0.0.1", port=0)` with
`start(background=True)` inside the job's process, the first time the script
uses Spark (`SparkContext()`, `GlueContext(...)`,
`SparkSession.builder.getOrCreate()` / `.create()`). It reads the bound
address from `listening_address`, sets `SPARK_REMOTE` to it, and stops the
session and server in an `atexit` handler. If `SPARK_REMOTE` is already set,
prytt uses that server instead and starts nothing.

## Alternatives

- **Separate daemon** the developer starts once. Faster per run, but one more
  thing to start, state shared between runs, and port management.
- **Subprocess per job.** Isolates crashes, but adds process management and
  start-up latency; the spike found no need for it.

## Consequences

- One command runs a job; nothing to start or clean up. ~0.75 s for a whole
  script once warm; the first run after install takes ~30 s while the native
  library loads.
- An ephemeral port per process, so parallel jobs and test workers don't
  collide.
- Sail's environment (`SAIL_CATALOG__*`, `RUST_LOG`) must be set before
  `start()`, which is what the `on_before_start` seam is for.
- Sail's catalogs live and die with the process (see ADR 0003, ADR 0004).
- A crash in Sail takes the job down with it.
