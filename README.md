# prytt

Run unmodified AWS Glue 6 job scripts on your laptop. prytt is a drop-in
`awsglue` + PySpark shim backed by [Sail](https://github.com/lakehq/sail), a
Rust Spark Connect server. No JVM, no Docker, no script changes.

```bash
python jobs/bronze_to_silver.py --JOB_NAME local --source_path s3://raw-bucket/orders/
```

```
[prytt] INFO prytt 0.1.0, pysail 0.7.3, pyspark-client 4.1.4; sail at sc://127.0.0.1:52341
[prytt] INFO Job.init('local'), bookmarks off
[prytt] MAP  s3://raw-bucket/orders/ -> /home/me/proj/.prytt/s3/raw-bucket/orders/
[prytt] WARN Filter not applied locally (v1); data passed through unchanged
[prytt] INFO Job.commit('local')
```

## Install

Add it as a dev dependency from git (pin a tag):

```bash
uv add --dev "prytt @ git+https://<host>/<org>/prytt.git@v0.1.0"
```

This pulls in `pysail` and `pyspark-client` (4.1.x). Nothing else needs
configuring for the default setup.

**The first run after install can take about 30 s** before Sail answers,
because it loads its native library once (most noticeable on macOS). Later
runs start in under a second. It is not hung.

### Name clash: don't install alongside other `awsglue` packages

prytt provides the `awsglue` import name. Do not install it in the same
environment as `AWSGlueDataplanePython`, `awsglue-local`, or the `awsglue`
libs from the Glue Docker images. Only one of them can own `awsglue`, and you
will get whichever was installed last. prytt also ships `awsgluedq`, so Glue
Studio scripts that import `EvaluateDataQuality` load.

## Run a job

Run the script with Python as usual, with the same `--KEY value` arguments
Glue would pass:

```bash
python jobs/my_job.py --JOB_NAME local --source_path s3://raw-bucket/orders/
```

What happens:

- A `.pth` file installs a tiny import hook at interpreter start. It costs
  under 1 ms, and nothing else loads until your script imports PySpark or
  `awsglue`.
- On first Spark use (`SparkContext()`, `GlueContext(...)` or
  `SparkSession.builder.getOrCreate()`), prytt starts an in-process Sail server
  on an ephemeral port and points `SPARK_REMOTE` at it. It stops when the
  process exits.
- `s3://bucket/key` (also `s3a://`, `s3n://`) maps to
  `./.prytt/s3/bucket/key`, and every mapping is logged as `MAP`. Put your
  input files there. Writes create directories as needed.
- `glueContext.create_dynamic_frame.from_catalog(database=..., table_name=...)`
  reads whatever path you mapped for that table in config (see
  [Catalog tables](#catalog-tables)).
- Glue features prytt can't do locally are logged rather than failing (see
  [What's real and what's stubbed](#whats-real-and-whats-stubbed) and
  [Strict mode](#strict-mode)).

Plain PySpark scripts with no `awsglue` and no `SparkContext` work too: the
`SparkSession.builder` hook starts Sail for them.

If `SPARK_REMOTE` is already set, prytt uses that server and doesn't start
Sail. Leave it **unset** for the normal local flow.

### Log output

prytt writes to stderr as `[prytt] <TAG> <message>`:

| Tag | Meaning |
|---|---|
| `INFO` | Server start (versions, address), `Job.init` / `Job.commit` |
| `MAP` | An `s3://` path or catalog table was rewritten to a local location |
| `WARN` | A known gap: the data passed through unchanged (e.g. `Filter.apply`, `EvaluateDataQuality`) |
| `STUB` | A call prytt doesn't implement was logged and ignored (raises in strict mode) |

`PRYTT_LOG_LEVEL=WARNING` hides `INFO`/`MAP`/`STUB`. Sail's own Rust logs
default to `RUST_LOG=error`; set `RUST_LOG` yourself, or use
`PRYTT_LOG_LEVEL=DEBUG`, to see more.

## Test

prytt registers a pytest plugin automatically. It provides these fixtures:

| Fixture | Scope | What |
|---|---|---|
| `spark` | session | A Sail-backed `SparkSession`. One server for the whole test session. |
| `glue_context` | session | An `awsglue.context.GlueContext` on that session. |
| `s3_root` | per test | A fresh `tmp_path / "s3"`. `s3://bucket/key` maps to `s3_root / "bucket" / "key"` for the duration of the test. |

```python
def test_cleans_orders(glue_context, s3_root):
    (s3_root / "raw" / "orders").mkdir(parents=True)
    (s3_root / "raw" / "orders" / "a.json").write_text('{"id": 1}\n')

    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://raw/orders/"]},
        format="json",
    )

    assert dyf.count() == 1
```

Fixtures with the same names in your own `conftest.py` take precedence.
Loading the plugin costs nothing until you use a fixture.

To test a whole job end to end, run the script with `subprocess` and
`sys.executable`, then check its output. That is how prytt's own acceptance
tests work (`tests/acceptance/`).

## Configuration

Settings come from these sources, highest priority first:

1. `PRYTT_*` environment variables
2. `prytt.toml` (top-level keys), searched upward from cwd
3. `[tool.prytt]` in the nearest `pyproject.toml`, searched upward from cwd
4. defaults

| Key | Env | Default | |
|---|---|---|---|
| `disable` | `PRYTT_DISABLE` | `false` | Skip prytt entirely |
| `log_level` | `PRYTT_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `strict` | `PRYTT_STRICT` | `true` if `CI` is `1`/`true`/`yes`, else `false` | See [Strict mode](#strict-mode) |
| `sail.port` | `PRYTT_SAIL_PORT` | `0` (ephemeral) | |
| `s3.mode` | `PRYTT_S3_MODE` | `local` | `local` or `passthrough` |
| `s3.root` | `PRYTT_S3_ROOT` | `./.prytt/s3` | Relative to cwd |
| `catalog.on_missing` | `PRYTT_CATALOG_ON_MISSING` | `error` | `error` or `empty` |
| `catalog."db.table"` | | none | See below |
| `iceberg.warehouse` | `PRYTT_ICEBERG_WAREHOUSE` | `./.prytt/warehouse` | Made absolute |

In TOML, dotted keys are written as tables, and unknown keys are an error:

```toml
[tool.prytt]
log_level = "INFO"

[tool.prytt.s3]
root = "local-data/s3"

[tool.prytt.catalog]
on_missing = "empty"
```

### Catalog tables

Map each Glue Data Catalog table that your job reads or writes to a path and
a format:

```toml
[tool.prytt.catalog."raw_db.orders"]
path = "s3://raw-bucket/orders/"     # goes through the S3 mapping
format = "json"
options = { multiLine = true }       # Spark/Glue format options

[tool.prytt.catalog."curated_db.orders"]
path = "s3://curated-bucket/orders/"
format = "parquet"
options = { partitionKeys = ["country"] }   # used by write_dynamic_frame.from_catalog
```

- Each entry takes only `path`, `format` and `options`. Writes through
  `from_catalog` take their partition columns from `options.partitionKeys`.
- Reading an unmapped table raises an error that contains the exact TOML to
  add. With `on_missing = "empty"`, the read logs a `WARN` and returns an
  empty frame instead. Writing to an unmapped table always raises.
- Every mapped table is also registered as a **temp view** named `db_table`
  (e.g. `raw_db_orders`), so `spark.sql("select * from raw_db_orders")` works.
  Views are built when the session is created. A table that has no data yet
  is skipped with a `WARN` and gets no view. `spark.sql("... raw_db.orders
  ...")` with a dotted name is **not** resolved through the map in v1.

### S3 passthrough (best-effort)

`s3.mode = "passthrough"` leaves `s3://` URIs alone and lets Sail read real S3
through the standard AWS credential chain (env, profile, web identity, ECS,
IMDS). A region is required (`AWS_REGION`, `AWS_DEFAULT_REGION` or the
profile's `region`), and prytt fails early if none is set.

**This mode is unverified.** Anonymous public reads
(`AWS_SKIP_SIGNATURE=true`) were tested, but SSO profiles and other
credential setups were not. prytt logs a one-line `WARN` when passthrough is
on. Without credentials, Sail waits about 1 s on IMDS and then fails.

### Strict mode

By default, a call prytt doesn't implement (an unknown `GlueContext`,
`DynamicFrame`, `Job` or `SparkContext` method, or an unknown transform) logs
`STUB` and returns something chainable, so the rest of the job runs. In
strict mode the same call raises `prytt.stub.GlueLocalUnsupported` (a
`NotImplementedError`), so CI catches the gaps.

Strict mode is on when `CI` is truthy, unless `strict` is set explicitly:
`CI=true PRYTT_STRICT=0` runs non-strict. Calls that are logged as `WARN`
pass-throughs (`Filter`, `Map`, `Relationalize`, `EvaluateDataQuality.apply`,
...) don't raise in strict mode.

Some common Glue boilerplate goes through that catch-all, so it **raises in
strict mode**: `sc.setLogLevel(...)`,
`sc._jsc.hadoopConfiguration().set(...)`, `job.isInitialized()` and
`EvaluateDataQuality().process_rows(...)`. Set `PRYTT_STRICT=0` for jobs that
use them.

## Turn it off

```bash
PRYTT_DISABLE=1 python jobs/my_job.py ...
```

This skips the bootstrap entirely, so `awsglue` imports still resolve to
prytt's package but no hooks are installed and no server starts. To remove
prytt completely, uninstall it.

## What's real and what's stubbed

- **Real:** `getResolvedOptions`, `GlueContext` S3/catalog readers and
  writers (`json`, `csv`, `parquet`/`glueparquet`, `delta`, `iceberg`;
  `partitionKeys`), `DynamicFrame` (`fromDF`, `toDF`, `apply_mapping`,
  `select_fields`, `drop_fields`, `rename_field`, `join`, `resolveChoice`
  `cast:` specs, ...), the transforms `ApplyMapping`, `SelectFields`,
  `DropFields`, `RenameField`, `Join`, `DropNullFields`, `ResolveChoice`,
  `SelectFromCollection`, and `Job.init` / `Job.commit` (bookmarks are off).
- **Pass-through with `WARN`:** `Filter`, `Map`, `Relationalize`, `Unbox`,
  `UnnestFrame`, `SplitFields`, `SplitRows`, `Spigot`, `ErrorsAsDynamicFrame`,
  `EvaluateDataQuality.apply` (no rules run).
- **Not supported (raise `GlueLocalUnsupported`):** RDD APIs
  (`sc.parallelize`, `sc.textFile`, ...), non-S3 connection types (JDBC,
  DynamoDB, Kafka, ...) and ORC (Sail has no ORC reader).
- **Spark Connect limits:** `spark.sparkContext` and other JVM-only APIs are
  not available on a Spark Connect session. Use the `SparkContext()` that
  prytt provides, or `glueContext`, instead.

## Iceberg (`glue_catalog`)

_To be filled in by T10._

## Developing prytt

```bash
uv sync            # installs prytt editable, including prytt.pth
uv run pytest      # SPARK_REMOTE must be unset
```
