# prytt

Run unmodified AWS Glue 6 job scripts on your laptop. prytt is a drop-in
`awsglue` + PySpark shim backed by [Sail](https://github.com/lakehq/sail), a
Rust Spark Connect server. No JVM, no Docker, no script changes.

```bash
python jobs/bronze_to_silver.py --JOB_NAME local --source_path s3://raw-bucket/orders/
```

```
[prytt] INFO prytt 0.1.1, pysail 0.7.3, pyspark-client 4.1.4; sail at sc://127.0.0.1:52341
[prytt] INFO Job.init('local'), bookmarks off
[prytt] MAP  s3://raw-bucket/orders/ -> /home/me/proj/.prytt/s3/raw-bucket/orders/
[prytt] WARN Filter not applied locally (v1); data passed through unchanged
[prytt] INFO Job.commit('local')
```

## Install

prytt needs **Python 3.13**, the version AWS Glue 6 runs. Your project's
`requires-python` must allow it, or uv fails with "requested Python version
does not satisfy Python>=3.13". Pin 3.13 in the project that uses prytt:

```bash
uv python pin 3.13                                  # writes .python-version
# pyproject.toml: requires-python = "==3.13.*"      # matches Glue 6 exactly
```

(`uv init --python 3.13` sets both for a new project.) Then add it as a dev
dependency from git (pin a tag):

```bash
uv add --dev "prytt @ git+https://github.com/JoshuaBotha/prytt.git@v0.1.1"
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

See [Job arguments](#job-arguments) to set arguments in config instead,
and for what happens when one is missing.

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

### Job arguments

`getResolvedOptions(sys.argv, [...])` looks up each requested option in
these places, first match wins:

| # | Source | Example |
|---|---|---|
| 1 | Command line | `--JOB_NAME local` or `--JOB_NAME=local` |
| 2 | `PRYTT_ARG_<name>` env var | `PRYTT_ARG_JOB_NAME=local` |
| 3 | `[args]` in `prytt.toml` | `JOB_NAME = "local"` |
| 4 | `[tool.prytt.args]` in `pyproject.toml` | `JOB_NAME = "local"` |
| 5 | Nowhere | `None`, logged `WARN` (raises in strict mode) |

Each option is looked up on its own, so you can mix sources: put stable
values in config and override one on the command line.

```toml
# prytt.toml (searched upward from cwd, like the rest of the config)
[args]
JOB_NAME = "local"
source_path = "s3://raw-bucket/orders/"
target_path = "s3://curated-bucket/orders/"
```

```toml
# or in pyproject.toml
[tool.prytt.args]
JOB_NAME = "local"
```

```bash
python jobs/my_job.py                                     # uses prytt.toml
python jobs/my_job.py --target_path s3://scratch/orders/  # overrides one
PRYTT_ARG_JOB_NAME=nightly python jobs/my_job.py          # env beats config
```

Details:

- **Names** match the option exactly as the script requests it
  (case-sensitive). For an option with a hyphen, such as `source-path`,
  prytt also tries the underscore form (`source_path`), because env var
  names can't contain `-`. The result is keyed `source_path`, as on Glue.
- **Values are strings**, as Glue passes them. TOML numbers become strings
  (`10` → `"10"`) and booleans become `"true"` / `"false"`.
- **Logging:** a value taken from an env var or config logs
  `INFO --JOB_NAME not on the command line; using 'local' from prytt config`.
  A missing option logs a `WARN` naming it, and its value is `None`. The
  job keeps running, which is what you want when the script never uses that
  argument (often the case for `JOB_NAME`). If the script does use it, it
  fails later, wherever it uses the `None`.
- **Strict mode** (on by default when `CI` is set; see
  [Strict mode](#strict-mode)): a missing option raises `GlueArgumentError`
  with the same message Glue gives, e.g. `the following arguments are
  required: --JOB_NAME`. Options set by env var or config count as present.
- `TempDir` and `RedshiftTempDir` are never required and mirror each other,
  as on Glue.
- prytt doesn't change `sys.argv`. A script that checks `"--name" in
  sys.argv` before calling `getResolvedOptions` (a common way to make an
  argument optional) only sees the command line, so pass those arguments
  there.

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

### Extra `SparkSession`s

If a test makes a second session with `SparkSession.builder.create()`, call
`stop()` on it when the test is done:

```python
other = SparkSession.builder.create()
try:
    ...
finally:
    other.stop()
```

Background: in pyspark-client, a session that is dropped without `stop()` is
closed by its `__del__` once garbage collection reaches it. That close also
shuts down a thread pool that all sessions share. If the collection happens
while another session is mid-query, the process deadlocks: the run hangs with
no error and 0% CPU, in whichever test is running at the time. prytt patches
that `__del__` so a collected session closes only its own connection, which
avoids the hang. Stopping extra sessions yourself is still cleaner and doesn't
depend on the patch.

prytt's own test config dumps every thread's stack after 60 s and fails the
run after 180 s, so a hang shows where it's stuck instead of stalling CI.
Copy that if your suite needs it (it uses the `pytest-timeout` plugin):

```toml
[tool.pytest.ini_options]
faulthandler_timeout = 60
timeout = 180
timeout_method = "thread"
```

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
| `args.<name>` | `PRYTT_ARG_<name>` | none | Job arguments for `getResolvedOptions`; see [Job arguments](#job-arguments) |

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
`NotImplementedError`), so CI catches the gaps. A job argument missing from
the command line, env and config raises `GlueArgumentError`, as on real
Glue, instead of coming back as `None` (see [Job arguments](#job-arguments)).

Strict mode is on when `CI` is truthy, unless `strict` is set explicitly:
`CI=true PRYTT_STRICT=0` runs non-strict. Calls that are logged as `WARN`
pass-throughs (`Filter`, `Map`, `Relationalize`, `EvaluateDataQuality.apply`,
...) don't raise in strict mode.

Common Glue boilerplate never raises, strict or not: `sc.setLogLevel(...)`,
`sc.addPyFile(...)` and `sc.setCheckpointDir(...)` log `INFO` and do
nothing; `sc._jsc` / `sc._jvm` / `sc._conf` call chains (such as
`sc._jsc.hadoopConfiguration().set(...)`) log `STUB` and keep going;
`job.isInitialized()` is `True` after `job.init(...)`; and
`EvaluateDataQuality().process_rows(...)` logs `WARN` and returns a collection
whose `originalData` and `rowLevelOutcomes` are the input frame.

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
  `SelectFromCollection`, and `Job.init` / `Job.commit` / `Job.isInitialized`
  (bookmarks are off).
- **Pass-through with `WARN`:** `Filter`, `Map`, `Relationalize`, `Unbox`,
  `UnnestFrame`, `SplitFields`, `SplitRows`, `Spigot`, `ErrorsAsDynamicFrame`,
  `EvaluateDataQuality.apply` and `EvaluateDataQuality().process_rows` (no
  rules run).
- **Not supported (raise `GlueLocalUnsupported`):** RDD APIs
  (`sc.parallelize`, `sc.textFile`, ...), non-S3 connection types (JDBC,
  DynamoDB, Kafka, ...) and ORC (Sail has no ORC reader).
- **Spark Connect limits:** `spark.sparkContext` and other JVM-only APIs are
  not available on a Spark Connect session. Use the `SparkContext()` that
  prytt provides, or `glueContext`, instead.

## Iceberg (`glue_catalog`)

Glue 6 jobs that use Iceberg through the `glue_catalog` Spark catalog run
unchanged. These work against `glue_catalog.<db>.<tbl>`:

- `CREATE TABLE ... USING iceberg`
- `INSERT INTO`
- `MERGE INTO`
- `CREATE TABLE ... AS SELECT` (CTAS)
- `df.write.saveAsTable(...)` and `df.writeTo(...).append()`

```python
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.sales")
spark.sql("CREATE TABLE IF NOT EXISTS glue_catalog.sales.orders (id bigint, amount double) USING iceberg")
spark.sql("MERGE INTO glue_catalog.sales.orders t USING updates u ON t.id = u.id "
          "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
```

**Where the data lives.** Tables are stored under `iceberg.warehouse`
(`PRYTT_ICEBERG_WAREHOUSE`, default `./.prytt/warehouse`, made absolute) as
`<warehouse>/<db>/<tbl>`. prytt sets `spark.sql.warehouse.dir` to that path on
every session, and adds `LOCATION '<warehouse>/<db>'` to a script's
`CREATE DATABASE glue_catalog.<db>` that has none.

**Each run.** Sail's catalog lives in memory, so it is empty when a new
server or session starts. On every new session prytt scans the warehouse and
re-registers each database and each Iceberg table it finds, logging one
`MAP` line per table:

```
[prytt] MAP  glue_catalog.sales.orders -> /path/to/project/.prytt/warehouse/sales/orders
```

**Create once.** `MERGE INTO` or `INSERT` on a table that doesn't exist yet
fails with Sail's error. Run `CREATE DATABASE` and `CREATE TABLE ... USING
iceberg` once (or keep the `IF NOT EXISTS` forms in the job); later runs pick
the table up from disk.

**Your own catalog list.** If you set `SAIL_CATALOG__LIST` yourself, prytt
keeps it. If it has no `glue_catalog`, prytt logs a `WARN` and doesn't
register any tables.

Limits:

- `LOCATION 's3://...'` inside SQL is not mapped to a local path (only
  `GlueContext` readers/writers and catalog config go through `s3.root`).
- A database or table with an explicit `LOCATION` outside
  `<warehouse>/<db>/<tbl>` works in that run but is not re-registered in the
  next one. prytt logs a `WARN` when a script's `CREATE DATABASE` sets its
  own location, or when it can't add one (for example, several statements in
  one `spark.sql` string).
- With an external server (`SPARK_REMOTE` set), prytt doesn't set Sail's
  catalog environment. That server needs its own `glue_catalog`.

## Developing prytt

```bash
uv sync            # installs prytt editable, including prytt.pth
uv run pytest      # SPARK_REMOTE must be unset
```

### Releasing

Bump `version` in `pyproject.toml` and merge to `main`. The Release workflow
runs `mise run release:publish`, which tags `vX.Y.Z` and publishes a GitHub
Release with the wheel and sdist. If that version is already published it
does nothing, so merging unrelated changes never re-releases. If a release
run fails, fix the cause and use "Run workflow" on the Release workflow (on
`main`); a tag without a published release, or a leftover draft, is retried.
