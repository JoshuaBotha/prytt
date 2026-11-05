# Run Glue jobs locally

This guide covers running an unmodified Glue 6 job script on your machine:
passing job arguments, where `s3://` data lives, mapping Data Catalog tables,
Iceberg tables, and reading prytt's log. For every setting, see the
[configuration reference](configuration.md). For what is and isn't
emulated, see [Compatibility and limits](compatibility.md).

## Run a script

Run the script with Python as usual, with the same `--KEY value` arguments
Glue would pass:

```bash
python jobs/bronze_to_silver.py --JOB_NAME local --source_path s3://raw-bucket/orders/
```

```
[prytt] INFO prytt 0.3.0, pysail 0.7.3, pyspark-client 4.1.4; sail at sc://127.0.0.1:52341
[prytt] INFO Job.init('local'), bookmarks off
[prytt] MAP  s3://raw-bucket/orders/ -> /home/me/proj/.prytt/s3/raw-bucket/orders/
[prytt] WARN Filter not applied locally (v1); data passed through unchanged
[prytt] INFO Job.commit('local')
```

The script needs no changes. The usual Glue boilerplate works:

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
...
job.commit()
```

Plain PySpark scripts, with no `awsglue` and no `SparkContext`, work too:
`SparkSession.builder.getOrCreate()` starts Sail for them.

### What happens when the script runs

1. At interpreter start, `prytt.pth` installs an import hook (< 1 ms).
   Nothing else loads until the script imports PySpark or `awsglue`.
2. On first Spark use (`SparkContext()`, `GlueContext(...)` or
   `SparkSession.builder.getOrCreate()`), prytt starts a Sail server inside
   the Python process on a free local port and points `SPARK_REMOTE` at it.
3. When the session is created, prytt registers your
   [mapped catalog tables](#catalog-tables) and any
   [Iceberg tables](#iceberg-tables-glue_catalog) already on disk.
4. `s3://` paths given to Glue readers and writers are rewritten to local
   directories ([S3 paths](#s3-paths)).
5. Glue features prytt can't do locally are logged rather than failing
   ([Strict mode](#strict-mode)).
6. At exit, prytt stops the session and the server.

The [architecture overview](../architecture/overview.md) explains each step
in depth.

### Using a server you run yourself

If `SPARK_REMOTE` is already set when Spark is first used, prytt connects to
that server and does not start Sail. Leave it **unset** for the normal local
flow. With an external server:

- Mapped catalog tables are registered on that server, so it must be able to
  read the mapped local paths.
- prytt does not set Sail's catalog environment, so Iceberg's `glue_catalog`
  exists only if that server declares it.

## Job arguments

`getResolvedOptions(sys.argv, [...])` looks up each requested option in these
places, first match wins:

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
- **Extra arguments** on the command line that the script doesn't request
  are ignored, as on Glue.
- **Logging:** a value taken from an env var or config logs
  `INFO --JOB_NAME not on the command line; using 'local' from prytt config`.
  A missing option logs a `WARN` naming it, and its value is `None`. The job
  keeps running, which is what you want when the script never uses that
  argument (often the case for `JOB_NAME`). If the script does use it, it
  fails later, wherever it uses the `None`.
- **Strict mode:** a missing option raises `GlueArgumentError` with the same
  message Glue gives, e.g. `the following arguments are required:
  --JOB_NAME`. Options set by env var or config count as present.
- `TempDir` and `RedshiftTempDir` are never required and mirror each other,
  as on Glue.
- prytt doesn't change `sys.argv`. A script that checks `"--name" in
  sys.argv` before calling `getResolvedOptions` (a common way to make an
  argument optional) only sees the command line, so pass those arguments
  there.

## S3 paths

In the default `s3.mode = "local"`, every `s3://bucket/key` (also `s3a://`
and `s3n://`) given to a Glue reader or writer becomes
`<s3.root>/bucket/key`. `s3.root` defaults to `./.prytt/s3`, relative to the
directory you run the script from. Each rewrite is logged:

```
[prytt] MAP  s3://raw-bucket/orders/ -> /home/me/proj/.prytt/s3/raw-bucket/orders/
```

Put input files where the job expects them:

```
.prytt/s3/
└── raw-bucket/
    └── orders/
        ├── part-0001.json
        └── part-0002.json
```

Writes create directories as needed and append, as Glue's sinks do by
default. Running a job twice therefore doubles its output; delete the target
directory between runs if the job isn't idempotent.

Paths are mapped in:

- `glueContext.create_dynamic_frame` / `create_data_frame` /
  `write_dynamic_frame` / `write_data_frame` (`from_options` and
  `from_catalog`), including the flat `create_dynamic_frame_from_options`
  spellings.
- `path` values in [catalog table mappings](#catalog-tables).

Paths are **not** mapped in plain Spark calls (`spark.read.parquet("s3://...")`)
or in SQL (`LOCATION 's3://...'`). Those reach Sail unchanged and fail unless
you use passthrough mode. See
[Compatibility and limits](compatibility.md#s3-and-paths).

Plain Spark writes to `s3://` (`df.write.parquet("s3://...")`), SQL writes
to an S3 location, and writes into a passthrough table are blocked by the
S3 write lock in every mode: prytt raises `PryttS3WriteBlocked` rather than
let a local run write to real S3 with your shell's credentials. Write to a
local path or use a Glue writer instead.

### Reading real S3

To read real S3, set `s3.mode = "passthrough"` for everything, or
`mode = "passthrough"` on just the catalog tables you need. Passthrough
reads have been verified against real AWS with an SSO profile. Pick the AWS
identity with `s3.profile` (SSO profiles included); no region is needed.

[S3 and AWS](s3-and-aws.md) covers all of it: which mode suits your case,
what each call does in each mode, per-table `mode`, the S3 write lock and
how to unlock it, AWS profiles with an SSO walkthrough, and the
one-profile-per-process rule.

## Catalog tables

Jobs that read `create_dynamic_frame.from_catalog(database=..., table_name=...)`
need a local stand-in for each Glue Data Catalog table. Map each one to a
path and a format:

```toml
# pyproject.toml (or the same without "tool.prytt." in prytt.toml)
[tool.prytt.catalog."raw_db.orders"]
path = "s3://raw-bucket/orders/"     # goes through the S3 mapping
format = "json"
options = { multiLine = true }       # Spark/Glue format options

[tool.prytt.catalog."curated_db.orders"]
path = "s3://curated-bucket/orders/"
format = "parquet"
options = { partitionKeys = ["country"] }   # used by write_dynamic_frame.from_catalog
```

- Each entry takes only `path`, `format` and `options`. A missing `path` or
  `format`, or any other key, is a config error.
- `path` may be an `s3://` URI (mapped like any other) or a local path.
- `format` is one of `json`, `csv`, `parquet` (or `glueparquet`), `delta`,
  `iceberg`. `orc` is rejected.
- `options` are format options in Glue or Spark spelling (`withHeader`,
  `separator`, `multiline`, ... are translated; see the
  [configuration reference](configuration.md#format-names-and-options)).
- Writes through `write_dynamic_frame.from_catalog` take their partition
  columns from `options.partitionKeys`.

### Reading through `GlueContext`

`from_catalog` looks the table up in the map (exact name first, then
case-insensitive, as Glue names are) and reads it like `from_options`.
`additional_options` passed in the call override the mapped `options`.

Reading an unmapped table raises `CatalogTableNotFound` with the exact TOML
to add:

```
prytt: catalog table raw_db.customers is not mapped. Add this to pyproject.toml (or the same without 'tool.prytt.' to prytt.toml):

[tool.prytt.catalog."raw_db.customers"]
path = "s3://<bucket>/<prefix>/customers/"
format = "parquet"
options = {}

or set catalog.on_missing = "empty" to read it as an empty frame
```

With `catalog.on_missing = "empty"`, the read logs a `WARN` and returns an
empty frame with no columns instead. Writing to an unmapped table always
raises.

### Querying by name in Spark SQL

Every mapped table is also a **registered table**, so a script can use its
Glue name in SQL:

```python
spark.table("raw_db.orders")
spark.sql("SELECT o.id, c.name FROM raw_db.orders o JOIN raw_db.customers c ON o.cid = c.id")
```

Both read the mapped path with the configured format and options. Joins
across mapped tables work, and the name is case-insensitive
(`Raw_DB.Orders` works too), as on Glue.

Every mapped table is also a **temp view** named `db_table` (for example
`raw_db_orders`), so `spark.sql("select * from raw_db_orders")` works too.
View names are case-sensitive.

Tables and views are registered when the session is created, and the log
shows one line per table, ending with the table's S3 mode and whether it
came from the table's own `mode` or the global `s3.mode`:

```
[prytt] MAP  raw_db.orders -> /home/me/proj/.prytt/s3/raw-bucket/orders/ (table raw_db.orders, view raw_db_orders, mode=local (global))
[prytt] MAP  ref_db.prices -> s3://gold-bucket/prices/ (table ref_db.prices, view ref_db_prices, mode=passthrough (table))
```

A local table that has no data yet is skipped with a `WARN`, so neither name
resolves for it in that session, even after the job writes it. A
passthrough table is not checked for local data; if Sail can't read it, it
is skipped with a `WARN` naming its location.

Querying an unmapped `db.table` with `spark.sql` / `spark.table` fails at
that call with Spark's "table or view not found" error, and the traceback
ends with the TOML to add. The hint is an exception note (Python's
`add_note`), so it is not part of `str(e)`.

The rules and limits of SQL name resolution are listed in
[Compatibility and limits](compatibility.md#catalog-tables-in-sql); the
design is in [ADR 0003](../adr/0003-mapped-tables-as-sail-external-tables.md).

## Iceberg tables (`glue_catalog`)

Glue 6 jobs that use Iceberg through the `glue_catalog` Spark catalog run
unchanged. These work against `glue_catalog.<db>.<tbl>`:

- `CREATE DATABASE` / `CREATE TABLE ... USING iceberg`
- `INSERT INTO`, `MERGE INTO`
- `CREATE TABLE ... AS SELECT` (CTAS)
- `df.write.saveAsTable(...)` and `df.writeTo(...).append()`

```python
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.sales")
spark.sql(
    "CREATE TABLE IF NOT EXISTS glue_catalog.sales.orders (id bigint, amount double) USING iceberg"
)
spark.sql(
    "MERGE INTO glue_catalog.sales.orders t USING updates u ON t.id = u.id "
    "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
)
```

The `--conf spark.sql.catalog.glue_catalog...` job parameters you set on
Glue are not needed locally.

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
[prytt] MAP  glue_catalog.sales.orders -> /home/me/proj/.prytt/warehouse/sales/orders
```

**Create once.** `MERGE INTO` or `INSERT` on a table that doesn't exist yet
fails with Sail's error. Run `CREATE DATABASE` and `CREATE TABLE ... USING
iceberg` once (or keep the `IF NOT EXISTS` forms in the job); later runs pick
the table up from disk.

**Start fresh.** Delete the warehouse directory (`rm -rf .prytt/warehouse`).

**Your own catalog list.** If you set `SAIL_CATALOG__LIST` yourself, prytt
keeps it. If it has no `glue_catalog`, prytt logs a `WARN` and doesn't
register any tables.

Limits are in [Compatibility and limits](compatibility.md#iceberg).

## Strict mode

By default, a call prytt doesn't implement (an unknown `GlueContext`,
`DynamicFrame`, `Job` or `SparkContext` method, or an unknown transform) logs
`STUB` and returns something chainable, so the rest of the job runs. In
strict mode the same call raises `prytt.stub.GlueLocalUnsupported` (a
`NotImplementedError`), so CI catches the gaps. A job argument missing from
the command line, env and config raises `GlueArgumentError`, as on real
Glue, instead of coming back as `None`.

Strict mode is on when `CI` is `1`, `true` or `yes`, unless `strict` is set
explicitly: `CI=true PRYTT_STRICT=0` runs non-strict, and
`PRYTT_STRICT=1` turns it on locally.

Some calls never raise, strict or not:

- `WARN` pass-throughs (`Filter`, `Map`, `Relationalize`,
  `EvaluateDataQuality.apply`, ...): the data passes through unchanged.
- Common Glue boilerplate: `sc.setLogLevel(...)`, `sc.addPyFile(...)` and
  `sc.setCheckpointDir(...)` log `INFO` and do nothing; `sc._jsc` /
  `sc._jvm` / `sc._conf` call chains (such as
  `sc._jsc.hadoopConfiguration().set(...)`) log `STUB` and keep going;
  `job.isInitialized()` is `True` after `job.init(...)`; and
  `EvaluateDataQuality().process_rows(...)` returns a collection whose
  `originalData` and `rowLevelOutcomes` are the input frame.

A good routine: run a job locally without strict mode while porting it, read
the `STUB` and `WARN` lines, then run it with `PRYTT_STRICT=1` in CI.

## Log output

prytt writes to stderr as `[prytt] <TAG> <message>`:

| Tag | Meaning |
|---|---|
| `INFO` | Server start (versions, address), `Job.init` / `Job.commit`, no-op boilerplate, values taken from config |
| `MAP` | An `s3://` path or catalog table was rewritten to a local location |
| `WARN` | A known gap: the data passed through unchanged (e.g. `Filter.apply`, `EvaluateDataQuality`), or an option was ignored |
| `STUB` | A call prytt doesn't implement was logged and ignored (raises in strict mode) |

`PRYTT_LOG_LEVEL=WARNING` hides `INFO`, `MAP` and `STUB`. Sail's own Rust
logs default to `RUST_LOG=error`; set `RUST_LOG` yourself, or use
`PRYTT_LOG_LEVEL=DEBUG`, to see more.

`glueContext.get_logger()` returns a standard `logging.Logger`
(`prytt.glue`) with `info`, `warn` and `error`, writing through the same
handler.

## Local data layout

By default everything prytt writes lives under `.prytt/` in the directory
you run from. Add it to `.gitignore`.

```
.prytt/
├── s3/          # s3.root: s3://bucket/key -> .prytt/s3/bucket/key
└── warehouse/   # iceberg.warehouse: glue_catalog.<db>.<tbl>
```

Run jobs from the same directory each time (usually the project root), or
set `s3.root` and `iceberg.warehouse` to absolute paths; both default to
paths relative to the current directory.
