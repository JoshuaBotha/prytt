# Spec: prytt — drop-in `awsglue` + PySpark for local Glue 6 dev on Sail

Status: draft for a Claude Code build. Project name: `prytt` (deliberate spelling, distinct from the "Pritt" glue stick trademark). Distribution, internal package, config keys, env vars and log prefix all use this name.

Companion doc: [`prytt-spec-sql-catalog.md`](prytt-spec-sql-catalog.md) (future feature, not v1).

## 1. Goal

Install one package into a Python venv. After that, an unmodified AWS Glue 6 PySpark job script runs locally with:

```bash
python my_job.py --JOB_NAME local --some_arg value
```

No edits to the script, no extra imports, no wrapper command, no JVM, no Docker.

Behind the scenes:

- `pyspark` is the real **`pyspark-client`** (Spark Connect thin client, pure Python, no JARs).
- The engine is **Sail** (`pysail`), a Rust Spark Connect server, started automatically.
- `awsglue` is **our own shim package**. `GlueContext` hands back the Sail-backed `SparkSession`. `DynamicFrame` wraps a real DataFrame, so transforms run and `.show()` prints real rows.
- Anything we have not implemented is a logging stub. It records the call and carries on.

### Fidelity stance (important)

This is **not** a correctness or parity test of Glue. It is a fast local feedback loop: does the script import, wire up, and push data through its transforms, and what do the rows look like. Hollow behaviour is acceptable where real behaviour is hard. Real rows where cheap are the bonus.

## 2. Decisions already made (and why)

| Decision | Reason |
|---|---|
| Target **Glue 6.0** → Spark **4.1.x**, Python **3.13** | Glue 6.0 runtime. Spark 4 ships `pyspark-client` (no JARs). |
| Engine: **Sail + `pyspark-client`** | Real PySpark API surface (not a clone), no JVM. |
| Rejected: `sparkless` / `robin-sparkless`, SQLFrame | API clones with known gaps. Developer has hit `sparkless` limits before. |
| Rejected: AWS Glue Docker image | ~3 GB, heavy. |
| Rejected: real `awsglue` (`AWSGlueDataplanePython` 6.0.0 on PyPI) | Thin py4j wrapper; needs JVM + Glue ETL jar. Cannot work over Spark Connect. |
| `awsglue`: **own shim**, not `MagicMock` | `MagicMock` returns more mocks (not DataFrames), swallows typos, gives no IDE help. `MagicMock(wraps=...)` stays useful *inside tests* for call assertions. |
| Shim style: thin bridge + logging catch-all | Real behaviour for core paths. A `__getattr__` catch-all logs and returns `self` for everything else, so scripts never crash on unknown calls. |
| Distribution: **git dependency** for now, PyPI later | No index to run. Packaging stays PyPI-ready (hatchling, clean sdist + wheel, `.pth` placed via `force-include` in the wheel) so publishing later needs no code change. |
| Strict mode **on by default in CI** | Gaps surface in CI; local runs stay forgiving. See section 11. |
| Iceberg `glue_catalog.*`: **in scope** (ticket T10) | Spike S7 positive: Sail handles Iceberg and Delta, including `MERGE INTO`, and can expose a `glue_catalog` catalog. |
| Sail runs **in-process** | Spike S3: ephemeral port, clean stop, clean exit. No subprocess needed. |
| `spark.sql` `db.table` resolution: **deferred** | Not in v1. Light design in `prytt-spec-sql-catalog.md`. |

## 3. Non-goals (v1)

- Behavioural parity with Glue (choice types, DynamicRecord semantics, error records).
- Job bookmarks, Glue connections/connectors (JDBC, Kafka, Kinesis), streaming jobs, Scala jobs.
- RDD API / `SparkContext` data methods (Spark Connect has no RDDs).
- Real AWS calls. S3 and the Glue Data Catalog are mapped to local files (passthrough to real S3 is an optional mode).
- Running in AWS. This package must **never** be shipped to a Glue job (`--additional-python-modules`). Glue supplies the real `awsglue`.
- `spark.sql(...)` / `spark.table(...)` resolving `db.table` through the catalog map. Deferred; see `prytt-spec-sql-catalog.md`. v1 offers temp views named `db_table` only.
- Glue-catalog persistence: `glue_catalog` tables are re-registered from disk each run, not kept in a metastore (section 10).
- ORC: Sail has no ORC data source (spike S2).

## 4. User experience

### Install

```bash
uv add --dev "prytt @ git+https://<host>/<org>/prytt.git@v0.1.0"   # pulls pysail + pyspark-client>=4.1,<4.2
```

Installed only as a dev dependency, from git (pin a tag). The very first run after install can take ~30 s before Sail answers (one-off native library load on macOS); later runs start in well under a second. PyPI publishing may follow later; the install line then becomes `uv add --dev prytt`. Nothing else to configure for the default path.

### Run a job

```bash
python jobs/bronze_to_silver.py --JOB_NAME local --source_path s3://raw-bucket/orders/
```

On first Spark use, an in-process Sail server starts on an ephemeral port. `s3://raw-bucket/orders/` is read from `./.prytt/s3/raw-bucket/orders/`. Stubbed calls appear in the log, e.g.:

```
[prytt] INFO Job.commit('local')
[prytt] MAP  s3://raw-bucket/orders/ -> .prytt/s3/raw-bucket/orders/
[prytt] WARN Filter.apply: not implemented, frame passed through unchanged
```

### Test

A pytest plugin (auto-registered) gives `spark`, `glue_context` and `s3_root` fixtures, sharing one Sail server per session.

### Turn it off

`PRYTT_DISABLE=1` skips the bootstrap entirely.

## 5. Architecture

One distribution, three top-level import packages plus a `.pth` file:

```
prytt/
  pyproject.toml
  src/
    prytt/              # internals
      __init__.py
      _bootstrap.py          # imported by .pth at interpreter start; must be cheap
      _hooks.py              # lazy post-import hooks (meta path finder)
      config.py              # env + pyproject [tool.prytt] / prytt.toml
      server.py              # Sail lifecycle, sets SPARK_REMOTE
      paths.py               # s3:// and catalog -> local mapping
      stub.py                # LogAny base, logger, strict mode
      shims/
        pyspark_context.py   # SparkContext replacement
      pytest_plugin.py
    awsglue/                 # public shim, mirrors real module layout
      __init__.py            # exports DynamicFrame
      context.py             # GlueContext
      dynamicframe.py        # DynamicFrame, DynamicFrameCollection, Reader, Writer
      job.py                 # Job
      utils.py               # getResolvedOptions
      transforms/__init__.py # ApplyMapping, Join, ... with __all__
      gluetypes.py           # minimal type names
      (other modules as stubs: data_sink, data_source, ...)
    awsgluedq/               # Glue Studio inserts EvaluateDataQuality; stub it
      transforms.py
  prytt.pth             # single line: import prytt._bootstrap
```

Dependencies: `pysail>=0.7.3`, `pyspark-client>=4.1,<4.2` (spike S2: tested with 0.7.3 + 4.1.4). Use hatchling `force-include` (or equivalent) to place `prytt.pth` at the site-packages root.

### Name clash

Our package provides the `awsglue` import name. It must not be installed alongside `AWSGlueDataplanePython` or `awsglue-local`. Document this; optionally detect and warn in the bootstrap.

## 6. Bootstrap (how "no script changes" works)

Python executes `import` lines in `.pth` files at startup. `prytt.pth` runs `import prytt._bootstrap`.

`_bootstrap` must be **cheap** (target < 20 ms added to every interpreter start in the venv). It must **not** import pyspark, pysail or grpc. It only:

1. Returns immediately if `PRYTT_DISABLE` is set.
2. Installs a meta path finder (`_hooks.py`) that watches a small set of module names.
3. Nothing else. Config loading and server start are lazy.

Import rule for `_bootstrap` and `_hooks`: stdlib only, and none of `importlib.abc`, `importlib.util`, `typing`, `inspect`, `logging`, `tomllib`. Write the finder and loaders as plain classes (duck-typed `find_spec` / `create_module` / `exec_module`) and build specs with `importlib.machinery.ModuleSpec`. Spike S4: `importlib.abc` alone costs ~16 ms (pulls in `typing`, `inspect`, `ast`); the plain version costs < 1 ms.

Post-import hooks:

| Module imported | Hook action |
|---|---|
| `pyspark` | Set `pyspark.SparkContext` to the shim (pyspark-client has no such attribute; spike S1). Importing `pyspark.context` here goes through the finder below. |
| `pyspark.context` | Serve our shim module. pyspark-client has no `pyspark.context` at all (spike S1). |
| `pyspark.sql.session` | Wrap `SparkSession.Builder.getOrCreate` so it calls `server.ensure_running()` first. Covers scripts that never touch `SparkContext`. |

`server.ensure_running()` is idempotent and is also called from the `SparkContext` shim and `GlueContext`.

Hook firing order seen in spike S4: `pyspark.sql.session` first (pyspark's `__init__` imports it), then `pyspark`, then `pyspark.context`. The hooks must not assume any other order.

## 7. Sail server lifecycle (`server.py`)

- If `SPARK_REMOTE` is already set (user runs their own server), use it. Do not start Sail.
- Otherwise start Sail **in-process** in the background on an ephemeral port: `SparkConnectServer(ip="127.0.0.1", port=0).start(background=True)` (returns in ~20 ms). Read the bound address from `server.listening_address` (a `(host, port)` tuple; `None` once stopped) and set `os.environ["SPARK_REMOTE"] = "sc://127.0.0.1:<port>"` before any session is built.
- Set Sail's own environment before `start()`: `SAIL_CATALOG__LIST` / `SAIL_CATALOG__DEFAULT_CATALOG` for `glue_catalog` (section 10). Sail reads it at start.
- Register `atexit` to stop the session and then the server. Spike S3: the process exits cleanly even without `stop()`, but the explicit stop gives ordered shutdown and logs.
- Sail logs to stderr at INFO by default. prytt sets `RUST_LOG=error` unless `RUST_LOG` is already set, and `log_level=DEBUG` leaves Sail at its default. `error` rather than `warn` because Sail logs benign warnings on every normal shutdown, which would add noise to every run.
- Subprocess mode (`sail spark server --port <free port>`) is not needed (spike S3). Keep it only as a documented escape hatch, not built in v1.
- Config can pin the port (`sail.port`) for debugging with an external client.

## 8. PySpark shims

`pyspark-client` is used as-is for `pyspark.sql` (SparkSession, DataFrame, functions, types, Window). Only the classic-only entry points get shimmed.

### `SparkContext` shim

| Member | Behaviour |
|---|---|
| `SparkContext(...)`, `SparkContext.getOrCreate(conf=None)` | Ensure server; singleton; log args. |
| `appName`, `master`, `version` | Plausible values; `version` from the session. |
| `setLogLevel`, `addPyFile`, `setCheckpointDir` | Explicit no-ops: log `INFO`, never raise (not even in strict mode). Other unknown methods go through the catch-all. |
| `_jsc`, `_jvm`, `_conf`, `hadoopConfiguration()` chains | Non-strict `LogAny` objects, so `sc._jsc.hadoopConfiguration().set(k, v)` logs `STUB` and works, also in strict mode. |
| `parallelize`, `textFile`, `wholeTextFiles`, `emptyRDD`, `broadcast`, `accumulator` | Raise `GlueLocalUnsupported` with a clear message (RDDs do not exist in Spark Connect). Returning a stub here would only fail later and more confusingly. |
| `stop()` | Stop the session. |

`SparkConf` (`pyspark.conf`) and `SQLContext` (`pyspark.sql`) import and work under the client (spike S1). No shim.

## 9. `awsglue` shim surface

### Shared catch-all (`prytt/stub.py`)

```python
class LogAny:
    def __getattr__(self, name):
        def _noop(*args, **kwargs):
            stub_log(type(self).__name__, name, args, kwargs)
            return self
        return _noop
```

Calling a missing method on any shim class logs `STUB` and returns `self`, so chained calls keep going. In **strict mode** (`strict = true`) the catch-all raises instead, which is useful for finding gaps. Strict mode is on by default when running in CI (section 11).

### `awsglue.utils.getResolvedOptions(args, options)`

Real behaviour, pure Python. Parse `--KEY value` and `--KEY=value` from `args`, return a dict for the requested `options`, and raise the same way Glue does if a required option is missing. Ignore extra args. No hidden defaults; locally the user passes `--JOB_NAME`.

### `awsglue.context.GlueContext(sc, **options)`

| Member | Behaviour |
|---|---|
| `spark_session` | The Sail-backed `SparkSession` (`SparkSession.builder.getOrCreate()`). |
| `create_dynamic_frame.from_options(connection_type, connection_options, format, format_options, transformation_ctx)` | For `s3`: map `paths` through `paths.py`, then `spark.read.format(fmt).options(...).load(paths)`. Wrap the result in a `DynamicFrame`. Other connection types: log and raise unsupported. |
| `create_dynamic_frame.from_catalog(database, table_name, ...)` | Resolve via the catalog map (section 10), then same as above. |
| `create_data_frame.from_options` / `.from_catalog` | Same, but return a DataFrame. |
| `write_dynamic_frame.from_options(frame, connection_type, connection_options, format, ...)` | For `s3`: map path, `frame.toDF().write.format(fmt).mode("append")`, `partitionBy(partitionKeys)` if given, `save(path)`. Glue's default write mode is append. |
| `write_dynamic_frame.from_catalog(...)` | Resolve via the catalog map, then as above. |
| `write_data_frame.*` | DataFrame equivalents. |
| `get_logger()` | A standard `logging.Logger` with `info` / `warn` / `error`. |
| `purge_s3_path`, `purge_table`, `getSink`, `getSource`, `forEachBatch`, ... | Catch-all stubs (log). |
| Any other attribute | If `SparkSession` has it (e.g. `sql`, `read`, `table`), delegate. Otherwise catch-all. Real `GlueContext` extends `SQLContext`, so scripts sometimes call `glueContext.sql(...)`. |

Format name mapping: `glueparquet` → `parquet`. Pass through `parquet`, `csv`, `json`; also `delta` and `iceberg`. `orc`: Sail has no ORC source (spike S2), so log WARN and raise unsupported. Map common `format_options` (`withHeader` → `header`, `separator` → `sep`, `multiline` → `multiLine`). Log anything not mapped.

### `awsglue.dynamicframe.DynamicFrame`

Wraps a DataFrame (`self._df`).

| Member | Behaviour |
|---|---|
| `toDF()` | Return the DataFrame. |
| `DynamicFrame.fromDF(df, glue_ctx, name)` | Wrap. |
| `count`, `show`, `printSchema`, `schema` | Delegate to the DataFrame. |
| `select_fields`, `drop_fields`, `rename_field`, `apply_mapping`, `repartition`, `coalesce` | Real, via the DataFrame. |
| `resolveChoice(specs=..., choice=...)` | `cast:<type>` specs become real casts; everything else is logged and passed through. |
| `filter(f)`, `map(f)` | v1: log WARN and pass through unchanged. v2 (optional): implement with `mapInPandas` and dict rows (spike S6 confirms it works). See notes below. |
| `errorsCount`, `stageErrorsCount` | Return 0. |
| `relationalize`, `unnest`, `spigot`, `split_fields`, `split_rows`, ... | Catch-all (log, return `self`). |

v2 `filter` / `map` requirements (spike S6):

- **Nulls:** under pandas 3, a SQL `NULL` in a string column reaches Python as `NaN`. Convert before calling `f`: `pdf.astype(object).where(pdf.notna(), None).to_dict("records")`. Otherwise code like `rec["v"] or ""` breaks.
- **Errors:** an exception in the worker comes back as `PythonException()` with an empty message. Catch inside the generator and re-raise with the formatted traceback in the message.

`DynamicFrameCollection`: dict-backed. `keys()`, `select(key)`, `values()`, `map(f)`.

### `awsglue.transforms`

`from awsglue.transforms import *` must work, so define `__all__`. Each transform is a class with a classmethod `apply(...)` matching Glue's keyword signatures.

| Real (v1) | Logged pass-through (v1) |
|---|---|
| `ApplyMapping` (tuples `(src, src_type, tgt, tgt_type)`; dotted nested paths; cast + alias) | `Filter`, `Map` (see above) |
| `SelectFields`, `DropFields`, `RenameField` | `Relationalize`, `Unbox`, `UnnestFrame` |
| `Join` (`frame1, frame2, keys1, keys2`) | `SplitFields`, `SplitRows`, `Spigot` |
| `DropNullFields` (drop all-null columns) | `ErrorsAsDynamicFrame`, anything else |
| `ResolveChoice` (cast specs only) | |

### `awsglue.job.Job`

`Job(glue_context)`, `init(name, args)` and `commit()` log and do nothing else. `isInitialized()` returns `True` after `init` (logged `INFO`). None of these raise in strict mode. Bookmarks are a no-op.

### `awsgluedq.transforms.EvaluateDataQuality`

`apply(frame, ruleset, publishing_options)`: log the ruleset name, return the frame unchanged. `EvaluateDataQuality().process_rows(frame, ...)`: log `WARN`, return a `DynamicFrameCollection` whose `originalData` and `rowLevelOutcomes` are the input frame. Neither raises in strict mode. Glue Studio-generated scripts import this, so it must import cleanly.

## 10. Paths and catalog (`paths.py`)

### S3

- `s3.mode = "local"` (default): `s3://bucket/key` → `<s3.root>/bucket/key`, with `s3.root` defaulting to `./.prytt/s3`. `s3a://` and `s3n://` are treated the same. Log every mapping at INFO.
- `s3.mode = "passthrough"`: leave URIs alone and let Sail read S3 itself. Sail uses the standard AWS Rust SDK credential chain (env, profile, web identity, ECS, IMDS). A region is required (`AWS_REGION` or the profile's region); fail early with a clear message if none is set. Without credentials Sail waits ~1 s on IMDS and then fails. Spike S5 confirmed anonymous reads (`AWS_SKIP_SIGNATURE=true`). The SSO-profile case was **skipped** (decision 2026-11-01): passthrough ships as an unverified, best-effort mode; say so in the README and log a one-line WARN when it is enabled.
- Writes create local directories as needed.

### Catalog

Mapping lives in config:

```toml
[tool.prytt.catalog."raw_db.orders"]
path = "s3://raw-bucket/orders/"   # goes through S3 mapping
format = "json"
options = { multiLine = true }
```

If a table is not mapped: `catalog.on_missing = "error"` (default) raises with a message showing the exact TOML to add. `"empty"` logs and returns an empty frame instead.

Also register every mapped table as a temp view named `db_table`. Resolving `spark.sql("... db.table ...")` through the map is **not** in v1; design notes live in `prytt-spec-sql-catalog.md` (spike S-SQL1 showed real databases + external tables work, so it is cheap to add later).

### Iceberg (`glue_catalog.*`), ticket T10

Glue 6 jobs often run `spark.sql("MERGE INTO glue_catalog.db.tbl ...")`. Spike S7: Sail supports Iceberg and Delta locally (write, read, `CREATE TABLE ... LOCATION`, `INSERT`, `MERGE INTO`, CTAS, `saveAsTable`, `writeTo().append()`).

- **Catalog:** before the server starts, set
  ```
  SAIL_CATALOG__LIST='[{name="sail", type="memory", initial_database=["default"]}, {name="glue_catalog", type="memory", initial_database=["default"]}]'
  SAIL_CATALOG__DEFAULT_CATALOG=sail
  ```
  so `glue_catalog.<db>.<tbl>` resolves. Skip this when the user brings their own server (`SPARK_REMOTE` set).
- **Warehouse:** set `spark.sql.warehouse.dir` to the absolute form of `iceberg.warehouse` (default `./.prytt/warehouse`) on every session prytt builds. Iceberg CTAS / `saveAsTable` fail on a relative location, and Delta otherwise writes `./spark-warehouse` into cwd.
- **Re-register each run:** the catalog is in memory, so table entries vanish with the server (and are not shared across sessions). Data files persist. On the first session, for each `<warehouse>/<db>/<tbl>` that holds Iceberg metadata, run `CREATE DATABASE IF NOT EXISTS glue_catalog.<db>` and `CREATE TABLE IF NOT EXISTS glue_catalog.<db>.<tbl> USING iceberg LOCATION '<abs path>'`. Log each as `MAP`. Create each database with `LOCATION '<warehouse>/<db>'` (and recreate the built-in `default` that way), and add that `LOCATION` to a script's own `CREATE DATABASE glue_catalog.<db>` that has none: otherwise Sail (0.7.3) puts the tables flat in the warehouse as `<tbl>-<uuid>`, and the next run cannot tell their database (Sail's memory catalog has no default-location setting, so this is a comment- and quote-aware rewrite of a single statement; multi-statement strings and other explicit `LOCATION`s are sent unchanged with a `WARN`). Table dirs directly under the warehouse (default `sail` catalog tables) are not databases. Sail needs the existing path for a schema-less `CREATE TABLE ... LOCATION`. If the server has no `glue_catalog` (user's own `SAIL_CATALOG__LIST`), log `WARN` and skip. Sail reads an unknown `glue_catalog.x` as a nested namespace, so check `listCatalogs()`.
- **Missing table:** `MERGE INTO` / `INSERT` on an unknown `glue_catalog` table fails with Sail's error. Users create it once (`CREATE TABLE glue_catalog.db.tbl ... USING iceberg`), after which re-registration picks it up. Seeding from config can come later.
- The pytest plugin registers per session, so each test session sees the tables on disk.

## 11. Configuration (`config.py`)

Sources, highest priority first: `PRYTT_*` env vars → `prytt.toml` → `[tool.prytt]` in the nearest `pyproject.toml` (searched upward from cwd) → defaults. Loaded lazily on first use.

| Key | Env | Default |
|---|---|---|
| `disable` | `PRYTT_DISABLE` | false |
| `log_level` | `PRYTT_LOG_LEVEL` | `INFO` |
| `strict` | `PRYTT_STRICT` | `true` if `CI` env var is truthy (`1`/`true`/`yes`), else `false` |
| `sail.port` | `PRYTT_SAIL_PORT` | 0 (ephemeral) |
| remote server | `SPARK_REMOTE` | unset → start Sail |
| `s3.mode` | `PRYTT_S3_MODE` | `local` |
| `s3.root` | `PRYTT_S3_ROOT` | `./.prytt/s3` |
| `catalog.on_missing` | `PRYTT_CATALOG_ON_MISSING` | `error` |
| `catalog.*` | — | none |
| `iceberg.warehouse` | `PRYTT_ICEBERG_WAREHOUSE` | `./.prytt/warehouse` (made absolute) |

An explicit `strict` (env or config file) always beats the CI-derived default, so `CI=true PRYTT_STRICT=0` runs non-strict.

## 12. Logging

One logger, `prytt`, with a `[prytt]` prefix. Tags: `STUB` (catch-all hit), `MAP` (path or catalog rewrite), `WARN` (known-unimplemented behaviour, data passed through), `INFO` (server start, address, versions). The first line of a run should log the versions of pysail and pyspark-client and the server address. Sail's own Rust logs are quieted by default (section 7).

## 13. Testing and acceptance

Build test-first (TDD skill). Unit tests for `getResolvedOptions`, path mapping, config precedence, `ApplyMapping` and the other real transforms, and catch-all and strict-mode behaviour (including the CI default: `CI=true` → strict; `CI=true PRYTT_STRICT=0` → non-strict; no `CI` → non-strict).

### Acceptance scripts

Kept verbatim in `tests/acceptance/scripts/`, each run as a **subprocess** (`python script.py --JOB_NAME t ...`) in a fresh venv-like environment, so the `.pth` path is exercised for real:

1. **Canonical boilerplate** (below) plus S3 JSON read, `ApplyMapping`, and a parquet write with `partitionKeys`. Assert the output parquet content.
2. **Glue Studio-style script** with `awsgluedq` `EvaluateDataQuality`, `DynamicFrame.fromDF`, a `sparkSqlQuery` helper using temp views, and catalog reads.
3. **Spark-only script** (no `SparkContext`, just `SparkSession.builder.getOrCreate()`), to prove the builder hook.
4. **Gap script** calling unknown `GlueContext` / `DynamicFrame` methods: exit 0 when non-strict, non-zero in strict mode. The test sets `PRYTT_STRICT` explicitly for each case so results do not depend on whether `CI` is set.

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
# ... transforms ...
job.commit()
```

### Non-functional

- Interpreter start overhead with the package installed: < 20 ms (measure `python -X importtime -c pass`). Spike S4 prototype measured < 1 ms; a regression test should fail above 20 ms.
- Process exits cleanly. No hanging Sail thread, no orphan subprocess.

## 14. Verify-first spikes (done 2026-10-30)

| ID | Question |
|---|---|
| S1 | With only `pyspark-client` 4.1.x installed: what do `import pyspark`, `from pyspark import SparkContext`, `import pyspark.context`, `from pyspark.conf import SparkConf` and `from pyspark.sql import SQLContext` do? Decides exactly what the hooks must replace. |
| S2 | Does the current `pysail` work with a `pyspark-client` 4.1.x client? If not, pin to 4.0.x and record it. |
| S3 | In-process server API: does `SparkConnectServer(port=0)` bind an ephemeral port, how is the bound address read, does `background=True` block process exit, does `stop()` work? Pick in-process vs subprocess. |
| S4 | Does the meta-path post-import hook reliably intercept `pyspark.context` both when imported directly and via `pyspark/__init__`? |
| S5 | Can Sail read `s3://` with the ambient AWS profile (for passthrough mode)? |
| S6 | Sail support for `mapInPandas` / Python UDFs in-process (gates DynamicFrame `filter` / `map` v2). |
| S7 | Does Sail support Iceberg (or Delta) tables locally over Spark Connect: catalog config, create/read/write, `MERGE INTO`? Gates T10. |

Findings: [`spikes/NOTES.md`](spikes/NOTES.md). All answered; the S5 SSO-profile check was skipped by decision (passthrough is best-effort). Short version: S1 shim `pyspark.context` + `pyspark.SparkContext` only; S2 works with `pysail` 0.7.3, no ORC; S3 in-process; S4 hook works, < 1 ms; S5 passthrough viable; S6 `mapInPandas` works (null + traceback caveats); S7 Iceberg/Delta + `glue_catalog` work.

## 15. Resolved questions

- Q1 Distribution: git dependency for now; PyPI possibly later. Packaging kept PyPI-ready. Name changed to `prytt` to avoid the "Pritt" trademark. (Sections 2, 4.)
- Q2 Iceberg: map `glue_catalog.*` to local tables only if Sail supports it. S7 says it does, so ticket T10 is in scope. (Section 10.)
- Q3 `spark.sql` `db.table` resolution: not in v1. Light future spec in `prytt-spec-sql-catalog.md`. (Sections 3, 10.)
- Q4 Strict mode in CI: yes, default on when `CI` is truthy; explicit `PRYTT_STRICT` wins. (Sections 9, 11, 13.)

## 16. Suggested ticket graph

```
S (spikes S1–S7, done)
 └─ T1 skeleton: packaging, .pth, lazy config, logging, LogAny, strict mode
     ├─ T2 server.py + import hooks + SparkContext shim + builder hook
     │   └─ T4 GlueContext + DynamicFrame + reader/writer + S3 mapping
     │       ├─ T5 catalog mapping + temp views
     │       ├─ T6 transforms (real set + pass-through set)
     │       └─ T8 pytest plugin (also needs T2)
     ├─ T3 getResolvedOptions + Job (pure Python, parallel to T2)
     └─ T7 awsgluedq stub
T9 acceptance scripts + README (needs T3, T5, T6, T7)
T10 Iceberg glue_catalog: Sail catalog env, warehouse dir, re-register on start (needs T2, T5)
```

## 17. Suggested skills for the next agent

- `to-tickets`: turn section 16 into tracker tickets with blocking edges.
- `implement-spec`: drive the build across the ticket frontier.
- `tdd`: red-green-refactor for the shim surface and transforms.
- `codebase-design`: keep `paths`, `server` and `config` as deep modules behind small interfaces.
- `diagnosing-bugs`: if the spikes or hooks misbehave.
