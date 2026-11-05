# Compatibility and limits

prytt aims to run an unmodified Glue 6 script locally and get the same
answer for the parts that matter in development: reading, reshaping and
writing data. It is not a full Glue emulator. This page lists, by area, what
is real, what is passed through, what fails, and where local behaviour
differs from AWS Glue.

Three outcomes, used throughout:

| Outcome | Log | Strict mode | What you get |
|---|---|---|---|
| **Real** | none, or `MAP` / `INFO` | same | The Glue behaviour, implemented on Spark. |
| **Pass-through** | `WARN` | same (never raises) | The input frame, unchanged. The job keeps going, but the data is not what Glue would produce. |
| **Stub** | `STUB` | raises `GlueLocalUnsupported` | A no-op that returns something chainable (usually the object itself). |
| **Unsupported** | `WARN` or none | raises | `GlueLocalUnsupported` (a `NotImplementedError`), always. |

## At a glance

- **Real:** `getResolvedOptions`; `GlueContext` S3 and catalog readers and
  writers (`json`, `csv`, `parquet`/`glueparquet`, `delta`, `iceberg`;
  `partitionKeys`); `DynamicFrame` (`fromDF`, `toDF`, `apply_mapping`,
  `select_fields`, `drop_fields`, `rename_field`, `join`, `resolveChoice`
  `cast:` specs, ...); the transforms `ApplyMapping`, `SelectFields`,
  `DropFields`, `RenameField`, `Join`, `DropNullFields`, `ResolveChoice`,
  `SelectFromCollection`; `Job.init` / `Job.commit` / `Job.isInitialized`
  (bookmarks are off); `db.table` names for mapped tables in
  `spark.sql` / `spark.table`; Iceberg through `glue_catalog`; the whole
  Spark DataFrame and SQL API that Sail supports.
- **Pass-through:** `Filter`, `Map`, `Relationalize`, `Unbox`,
  `UnnestFrame`, `SplitFields`, `SplitRows`, `Spigot`,
  `ErrorsAsDynamicFrame`, `DynamicFrame.filter` / `.map`, non-`cast:`
  `resolveChoice` actions, `EvaluateDataQuality.apply` and
  `EvaluateDataQuality().process_rows` (no rules run).
- **Stub:** any other `GlueContext`, `DynamicFrame`, `Job` or
  `SparkContext` method (`getSink`, `purge_s3_path`, `relationalize`, ...),
  and the transforms `FlatMap`, `MapToCollection`, `FillMissingValues`,
  `FindMatches`, `FindIncrementalMatches`.
- **Unsupported:** RDD APIs (`sc.parallelize`, `sc.textFile`, ...),
  connection types other than `s3` (JDBC, DynamoDB, Kafka, Redshift, ...),
  and ORC.

## Job set-up: `awsglue.utils`, `awsglue.job`, `SparkContext`

| API | Outcome | Notes |
|---|---|---|
| `getResolvedOptions(sys.argv, [...])` | Real | Also reads env vars and config; see [Job arguments](running-jobs.md#job-arguments). |
| `Job(glueContext)`, `.init(name, args)`, `.commit()` | Real | Log `INFO`. **Bookmarks are off**: every run reads all input. |
| `Job.isInitialized()` | Real | `True` after `init`. |
| Other `Job` methods | Stub | |
| `SparkContext()`, `SparkContext.getOrCreate()` | Real | A process-wide singleton that starts Sail. `appName`, `master`, `version` work. |
| `sc.setLogLevel`, `sc.addPyFile`, `sc.setCheckpointDir` | No-op | `INFO`, never raise. |
| `sc._jsc`, `sc._jvm`, `sc._conf` chains | Stub | Never raise, even in strict mode, so `sc._jsc.hadoopConfiguration().set(...)` keeps working. They have no effect. |
| RDD methods: `parallelize`, `textFile`, `wholeTextFiles`, `binaryFiles`, `emptyRDD`, `range`, `union`, `broadcast`, `accumulator`, `pickleFile`, `sequenceFile`, `newAPIHadoopFile`, `hadoopFile` | Unsupported | Spark Connect has no RDDs. Use `spark.createDataFrame` or `spark.read`. |
| Other `SparkContext` methods | Stub | |
| `SparkConf`, `SQLContext` | Real | pyspark-client's own. |
| `spark.sparkContext`, `df.rdd`, `_jvm` on a `SparkSession` | Fails | Spark Connect sessions have no JVM. Use prytt's `SparkContext()` or `glueContext`. |

## Reading and writing: `GlueContext`

| API | Outcome | Notes |
|---|---|---|
| `create_dynamic_frame.from_options(connection_type="s3", ...)` | Real | `connection_options["paths"]` (or `path`) mapped to local files. |
| `create_data_frame.from_options(...)` | Real | Same, returns a `DataFrame`. |
| `create_dynamic_frame.from_catalog(database, table_name)` | Real | Through the [catalog map](running-jobs.md#catalog-tables). `push_down_predicate` and `catalog_id` are ignored. |
| `write_dynamic_frame.from_options(frame, connection_type="s3", ...)` | Real | Appends. `partitionKeys` → `partitionBy`. |
| `write_dynamic_frame.from_catalog(frame, database, table_name)` | Real | Path and format from the map; `options.partitionKeys`. |
| Flat spellings (`create_dynamic_frame_from_options`, `write_dynamic_frame_from_catalog`, ...) | Real | |
| `connection_type` other than `s3` | Unsupported | |
| `connection_options` other than `paths`, `path`, `partitionKeys` | Ignored | `WARN` per option (`recurse`, `exclusions`, `groupFiles`, ...). |
| `glueContext.spark_session`, `.sparkSession` | Real | |
| `glueContext.sql`, `.read`, `.table`, ... | Real | Any `SparkSession` attribute is delegated to the session. |
| `glueContext.get_logger()` | Real | A `logging.Logger` with `warn`. |
| `getSink(...)`, `getSource(...)`, `purge_s3_path`, `purge_table`, `transition_s3_path`, ... | Stub | **`getSink(...).writeFrame(frame)` writes nothing** in non-strict mode. Use `write_dynamic_frame.from_options` or strict mode to catch it. |

Formats: `json`, `csv`, `parquet`, `glueparquet`, `delta`, `iceberg` are
real. `orc` is unsupported (Sail has no ORC reader or writer). Unknown
formats are handed to Sail with a `WARN`. Glue format option names are
translated; see the [configuration reference](configuration.md#format-names-and-options).

## `DynamicFrame`

A local `DynamicFrame` is a thin wrapper around a Spark `DataFrame`. Its
schema is the DataFrame's schema: there are **no choice types**, so a column
that has mixed types in the input gets whatever type Spark infers (often
`string`), not a `choice`.

| API | Outcome | Notes |
|---|---|---|
| `DynamicFrame(df, glue_ctx, name)`, `fromDF`, `toDF` | Real | |
| `count`, `show`, `printSchema`, `schema()` | Real | `schema()` returns a Spark `StructType`, not a Glue one. |
| `select_fields`, `drop_fields`, `rename_field` | Real | Dotted paths for nested fields; backticks for names containing a dot. |
| `apply_mapping` | Real | 4-tuples or 3-tuples. Casts to Glue type names; `struct`/`array`/`map` targets aren't cast. Unmapped fields are dropped. `case_sensitive` is ignored. |
| `resolveChoice(specs=[(path, "cast:<type>")])` | Real | |
| `resolveChoice` with `make_struct`, `make_cols`, `project:`, `match_catalog`, or `choice=` | Pass-through | Nothing to resolve without choice types. |
| `join(paths1, paths2, frame2)` | Real | Inner equi-join. Same key names appear once; different ones are both kept. |
| `repartition`, `coalesce` | Real | |
| `filter(f)`, `map(f)` | Pass-through | **Rows are not filtered or changed.** |
| `errorsCount`, `stageErrorsCount` | Real | Always `0`: there are no error records locally. |
| Anything else (`relationalize`, `unnest`, `spigot`, `unbox`, ...) | Stub | |
| `DynamicFrameCollection`: `keys`, `values`, `select`, `map`, `[key]`, `len` | Real | |

`awsglue.gluetypes` provides the type classes so imports and simple schema
construction work; prytt doesn't use them for typing.

## Transforms: `awsglue.transforms`

| Transform | Outcome | Local result |
|---|---|---|
| `ApplyMapping`, `SelectFields`, `DropFields`, `RenameField`, `Join` | Real | Same as the `DynamicFrame` methods. |
| `DropNullFields` | Real | Drops fields typed `NullType` (top level, in structs, in structs inside arrays), as Glue does. A typed column that happens to be all null is kept. Maps aren't walked. |
| `ResolveChoice` | Real for `cast:` | As `resolveChoice`. |
| `SelectFromCollection` | Real | |
| `Filter`, `Map` | Pass-through | Input unchanged. |
| `Unbox`, `UnnestFrame`, `Spigot` | Pass-through | Input unchanged. `Spigot` writes no sample. |
| `Relationalize` | Pass-through | A collection holding the input frame under the root name only; no child tables. |
| `SplitFields`, `SplitRows` | Pass-through | A collection with **both** names holding the whole input frame. |
| `ErrorsAsDynamicFrame` | Pass-through | An empty frame. |
| `FlatMap`, `MapToCollection`, `FillMissingValues`, `FindMatches`, `FindIncrementalMatches` | Stub | Input returned. |

Every transform also supports `Transform()(frame=...)` and `describe()`.

## Data quality: `awsgluedq`

`EvaluateDataQuality.apply(frame, ruleset, publishing_options)` and
`EvaluateDataQuality().process_rows(...)` load and run but **evaluate no
rules**. `apply` returns the input; `process_rows` returns a collection whose
`originalData` and `rowLevelOutcomes` are the input frame, with no outcome
columns added. A script that selects `DataQualityEvaluationResult` columns
from `rowLevelOutcomes` fails with Spark's "cannot resolve" error.

## S3 and paths

- Only `GlueContext` readers and writers and catalog `path`s are mapped.
  `spark.read...load("s3://...")` and SQL `LOCATION 's3://...'` are passed
  to Sail unchanged and fail in `local` mode (Sail has no credentials or the
  bucket doesn't exist). `df.write...save("s3://...")` is blocked by the
  [S3 write lock](#s3-write-lock).
- Writes append. Glue's sinks append too, but a local re-run sees the
  previous run's output; delete the target directory between runs.
- There is no S3 consistency, listing or permission emulation. A path that
  doesn't exist fails at the first action on the frame (Spark Connect is
  lazy), not at the `from_options` call.
- `passthrough` mode is best-effort: tested only with anonymous public
  reads. No region is required (Sail discovers each bucket's), and the AWS
  profile comes from `s3.profile`, one per process.

## S3 write lock

prytt refuses writes to real S3 by raising `PryttS3WriteBlocked` (a
`PermissionError`), in every `s3.mode` and even with strict mode off. Unlock
with `PRYTT_ALLOW_S3_WRITES=1`; see
[Configuration](configuration.md#s3-write-lock-prytt_allow_s3_writes).

| Write | Blocked when |
|---|---|
| `df.write.save(path)` and the shortcuts `parquet`, `json`, `csv`, `text`, `xml`, `orc` (also `.option("path", ...)`) | `path` is `s3://`, `s3a://` or `s3n://`, in any mode. These paths are never mapped, so in `local` mode the error suggests a local path or a Glue writer. |
| `write_dynamic_frame.from_options`, `write_dynamic_frame.from_catalog` (and `write_data_frame`) | The target is still `s3://` after mapping: `from_options` in `s3.mode = "passthrough"`, `from_catalog` to a passthrough table (its own `mode`, else `s3.mode`; so a table with `mode = "passthrough"` is blocked even in `local` mode). Writes mapped under `s3.root` are never blocked. |
| `iceberg.warehouse` | Set to an `s3://` URI: a config error when the session starts. |

Not covered yet: `saveAsTable`, `insertInto`, `DataFrameWriterV2`
(`writeTo(...)`) and SQL writes (`INSERT`, `CREATE TABLE ... AS SELECT`,
`MERGE`, ...) to S3 locations. Never covered: `boto3`, the AWS CLI, and side
effects inside `mapInPandas` or UDFs. Reads are never blocked.

## Catalog tables in SQL

`spark.sql` and `spark.table` resolve mapped `db.table` names. The limits:

- **Registered once per session.** Tables are registered when the session is
  created. A mapped table with no data at that point is skipped with a
  `WARN` and stays unresolvable for that session, even after the job writes
  it. `from_catalog` reads are not affected.
- **No SQL writes to mapped tables.** `INSERT INTO raw_db.orders` and
  `df.write.saveAsTable("raw_db.orders")` are not mapped. Use
  `write_dynamic_frame.from_catalog`.
- **No three-part names.** `glue_catalog.raw_db.orders` is not a mapped
  table; `glue_catalog` belongs to Iceberg.
- **`on_missing = "empty"` doesn't apply to SQL.** An unmapped name in SQL
  always errors, with the TOML hint as an exception note.
- **A script can't re-create a mapped name.** Its own
  `CREATE DATABASE raw_db` / `CREATE TABLE raw_db.orders` fails with Spark's
  "already exists" error. `IF NOT EXISTS` forms are fine.
- **Name rewriting is a heuristic.** Sail 0.7.3 treats table names
  case-sensitively; prytt rewrites mapped names in table positions (after
  `FROM`, `JOIN`, `TABLE`, `INTO`, `UPDATE`, `USING`, or in a `FROM` list)
  to their configured spelling. Names in other positions (for example
  `DESCRIBE raw_db.orders`; `DESCRIBE TABLE raw_db.orders` is fine) aren't
  rewritten, so they must match the configured case exactly.
- **Hint limits.** The unmapped-table hint is matched by name only, so a
  table the script created and then dropped, or a typo in a database the
  script owns, also gets it. When two unmapped names share a table name
  (`a.orders`, `b.orders`) there is no hint, because Sail's error names only
  the last part.
- **Eager error.** On Glue a missing table fails at `spark.sql(...)`. Over
  Spark Connect it would fail only at the action; prytt forces analysis for
  queries that name an unmapped two-part table, so the error comes at the
  call, as on Glue. Other errors still surface at the action.
- **External server.** With `SPARK_REMOTE` set, tables are registered with
  `IF NOT EXISTS`, so a persistent catalog keeps the old location if you
  change a table's `path` (untested).
- A passthrough table (`s3.mode = "passthrough"` or the table's own
  `mode = "passthrough"`) with no data at its `s3://` location registers as
  an empty table with no columns (unverified against real S3).

## Iceberg

- Only the `glue_catalog` name is wired up, as an in-memory Sail catalog
  rebuilt from the warehouse on disk each session.
- `LOCATION 's3://...'` in SQL is not mapped.
- A database or table with an explicit `LOCATION` outside
  `<warehouse>/<db>/<tbl>` works in that run but is not re-registered in the
  next. prytt logs a `WARN` when a script's `CREATE DATABASE` sets its own
  location, or when it can't add one (several statements in one `spark.sql`
  string, or the `CREATE` isn't the first statement).
- `MERGE INTO` / `INSERT` on a table that doesn't exist yet fails; create it
  once.
- No Glue Data Catalog features beyond names: no table properties from AWS,
  no Lake Formation permissions, no snapshot expiry jobs.
- With an external server, that server needs its own `glue_catalog`.

## Spark itself

Everything above runs on [Sail](https://github.com/lakehq/sail) 0.7.x over
Spark Connect, not on Apache Spark. Differences that show up in Glue jobs:

- **No JVM.** No Java UDFs, no JDBC drivers, no Hadoop configuration, no
  `spark.jars`, no Glue connectors.
- **Table, database and temp view names are case-sensitive** in Sail 0.7.3,
  even with `spark.sql.caseSensitive=false`. prytt corrects mapped names
  only.
- **Lazy errors.** A bad path, a missing table or a bad column surfaces at
  the first action (`count`, `show`, `write`), not where the frame was
  defined.
- **Not every Spark SQL function or option is implemented in Sail.**
  Unsupported ones fail with Sail's error. Check Sail's documentation for
  coverage.
- Python UDFs, `pandas_udf`, `mapInPandas`, `mapInArrow` and
  `applyInPandas` work. Under pandas 3, `NULL` strings reach `mapInPandas`
  as `NaN`, and an exception inside a worker arrives as an empty
  `PythonException()`.
- Performance is that of a single local process. It is not a guide to
  Glue DPU sizing.

## Not emulated at all

Job bookmarks, Glue workflows and triggers, crawlers, the Glue Data Catalog
API (`boto3` calls go to real AWS if credentials exist), Glue connections,
Lake Formation, CloudWatch metrics and the Spark UI, continuous logging,
auto-scaling, and job run IDs. Calls through `boto3` are not intercepted.
