# Troubleshooting

Symptoms, their usual cause, and the fix. Start by reading prytt's own lines
on stderr (`[prytt] ...`): most problems show up there as a `WARN`, `STUB` or
`MAP` line before the traceback.

## Start-up

### The first run sits for about 30 seconds

Sail loads its native library on the first run after install (or after a
`pysail` upgrade), most noticeably on macOS. Later runs start in under a
second. Wait it out once, or warm it:

```bash
python -c "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().range(1).collect()"
```

### `ModuleNotFoundError: No module named 'pyspark.context'`

prytt's import hook is not active. Check, in order:

1. `PRYTT_DISABLE` is not set to a truthy value.
2. You are running the Python from the environment prytt is installed in
   (`which python`, or use `uv run python ...`).
3. `prytt.pth` exists at the site-packages root:

   ```bash
   python -c "import sysconfig, pathlib; print((pathlib.Path(sysconfig.get_path('purelib')) / 'prytt.pth').exists())"
   ```

   If it prints `False`, reinstall prytt.
4. Python is not started with `-S`, which skips `.pth` files.

### `[CONNECT_URL_NOT_SET] Cannot create a Spark Connect session`

Same cause as above: the hook didn't run, so nothing started Sail. If you
disabled prytt on purpose, set `SPARK_REMOTE` to a Spark Connect server.

### prytt connects to the wrong server, or doesn't start Sail

The `INFO` line says `using external server SPARK_REMOTE=...`. Something set
`SPARK_REMOTE` in your environment (a shell profile, an IDE run
configuration, a `.env` file). Unset it:

```bash
unset SPARK_REMOTE
```

In a mise project, `[env] SPARK_REMOTE = false` unsets it for every task.

### `requested Python version does not satisfy Python>=3.13`

Your project allows a Python older than 3.13. Pin it: see
[Install](install.md#add-prytt-to-a-project).

### `import awsglue` gives a different package

Another `awsglue` distribution is installed in the same environment
(`AWSGlueDataplanePython`, `awsglue-local`, the Glue Docker libs). Remove it
and reinstall prytt. Check with:

```bash
python -c "import awsglue; print(awsglue.__file__)"
```

## Data and paths

### The job reads no rows, or fails with "path does not exist"

Look at the `MAP` line for that read: it shows the local directory prytt
looked in. Usual causes:

- Files are not under `.prytt/s3/<bucket>/<key>` relative to the directory
  you ran from. Run from the project root, or set `s3.root` to an absolute
  path.
- The script reads with plain Spark (`spark.read.json("s3://...")`), which
  prytt doesn't map. Only `GlueContext` readers and catalog paths are
  mapped; see [S3 paths](running-jobs.md#s3-paths).

Errors from a bad path appear at the first action (`count`, `show`, write),
not at the read call: Spark Connect is lazy.

### Output has twice as many rows after a second run

Writes append, as Glue's sinks do. Delete the target directory under
`.prytt/s3/` between runs.

### `ValueError: prytt config: ...`

A config file has an unknown key, a bad boolean, or an `s3.mode` /
`catalog.on_missing` value outside the allowed ones. The message names the
key and the file. See the [configuration reference](configuration.md).

### `ValueError: prytt config: AWS_ACCESS_KEY_ID ... set in the environment with s3.profile`

Your shell has static AWS keys and `s3.profile` is set while passthrough is
in use. The AWS chain would use the keys and ignore the profile. Unset
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or drop `s3.profile` (and
`PRYTT_S3_PROFILE`). See [Static keys shadow a profile](s3-and-aws.md#static-keys-shadow-a-profile).

### `RuntimeError: prytt: one AWS profile per process`

The resolved `s3.profile` changed after the first session in this process
(for example `PRYTT_S3_PROFILE` plus `config.reset()` in a test, or a
notebook cell). Sail loads AWS config once per process. Set the profile once
before the process starts (`PRYTT_S3_PROFILE` in the shell, `mise.toml` or
CI; per test run, not per test), or restart the process or kernel. See
[One AWS profile per process](s3-and-aws.md#one-aws-profile-per-process).

### Passthrough mode hangs for a second, then fails with a credentials error

Sail tried every credential source and found none (the last one, IMDS,
times out after about 1 s). Log in (`aws sso login`, or export credentials)
or, for public buckets, set `AWS_SKIP_SIGNATURE=true`. Passthrough is
best-effort and only verified for anonymous public reads.

### `GlueLocalUnsupported: ORC is not supported locally`

Sail has no ORC data source. Convert the local sample data to Parquet and
map the table with `format = "parquet"`.

## Catalog tables

### `CatalogTableNotFound: prytt: catalog table db.tbl is not mapped`

Add the TOML the message prints to `pyproject.toml` (or `prytt.toml`), or
set `catalog.on_missing = "empty"` to read unmapped tables as empty frames.

### `spark.sql("... FROM raw_db.orders")` says table or view not found

- **Not mapped:** the traceback ends with the TOML to add. In code that
  catches the exception, the hint is in `e.__notes__`, not `str(e)`.
- **Mapped but skipped:** look for
  `WARN catalog table raw_db.orders skipped: no data at ... yet` at session
  start. The table had no data when the session was created. Put data there
  before the run, or read it with `from_catalog`, which isn't affected.
- **Name in an unusual position:** prytt only rewrites names after `FROM`,
  `JOIN`, `TABLE`, `INTO`, `UPDATE`, `USING` or in a `FROM` list. Elsewhere
  the name must match the configured case exactly, because Sail is
  case-sensitive.

### `CREATE TABLE raw_db.orders` fails with "already exists"

prytt already registered that mapped table. Use `CREATE TABLE IF NOT EXISTS`
or drop the mapping.

## Iceberg

### `glue_catalog.db.tbl` not found on the second run

The table was created with an explicit `LOCATION` outside
`<warehouse>/<db>/<tbl>`, or its database was created in a multi-statement
`spark.sql` string. prytt logged a `WARN` about it in the first run. Let
prytt choose the location (no `LOCATION` clause), or create the database in
its own `spark.sql` call.

### `MERGE INTO` fails because the table doesn't exist

Iceberg tables must be created once. Keep `CREATE TABLE IF NOT EXISTS ...
USING iceberg` in the job.

### `WARN glue_catalog not available on this server`

You set `SAIL_CATALOG__LIST` without a `glue_catalog` entry, or you are
using an external server. Remove your `SAIL_CATALOG__LIST`, or add
`{name="glue_catalog", type="memory", initial_database=["default"]}` to it.

### `Iceberg table location must be an absolute path`

`spark.sql.warehouse.dir` was overridden with a relative path. Remove the
override; prytt sets it to the absolute `iceberg.warehouse`.

## Strict mode and CI

### `GlueLocalUnsupported: X is not implemented in prytt (strict mode ...)`

The job calls something prytt doesn't emulate, and strict mode is on (it is
when `CI` is set). Either change the job, set `PRYTT_STRICT=0` to log and
continue, or check [Compatibility and limits](compatibility.md) for an
alternative.

### `GlueArgumentError: the following arguments are required: --X`

Strict mode, and the argument is missing from the command line, env and
config. Pass it, or set `PRYTT_ARG_X` or `[args] X = "..."`.

## Hangs

### Tests hang with 0% CPU

A `SparkSession` created with `builder.create()` was dropped without
`stop()`, and the garbage collector closed it while another session was
mid-query. prytt patches this, but stop extra sessions explicitly; see
[Testing](testing.md#extra-sparksessions). Configure `faulthandler_timeout`
so a hang dumps every thread's stack.

## Getting more detail

| Want | Set |
|---|---|
| Everything prytt does | `PRYTT_LOG_LEVEL=DEBUG` |
| Sail's own logs | `RUST_LOG=info` (or `debug`) |
| Only problems | `PRYTT_LOG_LEVEL=WARNING` |
| Fail on every gap | `PRYTT_STRICT=1` |
| Run without prytt | `PRYTT_DISABLE=1` |

When reporting a bug, include the first `[prytt] INFO` line (it has the
prytt, pysail and pyspark-client versions), the OS, and a minimal script.
