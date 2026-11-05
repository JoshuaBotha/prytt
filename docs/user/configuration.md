# Configuration reference

prytt needs no configuration for the default setup. Everything here is
optional.

## Sources and precedence

Settings come from these sources, highest priority first:

1. `PRYTT_*` environment variables
2. `prytt.toml` (top-level keys), the nearest one searched upward from the
   current directory
3. `[tool.prytt]` in the nearest `pyproject.toml`, searched upward from the
   current directory
4. defaults

Sources override **per key**, not per table: `PRYTT_S3_ROOT` replaces
`s3.root` but keeps `s3.mode` from the file. Catalog entries and job
arguments merge per name in the same order.

Config is read once per process, on first use, and cached. Bad values (an
unknown key, a bad boolean, an unknown `s3.mode` or table `mode`) raise `ValueError` when it
loads, which is at the first Spark or `awsglue` call that needs it.

The same settings in both file forms:

```toml
# pyproject.toml
[tool.prytt]
log_level = "INFO"

[tool.prytt.s3]
root = "local-data/s3"

[tool.prytt.catalog]
on_missing = "empty"

[tool.prytt.catalog."raw_db.orders"]
path = "s3://raw-bucket/orders/"
format = "json"

[tool.prytt.args]
JOB_NAME = "local"
```

```toml
# prytt.toml
log_level = "INFO"

[s3]
root = "local-data/s3"

[catalog]
on_missing = "empty"

[catalog."raw_db.orders"]
path = "s3://raw-bucket/orders/"
format = "json"

[args]
JOB_NAME = "local"
```

Use `prytt.toml` when the job scripts don't live in a Python project, or to
keep local-only settings out of `pyproject.toml` (and out of git, if you
ignore it).

## Settings

| Key | Env | Default | Values |
|---|---|---|---|
| `disable` | `PRYTT_DISABLE` | `false` | Boolean. Skip the import hook. Read from the **env var only**, at interpreter start; the file key is accepted but has no effect. See [Install](install.md#turn-it-off-or-remove-it). |
| `log_level` | `PRYTT_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`. `DEBUG` also leaves Sail's `RUST_LOG` alone. |
| `strict` | `PRYTT_STRICT` | `true` if `CI` is `1`/`true`/`yes`, else `false` | Boolean. See [Strict mode](running-jobs.md#strict-mode). An explicit value from any source beats the `CI` default. |
| `sail.port` | `PRYTT_SAIL_PORT` | `0` | Integer. Port for prytt's Sail server on `127.0.0.1`; `0` picks a free one. |
| `s3.mode` | `PRYTT_S3_MODE` | `local` | `local` (map to `s3.root`) or `passthrough` (read real S3; best-effort). A catalog table's own [`mode`](#catalog-table-entries) overrides it for that table. See [S3 and AWS](s3-and-aws.md). |
| `s3.root` | `PRYTT_S3_ROOT` | `./.prytt/s3` | Path, relative to the current directory unless absolute. |
| `s3.profile` | `PRYTT_S3_PROFILE` | none | AWS profile name Sail uses for S3, exported as `AWS_PROFILE`. An empty `PRYTT_S3_PROFILE` unsets a file value. See [AWS profile](#aws-profile-s3profile). |
| `catalog.on_missing` | `PRYTT_CATALOG_ON_MISSING` | `error` | `error` or `empty`. Applies to `from_catalog` reads only. |
| `catalog."db.table"` | | none | Table mapping; see below. |
| `iceberg.warehouse` | `PRYTT_ICEBERG_WAREHOUSE` | `./.prytt/warehouse` | Path, made absolute against the current directory. An `s3://` URI is a config error unless `PRYTT_ALLOW_S3_WRITES` unlocks S3 writes. |
| `args.<name>` | `PRYTT_ARG_<name>` | none | Job argument for `getResolvedOptions`; see [Job arguments](running-jobs.md#job-arguments). |

Booleans accept `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off`/empty
(case-insensitive), or TOML `true`/`false`.

Unknown keys are an error, so a typo fails loudly rather than being ignored.

## S3 write lock: `PRYTT_ALLOW_S3_WRITES`

By default prytt refuses every write to real S3 it can detect, in every
`s3.mode`, by raising `prytt.PryttS3WriteBlocked` (a `PermissionError`),
even with strict mode off. The only unlock is the environment variable
`PRYTT_ALLOW_S3_WRITES` set to `1` or `true` (any case); any other value
keeps the lock on. There is deliberately **no config-file key**, and the
pytest plugin's fixtures always run locked.

```bash
PRYTT_ALLOW_S3_WRITES=1 python my_job.py
```

See [S3 and AWS: the S3 write lock](s3-and-aws.md#the-s3-write-lock) for
why, the unlock `WARN`, and the known gaps, and
[Compatibility](compatibility.md#s3-write-lock) for every write it covers.

## AWS profile: `s3.profile`

```toml
[tool.prytt.s3]
profile = "my-sso-profile"   # or PRYTT_S3_PROFILE=my-sso-profile for one run
```

The AWS profile Sail uses for S3; any profile type works (SSO, assume-role,
`credential_process`, static keys in the credentials file). Whenever it is
set, prytt exports it as `AWS_PROFILE` before Sail starts, in every mode,
overriding a different shell `AWS_PROFILE` with a `WARN`. Without it, prytt
leaves every `AWS_*` variable alone. No region is needed.

When passthrough is in use (`s3.mode` or any mapped table's effective
`mode` is `passthrough`), session start also:

- raises `ValueError` if `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY` is
  in the environment, since the AWS chain would prefer them over the
  profile;
- for an SSO profile, raises `RuntimeError` with
  `run: aws sso login --profile <profile>` if its cached token is missing or
  expired (a `WARN` instead if prytt can't read the cache, or if the token
  is expired but the AWS SDK can refresh it).

One AWS profile per process: a later session start that resolves a
different `s3.profile` (or none, after one) raises `RuntimeError`.

[S3 and AWS: AWS credentials](s3-and-aws.md#aws-credentials) explains each
rule, with an SSO walkthrough and the exact log lines and errors.

## Catalog table entries

```toml
[tool.prytt.catalog."<database>.<table>"]
path = "s3://bucket/prefix/"   # required: s3:// URI (mapped) or local path
format = "parquet"             # required
options = { ... }              # optional: format options
mode = "passthrough"           # optional: "local" or "passthrough"; default s3.mode
```

- The key must be `"database.table"` with both parts non-empty. Quote it in
  TOML because it contains a dot.
- Allowed keys: `path`, `format`, `options`, `mode`. Anything else is an
  error when the config loads.
- `mode` sets this table's S3 mode: `"local"` makes it a
  [local table](../../GLOSSARY.md#language) (its `s3://` path maps under
  `s3.root`), `"passthrough"` a
  [passthrough table](../../GLOSSARY.md#language) (read from real S3, with no
  local "no data yet" check; writes to it are blocked by the
  [S3 write lock](#s3-write-lock-prytt_allow_s3_writes)). Without it the
  table follows the global `s3.mode` from whatever source; an explicit
  `mode` wins over `PRYTT_S3_MODE`, and there is no per-table env var. It
  applies to everything that names the table (`spark.sql`, `spark.table`,
  `from_catalog` reads and writes), not to `from_options` or plain Spark
  paths. A bad value (`"passthru"`) fails when the config loads. The `MAP`
  line for each registered table ends with `mode=<mode> (table|global)`.
  See [S3 and AWS: per-table mode](s3-and-aws.md#per-table-mode).
- `options` must be a table. `partitionKeys` in `options` is used by
  `write_dynamic_frame.from_catalog`; every other option is a format option.
- Lookup is exact first, then case-insensitive.

## Format names and options

Glue format names map to Sail data sources:

| Glue `format` | Sail | Note |
|---|---|---|
| `json` | `json` | |
| `csv` | `csv` | Reads infer column types (`inferSchema` defaults to true), as Glue does. |
| `parquet`, `glueparquet` | `parquet` | |
| `delta` | `delta` | |
| `iceberg` | `iceberg` | |
| `orc` | — | Raises `GlueLocalUnsupported`: Sail has no ORC data source. |
| anything else | as given | Logged `WARN`; Sail decides. |

Format option names (case-insensitive) are translated from Glue to Spark:

| Glue option | Spark option |
|---|---|
| `withHeader`, `writeHeader` | `header` |
| `separator` | `sep` |
| `quoteChar` | `quote` |
| `escaper` | `escape` |
| `multiline` | `multiLine` |

Glue-only options with no Spark meaning are dropped with a `WARN`:
`useGlueParquetWriter`, `blockSize`, `pageSize`, `jsonPath`,
`optimizePerformance`, `strictCheckForQuoting`, `skipFirst`, `groupFiles`,
`groupSize`.

Spark option names (`header`, `sep`, `quote`, `escape`, `multiLine`,
`inferSchema`, `delimiter`, `encoding`, `schema`) pass through silently. Any
other option passes through to Sail with an `INFO` line.

`connection_options` other than `paths`, `path` and `partitionKeys` (for
example `recurse`, `groupFiles`, `compressionType` given as a connection
option) are ignored with a `WARN`.

## Environment variables prytt reads or sets

| Variable | Read / set | Meaning |
|---|---|---|
| `PRYTT_*` | read | The settings above. |
| `PRYTT_ALLOW_S3_WRITES` | read | `1`/`true` unlocks the [S3 write lock](#s3-write-lock-prytt_allow_s3_writes). Env only; no config key. Removed by the pytest plugin's fixtures. |
| `CI` | read | Truthy (`1`/`true`/`yes`) turns on strict mode by default. |
| `SPARK_REMOTE` | read, set | If set before Spark is first used, prytt connects there and starts no server. Otherwise prytt sets it to its own Sail server and clears it at exit. |
| `RUST_LOG` | read, set | Sail's log filter. prytt sets `error` unless it is already set or `log_level` is `DEBUG`. |
| `SAIL_CATALOG__LIST`, `SAIL_CATALOG__DEFAULT_CATALOG` | read, set | Sail's catalogs. prytt declares `sail` (default) and `glue_catalog` unless already set. Not touched for an external server. |
| `AWS_PROFILE` | read, set | Set to `s3.profile` before Sail starts, when that is set (a different shell value is overridden with a `WARN`). Otherwise left alone. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | read | With `s3.profile` set and passthrough in use, either one is an error ([AWS profile](#aws-profile-s3profile)). |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | read | Only for the region `INFO` line when passthrough is in use; no region is required. |
| `AWS_CONFIG_FILE`, `HOME` | read | Where prytt finds the AWS config file (region line, SSO profile detection) and the AWS CLI SSO token cache for the [SSO check](#aws-profile-s3profile). |

prytt never reads credentials itself (the SSO check only reads a token's expiry); Sail does, through the AWS credential
chain, from the `AWS_*` variables in the process.

Any other `SAIL_*` variable you set reaches Sail unchanged, because the
server runs in the same process.
