# S3 and AWS

How a local run treats `s3://` paths, how to read some or all of your data
from real S3, what stops a local run from writing to S3, and how prytt picks
the AWS identity. The exact keys and env vars are in the
[Configuration reference](configuration.md); this page explains how they fit
together.

Terms, from the [glossary](../../GLOSSARY.md#language):

- **Mapped table**: a `[tool.prytt.catalog."db.table"]` entry.
- **Local table**: a mapped table read from and written to a directory under
  `s3.root` on your disk.
- **Passthrough table**: a mapped table read from its real `s3://` location.
- **S3 write lock**: prytt's refusal of every write it can detect that would
  reach real S3, in every mode.

## Pick a setup

| I want to... | Set |
|---|---|
| Run everything on local files (the default) | Nothing. `s3.mode` is `local`; put input files under `.prytt/s3/<bucket>/<key>`. |
| Read one real table from S3, keep the rest local | `mode = "passthrough"` on that mapped table; leave `s3.mode = "local"`. See [Per-table mode](#per-table-mode). |
| Read everything from real S3 | `s3.mode = "passthrough"` (or `PRYTT_S3_MODE=passthrough` for one run). Writes are still blocked. |
| Read everything real but keep one input on local fixtures | `s3.mode = "passthrough"` and `mode = "local"` on that mapped table. |
| Deliberately write to real S3 | `PRYTT_ALLOW_S3_WRITES=1` in the environment of that one command. There is no config key. See [The S3 write lock](#the-s3-write-lock). |
| Use an SSO (IAM Identity Center) profile | `s3.profile = "<profile>"` (or `PRYTT_S3_PROFILE`), then `aws sso login --profile <profile>`. See [Worked example: SSO](#worked-example-sso). |

## Local mode and passthrough mode

`s3.mode` (env `PRYTT_S3_MODE`) is the global S3 mode:

- **`local`** (default): prytt rewrites `s3://bucket/key` (also `s3a://`,
  `s3n://`) to `<s3.root>/bucket/key` on your disk, `./.prytt/s3` by default.
  Nothing prytt maps touches AWS.
- **`passthrough`**: prytt leaves the URI alone and Sail reads it from real
  S3 with the AWS credential chain. Passthrough is **best-effort**: only
  anonymous public reads have been verified; other credential setups,
  SSO included, have not been run against real AWS yet.

What each kind of call does in each mode:

| Call | `local` | `passthrough` |
|---|---|---|
| Catalog tables: `spark.sql("... FROM db.table")`, `spark.table`, `create_dynamic_frame.from_catalog` | Reads the mapped directory under `s3.root` (a local table). | Reads the real `s3://` path (a passthrough table). |
| `write_dynamic_frame.from_catalog` | Writes under `s3.root`. | Blocked by the S3 write lock. |
| `create_dynamic_frame.from_options` (and `create_data_frame`) | Reads under `s3.root`. | Reads real S3. |
| `write_dynamic_frame.from_options` (and `write_data_frame`) | Writes under `s3.root`. | Blocked by the S3 write lock. |
| Plain Spark reads: `spark.read.parquet("s3://...")`, SQL `LOCATION 's3://...'` | Not mapped: the URI reaches Sail unchanged, so it reads real S3 if the process has credentials and fails if not. | The same: Sail reads real S3. |
| Plain Spark and SQL writes to `s3://` | Blocked by the S3 write lock. | Blocked by the S3 write lock. |

Catalog tables follow their own [per-table mode](#per-table-mode);
`from_options` and plain Spark paths always follow the global `s3.mode`.

Every rewrite to local disk logs a `MAP` line, and every registered table
logs one `MAP` line with its effective mode and where that came from:

```
[prytt] MAP  s3://raw-bucket/orders/ -> /home/me/proj/.prytt/s3/raw-bucket/orders/
[prytt] MAP  raw_db.orders -> /home/me/proj/.prytt/s3/raw-bucket/orders/ (table raw_db.orders, view raw_db_orders, mode=local (global))
[prytt] MAP  ref_db.prices -> s3://gold-bucket/prices/ (table ref_db.prices, view ref_db_prices, mode=passthrough (table))
```

## Per-table mode

A mapped table can set its own `mode`, `"local"` or `"passthrough"`:

```toml
# pyproject.toml (in prytt.toml, drop "tool.prytt.")
[tool.prytt.s3]
mode = "local"                         # the global default; also the built-in default

[tool.prytt.catalog."ref_db.prices"]   # read from real S3
path = "s3://gold-bucket/prices/"
format = "parquet"
mode = "passthrough"

[tool.prytt.catalog."raw_db.orders"]   # no mode: follows s3.mode, so local
path = "s3://raw-bucket/orders/"
format = "json"
```

- A table's **effective mode** is its own `mode` if set, else the global
  `s3.mode` from whatever source (file or `PRYTT_S3_MODE`).
- An explicit table `mode` wins over `PRYTT_S3_MODE`, so flipping the global
  mode from your shell never changes a table you pinned. There is no
  per-table env var.
- Keep the table's real `s3://` URI as `path` in both modes; the mode decides
  whether it is mapped under `s3.root` or read as is.
- A passthrough table is registered without prytt's local "no data yet"
  check. Partitioned data (`year=2026/month=11/` directories) under its path
  shows up as partition columns, as it does for a local table.
- A bad value such as `"passthru"` fails when the config loads, naming the
  allowed values.

## The S3 write lock

The S3 write lock is **on by default, in every mode**. A write prytt can
detect that would reach real S3 raises `prytt.PryttS3WriteBlocked` (a
`PermissionError`) before Sail is called, even with strict mode off:

```
prytt.s3lock.PryttS3WriteBlocked: prytt: write to s3://out-bucket/report/ blocked by the S3 write lock (it would write to real S3). Set PRYTT_ALLOW_S3_WRITES=1 in the environment to allow writes to S3. prytt does not map plain Spark paths to s3.root in s3.mode = 'local': write to a local path, or use glueContext.write_dynamic_frame.from_options to write under s3.root.
```

Why it exists: plain Spark writes are never mapped, even in `local` mode. If
your shell has AWS credentials, which is normal when you also deploy the
job, a "local" run could otherwise overwrite production data.

**What it covers** (the full table is in
[Compatibility: S3 write lock](compatibility.md#s3-write-lock)):

- `df.write.save(...)` and its shortcuts (`parquet`, `csv`, ...) to an
  `s3://`, `s3a://` or `s3n://` path.
- Glue writers whose target is still `s3://` after mapping:
  `from_options` in passthrough mode, `from_catalog` to a passthrough table.
- `saveAsTable`, `insertInto` and `writeTo(...)` into a passthrough table, a
  table this session created at an S3 location, or with an S3 `path`.
- `spark.sql` writes (`INSERT`, `CREATE TABLE ... AS SELECT`,
  `REPLACE TABLE ... AS`, `MERGE`, `UPDATE`, `DELETE`, `TRUNCATE`) to an S3
  location, a passthrough table or a table this session created at an S3
  location. A bare `CREATE TABLE ... LOCATION 's3://...'` without `AS` is
  allowed and remembered, so a later `INSERT` into it is blocked.
- An `iceberg.warehouse` on `s3://`: a config error at session start.

Writes prytt maps to local disk (Glue writers in `local` mode, local tables),
`DROP TABLE` and every read are never blocked.

**Known gaps.** The lock covers every write path prytt knows about, not every
possible S3 write:

- `boto3`, the AWS CLI or any other client that talks to S3 directly.
- Side effects inside `mapInPandas`, `applyInPandas`, `foreach` or UDFs.
- SQL the detector misses. It reads the statement text heuristically and is
  not a SQL parser; see [Compatibility](compatibility.md#s3-write-lock) for
  examples.

**Unlocking.** Set `PRYTT_ALLOW_S3_WRITES` to `1` or `true` (any case) for
the one command that should write; any other value keeps the lock on:

```bash
PRYTT_ALLOW_S3_WRITES=1 python my_job.py
```

Each Spark session then logs:

```
[prytt] WARN S3 writes unlocked (PRYTT_ALLOW_S3_WRITES is set): this session can write to real S3
```

There is deliberately **no config key**: a value in `pyproject.toml` or
`prytt.toml` would be committed, and would unlock writes for everyone on the
team and in CI. The pytest plugin's fixtures remove `PRYTT_ALLOW_S3_WRITES`,
so tests run locked even if your shell sets it.

## AWS credentials

prytt never reads credentials. Sail runs in the same process and uses the
standard AWS credential chain (environment, profile, SSO, assume-role,
`credential_process`, web identity, ECS, IMDS). It reads only `AWS_*`
environment variables, and loads AWS config once per process.

### Choose the profile: `s3.profile`

```toml
[tool.prytt.s3]
profile = "team-sso"
```

`PRYTT_S3_PROFILE=team-sso` overrides it for one run; an empty
`PRYTT_S3_PROFILE` unsets a file value. Any profile type in your AWS config
works.

Whenever `s3.profile` is set, in every mode, prytt sets `AWS_PROFILE` in the
process before Sail starts and logs it:

```
[prytt] INFO AWS_PROFILE=team-sso (s3.profile)
```

If your shell had a different `AWS_PROFILE`, prytt overrides it and says so:

```
[prytt] WARN s3.profile 'team-sso' overrides AWS_PROFILE=personal from the environment
```

Without `s3.profile`, prytt leaves every `AWS_*` variable alone and the
default chain applies as usual (including a shell `AWS_PROFILE`).

The checks below run at session start only when **passthrough is in use**:
`s3.mode` is `passthrough`, or at least one mapped table's effective mode is.
A purely local run never depends on your AWS setup.

### Worked example: SSO

1. Create the profile once (`aws configure sso` writes this for you). In
   `~/.aws/config`:

   ```ini
   [profile team-sso]
   sso_session = my-org
   sso_account_id = 111122223333
   sso_role_name = DataEngineer
   region = eu-west-1

   [sso-session my-org]
   sso_start_url = https://my-org.awsapps.com/start
   sso_region = eu-west-1
   sso_registration_scopes = sso:account:access
   ```

2. Sign in. The token lands in the AWS CLI cache (`~/.aws/sso/cache`):

   ```bash
   aws sso login --profile team-sso
   ```

3. Point prytt at the profile and make the table you want to read a
   passthrough table. Here the data is partitioned by date
   (`s3://gold-bucket/prices/year=2026/month=11/part-0000.parquet`, ...):

   ```toml
   # pyproject.toml
   [tool.prytt.s3]
   profile = "team-sso"

   [tool.prytt.catalog."ref_db.prices"]
   path = "s3://gold-bucket/prices/"
   format = "parquet"
   mode = "passthrough"
   ```

4. Run the job (or this snippet):

   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession.builder.getOrCreate()
   spark.sql("SELECT year, month, count(*) FROM ref_db.prices GROUP BY year, month").show()
   ```

   The log shows the identity, the region and the table's mode:

   ```
   [prytt] INFO AWS_PROFILE=team-sso (s3.profile)
   [prytt] INFO S3 passthrough: region eu-west-1 (AWS profile team-sso)
   [prytt] MAP  ref_db.prices -> s3://gold-bucket/prices/ (table ref_db.prices, view ref_db_prices, mode=passthrough (table))
   ```

   `year` and `month` are columns of `ref_db.prices`, discovered from the
   directory names.

Because `s3.profile` is an SSO profile (its section has `sso_session`, or a
legacy `sso_start_url`), prytt checks for its token before Sail starts. A
missing or expired token stops the run with the command to fix it:

```
RuntimeError: prytt: no SSO token for AWS profile 'team-sso' (s3.profile); run: aws sso login --profile team-sso
RuntimeError: prytt: the SSO token for AWS profile 'team-sso' (s3.profile) expired at 2026-11-05T08:00:00+00:00; run: aws sso login --profile team-sso
```

An expired token on an `sso_session` profile that the AWS SDK can refresh
by itself (the cache entry has a refresh token and a client registration
that hasn't expired, as `aws sso login` writes) only logs a `WARN`, and Sail
refreshes it on first S3 access:

```
[prytt] WARN the SSO token for AWS profile 'team-sso' (s3.profile) expired at 2026-11-05T08:00:00+00:00; the AWS SDK will try to refresh it (if S3 access fails, run: aws sso login --profile team-sso)
```

Legacy `sso_start_url` profiles can't refresh, so an expired token there
always stops the run.

The cache layout belongs to the AWS CLI, so if prytt can't find the cache
directory or read or parse the token file (permission denied, for example)
it logs a `WARN` and carries on. The check only runs for the profile set
through `s3.profile`, and prytt never logs in or refreshes a token itself. prytt finds the AWS config through
`AWS_CONFIG_FILE` (default `~/.aws/config`) and the cache under `$HOME`.

### Static keys shadow a profile

The AWS chain prefers `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in the
environment over any profile. So with `s3.profile` set and passthrough in
use, either key in the environment stops the session start, rather than
letting the run silently use someone else's keys:

```
ValueError: prytt config: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY set in the environment with s3.profile 'team-sso' and S3 passthrough in use: the AWS credential chain prefers the keys and would ignore the profile. Unset AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or drop s3.profile (and PRYTT_S3_PROFILE).
```

Static keys without `s3.profile` are fine: the chain uses them as usual.

### One AWS profile per process

Sail loads AWS config once per process, at its first S3 access, and keeps
that identity. So prytt records the profile in effect at the first session
start, and "no `s3.profile`" counts as a choice too. A later session start in
the same process that resolves a different one raises:

```
RuntimeError: prytt: one AWS profile per process. This process started with no s3.profile, now the config resolves to s3.profile 'team-sso'. Sail loads AWS config once per process, so it would keep the first identity. To fix: set the profile once for the whole process, before it starts (for example PRYTT_S3_PROFILE in the shell, in mise.toml [env] or in the CI job, not per test), or restart the process (or notebook kernel) to switch profiles.
```

It happens when the config changes inside a running process: a notebook
cell, or a test that sets `PRYTT_S3_PROFILE` and calls `config.reset()`. Set
the profile before the process starts, or start a new process (restart the
kernel) to switch.

### Region

No region is needed: Sail discovers each bucket's region. When passthrough is
in use, prytt logs one `INFO` line per session start, for information only:

```
[prytt] INFO S3 passthrough: region eu-west-1 (AWS_REGION)
[prytt] INFO S3 passthrough: region eu-west-1 (AWS profile team-sso)
[prytt] INFO S3 passthrough: no region set; bucket region auto-discovered by Sail
```

It takes `AWS_REGION`, then `AWS_DEFAULT_REGION`, then the `region` of the
profile in effect (`s3.profile` once exported) in the AWS config file.

## Testing with prytt

The one-profile rule applies to a pytest run too: the plugin's `spark`
fixture starts one session for the whole run, and Sail keeps the first
identity. Pick the profile **per test run, not per test**:

```bash
PRYTT_S3_PROFILE=team-sso pytest
```

or set it in `mise.toml` `[env]`, `s3.profile` in the project config, or the
CI job's environment. Don't `monkeypatch.setenv("PRYTT_S3_PROFILE", ...)` in
individual tests: the first session fixes the profile (or no profile), and a
later test that resolves a different one raises the error above. Keep tests
that need real S3 in their own pytest run, apart from tests that use no
profile.

Tests run with the S3 write lock on; see [Test with pytest](testing.md).
