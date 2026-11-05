# Developer guide

For people changing prytt itself. Process rules (branches, PR titles, the
definition of done, releasing) are in [CONTRIBUTING.md](../../CONTRIBUTING.md);
this guide covers the code. Read the
[architecture overview](../architecture/overview.md) first, and the gotchas
in [CLAUDE.md](../../CLAUDE.md) before touching start-up, sessions or SQL:
each one cost a debugging session to learn.

## Set up

```bash
git clone https://github.com/JoshuaBotha/prytt.git
cd prytt
mise run setup:dev   # Python 3.13 + uv, uv sync, checks prytt.pth, pre-commit hooks, warms Sail
mise run test        # unit + acceptance
mise run ci          # everything CI runs, with CI=true (strict mode)
```

`uv sync` installs prytt editable, which puts `prytt.pth` in
`.venv`'s site-packages, so every `.venv` Python runs the import hook. Run
tests through `mise run`: `mise.toml` unsets `SPARK_REMOTE` for every task,
and prytt only starts its own Sail when it is unset.

| Task | What |
|---|---|
| `setup:dev` | Fresh clone to ready, installs the pre-commit hooks (idempotent). |
| `test` | `test:unit` then `test:acceptance`. |
| `test:unit` / `test:acceptance` | One suite; extra args go to pytest (`mise run test:unit -k catalog`). |
| `test:s3-live` | Optional manual check against real S3 (see [Checking against real S3](#checking-against-real-s3)). Not in `test` or `ci`. |
| `build:dist` | Wheel and sdist into `dist/`. |
| `build:check-wheel` | Install the wheel into a clean venv; check `prytt.pth` lands at the site-packages root and the packages import. |
| `lint` / `lint:fix` | `ruff check` + `ruff format --check`; `:fix` applies both. |
| `ci` | `CI=true`, lint, unit, acceptance, wheel check. Done = this passes. |
| `release:publish` | CI only: tag and release the `pyproject.toml` version. |
| `repo:settings` | Apply GitHub repo settings and the `main` ruleset (idempotent). |

Tasks are bash scripts in `.mise/tasks/` (`test/unit` is `test:unit`).
Workflows only call `mise run`; put new logic in a task, not in YAML.

## Code map

```
prytt.pth                      one line: import prytt._bootstrap
src/prytt/
  _bootstrap.py                checks PRYTT_DISABLE, installs the finder
  _hooks.py                    meta path finder: SERVE / POST tables
  shims/pyspark_context.py     SparkContext stand-in (served as pyspark.context)
  shims/pyspark_hooks.py       post-import hooks; register_session_setup()
  server.py                    Sail lifecycle; on_before_start / on_new_session
  config.py                    Config dataclass; env + TOML layering
  paths.py                     s3 mapping; catalog lookup; table registration; sql/table wrappers
  sqltext.py                   ident/literal quoting; mask; table_refs; statements (tokens)
  iceberg.py                   SAIL_CATALOG env; per-session glue_catalog rebuild
  io.py                        read_df / write_df
  s3lock.py                    S3 write lock: PryttS3WriteBlocked; check_write / check_table_write;
                               writer wrappers; spark.sql write classifier; S3-located table registry
  aws.py                       s3.profile -> AWS_PROFILE; session-start AWS checks (SSO token, region log)
  formats.py                   Glue format names and options -> Spark
  stub.py                      log(); LogAny; stub_log; GlueLocalUnsupported
  pytest_plugin.py             spark / glue_context / s3_root fixtures
src/awsglue/
  context.py                   GlueContext, readers, writers
  dynamicframe.py              DynamicFrame, DynamicFrameCollection, glue_type()
  transforms/__init__.py       ApplyMapping, Filter, ... (real, pass-through, stub)
  job.py                       Job
  utils.py                     getResolvedOptions
  gluetypes.py, data_sink.py, data_source.py   import-compat stand-ins
src/awsgluedq/transforms.py    EvaluateDataQuality
tests/unit/                    one module, in process
tests/acceptance/              scripts as subprocesses; real .pth path
tests/acceptance/scripts/      canonical, Glue Studio, Spark-only and gap jobs
examples/glue_selftest_job.py  runs on Glue 6 and locally; compare results
```

Each module starts with an **Interface** docstring: its public functions and
their contracts. Keep it current when you change behaviour; it is the
module's spec.

## Rules that keep prytt working

- **Start-up path is stdlib only.** `_bootstrap` and `_hooks` run in every
  interpreter in the environment. No `importlib.abc`, `importlib.util`,
  `typing`, `inspect`, `logging`, `tomllib`, and nothing third-party.
  `tests/unit/test_bootstrap.py` guards this. Everything else is imported
  lazily, inside the function that needs it.
- **`prytt` never imports `awsglue`.** Shared logic lives in `prytt`
  (`io.read_df` is used by both `GlueContext` and table registration).
- **Log, don't fail, for Glue gaps.** A Glue feature that can't work locally
  logs (`WARN` for a data pass-through, `STUB` through `LogAny`) and keeps
  the job running; strict mode makes `STUB` raise. Add every new gap to
  [Compatibility and limits](../user/compatibility.md).
- **Per-session state goes in `on_new_session`.** Sail's catalogs are in
  memory per session. Anything registered (tables, views, conf) must be
  redone for each session.
- **Sail env goes in `on_before_start`.** `SAIL_*` must be set before
  `start()`. Register callbacks in `pyspark_hooks.register_session_setup`,
  which `ensure_running` calls first, so every start path gets them.
- **Spark Connect is lazy.** `spark.sql(...)` and `spark.table(...)` don't
  fail for a missing table until an action. Force analysis with `df.schema`
  when you need the error at the call.
- **One wrapper flag per wrapper.** `functools.wraps` copies `__dict__`, so a
  flag on an inner wrapper appears on the outer one. `spark.sql` is wrapped
  three times (`__prytt_s3lock_sql_wrapped__` in s3lock, innermost;
  `__prytt_names_wrapped__` in paths; `__prytt_wrapped__` in iceberg). Each
  writer method s3lock wraps has its own `__prytt_s3lock_<method>_wrapped__`.
- **Names are case-sensitive in Sail 0.7.3**, whatever
  `spark.sql.caseSensitive` says.

## Extension seams

### Session set-up callbacks (`prytt.server`)

```python
server.on_before_start(fn)  # fn() just before Sail starts; not for external servers
server.on_new_session(fn)  # fn(spark) once per new SparkSession, before the caller gets it
```

Register both in `prytt.shims.pyspark_hooks.register_session_setup()`.
Callbacks run in registration order; registering the same function twice is
a no-op; calling `getOrCreate()` inside a callback is safe. Today:
`iceberg.set_sail_catalog_env` (before start), then
`paths.register_mapped_tables` and `iceberg.setup_session` (per session).

### Import hooks (`prytt._hooks`)

`SERVE` maps a module name to a prytt module whose public names become the
served module. `POST` maps a module name to `"module:function"`, called with
the real module right after it executes. Add to these tables only when a
script-visible import needs changing, and keep the hook module itself
stdlib-only (the hook *function* may import anything; it runs later).

### `LogAny` (`prytt.stub`)

Subclass `LogAny` for any Glue object. Methods you define are real;
everything else logs `STUB` and returns `self`, or raises in strict mode.
Set `raises_in_strict = False` only for stand-ins whose every call is a
known no-op.

## Common changes

### Implement a Glue transform

1. Write a failing test in `tests/unit/test_transforms.py` (and
   `test_dynamicframe.py` if it's also a method).
2. Implement it with DataFrame operations in `awsglue/dynamicframe.py`
   (method) and a thin `apply` classmethod in `awsglue/transforms`, using
   Glue's keyword names and defaults (`transformation_ctx`, `info`,
   `stageThreshold`, `totalThreshold`).
3. Move it from the pass-through or stub section of the module docstring and
   of [Compatibility and limits](../user/compatibility.md) to the real list.
4. If it was a pass-through on purpose, check `examples/glue_selftest_job.py`:
   its check may move from `SMOKE` to `EXACT`.

For `Filter` / `Map` (Python callbacks per record), [spike S6](../research/spikes.md) showed
`mapInPandas` works; turn pandas `NaN` back into `None` before calling the
user function, and re-raise worker exceptions with the traceback, because
they otherwise arrive as an empty `PythonException()`.

### Add a config key

1. Add the field to `Config` and a row to `_KEYS` in `prytt/config.py`
   (dotted key → field, env var, parser). Validate in the parser so bad
   values fail when config loads.
2. Test it in `tests/unit/test_config.py` (env, both TOML forms, precedence).
3. Document it in [Configuration](../user/configuration.md).

### Add a format or format option

`prytt/formats.py`: `_KNOWN`, `_ALIASES`, `_RENAME`, `_GLUE_ONLY`. Test in
`tests/unit/test_formats.py`; document in
[Configuration](../user/configuration.md#format-names-and-options).

### Change SQL handling

`prytt/sqltext.py` is a tokenizer with masking, not a parser. Keep changes
conservative: a missed rewrite gives the user Spark's error, a wrong rewrite
silently changes their query. Tests: `tests/acceptance/test_catalog_scripts.py`,
`tests/unit/test_iceberg_sql_rewrite.py`.

## Tests

| Suite | Where | Use for |
|---|---|---|
| Unit | `tests/unit/` | One module's behaviour, in process. Fast. |
| Acceptance | `tests/acceptance/` | Anything touching start-up, hooks, or what a job script sees. Runs scripts with `subprocess` and `sys.executable`, so the installed `prytt.pth` runs for real. |
| Live S3 | `tests/live/` | Opt-in, marker `s3_live`, only via `mise run test:s3-live`. Not in `testpaths`, so plain `pytest`, `test` and `ci` never collect it. |

- Test-first: watch the new test fail for the expected reason before the fix.
- Every bug fix comes with a regression test that fails without it.
- Tests that create an extra `SparkSession` `stop()` it.
- `mise run ci` sets `CI=true`, so strict mode is on: a test that relies on
  a `STUB` being tolerated must set `PRYTT_STRICT=0` itself.
- The pytester plugin (`-p pytester`) is on for testing the pytest plugin.
- Unit and acceptance runs ignore your `PRYTT_*` / `AWS_*` env (shell or
  `mise.local.toml`): their conftests strip it (`tests/hermetic_env.py`). A
  test that needs one sets it with `monkeypatch.setenv`.

### When a test hangs

pytest-timeout dumps every thread's stack at 60 s and kills the run at 180 s
(`timeout_method = "thread"`, because a signal can't interrupt a blocked
gRPC call). Read the dump before retrying. A 0% CPU hang inside
`ThreadPoolExecutor.submit` / `shutdown` is the pyspark-client
`SparkSession.__del__` deadlock (see CLAUDE.md); a hang at the first Spark
call on a fresh machine is Sail's ~30 s native library load.

### Checking against real Glue

`examples/glue_selftest_job.py` needs only itself: it generates data, writes
and reads it through Glue sinks and sources, runs the transforms and a broad
slice of Spark SQL, and prints a table of `EXACT` (must match) and `SMOKE`
(must run) checks. Run it locally and as a Glue 6 job (instructions in its
docstring) after changes to behaviour that real Glue also has.

### Checking against real S3

`mise run test:s3-live` is optional manual verification of S3 passthrough
against real AWS. It is **not** part of `mise run test` or `mise run ci`, and
not required for a release. It maps a passthrough table at
`PRYTT_TEST_S3_URI` and reads it with `spark.sql` (schema present, at least
one row), then checks that a write to a new sibling prefix
(`<parent>/_prytt_live_write_check_<uuid>/`, default error-if-exists mode)
raises `PryttS3WriteBlocked`. It never writes to S3: the task unsets
`PRYTT_ALLOW_S3_WRITES`, and the lock raises before Sail.

```bash
PRYTT_TEST_S3_URI=s3://my-bucket/path/to/table/ \
PRYTT_S3_PROFILE=my-sso-profile \
mise run test:s3-live
```

- Without `PRYTT_TEST_S3_URI` the tests report as skipped and the task exits 0.
- Credentials come from your shell: `PRYTT_S3_PROFILE`, or `AWS_*` env vars
  and the default chain. No region needed: Sail discovers the bucket's
  region (optional hint: `AWS_REGION` only matters with a custom endpoint
  such as `AWS_ENDPOINT_URL`).
- `PRYTT_TEST_S3_FORMAT` (default `parquet`) is the data's format;
  `PRYTT_TEST_S3_PARTITIONS` (comma-separated) names partition columns the
  table must expose, if the data is partitioned.
- The tests live in `tests/live/` and run in their own pytest process:
  unit and acceptance tests point AWS at a dead endpoint with fake keys, and
  Sail loads AWS config once per process, so the two must never share one.

## Upgrading Sail or pyspark-client

1. Bump in `pyproject.toml` (`pysail`, `pyspark-client` stays `<4.2` until
   tested) and `uv lock`.
2. `mise run ci` green on Linux, macOS and Windows (CI runs all three).
3. Re-check the gotchas in CLAUDE.md that name a version (case sensitivity,
   empty-table registration, lazy errors, `__del__`), and the limits in
   [Compatibility](../user/compatibility.md). Update any that changed.
4. Run `examples/glue_selftest_job.py` locally.

## Docs

| What changed | Update |
|---|---|
| User-visible behaviour | The page in `docs/user/` that covers it, and `README.md` if it changes the quick start. |
| S3 modes, the S3 write lock or AWS auth | [S3 and AWS](../user/s3-and-aws.md), plus the keys in the configuration reference and the lock's coverage in compatibility. |
| A Glue gap opened or closed | [Compatibility and limits](../user/compatibility.md) and the module docstring. |
| A design decision with real alternatives | A new ADR in `docs/adr/`, next number. |
| Structure, flow or a seam | [Architecture overview](../architecture/overview.md) and this guide. |
| A term | `GLOSSARY.md`. |
| Something learned the hard way | Gotchas in `CLAUDE.md`. |
