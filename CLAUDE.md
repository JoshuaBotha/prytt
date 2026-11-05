# prytt

Drop-in `awsglue` + PySpark shim so unmodified AWS Glue 6 job scripts run locally on Sail (Rust Spark Connect server), no JVM.

- Docs: `README.md` (quick start) and `docs/` (index `docs/README.md`): user guides in `docs/user/` (behaviour, config, compatibility = what's real vs stubbed, `s3-and-aws.md` = S3 modes, write lock, AWS auth), `docs/architecture/overview.md`, `docs/developer/guide.md`, decisions in `docs/adr/` (`db.table` SQL resolution: ADR 0003).
- Spike findings: `docs/research/spikes.md` (scripts removed; recover with `git show 6d9ad0e:spikes/<file>`). S5's SSO-profile spike was skipped; passthrough reads with an SSO profile (`s3.profile`) were since verified against real AWS with `mise run test:s3-live` (2026-11-05: partitioned Parquet, partition columns discovered). Other credential types and Delta/Iceberg on S3 are still unverified.
- Directory is still named `pritt/`; the project name is `prytt` everywhere else.
- Before any change: follow `CONTRIBUTING.md` (branch names, PR titles, tests, done = `mise run ci` green), including its "For coding agents" section.

## Environment

- `mise.toml`: Python 3.13, uv. `.venv` is Python 3.13 with `pysail` 0.7.3, `pyspark-client` 4.1.4 (`uv sync`).
- `SPARK_REMOTE` must be **unset** for prytt to start its own Sail server. Never give it a value in `mise.toml`; `[env] SPARK_REMOTE = false` unsets it for every `mise run` task.
- `uv sync` installs prytt itself (editable), which puts `prytt.pth` in site-packages.
- Acceptance tests run scripts as subprocesses so the `.pth` path is exercised for real.

## mise

mise is the single entry point for tooling, tasks and env vars, so a task runs the same locally and in CI.

- **Tasks are bash scripts** in `.mise/tasks/`, not inline TOML. Start each with `#!/usr/bin/env bash`, a `#MISE description="..."` line and `set -euo pipefail`; `chmod +x`.
- **Organise with folders**: `.mise/tasks/test/unit` is `mise run test:unit`; `test/_default` is plain `mise run test`.
- **Workflows call tasks** (`mise run ci`), they don't inline logic. CI installs tools with `jdx/mise-action`. If a workflow step is more than a one-liner, make it a task.
- **Env vars** go in `mise.toml` `[env]` (or `#MISE env=` on one task) when they apply to the project.
- Fresh clone: `mise run setup:dev` (also installs the pre-commit hooks). Everything CI runs, with `CI=true`: `mise run ci`.
- Tasks: `setup:dev`, `lint`, `lint:fix`, `test`, `test:unit`, `test:acceptance`, `test:s3-live` (optional manual check against real S3 at `PRYTT_TEST_S3_URI`, skips if unset; never in `test`/`ci`/releases), `build:dist`, `build:check-wheel`, `release:prepare <patch|minor|major>` (release branch + bump + relock + prytt version in `README.md`/`docs/user/` + push; skills `/release-patch|minor|major` first check the docs cover every feat/fix since the last tag, then add the PR), `release:publish` (CI-only: tags + releases the pyproject version on merge to `main` or manual dispatch), `repo:settings` (GitHub features, merge rules, `main` ruleset; idempotent), `ci`.

## Gotchas from the spikes (don't relearn these)

- `_bootstrap` / `_hooks` run at every interpreter start: stdlib only, **no `importlib.abc`, `importlib.util`, `typing`, `inspect`, `logging`, `tomllib`**. Plain duck-typed finder/loaders + `importlib.machinery.ModuleSpec`. Real implementation: `src/prytt/_hooks.py` (< 1 ms at start-up).
- pyspark-client has no `pyspark.context` and no `pyspark.SparkContext`. `SparkConf` and `SQLContext` work as-is.
- Hook firing order: `pyspark.sql.session` → `pyspark` → `pyspark.context`.
- Sail: `SparkConnectServer(ip="127.0.0.1", port=0).start(background=True)`; address from `.listening_address`. Set `SAIL_CATALOG__*` env **before** `start()`. Quiet Rust logs (`RUST_LOG`) by default.
- First-ever run after install can take ~30 s (native lib load); later runs < 1 s. Don't mistake it for a hang.
- No ORC in Sail. Parquet/JSON/CSV/Delta/Iceberg fine.
- Always set `spark.sql.warehouse.dir` to an absolute path, or Delta writes `./spark-warehouse` into cwd and Iceberg CTAS fails.
- Sail catalogs are in-memory per session: re-register `glue_catalog` / mapped tables on every new session.
- Sail 0.7.3 table, database and temp-view names are **case-sensitive** (`spark.sql.caseSensitive=false` has no effect). prytt rewrites mapped `db.table` names in `spark.sql`/`spark.table` (`paths._wrap_table_names`).
- Over Spark Connect, `spark.sql("SELECT ...")` and `spark.table(...)` are **lazy**: a missing table raises only at the action (`collect`, `show`), not in the call (classic Spark/Glue raises eagerly). To catch it in a wrapper, force analysis with `df.schema`. Sail's not-found error names only the last part (`Table or view not found: orders`).
- Sail registers `CREATE TABLE ... USING parquet|json|csv LOCATION '<missing dir>'` (and a temp view over it) without error, as an empty table with no columns. Only delta fails. Check for data yourself before registering.
- `functools.wraps` copies the wrapped function's `__dict__`, so a `__prytt_wrapped__`-style flag on an inner wrapper shows up on the outer one. Use a distinct flag per wrapper (iceberg checks `__prytt_wrapped__` on `spark.sql`).
- Local paths handed to Spark, SQL `LOCATION` literals or `MAP` logs go out as `Path.as_posix()` (`C:/...` on Windows; Sail accepts it). `str(path)` gives backslashes that `sqltext.literal` doubles and that tests comparing paths won't match. Tests build expected paths with `.as_posix()` too.
- Sail 0.7.3 SQL quirks: plain `USE db` doesn't parse (write `USE DATABASE db`; the database must exist); `spark.sql` takes one statement per call; `spark.catalog.tableExists` is case-sensitive and returns `False` for a `spark_catalog.`-prefixed name; `.collect()` on an `INSERT` result fails with "uint64 not supported in conversion to Arrow" (SQL commands already run at `spark.sql(...)`, so don't collect them).
- `mapInPandas` under pandas 3: `NULL` strings arrive as `NaN` (convert to `None`); worker exceptions arrive as empty `PythonException()` (re-raise with traceback).
- pyspark-client `SparkSession.__del__` shuts down a process-wide thread pool; if the GC runs it inside that pool's `submit()` it self-deadlocks (0% CPU hang). prytt replaces `__del__` (`pyspark_hooks.on_connect_session`, test `tests/unit/test_session_gc.py`). Tests that `builder.create()` an extra session should still `stop()` it.
- Sail reads AWS settings only from `AWS_*` env vars (never `PRYTT_*`), and loads AWS config (credentials chain, profile) once per process, at the first S3 access (`sail-object-store` `DEFAULT_AWS_CONFIG` `OnceCell`). So prytt exports `AWS_PROFILE` before Sail starts and allows one `s3.profile` per process. For plain `s3://bucket` URLs Sail 0.7.3 discovers the bucket region and ignores `AWS_REGION` (used only with a custom endpoint).
- Tests that fake AWS env in-process (dead `AWS_ENDPOINT_URL`, fake keys) poison Sail's AWS config for the rest of that process: real-S3 tests must never share a process with them. Hence `tests/live/` (no shared conftest) run alone by `mise run test:s3-live`.
- Pre-commit hooks run `uv run --frozen ruff`, the version in `uv.lock`: bump ruff with `uv lock --upgrade-package ruff`, not in `.pre-commit-config.yaml`.
- In-process Spark sessions (the `spark` fixture) belong in `tests/unit`, not `tests/acceptance`: a gRPC channel in the pytest process makes every later `subprocess` fork print gRPC fork warnings to stderr, breaking acceptance tests that assert a clean stderr.
- A raise from an `on_new_session` callback leaves a built but unreturned session nobody can `stop()`, and the test run hangs at exit. Check config that should fail session setup before the build (`pyspark_hooks._wrap` calls `s3lock.check_config()` first).
- To let a test write to `s3://` without reaching AWS, set fake `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL=http://127.0.0.1:9` and `AWS_EC2_METADATA_DISABLED=true`: Sail fails in microseconds.
