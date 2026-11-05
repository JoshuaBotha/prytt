# Spike notes (S1–S7)

Run 2026-10-30 on macOS (arm64), Python 3.13, `pysail` 0.7.3, `pyspark-client` 4.1.4, pandas 3.1.0, pyarrow 26.0.0.
Clean venv (not the project `.venv`). Each script ran standalone with `SPARK_REMOTE` unset. The scripts are no longer in the tree; recover one with `git show 6d9ad0e:spikes/<file>` (for example `git show 6d9ad0e:spikes/s6_udf.py > s6_udf.py`; S4 is a folder: `git checkout 6d9ad0e -- spikes/s4_hook`).

## Summary

| ID | Result | Decision |
|---|---|---|
| S1 | `pyspark.context` missing; `pyspark.SparkContext` missing. `SparkConf`, `SQLContext`, `pyspark.sql.*` import fine. No py4j. | Hooks replace `pyspark.context` and set `pyspark.SparkContext`. No shim for `SparkConf` / `SQLContext`. |
| S2 | pysail 0.7.3 + pyspark-client 4.1.4 work (`spark.version` = 4.1.4). parquet/json/csv read+write+`partitionBy` ok. **ORC not supported** ("No data source found for: orc"). | Keep `pyspark-client>=4.1,<4.2`. Pin `pysail>=0.7.3`. `orc` → log WARN and raise unsupported. |
| S3 | In-process works. | **In-process**, no subprocess fallback needed. |
| S4 | `.pth` + meta path finder intercepts every import order tried. Startup cost < 1 ms once `.pyc` exists. | Ship as designed. Do **not** import `importlib.abc` / `importlib.util` in the hook. |
| S5 | Sail reads `s3://` directly (AWS Rust SDK credential chain). Anonymous public read works. SSO profile **not tested — skipped by decision (2026-11-01)**. | Passthrough mode ships as best-effort, unverified for SSO profiles. **Superseded (2026-11-05):** an SSO-profile passthrough read passed `mise run test:s3-live`. |
| S6 | Scalar UDF, `pandas_udf`, `mapInPandas`, `mapInArrow`, `applyInPandas` all work. | DynamicFrame `filter`/`map` v2 via `mapInPandas` is viable. Two caveats below. |
| S7 | Delta **and** Iceberg: write, append, read, `CREATE TABLE ... LOCATION`, `INSERT`, `MERGE INTO`, CTAS, `saveAsTable`, `writeTo().append()`. A `glue_catalog` catalog can be declared via `SAIL_CATALOG__LIST`. | **Positive → T10 in scope.** Details below. |
| S-SQL1 (future spec) | `CREATE DATABASE raw_db` + `CREATE TABLE raw_db.orders USING json LOCATION ...` works; `spark.sql` and `spark.table` resolve `raw_db.orders`. | Approach 1 (real DBs + external tables) works; see `docs/adr/0003-mapped-tables-as-sail-external-tables.md`. |

## S1: imports under pyspark-client only (`s1_imports.py`)

```
OK   import pyspark                      (no SparkContext attr)
FAIL from pyspark import SparkContext    ImportError
FAIL import pyspark.context              ModuleNotFoundError
OK   from pyspark.conf import SparkConf  (set/get work)
OK   from pyspark.sql import SQLContext
FAIL import py4j                         ModuleNotFoundError
```

## S2 / S3: server lifecycle (`s2_s3_server.py`)

- `SparkConnectServer(ip="127.0.0.1", port=0)` binds an ephemeral port. `start(background=True)` returns in ~10–25 ms.
- Bound address: `srv.listening_address` → `('127.0.0.1', 51772)`; `None` once stopped. `srv.running` is a bool.
- `stop()` works and logs a clean shutdown.
- **Exit:** without calling `stop()` the process still exits normally (exit 0, Sail shuts itself down at interpreter exit). Keep the `atexit` stop anyway for ordering and logs.
- Whole script (start, session, a few queries, 4 writes) takes **~0.75 s** wall time.
- **Cold first run took 26 s** before the first session was created (once per fresh install, likely macOS first-load scanning of the 100+ MB native lib). Every run after that was fast. Mention in the README so nobody thinks it hangs.
- Sail logs to stderr at INFO (`sail_python::spark::server ...`). prytt should lower this by default (e.g. `RUST_LOG`) and show it at `log_level=DEBUG`.

## S4: import hook (`s4_hook/prytt_spike/`)

Installed as a real `.pth` (`import prytt_spike._bootstrap`) in site-packages. Every case gave the shim:

| Script does | Result |
|---|---|
| `from pyspark.context import SparkContext` | shim; singleton; Sail started |
| `from pyspark import SparkContext` | shim (attr set in the `pyspark` post-import hook) |
| `import pyspark; pyspark.SparkContext` | shim class; `pyspark.context` is our module |
| only `SparkSession.builder.getOrCreate()` | builder hook starts Sail |
| canonical Glue boilerplate | works |
| `PRYTT_DISABLE=1` | finder not installed |

Hook order observed: `pyspark.sql.session` post-import fires first (pyspark's `__init__` imports it), then `pyspark`, then the served `pyspark.context`.

Startup cost (`python -X importtime -c pass`):

- First version used `importlib.abc` → pulled in `typing`, `inspect`, `ast`, `dis` → **~16 ms**.
- Rewritten with duck-typed loaders and `importlib.machinery.ModuleSpec` → **~0.9 ms** import, **0.4 ms** median wall-clock delta (15 runs, on vs `PRYTT_DISABLE=1`). Budget is 20 ms.
- Rule for `_bootstrap` / `_hooks`: stdlib only, and none of `importlib.abc`, `importlib.util`, `typing`, `inspect`, `logging`, `tomllib`.

## S5: S3 passthrough (`s5_s3.py`)

- Public object `s3://ookla-open-data/...parquet` (us-west-2):
  - no creds, no region: fails. Error lists the chain tried: Environment, Profile, WebIdentityToken, EcsContainer, Ec2InstanceMetadata (IMDS 1 s connect timeout).
  - `AWS_REGION=us-west-2 AWS_SKIP_SIGNATURE=true`: **works** (77 s, because that file is large).
- So Sail uses the standard AWS Rust SDK chain, which includes a Profile provider (the Rust SDK supports SSO profiles).
- **Skipped (decision 2026-11-01): `AWS_PROFILE=<sso profile>`.** Not run; passthrough is best-effort. **Superseded (2026-11-05):** covered by `mise run test:s3-live`, which passed with an SSO profile. The original spike, for reference:
  ```bash
  git show 6d9ad0e:spikes/s5_s3.py > s5_s3.py
  aws sso login --profile sso-admin
  AWS_PROFILE=sso-admin AWS_REGION=us-west-2 PRYTT_DISABLE=1 \
    <spike-venv>/bin/python s5_s3.py \
    s3://ookla-open-data/parquet/performance/type=fixed/year=2019/quarter=1/2019-01-01_performance_fixed_tiles.parquet
  ```
  `OK` means passthrough works with the ambient profile. Pick a smaller object from your own bucket to make it quicker.
- ~~Passthrough mode should require a region (`AWS_REGION` or profile region) and say so in the error message.~~ **Superseded:** no region needed, Sail 0.7.3 discovers each bucket's region; prytt only logs one. See [S3 and AWS](../user/s3-and-aws.md#region).

## S6: Python UDFs (`s6_udf.py`)

All ran in-process: `@udf`, `@pandas_udf`, `mapInPandas` (filter and map shapes), `mapInArrow`, `groupBy().applyInPandas`.

Caveats for DynamicFrame `filter` / `map` v2:

1. **pandas 3 nulls:** string columns come in as `StringDtype(na_value=nan)`, so a SQL `NULL` reaches Python as `nan`, not `None`. Glue user code does `rec["v"] or ""` and breaks (`nan` is truthy). The shim must turn `NaN` back into `None` before calling `f`, e.g. `pdf.astype(object).where(pdf.notna(), None).to_dict("records")`. Map/struct columns arrive as plain dicts.
2. **Lost tracebacks:** an exception inside the worker comes back as `PythonException()` with an **empty message**. The shim should catch exceptions inside the generator and re-raise with the formatted traceback in the message, or users get no clue.

## S7: Delta / Iceberg / `glue_catalog` (`s7_tables.py`, `s7b_glue_catalog.py`)

- Both `delta` and `iceberg` formats: DataFrame write/append/read, `CREATE TABLE ... USING <fmt> LOCATION`, `INSERT`, `MERGE INTO ... WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT` all correct.
- Default catalog list is only `sail` (+ `system`); `glue_catalog.db.tbl` fails out of the box.
- Declare it before the server starts:
  ```bash
  SAIL_CATALOG__LIST='[{name="sail", type="memory", initial_database=["default"]}, {name="glue_catalog", type="memory", initial_database=["default"]}]'
  SAIL_CATALOG__DEFAULT_CATALOG=sail
  ```
  Then `CREATE DATABASE glue_catalog.db`, `CREATE TABLE glue_catalog.db.tbl USING iceberg LOCATION ...`, `MERGE INTO glue_catalog.db.tbl`, CTAS, `saveAsTable`, `writeTo().append()` all work.
- Set `spark.sql.warehouse.dir` to an **absolute** path. Iceberg `saveAsTable`/CTAS without one fails ("Iceberg table location must be an absolute path"), and Delta silently writes `./spark-warehouse` into cwd.
- **The catalog is in-memory**: table entries vanish with the server (even a second session on the same server could not see `glue_catalog.db.tbl`). Data files persist on disk. So T10 must re-register tables every run: on startup, scan `<iceberg.warehouse>/<db>/<tbl>` (or read them from config) and run `CREATE TABLE IF NOT EXISTS glue_catalog.<db>.<tbl> USING iceberg LOCATION ...`. The per-session result also matters for the pytest plugin (register per session).
- Only `type="memory"` catalog confirmed. Sail also ships an Iceberg REST provider; not needed locally.

## Other findings worth acting on

- **Fixed:** `mise.toml` used to set `SPARK_REMOTE=sc://localhost:50051`; removed. Under the spec's rule ("if `SPARK_REMOTE` is set, use it") prytt would never have started Sail in this repo.
- Project `.venv` is **Python 3.14** with `pysail` 0.7.2; spec targets 3.13 and `mise.toml` asks for 3.13. Spikes used 3.13 + 0.7.3; recreate the venv.

## Spec updates (applied 2026-10-30)

- §5 deps: `pysail>=0.7.3`.
- §6: add the "no `importlib.abc`" rule; record measured cost.
- §7: in-process confirmed; drop the subprocess fallback to a footnote; lower Sail's log level by default; note the first-run delay.
- §9 format mapping: `orc` unsupported.
- §9 DynamicFrame `filter`/`map`: v2 via `mapInPandas` confirmed viable; add NaN→None and traceback re-raise requirements.
- §10 Iceberg: T10 in scope; add `SAIL_CATALOG__LIST`, absolute `spark.sql.warehouse.dir`, re-register-on-start.
- §10 S3 passthrough: viable; needs region; SSO check skipped, mode is best-effort. **Superseded:** no region needed, and prytt now checks the SSO token before Sail starts; see [S3 and AWS](../user/s3-and-aws.md).
- SQL catalog design note (now `docs/adr/0003-mapped-tables-as-sail-external-tables.md`): S-SQL1 positive → approach 1.
