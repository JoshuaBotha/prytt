# 0002: Activate through a `.pth` file and a stdlib-only meta path finder

**Status:** accepted (2026-10-30, spike S4)

## Context

Scripts must run unchanged, but pyspark-client has no `pyspark.context`
module and no `pyspark.SparkContext`, and `SparkSession.builder` needs a
server address. prytt has to change what those imports produce without the
script importing prytt. Whatever runs at interpreter start runs in **every**
Python process in the environment, so it must cost almost nothing.

## Decision

The wheel installs `prytt.pth` at the site-packages root with one line,
`import prytt._bootstrap`. The bootstrap (unless `PRYTT_DISABLE` is truthy)
inserts one meta path finder at the front of `sys.meta_path`. The finder
returns `None` for everything except a fixed set of modules:

- `pyspark.context` is **served** from `prytt.shims.pyspark_context`.
- `pyspark`, `pyspark.sql.session` and `pyspark.sql.connect.session` load
  normally, then a **post-import hook** in `prytt.shims.pyspark_hooks` patches
  them (set `SparkContext`, wrap the builder, replace `SparkSession.__del__`).

`_bootstrap` and `_hooks` use the stdlib only and never import
`importlib.abc`, `importlib.util`, `typing`, `inspect`, `logging` or
`tomllib`. Loaders are duck-typed classes; specs are
`importlib.machinery.ModuleSpec`.

## Alternatives

- **Ship a `pyspark` package that shadows pyspark-client.** Fragile: two
  distributions owning one import name.
- **`sitecustomize.py`.** Only one per environment; clashes with other tools.
- **Ask scripts to `import prytt` first.** Breaks "no script changes".
- **Patch at the first `awsglue` import.** Misses Spark-only scripts and
  scripts that import PySpark first.

## Consequences

- Measured cost: ~0.9 ms import, 0.4 ms median wall-clock delta. The first
  version used `importlib.abc`, which pulled in `typing`, `inspect`, `ast`
  and `dis` for ~16 ms; hence the import rule.
- Works for every import order tried (`from pyspark.context import ...`,
  `from pyspark import SparkContext`, builder only).
- prytt is active in every interpreter in the environment; `PRYTT_DISABLE=1`
  turns the hook off for one run.
- Python started with `-S` skips `.pth` files, and prytt with them.
- Acceptance tests must run scripts as subprocesses to exercise this path.
