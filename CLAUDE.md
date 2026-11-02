# prytt

Drop-in `awsglue` + PySpark shim so unmodified AWS Glue 6 job scripts run locally on Sail (Rust Spark Connect server), no JVM.

- Spec: `prytt-spec.md` (source of truth). Future feature: `prytt-spec-sql-catalog.md` (not v1).
- Spike findings: `spikes/NOTES.md`. All spikes done; S5 SSO-profile check skipped by decision (S3 passthrough is best-effort).
- Directory is still named `pritt/`; the project name is `prytt` everywhere else.

## Status (2026-11-01)

Spec settled, spikes done, env ready. **No package code yet.** Next: build T1 onward (spec §16).

Build order (ticket graph, spec §16):

1. **T1** skeleton: `pyproject.toml` build-system (hatchling, `src/` layout, `force-include` for `prytt.pth`), `.pth`, lazy config, logging, `LogAny`, strict mode (CI default).
2. In parallel after T1: **T2** server + import hooks + `SparkContext` shim + builder hook; **T3** `getResolvedOptions` + `Job`; **T7** `awsgluedq` stub.
3. **T4** `GlueContext` + `DynamicFrame` + reader/writer + S3 mapping (needs T2).
4. **T5** catalog mapping + temp views, **T6** transforms, **T8** pytest plugin.
5. **T9** acceptance scripts + README. **T10** Iceberg `glue_catalog`.

Work test-first. Not a git repo yet: run `git init` before starting T1 (ask Josh first).

## Environment

- `mise.toml`: Python 3.13, uv. `.venv` is Python 3.13 with `pysail` 0.7.3, `pyspark-client` 4.1.4 (`uv sync`).
- `SPARK_REMOTE` must be **unset** for prytt to start its own Sail server. Never give it a value in `mise.toml`; `[env] SPARK_REMOTE = false` unsets it for every `mise run` task.
- `pyproject.toml` has no `[build-system]` yet, so `uv sync` installs deps only. T1 adds it; after that, `uv sync` installs prytt itself (editable), which puts `prytt.pth` in site-packages.
- Acceptance tests run scripts as subprocesses so the `.pth` path is exercised for real.

## mise

mise is the single entry point for tooling, tasks and env vars, so a task runs the same locally and in CI.

- **Tasks are bash scripts** in `.mise/tasks/`, not inline TOML. Start each with `#!/usr/bin/env bash`, a `#MISE description="..."` line and `set -euo pipefail`; `chmod +x`.
- **Organise with folders**: `.mise/tasks/test/unit` is `mise run test:unit`; `test/_default` is plain `mise run test`.
- **Workflows call tasks** (`mise run ci`), they don't inline logic. CI installs tools with `jdx/mise-action`. If a workflow step is more than a one-liner, make it a task.
- **Env vars** go in `mise.toml` `[env]` (or `#MISE env=` on one task) when they apply to the project.
- Fresh clone: `mise run setup:dev`. Everything CI runs, with `CI=true`: `mise run ci`.
- Tasks: `setup:dev`, `test`, `test:unit`, `test:acceptance`, `build:dist`, `build:check-wheel`, `release:publish` (CI-only: tags + releases the pyproject version on merge to `main`), `repo:settings` (GitHub features, merge rules, `main` ruleset; idempotent), `ci`.

## Gotchas from the spikes (don't relearn these)

- `_bootstrap` / `_hooks` run at every interpreter start: stdlib only, **no `importlib.abc`, `importlib.util`, `typing`, `inspect`, `logging`, `tomllib`**. Plain duck-typed finder/loaders + `importlib.machinery.ModuleSpec`. Working prototype: `spikes/s4_hook/prytt_spike/` (< 1 ms).
- pyspark-client has no `pyspark.context` and no `pyspark.SparkContext`. `SparkConf` and `SQLContext` work as-is.
- Hook firing order: `pyspark.sql.session` → `pyspark` → `pyspark.context`.
- Sail: `SparkConnectServer(ip="127.0.0.1", port=0).start(background=True)`; address from `.listening_address`. Set `SAIL_CATALOG__*` env **before** `start()`. Quiet Rust logs (`RUST_LOG`) by default.
- First-ever run after install can take ~30 s (native lib load); later runs < 1 s. Don't mistake it for a hang.
- No ORC in Sail. Parquet/JSON/CSV/Delta/Iceberg fine.
- Always set `spark.sql.warehouse.dir` to an absolute path, or Delta writes `./spark-warehouse` into cwd and Iceberg CTAS fails.
- Sail catalogs are in-memory per session: re-register `glue_catalog` / mapped tables on every new session.
- `mapInPandas` under pandas 3: `NULL` strings arrive as `NaN` (convert to `None`); worker exceptions arrive as empty `PythonException()` (re-raise with traceback).

## Agent skills

### Issue tracker

Local markdown: issues and specs live under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five roles (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`), recorded as a `Status:` line in each issue file. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` + `docs/adr/` at repo root (created lazily). See `docs/agents/domain.md`.
