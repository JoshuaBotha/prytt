# Prytt - a drop-in local Glue replacement

[![CI](https://github.com/JoshuaBotha/prytt/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/JoshuaBotha/prytt/actions/workflows/ci.yml?query=branch%3Amain) [![Release](https://img.shields.io/github/v/release/JoshuaBotha/prytt)](https://github.com/JoshuaBotha/prytt/releases/latest) [![Python 3.13](https://img.shields.io/badge/python-3.13-blue)](docs/user/install.md#requirements) [![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Run unmodified AWS Glue 6 job scripts on your laptop. prytt is a drop-in
`awsglue` + PySpark shim backed by [Sail](https://github.com/lakehq/sail), a
Rust Spark Connect server. No JVM, no Docker, no script changes.

```bash
python jobs/bronze_to_silver.py --JOB_NAME local --source_path s3://raw-bucket/orders/
```

```
[prytt] INFO prytt 0.3.0, pysail 0.7.3, pyspark-client 4.1.4; sail at sc://127.0.0.1:52341
[prytt] INFO Job.init('local'), bookmarks off
[prytt] MAP  s3://raw-bucket/orders/ -> /home/me/proj/.prytt/s3/raw-bucket/orders/
[prytt] WARN Filter not applied locally (v1); data passed through unchanged
[prytt] INFO Job.commit('local')
```

## Quick start

prytt needs **Python 3.13**, the version AWS Glue 6 runs. In the project
that holds your job scripts:

```bash
uv python pin 3.13            # and requires-python = "==3.13.*" in pyproject.toml
uv add --dev "prytt @ git+https://github.com/JoshuaBotha/prytt.git@v0.3.0"
```

Put input data where the job's `s3://` paths point, under `.prytt/s3/`:

```bash
mkdir -p .prytt/s3/raw-bucket/orders
cp sample-orders.json .prytt/s3/raw-bucket/orders/
```

Run the job with the arguments Glue would pass:

```bash
uv run python jobs/bronze_to_silver.py --JOB_NAME local --source_path s3://raw-bucket/orders/
```

**The first run after install can take about 30 s** while Sail loads its
native library. Later runs start in under a second.

Don't install prytt alongside another `awsglue` package
(`AWSGlueDataplanePython`, `awsglue-local`, the Glue Docker libs): only one
can own the `awsglue` import name.

## What you get

- The Glue boilerplate (`getResolvedOptions`, `SparkContext()`,
  `GlueContext(sc)`, `Job.init` / `commit`) works as is.
- `GlueContext` S3 readers and writers for `json`, `csv`, `parquet`,
  `delta` and `iceberg`, with `s3://` mapped to local directories.
- Data Catalog tables mapped to local paths in config, readable through
  `from_catalog` and by name in `spark.sql` / `spark.table`.
- `DynamicFrame` and the common transforms (`ApplyMapping`, `SelectFields`,
  `DropFields`, `RenameField`, `Join`, `DropNullFields`, `ResolveChoice`).
- Iceberg through `glue_catalog`: `CREATE TABLE`, `INSERT`, `MERGE INTO`,
  CTAS, kept between runs.
- pytest fixtures: `spark`, `glue_context`, `s3_root`.
- Every local difference logged; strict mode (on when `CI` is set) turns
  gaps into errors.

Not emulated: bookmarks, RDD APIs, non-S3 connections, ORC, and the rules in
`Filter`, `Map`, `Relationalize` and data quality checks (logged and passed
through). Details: [Compatibility and limits](docs/user/compatibility.md).

## Documentation

| | |
|---|---|
| [Install](docs/user/install.md) | Requirements, upgrading, turning prytt off |
| [Run Glue jobs locally](docs/user/running-jobs.md) | Job arguments, S3 paths, catalog tables, Iceberg, strict mode, logs |
| [Test with pytest](docs/user/testing.md) | Fixtures, end-to-end job tests, CI |
| [Configuration reference](docs/user/configuration.md) | Every setting and env var |
| [Compatibility and limits](docs/user/compatibility.md) | What's real, passed through, stubbed or unsupported |
| [Troubleshooting](docs/user/troubleshooting.md) | Symptoms and fixes |
| [Solution architecture](docs/architecture/overview.md) | How prytt works, and why |
| [Developer guide](docs/developer/guide.md) | Changing prytt |

All docs: [docs/](docs/README.md).

## Developing prytt

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, branch and PR conventions,
tests, and releasing, and the [developer guide](docs/developer/guide.md) for
the code.
