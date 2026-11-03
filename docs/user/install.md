# Install prytt

prytt is a development dependency of the project that holds your Glue job
scripts. It is never deployed to AWS: on Glue the real `awsglue` package is
already there.

## Requirements

| Need | Why |
|---|---|
| Python **3.13** | The version AWS Glue 6 runs. prytt's `requires-python` is `>=3.13`. |
| macOS, Linux or Windows | The platforms CI tests (`ubuntu-latest`, `macos-latest`, `windows-latest`). |
| [uv](https://docs.astral.sh/uv/) (recommended) or pip | To install from git. |
| No JVM, no Docker | Sail is a Rust binary inside the `pysail` wheel. |

On Windows, prytt logs and passes Spark local paths with forward slashes
(`C:/Users/...`).

## Add prytt to a project

1. Pin Python 3.13. Your project's `requires-python` must allow 3.13, or uv
   fails with "requested Python version does not satisfy Python>=3.13".

   ```bash
   uv python pin 3.13                                  # writes .python-version
   # pyproject.toml: requires-python = "==3.13.*"      # matches Glue 6 exactly
   ```

   For a new project, `uv init --python 3.13` sets both.

2. Add prytt as a dev dependency from git. Pin a tag, so every machine and
   CI run gets the same version:

   ```bash
   uv add --dev "prytt @ git+https://github.com/JoshuaBotha/prytt.git@v0.3.0"
   ```

   This pulls in `pysail` (>=0.7.3) and `pyspark-client` (4.1.x). Releases
   are listed on the repository's GitHub Releases page; each has a wheel and
   an sdist attached, so you can also install from a downloaded wheel:

   ```bash
   uv add --dev ./prytt-0.3.0-py3-none-any.whl
   ```

   With pip: `pip install "prytt @ git+https://github.com/JoshuaBotha/prytt.git@v0.3.0"`.

3. Warm Sail once. **The first run after install can take about 30 s**
   before Sail answers, because it loads its native library (most noticeable
   on macOS). Later runs start in under a second. It is not hung.

   ```bash
   uv run python -c "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().range(1).collect()"
   ```

   You should see one line like:

   ```
   [prytt] INFO prytt 0.3.0, pysail 0.7.3, pyspark-client 4.1.4; sail at sc://127.0.0.1:52341
   ```

Nothing else needs configuring for the default setup. Next:
[Run a job](running-jobs.md).

## What the install puts in your environment

| Item | Purpose |
|---|---|
| `prytt.pth` at the site-packages root | Runs `import prytt._bootstrap` at every interpreter start, which installs a small import hook (< 1 ms). See [Architecture](../architecture/overview.md#start-up). |
| `prytt` package | Server lifecycle, config, path and catalog mapping, PySpark shims. |
| `awsglue` package | The Glue API your scripts import (`awsglue.context`, `awsglue.job`, `awsglue.transforms`, ...). |
| `awsgluedq` package | `EvaluateDataQuality`, which Glue Studio scripts import. |
| `pytest11` entry point | The `spark`, `glue_context` and `s3_root` fixtures. See [Testing](testing.md). |

Because of the `.pth` file, prytt is active for **every** Python process in
that environment, not only the job scripts. It does nothing until a process
imports PySpark or `awsglue`.

## Name clash: one `awsglue` per environment

prytt provides the `awsglue` import name. Do not install it in the same
environment as `AWSGlueDataplanePython`, `awsglue-local`, or the `awsglue`
libs from the Glue Docker images. Only one of them can own `awsglue`, and you
get whichever was installed last.

Also keep the full `pyspark` package (the JVM one) out of the environment.
prytt is built on `pyspark-client`, the Spark Connect-only distribution, and
both packages provide the `pyspark` import name.

## Upgrade

Change the tag and sync:

```bash
uv add --dev "prytt @ git+https://github.com/JoshuaBotha/prytt.git@v0.3.0"
```

While prytt is 0.x, a minor version bump (0.1 → 0.2) may contain breaking
changes; the GitHub Release notes say what changed. A `pysail` upgrade can
bring back a ~30 s first run once.

## Turn it off or remove it

Skip prytt for one run without uninstalling:

```bash
PRYTT_DISABLE=1 python jobs/my_job.py ...
```

This skips the import hook, so:

- `from pyspark.context import SparkContext` fails with
  `ModuleNotFoundError` (pyspark-client has no such module), and
  `SparkSession.builder.getOrCreate()` fails with `CONNECT_URL_NOT_SET`
  unless you set `SPARK_REMOTE` to a Spark Connect server yourself.
- `awsglue` still imports prytt's package, and constructing a
  `GlueContext` still starts Sail, because that call goes straight to the
  server module rather than through the hook.

To remove prytt completely, uninstall it (`uv remove --dev prytt`). That also
removes `prytt.pth`.

## Developing prytt itself

To work on prytt rather than use it, clone the repository and run
`mise run setup:dev`. See the [developer guide](../developer/guide.md).
