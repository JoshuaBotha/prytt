# 0004: Rebuild Iceberg `glue_catalog` from the warehouse directory every session

**Status:** accepted (2026-10-30, spike S7)

## Context

Glue 6 Iceberg jobs use a Spark catalog named `glue_catalog`
(`glue_catalog.<db>.<tbl>`), backed on AWS by the Glue Data Catalog. Sail
supports Iceberg reads, writes, `MERGE INTO`, CTAS, `saveAsTable` and
`writeTo().append()`, but by default has only its `sail` catalog, and its
catalogs live in memory: table entries vanish with the server, and a second
session on the same server couldn't see them. Iceberg data and metadata
files persist on disk.

## Decision

- Before Sail starts, prytt declares a second memory catalog through
  `SAIL_CATALOG__LIST` (`sail` stays the default), unless the user already
  set the variable.
- Every new session: set `spark.sql.warehouse.dir` to the absolute
  `iceberg.warehouse`; give `glue_catalog.default` a location; scan
  `<warehouse>/<db>/<tbl>` and run `CREATE DATABASE IF NOT EXISTS
  glue_catalog.<db> LOCATION '<warehouse>/<db>'` and
  `CREATE TABLE IF NOT EXISTS glue_catalog.<db>.<tbl> USING iceberg LOCATION
  '<dir>'` for each directory holding `metadata/*.metadata.json`.
- Wrap `spark.sql` so a script's `CREATE DATABASE glue_catalog.<db>` without
  `LOCATION` gets `LOCATION '<warehouse>/<db>'`. Without it, Sail puts the
  database's tables flat in the warehouse as `<tbl>-<uuid>`, and the next
  run couldn't tell which database they belong to.

## Alternatives

- **List Iceberg tables in config**, like mapped tables. More to maintain,
  and tables a job creates would need a config edit before the next run.
- **A persistent catalog** (Sail's Iceberg REST provider). Needs a REST
  catalog service; against the "nothing to run" goal.

## Consequences

- Iceberg jobs run unchanged, and tables survive between runs with no config.
- The directory layout is the catalog: a table created with a `LOCATION`
  outside `<warehouse>/<db>/<tbl>` works in that run but isn't found in the
  next. prytt warns when it sees one, or when it can't add a location
  (multi-statement SQL).
- The rewrite is a single-statement text rewrite (comments and quoted text
  masked), not a parser.
- `spark.sql.warehouse.dir` is always absolute, which also stops Delta from
  writing `./spark-warehouse` into the current directory and Iceberg CTAS
  from failing on a relative location.
- An external server must declare its own `glue_catalog`; prytt only warns.
