# 0003: Resolve `db.table` in SQL by registering mapped tables as Sail external tables

**Status:** accepted, implemented (2026-10-30, spike S-SQL1). Supersedes the
design note `prytt-spec-sql-catalog.md`.

## Context

Glue scripts query Data Catalog tables by name:

```python
spark.sql("SELECT o.id, c.name FROM raw_db.orders o JOIN raw_db.customers c ON o.cid = c.id")
spark.table("raw_db.orders")
```

prytt already maps each catalog table to a local path, format and options
(a **mapped table**, `[tool.prytt.catalog."raw_db.orders"]`), and v1 exposed
each as a **temp view** `raw_db_orders`. That doesn't help SQL that uses the
Glue name:

- Temp view names can't contain `.`.
- `raw_db.orders` parses as a database-qualified identifier, so Spark looks
  it up in the session catalog, not among temp views.
- Three-part names (`glue_catalog.raw_db.orders`) overlap with Iceberg's
  `glue_catalog` (ADR 0004).

## Decision

On every new session, for each mapped table, prytt runs:

```sql
CREATE DATABASE IF NOT EXISTS `raw_db`;
CREATE TABLE IF NOT EXISTS `raw_db`.`orders` USING json OPTIONS ('multiLine' 'true') LOCATION '<mapped local path>';
```

making it a **registered table** in the default `sail` catalog, and keeps
the temp view. Around that:

- Tables are registered from the `on_new_session` callback, next to the
  Iceberg re-registration, because Sail's catalog is in memory per session.
- Format and options go through `prytt.formats`, as for `from_catalog`, so
  ORC is rejected the same way.
- A location with no data is **skipped** with a `WARN`: Sail registers a
  missing parquet/json/csv location without error as an empty table with no
  columns, which would hide the problem.
- `spark.sql` / `spark.table` are wrapped to rewrite mapped two-part names in
  table positions to their configured spelling, because Sail 0.7.3 resolves
  names case-sensitively and Glue does not.
- For a query naming an unmapped two-part table, the wrapper forces analysis
  (`df.schema`) and, on Spark's not-found error for exactly one such name,
  adds the TOML to add as an exception note.

## Alternatives

1. **Real databases and external tables in Sail** (chosen). No SQL parsing
   needed for resolution; works for `spark.sql` and `spark.table` alike.
   Spike S-SQL1 confirmed Sail supports `CREATE DATABASE` and
   `CREATE TABLE ... USING <fmt> LOCATION` over Spark Connect and reads
   through them.
2. **SQL rewrite to temp views.** Wrap `spark.sql`, parse with `sqlglot`
   (Spark dialect), replace mapped `db.table` with `db_table`, re-render.
   Engine-independent, but adds a dependency, round-trips can break unusual
   SQL, and `spark.table` needs its own wrapper. Kept as the fallback; not
   needed.
3. **Global temp views (`global_temp.x`).** One fixed database name, so
   `raw_db.orders` still doesn't resolve. Ruled out.

The case-sensitivity rewrite (found after the spike) brought back a small
amount of SQL text handling, done with a masking tokenizer
(`prytt.sqltext`) rather than a full parser, and limited to names in table
positions.

## Consequences

- Mapped tables are queryable as `db.table`, case-insensitively, with joins.
  The `db_table` views still work.
- Registration happens at session creation, so a table with no data at that
  point stays unresolvable for the session even after the job writes it.
- `on_missing = "empty"` can't apply to SQL; an unmapped name errors (with
  the hint).
- SQL writes to mapped tables (`INSERT INTO`, `saveAsTable`) are not mapped.
- `glue_catalog.<db>.<tbl>` belongs to Iceberg; mapped tables aren't
  reachable through it.
- A script's own `CREATE DATABASE` / `CREATE TABLE` of a mapped name fails
  with "already exists".
- `IF NOT EXISTS` keeps registration safe on a persistent external catalog,
  but such a catalog keeps the old location if a table's `path` changes.
- The hint is matched by name only; when two unmapped names share a table
  name, no hint is added, because Sail's error names only the last part.
