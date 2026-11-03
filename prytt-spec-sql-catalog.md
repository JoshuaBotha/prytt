# Spec: `spark.sql` catalog resolution in prytt (implemented)

Status: **implemented** (approach 1). Kept as the design record; the README's [Catalog tables](README.md#catalog-tables) section describes the shipped behaviour and its limits.

## Goal

Let an unmodified Glue script query catalog tables by their Glue name in SQL:

```python
spark.sql("SELECT o.id, c.name FROM raw_db.orders o JOIN raw_db.customers c ON o.cid = c.id")
spark.table("raw_db.orders")
```

and have `raw_db.orders` resolve through the existing `[tool.prytt.catalog."raw_db.orders"]` map (path via S3 mapping, format, options), with no script edits.

## Why it is not trivial

- v1 registers each mapped table as a temp view named `db_table`. Temp view names cannot contain `.`.
- `raw_db.orders` parses as a database-qualified identifier, so Spark looks it up in the session catalog, not among temp views.
- Scripts also use three-part names (`glue_catalog.raw_db.orders`), which overlaps with the Iceberg `glue_catalog` support (spike S7).

## Approaches

1. **Real databases + external tables in Sail.** At first session use (or `GlueContext` init), for each mapped table run:
   ```sql
   CREATE DATABASE IF NOT EXISTS raw_db;
   CREATE TABLE IF NOT EXISTS raw_db.orders USING json OPTIONS (multiLine 'true') LOCATION '<mapped local path>';
   ```
   Cleanest: no SQL parsing, works for `spark.sql` and `spark.table`. **Chosen: spike S-SQL1 passed.**
2. **SQL rewrite wrapper** (fallback only, not needed). Wrap `SparkSession.sql` (the builder hook already exists) to parse with `sqlglot` (Spark dialect), replace table references matching mapped `db.table` with the `db_table` temp view, and re-render. Engine-independent, but adds a dependency, and parse/render round-trips can break unusual SQL. Also needs a `spark.table` wrapper.
3. **Global temp views (`global_temp.x`).** Only one fixed database name, so `raw_db.orders` still does not resolve. Listed to rule out.

## Spike

| ID | Question |
|---|---|
| S-SQL1 | Over Spark Connect, does Sail support `CREATE DATABASE` and `CREATE TABLE ... USING <fmt> LOCATION ...` for parquet/json/csv, and does `spark.table("db.tbl")` / `spark.sql` then read it? Is the catalog in-memory per server (fine) or persisted somewhere we must clean up? |

**Result (2026-10-30, pysail 0.7.3, see `spikes/NOTES.md`):** positive. `CREATE DATABASE IF NOT EXISTS raw_db` and `CREATE TABLE raw_db.orders USING json LOCATION '<path>'` work over Spark Connect; `spark.sql("SELECT count(*) FROM raw_db.orders")` and `spark.table("raw_db.orders")` both read the data. The catalog is in memory per session, so nothing to clean up, but tables must be registered on every new session (same as the `glue_catalog` re-registration; see the README's Iceberg section). Approach 1 it is.

Implementation notes:

- Register in the same place as the `glue_catalog` re-registration: on first session creation, from the `[tool.prytt.catalog]` map. Reuse the existing format mapping in `prytt.formats` (`orc` unsupported).
- Keep the v1 `db_table` temp views too; they cost nothing and existing local tests may use them.

## Behaviour sketch

- Mapped table: queryable as `db.table` in `spark.sql` and `spark.table`. Log `MAP raw_db.orders -> .prytt/s3/raw-bucket/orders/` once per table.
- Unmapped table: same `catalog.on_missing` rule as `from_catalog` (`error` raises with the TOML snippet to add; `empty` logs and yields an empty result). With approach 1 this only works for unknown tables Spark reports, so the error message may need a wrapper to add the TOML hint.
- Writes (`INSERT INTO raw_db.orders`, `saveAsTable`) are out of scope for the first cut.
- `glue_catalog.<db>.<tbl>`: owned by the Iceberg `glue_catalog` catalog (S7 positive; see the README's Iceberg section). Two-part `db.table` names resolve in the default `sail` catalog. A mapped table that scripts reach both ways would need registering in both catalogs; decide when building.

## Acceptance sketch

- Script reading `raw_db.orders` via `spark.sql` and `spark.table` returns the mapped rows.
- Join across two mapped tables in one query works.
- Unmapped table with `on_missing = "error"` exits non-zero with the TOML hint in stderr.
- No change to interpreter start cost (work happens lazily at session creation).
