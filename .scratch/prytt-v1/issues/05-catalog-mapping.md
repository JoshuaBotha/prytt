# T5 catalog mapping + temp views

Status: resolved
Blocked by: 04

Spec: §10 Catalog; §11 catalog.* keys.

## Scope
- Resolve `[tool.prytt.catalog."db.table"]` (path via S3 mapping, format, options); `on_missing` error (message with exact TOML) / empty.
- Wire `from_catalog` for create_dynamic_frame, create_data_frame, write_dynamic_frame, write_data_frame.
- Register every mapped table as temp view `db_table` on each new session (use T2 session-setup seam).

## Acceptance
- Tests: mapped read, missing→error text includes TOML, missing→empty, temp view queryable via spark.sql.

