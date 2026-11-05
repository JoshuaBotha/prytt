# prytt

Runs unmodified AWS Glue scripts locally. This glossary names the ways a Glue catalog table is represented, since "table" alone is ambiguous.

## Language

**Mapped table**:
A `[tool.prytt.catalog."db.table"]` config entry that maps a Glue table name to a local path, format and options.
_Avoid_: Catalog table, catalog entry

**Registered table**:
A mapped table made visible to SQL as `db.table` in the session's default catalog, built at session start.
_Avoid_: External table, SQL table

**Temp view**:
The alias `db_table` for a mapped table, registered alongside the registered table for scripts and tests that use the underscore name.
_Avoid_: View, alias

**Local table**:
A mapped table whose effective S3 mode is `local`: its `s3://` path is read from and written to the mapped directory under `s3.root`.
_Avoid_: Fake table, fixture table

**Passthrough table**:
A mapped table whose effective S3 mode is `passthrough`, from its own `mode` or the global `s3.mode`: its `s3://` path is handed to Sail unchanged and read from real S3.
_Avoid_: Real table, remote table

**S3 write lock**:
prytt's default refusal of any write it can detect that would reach real S3, in every mode. Lifted only by the env var `PRYTT_ALLOW_S3_WRITES=1`.
_Avoid_: Read-only mode, write guard
