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
