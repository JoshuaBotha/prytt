# T10 Iceberg glue_catalog

Status: resolved
Blocked by: 02, 05

Spec: §10 Iceberg; §11 iceberg.warehouse.

## Scope
- Set SAIL_CATALOG__LIST / DEFAULT_CATALOG before start (skip if SPARK_REMOTE set).
- spark.sql.warehouse.dir absolute on every prytt session.
- Re-register `<warehouse>/<db>/<tbl>` Iceberg tables in glue_catalog on each new session (MAP log).

## Acceptance
- Test: create glue_catalog table, MERGE INTO, new process sees table after re-register.

