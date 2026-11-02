# T6 transforms (real set + pass-through set)

Status: ready-for-agent
Blocked by: 04

Spec: §9 `awsglue.transforms` table.

## Scope
- `awsglue/transforms/__init__.py` with `__all__`; classes with classmethod `apply` matching Glue kwargs.
- Real: ApplyMapping (nested dotted paths, cast+alias), SelectFields, DropFields, RenameField, Join, DropNullFields, ResolveChoice (cast specs).
- Pass-through (log WARN/STUB): Filter, Map, Relationalize, Unbox, UnnestFrame, SplitFields, SplitRows, Spigot, ErrorsAsDynamicFrame, others.

## Acceptance
- `from awsglue.transforms import *` works; unit tests per real transform.

