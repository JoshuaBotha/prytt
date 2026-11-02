# T3 getResolvedOptions + Job

Status: ready-for-agent
Blocked by: 01

Spec: §9 `awsglue.utils.getResolvedOptions`, `awsglue.job.Job`.

## Scope
- `awsglue/utils.py`: pure Python, `--K v` and `--K=v`, raise like Glue on missing required option, ignore extras.
- `awsglue/job.py`: `Job(glue_context)`, `init`, `commit` log only; LogAny catch-all.

## Acceptance
- Unit tests for both forms, missing-option error, extras ignored; Job logs.

