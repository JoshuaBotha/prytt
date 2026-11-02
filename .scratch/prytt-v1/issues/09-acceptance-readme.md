# T9 acceptance scripts + README

Status: resolved
Blocked by: 03, 05, 06, 07

Spec: §13 Acceptance scripts 1–4; §4; §5 Name clash; §10 passthrough caveat.

## Scope
- `tests/acceptance/scripts/` four scripts, each run as subprocess; assert outputs/exit codes; gap script with explicit PRYTT_STRICT.
- README: install, run, test, config table, disable, name clash, passthrough best-effort, first-run 30 s note.

## Acceptance
- All four acceptance tests pass.


## Comments

Strict-mode gaps found (fix in review pass): `sc.setLogLevel`, `sc._jsc.hadoopConfiguration().set`, `job.isInitialized()`, `EvaluateDataQuality().process_rows` raise under strict. `spark.sparkContext` is a Spark Connect limit.
