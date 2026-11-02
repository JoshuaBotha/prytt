# T9 acceptance scripts + README

Status: ready-for-agent
Blocked by: 03, 05, 06, 07

Spec: §13 Acceptance scripts 1–4; §4; §5 Name clash; §10 passthrough caveat.

## Scope
- `tests/acceptance/scripts/` four scripts, each run as subprocess; assert outputs/exit codes; gap script with explicit PRYTT_STRICT.
- README: install, run, test, config table, disable, name clash, passthrough best-effort, first-run 30 s note.

## Acceptance
- All four acceptance tests pass.

