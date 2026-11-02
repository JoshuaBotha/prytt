# T8 pytest plugin

Status: ready-for-agent
Blocked by: 02, 04

Spec: §4 Test; §10 (plugin registers per session).

## Scope
- `prytt/pytest_plugin.py` auto-registered via `pytest11` entry point; session-scoped `spark`, `glue_context`; `s3_root` fixture (tmp dir wired into s3.root).

## Acceptance
- pytester test using the fixtures end to end.

