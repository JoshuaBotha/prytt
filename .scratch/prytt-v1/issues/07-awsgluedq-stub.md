# T7 awsgluedq stub

Status: ready-for-agent
Blocked by: 01

Spec: §9 `awsgluedq.transforms.EvaluateDataQuality`.

## Scope
- `awsgluedq/transforms.py`: `EvaluateDataQuality.apply(frame, ruleset, publishing_options)` logs ruleset name, returns frame. Also usable as `EvaluateDataQuality().process_rows(...)` via catch-all.

## Acceptance
- Imports cleanly; unit test returns same frame and logs.

