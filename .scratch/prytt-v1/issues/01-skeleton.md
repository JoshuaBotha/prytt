# T1 skeleton: packaging, .pth, lazy config, logging, LogAny, strict mode

Status: resolved
Blocked by: none

Spec: prytt-spec.md §5, §6 (bootstrap + import rules only; hooks themselves are T2), §9 'Shared catch-all', §11, §12, §13 (config/strict unit tests), §13 Non-functional (import-time regression test).

## Scope
- `pyproject.toml` [build-system] hatchling, `src/` layout, `force-include` placing `prytt.pth` at site-packages root; dev deps (pytest). Packages: `prytt`, `awsglue`, `awsgluedq` (empty stubs ok).
- `prytt.pth`, `prytt/_bootstrap.py` (PRYTT_DISABLE check; installs finder from `_hooks.py` — empty watch list ok, T2 fills it). Prototype: `spikes/s4_hook/prytt_spike/`.
- `prytt/config.py`: lazy, precedence env > prytt.toml > [tool.prytt] (upward search) > defaults; all §11 keys; CI-derived strict default.
- `prytt/stub.py`: `[prytt]` logger, tags STUB/MAP/WARN/INFO, `LogAny`, `stub_log`, strict mode raise, `GlueLocalUnsupported` exception.
- Test for < 20 ms interpreter-start overhead.
- `.gitignore` already exists; extend if needed.

## Acceptance
- `uv sync` installs prytt editable and `prytt.pth` lands in site-packages.
- Unit tests: config precedence, strict defaults (CI=true→strict; CI=true PRYTT_STRICT=0→non-strict; no CI→non-strict), LogAny log/return-self, strict raise.

