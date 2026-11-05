"""Make tests/unit and tests/acceptance independent of the developer's env.

mise loads ``mise.local.toml`` (and any parent directory's ``mise.local.toml``)
into every task, and the shell may export ``PRYTT_*`` / ``AWS_*`` too. Left in
place they change what prytt does: e.g. ``PRYTT_S3_PROFILE`` plus the fake AWS
keys the tests set trips the static-key conflict check. ``strip()`` removes
every ``PRYTT_*`` / ``AWS_*`` variable, then applies the ones ``mise.toml``
``[env]`` sets on purpose. Tests that need such a variable set it themselves
(``monkeypatch.setenv``).

Called at import of ``tests/unit/conftest.py`` and
``tests/acceptance/conftest.py``, so before any test module, fixture or
subprocess. ``tests/live`` has no conftest on purpose: it must inherit the
shell's AWS env and ``PRYTT_S3_PROFILE``.
"""

import os
import tomllib
from pathlib import Path

PREFIXES = ("PRYTT_", "AWS_")
_MISE_TOML = Path(__file__).resolve().parents[1] / "mise.toml"


def _mise_env() -> dict:
    """``PRYTT_*`` / ``AWS_*`` entries of ``mise.toml`` ``[env]`` (``False`` = unset)."""
    if not _MISE_TOML.is_file():
        return {}
    env = tomllib.loads(_MISE_TOML.read_text()).get("env", {})
    return {k: v for k, v in env.items() if k.startswith(PREFIXES)}


def strip() -> None:
    """Remove developer ``PRYTT_*`` / ``AWS_*`` env vars, then apply ``mise.toml``'s."""
    for name in [k for k in os.environ if k.startswith(PREFIXES)]:
        del os.environ[name]
    for name, value in _mise_env().items():
        if value is not False:
            os.environ[name] = str(value).lower() if isinstance(value, bool) else str(value)

    # prytt.pth ran before this, with the developer's env: if PRYTT_DISABLE was
    # set, the import hook is missing from this process. install() is idempotent.
    from prytt import config
    from prytt._hooks import install

    install()
    config.reset()
