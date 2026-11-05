"""Shared setup for tests/acceptance."""

import hermetic_env  # on sys.path via [tool.pytest.ini_options] pythonpath

# Before any test module, fixture or subprocess sees the env.
hermetic_env.strip()
