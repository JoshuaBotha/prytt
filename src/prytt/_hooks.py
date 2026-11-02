"""Meta path finder that fires post-import hooks for a few watched modules.

Imported at interpreter start by ``prytt._bootstrap``, so the same import rule
applies: stdlib only, no ``importlib.abc`` / ``importlib.util`` / ``typing`` /
``inspect`` / ``logging`` / ``tomllib``. Finder and loaders are duck-typed
classes; specs are built with ``importlib.machinery.ModuleSpec``. Working
prototype with real hooks: ``spikes/s4_hook/prytt_spike/_hooks.py``.

Interface:

- ``WATCH``: set of module names the finder intercepts. Empty in T1; T2 adds
  ``pyspark``, ``pyspark.context`` and ``pyspark.sql.session``.
- ``Finder``: ``find_spec`` returns ``None`` for anything not in ``WATCH``.
- ``install()``: put one ``Finder`` at the front of ``sys.meta_path``.
  Idempotent (site can process the ``.pth`` more than once).
"""

import sys

WATCH = set()


class Finder:
    def find_spec(self, fullname, path, target=None):
        if fullname not in WATCH:
            return None
        return None  # T2: serve shims / wrap real loaders with post-import hooks.


def install():
    if not any(isinstance(f, Finder) for f in sys.meta_path):
        sys.meta_path.insert(0, Finder())
