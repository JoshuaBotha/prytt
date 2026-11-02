"""Meta path finder that fires post-import hooks for a few watched modules.

Imported at interpreter start by ``prytt._bootstrap``, so the same import rule
applies: stdlib only, no ``importlib.abc`` / ``importlib.util`` / ``typing`` /
``inspect`` / ``logging`` / ``tomllib``. Finder and loaders are duck-typed
classes; specs are built with ``importlib.machinery.ModuleSpec``. Working
prototype with real hooks: ``spikes/s4_hook/prytt_spike/_hooks.py``.

Interface:

- ``WATCH``: set of module names the finder intercepts: ``pyspark``,
  ``pyspark.context``, ``pyspark.sql.session`` (spec §6).
- ``SERVE``: watched names prytt serves itself, mapped to the prytt module
  whose public names become the served module (``pyspark.context`` →
  ``prytt.shims.pyspark_context``; pyspark-client has no such module).
- ``POST``: watched names whose real module gets a post-import hook, mapped
  to ``"module:function"``; ``function(module)`` runs right after the real
  module executes. Hooks live in ``prytt.shims.pyspark_hooks`` and are
  imported only when they fire. Any firing order works.
- ``Finder``: ``find_spec`` returns ``None`` for anything not in ``WATCH``.
- ``install()``: put one ``Finder`` at the front of ``sys.meta_path``.
  Idempotent (site can process the ``.pth`` more than once).
"""

import sys
from importlib.machinery import ModuleSpec

SERVE = {"pyspark.context": "prytt.shims.pyspark_context"}
POST = {
    "pyspark": "prytt.shims.pyspark_hooks:on_pyspark",
    "pyspark.sql.session": "prytt.shims.pyspark_hooks:on_session",
}
WATCH = set(SERVE) | set(POST)


def _import(name):
    __import__(name)
    return sys.modules[name]


class _ShimLoader:
    def __init__(self, source):
        self.source = source

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        shim = _import(self.source)
        names = getattr(shim, "__all__", None) or [n for n in vars(shim) if not n.startswith("_")]
        for name in names:
            setattr(module, name, getattr(shim, name))
        module.__doc__ = shim.__doc__


class _PostLoader:
    def __init__(self, real, hook):
        self.real, self.hook = real, hook

    def create_module(self, spec):
        create = getattr(self.real, "create_module", None)
        return create(spec) if create is not None else None

    def exec_module(self, module):
        self.real.exec_module(module)
        source, _, func = self.hook.partition(":")
        getattr(_import(source), func)(module)

    def __getattr__(self, name):  # get_resource_reader, is_package, get_code, ...
        return getattr(self.real, name)


class Finder:
    def find_spec(self, fullname, path, target=None):
        if fullname not in WATCH:
            return None
        if fullname in SERVE:
            return ModuleSpec(fullname, _ShimLoader(SERVE[fullname]))
        for finder in sys.meta_path:
            if finder is self or not hasattr(finder, "find_spec"):
                continue
            spec = finder.find_spec(fullname, path, target)
            if spec is not None:
                if spec.loader is not None and hasattr(spec.loader, "exec_module"):
                    spec.loader = _PostLoader(spec.loader, POST[fullname])
                return spec
        return None


def install():
    if not any(isinstance(f, Finder) for f in sys.meta_path):
        sys.meta_path.insert(0, Finder())
