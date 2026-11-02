import sys
from importlib.machinery import ModuleSpec  # cheap: frozen; avoid importlib.abc (pulls typing/inspect)

WATCH = {"pyspark", "pyspark.context", "pyspark.sql.session"}
EVENTS = []  # for the spike: record what fired


class _ShimLoader:
    def create_module(self, spec):
        return None

    def exec_module(self, module):
        EVENTS.append("serve pyspark.context")
        from prytt_spike import shim_context
        module.__dict__.update({k: v for k, v in vars(shim_context).items() if not k.startswith("__")})


class _PostLoader:
    def __init__(self, real, name):
        self.real, self.name = real, name

    def create_module(self, spec):
        return self.real.create_module(spec)

    def exec_module(self, module):
        self.real.exec_module(module)
        _post_import(self.name, module)


def _post_import(name, module):
    EVENTS.append(f"post {name}")
    if name == "pyspark":
        # make `pyspark.context` resolvable and `from pyspark import SparkContext` work
        import pyspark.context as ctx
        module.SparkContext = ctx.SparkContext
    elif name == "pyspark.sql.session":
        B = module.SparkSession.Builder
        orig = B.getOrCreate
        def getOrCreate(self):
            from prytt_spike.server import ensure_running
            ensure_running()
            return orig(self)
        B.getOrCreate = getOrCreate


class Finder:
    def find_spec(self, fullname, path, target=None):
        if fullname not in WATCH:
            return None
        if fullname == "pyspark.context":
            return ModuleSpec(fullname, _ShimLoader())
        for f in sys.meta_path:
            if f is self or not hasattr(f, "find_spec"):
                continue
            spec = f.find_spec(fullname, path, target)
            if spec is not None:
                spec.loader = _PostLoader(spec.loader, fullname)
                return spec
        return None


def install():
    if not any(isinstance(f, Finder) for f in sys.meta_path):
        sys.meta_path.insert(0, Finder())
