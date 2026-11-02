class SparkContext:
    _active = None

    def __new__(cls, *a, **kw):
        if cls._active is None:
            from prytt_spike.server import ensure_running
            ensure_running()
            cls._active = super().__new__(cls)
        return cls._active

    def __init__(self, *a, **kw):
        pass

    @classmethod
    def getOrCreate(cls, conf=None):
        return cls()

    def setLogLevel(self, level):
        print(f"[prytt] STUB SparkContext.setLogLevel({level!r})")
