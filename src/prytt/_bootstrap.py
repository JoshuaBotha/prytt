"""Run at every interpreter start via ``prytt.pth`` (``import prytt._bootstrap``).

Must stay cheap (< 20 ms): stdlib only, and never ``importlib.abc``,
``importlib.util``, ``typing``, ``inspect``, ``logging``, ``tomllib``, pyspark,
pysail or grpc. It only installs the meta path finder from ``prytt._hooks``.
Config loading and the Sail server are lazy and happen elsewhere.

``PRYTT_DISABLE`` set to a truthy value (``1``/``true``/``yes``/``on``) skips it.
"""

import os

if os.environ.get("PRYTT_DISABLE", "").strip().lower() not in ("1", "true", "yes", "on"):
    from prytt._hooks import install

    install()
