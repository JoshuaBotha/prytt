"""``awsglue.utils`` shim. Pure Python, no Spark.

Interface:

- ``getResolvedOptions(args, options) -> dict``: parse ``--KEY value`` and
  ``--KEY=value`` from ``args`` (``sys.argv``; ``args[0]`` is the script and
  is skipped, as in Glue) and return ``{KEY: value}`` for each requested
  option, keyed by argparse dest (``source-path`` -> ``source_path``, as
  Glue does). Extra args are ignored. A missing option raises
  ``GlueArgumentError`` with argparse's message, as real Glue does
  (``the following arguments are required: --JOB_NAME``).
  ``TempDir`` / ``RedshiftTempDir`` are never required and mirror each other,
  like Glue.
- ``GlueArgumentError``: ``Exception`` subclass raised on bad arguments.
- ``GlueArgumentParser``: ``argparse.ArgumentParser`` that raises
  ``GlueArgumentError`` instead of exiting.
"""

import argparse

__all__ = ["GlueArgumentError", "GlueArgumentParser", "getResolvedOptions"]

_TEMP_DIRS = ("TempDir", "RedshiftTempDir")


class GlueArgumentError(Exception):
    pass


class GlueArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise GlueArgumentError(message)


def getResolvedOptions(args, options):
    parser = GlueArgumentParser()
    for name in _TEMP_DIRS:
        parser.add_argument("--" + name, required=False)
    for option in options:
        if option not in _TEMP_DIRS:
            parser.add_argument("--" + option, required=True)

    parsed, _extra = parser.parse_known_args(args[1:])
    values = vars(parsed)
    if values["TempDir"] is None:
        values["TempDir"] = values["RedshiftTempDir"]
    elif values["RedshiftTempDir"] is None:
        values["RedshiftTempDir"] = values["TempDir"]

    dests = (option.replace("-", "_") for option in options)
    return {dest: values[dest] for dest in dests}
