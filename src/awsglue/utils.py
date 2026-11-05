"""``awsglue.utils`` shim. Pure Python, no Spark.

Interface:

- ``getResolvedOptions(args, options) -> dict``: parse ``--KEY value`` and
  ``--KEY=value`` from ``args`` (``sys.argv``; ``args[0]`` is the script and
  is skipped, as in Glue) and return ``{KEY: value}`` for each requested
  option, keyed by argparse dest (``source-path`` -> ``source_path``, as
  Glue does). Extra args are ignored. An option missing from ``args`` falls
  back to the local job arguments in config (``PRYTT_ARG_<name>`` env var,
  then ``[args]`` in ``prytt.toml`` / ``[tool.prytt.args]``; looked up by
  option name, then dest), logged ``INFO``. Still missing: the value is
  ``None`` and a ``WARN`` names it; in strict mode it raises
  ``GlueArgumentError`` with argparse's message, as real Glue does
  (``the following arguments are required: --JOB_NAME``).
  ``TempDir`` / ``RedshiftTempDir`` are never required and mirror each other,
  like Glue.
- ``GlueArgumentError``: ``Exception`` subclass raised on bad arguments.
- ``GlueArgumentParser``: ``argparse.ArgumentParser`` that raises
  ``GlueArgumentError`` instead of exiting.
"""

import argparse

from prytt import config
from prytt.stub import log

__all__ = ["GlueArgumentError", "GlueArgumentParser", "getResolvedOptions"]

_TEMP_DIRS = ("TempDir", "RedshiftTempDir")


class GlueArgumentError(Exception):
    pass


class GlueArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise GlueArgumentError(message)


def _fill_from_config(values, options):
    """Fill options missing from argv from config; return the ones still missing."""
    local_args = config.get().args
    missing = []
    for option in options:
        dest = option.replace("-", "_")
        if option in _TEMP_DIRS or values[dest] is not None:
            continue
        for name in (option, dest):
            if name in local_args:
                values[dest] = local_args[name]
                log(
                    "INFO",
                    f"--{option} not on the command line; using {values[dest]!r} from prytt config",
                )
                break
        else:
            missing.append(option)
    return missing


def getResolvedOptions(args, options):
    parser = GlueArgumentParser()
    for name in _TEMP_DIRS:
        parser.add_argument("--" + name, required=False)
    for option in options:
        if option not in _TEMP_DIRS:
            parser.add_argument("--" + option, required=False)

    parsed, _extra = parser.parse_known_args(args[1:])
    values = vars(parsed)
    missing = _fill_from_config(values, options)
    if missing:
        flags = ", ".join("--" + option for option in missing)
        if config.get().strict:
            raise GlueArgumentError(f"the following arguments are required: {flags}")
        log(
            "WARN",
            f"missing job argument(s) {flags}: using None. Pass them on the command line, "
            f"set PRYTT_ARG_<name>, or add them to [args] in prytt.toml",
        )

    if values["TempDir"] is None:
        values["TempDir"] = values["RedshiftTempDir"]
    elif values["RedshiftTempDir"] is None:
        values["RedshiftTempDir"] = values["TempDir"]

    dests = (option.replace("-", "_") for option in options)
    return {dest: values[dest] for dest in dests}
