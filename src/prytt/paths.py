"""s3:// and catalog -> local mapping (spec §10).

Interface:

- ``map_path(uri, for_write=False) -> str``: map one URI. ``s3://``,
  ``s3a://`` and ``s3n://`` become ``<s3.root>/bucket/key`` (absolute; a
  trailing ``/`` is kept) and each rewrite logs ``MAP``. Anything else is
  returned unchanged. ``for_write=True`` also creates the local directory.
  ``s3.root`` is read from ``config.get()`` on every call, so changing
  ``PRYTT_S3_ROOT`` + ``config.reset()`` redirects later calls (pytest plugin).
  In ``s3.mode = "passthrough"`` URIs are left alone for Sail to read from S3:
  the first call checks a region is set (``AWS_REGION``,
  ``AWS_DEFAULT_REGION`` or the profile's ``region`` in the AWS config file)
  and raises ``RuntimeError`` if not, and logs one best-effort ``WARN``.
- ``map_paths(uris) -> list[str]``: ``map_path`` over a list (or one string).
- ``CatalogTable(path, format, options)``: what a catalog lookup returns.
  ``path`` is the configured (unmapped) location; callers run it through
  ``map_path``. ``options`` are Spark/Glue format options.
- ``resolve_table(database, table_name) -> CatalogTable``: seam for the
  catalog map (T5). Until then it raises ``GlueLocalUnsupported``.
"""

import os
from pathlib import Path
from typing import NamedTuple

from prytt import config
from prytt.stub import GlueLocalUnsupported, log

_S3_SCHEMES = ("s3://", "s3a://", "s3n://")
_MODES = ("local", "passthrough")
_passthrough_checked_for = None  # the Config passthrough was last checked under


class CatalogTable(NamedTuple):
    path: str
    format: str
    options: dict


def _aws_profile_region(env) -> str | None:
    import configparser

    config_file = Path(env.get("AWS_CONFIG_FILE") or Path.home() / ".aws" / "config")
    profile = env.get("AWS_PROFILE") or "default"
    parser = configparser.ConfigParser()
    try:
        parser.read(config_file)
    except configparser.Error:
        return None
    section = "default" if profile == "default" else f"profile {profile}"
    return parser.get(section, "region", fallback=None)


def _check_passthrough(cfg) -> None:
    global _passthrough_checked_for
    if _passthrough_checked_for is cfg:
        return
    env = os.environ
    region = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or _aws_profile_region(env)
    if not region:
        raise RuntimeError(
            "prytt s3.mode = 'passthrough' needs an AWS region: set AWS_REGION "
            "(or AWS_DEFAULT_REGION, or region in your AWS profile)"
        )
    log("WARN", f"s3.mode = 'passthrough' (region {region}): Sail reads S3 directly; best-effort, unverified")
    _passthrough_checked_for = cfg


def _s3_key(uri: str) -> str | None:
    for scheme in _S3_SCHEMES:
        if uri.startswith(scheme):
            return uri[len(scheme) :]
    return None


def map_path(uri: str, for_write: bool = False) -> str:
    key = _s3_key(uri)
    if key is None:
        return uri
    cfg = config.get()
    if cfg.s3_mode not in _MODES:
        raise ValueError(f"prytt config: s3.mode must be one of {_MODES}, got {cfg.s3_mode!r}")
    if cfg.s3_mode == "passthrough":
        _check_passthrough(cfg)
        return uri
    local_path = Path(cfg.s3_root).resolve() / key
    local = str(local_path)
    if key.endswith("/"):
        local += "/"
    log("MAP", f"{uri} -> {local}")
    if for_write:
        local_path.mkdir(parents=True, exist_ok=True)
    return local


def map_paths(uris, for_write: bool = False) -> list:
    if isinstance(uris, str):
        uris = [uris]
    return [map_path(uri, for_write=for_write) for uri in uris]


def resolve_table(database: str, table_name: str) -> CatalogTable:
    raise GlueLocalUnsupported(
        f"catalog table {database}.{table_name}: catalog mapping is not implemented yet "
        "(use from_options with an s3 path)"
    )
