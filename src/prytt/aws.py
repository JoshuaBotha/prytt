"""The AWS profile Sail uses for S3, checked at every session start.

Sail 0.7.3 runs in-process, uses the AWS SDK default credential chain, reads
only ``AWS_*`` env vars, and loads AWS config once per process at its first
S3 access. So prytt picks the profile by exporting ``AWS_PROFILE`` before
Sail starts, and can't switch it later.

Interface:

- ``UNSET``: the value of the one-profile-per-process record before the
  first session start.
- ``passthrough_in_use(cfg=None) -> bool``: whether the global ``s3.mode``
  or any mapped table's effective mode (``paths.table_mode``) is
  passthrough, so Sail may read real S3.
- ``prepare_session(cfg=None)``: called by ``server.ensure_running`` on
  every session start, before Sail starts. Raises before touching the env.
  With ``s3.profile`` set, sets ``AWS_PROFILE`` in ``os.environ`` (in every
  mode) and logs it at ``INFO``; a different ``AWS_PROFILE`` from the shell
  is overridden with a ``WARN`` naming both. Without ``s3.profile``, AWS env
  vars are left alone. When passthrough is in use and ``s3.profile`` is set,
  ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` in the env raise
  ``ValueError`` (the chain would prefer them over the profile). Logs at
  most once per ``Config``.
"""

import os

from prytt import config
from prytt.stub import log

UNSET = object()

_process_profile = UNSET  # the s3.profile in effect at the first session start
_prepared_for = None  # the Config last prepared (logs once per config)


def passthrough_in_use(cfg=None) -> bool:
    from prytt import paths

    cfg = config.get() if cfg is None else cfg
    if paths.is_passthrough(cfg.s3_mode):
        return True
    return any(
        paths.is_passthrough(paths.table_mode(table)[0]) for table in paths.mapped_tables().values()
    )


def prepare_session(cfg=None) -> None:
    """Callers serialise (``server.ensure_running`` holds its lock)."""
    global _prepared_for, _process_profile
    cfg = config.get() if cfg is None else cfg
    if _prepared_for is cfg:
        return
    profile = cfg.s3_profile
    _check_same_profile(profile)
    passthrough = passthrough_in_use(cfg)
    if profile is not None and passthrough:
        _check_no_static_keys(profile)
    if profile is not None:
        _export_profile(profile)
    if passthrough:
        _log_region(os.environ)
    _process_profile = profile
    _prepared_for = cfg


def _describe(profile) -> str:
    return "no s3.profile" if profile is None else f"s3.profile {profile!r}"


def _check_same_profile(profile) -> None:
    if _process_profile is UNSET or _process_profile == profile:
        return
    raise RuntimeError(
        f"prytt: one AWS profile per process. This process started with "
        f"{_describe(_process_profile)}, now the config resolves to {_describe(profile)}. "
        "Sail loads AWS config once per process, so it would keep the first identity. "
        "Run the new profile in a new process."
    )


_STATIC_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")


def _check_no_static_keys(profile: str) -> None:
    found = " and ".join(name for name in _STATIC_KEYS if os.environ.get(name))
    if found:
        raise ValueError(
            f"prytt config: {found} set in the environment with s3.profile {profile!r} "
            "and S3 passthrough in use: the AWS credential chain prefers the keys and "
            f"would ignore the profile. Unset {found}, or drop s3.profile "
            "(and PRYTT_S3_PROFILE)."
        )


def _export_profile(profile: str) -> None:
    shell = os.environ.get("AWS_PROFILE")
    os.environ["AWS_PROFILE"] = profile
    if shell and shell != profile:
        log("WARN", f"s3.profile {profile!r} overrides AWS_PROFILE={shell} from the environment")
    log("INFO", f"AWS_PROFILE={profile} (s3.profile)")


def _profile_region(env, profile: str) -> str | None:
    """``region`` of ``profile`` in the AWS config file, if it can be read."""
    import configparser
    from pathlib import Path

    config_file = Path(env.get("AWS_CONFIG_FILE") or Path.home() / ".aws" / "config")
    parser = configparser.ConfigParser()
    try:
        parser.read(config_file)
    except configparser.Error:
        return None
    section = "default" if profile == "default" else f"profile {profile}"
    return parser.get(section, "region", fallback=None)


def _log_region(env) -> None:
    """Sail finds each bucket's region itself; this is for the reader only.
    Runs after the export, so ``AWS_PROFILE`` is the configured profile."""
    for var in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        if env.get(var):
            log("INFO", f"S3 passthrough: region {env[var]} ({var})")
            return
    profile = env.get("AWS_PROFILE") or "default"
    region = _profile_region(env, profile)
    if region:
        log("INFO", f"S3 passthrough: region {region} (AWS profile {profile})")
    else:
        log("INFO", "S3 passthrough: no region set; bucket region auto-discovered by Sail")
