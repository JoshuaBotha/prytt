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
  ``ValueError`` (the chain would prefer them over the profile). In the same
  case, if the profile (read from ``AWS_CONFIG_FILE``) is an SSO profile, its
  AWS CLI SSO token (``$HOME/.aws/sso/cache``) must exist and be unexpired,
  else ``RuntimeError`` naming ``aws sso login --profile <profile>``; a
  cache prytt can't find or parse only logs a ``WARN``. Logs at most once
  per ``Config``.
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
    if profile is not None and passthrough:
        _check_sso_token(os.environ, profile)
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


def _home(env):
    from pathlib import Path

    return Path(env["HOME"]) if env.get("HOME") else Path.home()


def _read_aws_config(env):
    """The AWS config file as a ``ConfigParser``, or ``None`` if unreadable."""
    import configparser

    config_file = env.get("AWS_CONFIG_FILE") or _home(env) / ".aws" / "config"
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(config_file, encoding="utf-8")
    except (configparser.Error, UnicodeDecodeError):
        return None
    return parser


def _profile_section(profile: str) -> str:
    return "default" if profile == "default" else f"profile {profile}"


def _profile_region(env, profile: str) -> str | None:
    """``region`` of ``profile`` in the AWS config file, if it can be read."""
    parser = _read_aws_config(env)
    if parser is None:
        return None
    return parser.get(_profile_section(profile), "region", fallback=None)


def _sso_cache_key(env, profile: str) -> str | None:
    """What the AWS CLI hashes to name ``profile``'s SSO token cache file:
    the ``sso_session`` name, else the legacy ``sso_start_url``. ``None`` for
    a non-SSO profile (or an unreadable config file)."""
    parser = _read_aws_config(env)
    if parser is None:
        return None
    section = _profile_section(profile)
    return parser.get(section, "sso_session", fallback=None) or parser.get(
        section, "sso_start_url", fallback=None
    )


def _check_sso_token(env, profile: str) -> None:
    """Raise if ``profile`` is an SSO profile whose cached token is missing or
    expired. Best effort: the cache layout belongs to the AWS CLI, so a cache
    prytt can't read only logs a ``WARN``.

    Layout, as botocore (which the AWS CLI v2 shares) writes it:
    ``botocore.utils.SSOTokenLoader._generate_cache_key`` is
    ``hashlib.sha1(session_name or start_url).hexdigest()``,
    ``botocore.utils.JSONFileCache`` stores it as ``<key>.json``, in
    ``~/.aws/sso/cache`` (``botocore.credentials.SSOProvider._SSO_TOKEN_CACHE_DIR``),
    and the JSON has ``expiresAt`` as an ISO-8601 UTC timestamp.
    """
    import hashlib
    import json
    from datetime import UTC, datetime

    key = _sso_cache_key(env, profile)
    if key is None:
        return
    login = f"run: aws sso login --profile {profile}"
    cache_dir = _home(env) / ".aws" / "sso" / "cache"
    if not cache_dir.is_dir():
        log(
            "WARN",
            f"no AWS CLI SSO token cache at {cache_dir.as_posix()} for AWS profile "
            f"{profile!r}; skipping the SSO token check (if S3 access fails, {login})",
        )
        return
    cache_file = cache_dir / f"{hashlib.sha1(key.encode()).hexdigest()}.json"
    if not cache_file.exists():
        raise RuntimeError(f"prytt: no SSO token for AWS profile {profile!r} (s3.profile); {login}")
    try:
        expires_at = datetime.fromisoformat(json.loads(cache_file.read_text())["expiresAt"])
    except (OSError, ValueError, TypeError, KeyError) as exc:
        log(
            "WARN",
            f"can't read the SSO token cache {cache_file.as_posix()} for AWS profile "
            f"{profile!r} ({type(exc).__name__}: {exc}); skipping the SSO token check",
        )
        return
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    if expires_at <= datetime.now(UTC):
        raise RuntimeError(
            f"prytt: the SSO token for AWS profile {profile!r} (s3.profile) expired at "
            f"{expires_at.isoformat()}; {login}"
        )


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
