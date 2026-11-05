"""The AWS profile Sail uses for S3, checked at every session start.

Sail 0.7.3 runs in-process, uses the AWS SDK default credential chain, reads
only ``AWS_*`` env vars, and loads AWS config once per process at its first
S3 access. So prytt picks the profile by exporting ``AWS_PROFILE`` before
Sail starts, and can't switch it later.

Interface:

- ``UNSET``: the value of the one-profile-per-process record before the
  first session start.
- ``prepare_session(cfg=None)``: called by ``server.ensure_running`` on
  every session start, before Sail starts. Raises before touching the env.
  With ``s3.profile`` set, sets ``AWS_PROFILE`` in ``os.environ`` (in every
  mode) and logs it at ``INFO``; a different ``AWS_PROFILE`` from the shell
  is overridden with a ``WARN`` naming both. Without ``s3.profile``, AWS env
  vars are left alone. The rest runs only when passthrough is in use
  (``paths.passthrough_in_use``), and reads the AWS config file
  (``AWS_CONFIG_FILE``) once. With ``s3.profile`` set,
  ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` in the env raise
  ``ValueError`` (the chain would prefer them over the profile), and if the
  profile is an SSO profile its AWS CLI SSO token (``$HOME/.aws/sso/cache``)
  must exist and be unexpired, else ``RuntimeError`` naming
  ``aws sso login --profile <profile>``. An expired token the AWS SDK can
  refresh itself (``sso_session`` profile with a live client registration)
  and a cache prytt can't find, read or parse only log a ``WARN``. Then the
  region in effect is logged at ``INFO``. Logs at most once per ``Config``.
"""

import os

from prytt import config, paths
from prytt.stub import log

UNSET = object()

_process_profile = UNSET  # the s3.profile in effect at the first session start
_prepared_for = None  # the Config last prepared (logs once per config)


def prepare_session(cfg=None) -> None:
    """Callers serialise (``server.ensure_running`` holds its lock)."""
    global _prepared_for, _process_profile
    cfg = config.get() if cfg is None else cfg
    if _prepared_for is cfg:
        return
    env = os.environ
    profile = cfg.s3_profile
    _check_same_profile(profile)
    passthrough = paths.passthrough_in_use(cfg)
    if passthrough:
        # The profile Sail will use: s3.profile once exported, else the shell's.
        aws_profile = profile or env.get("AWS_PROFILE") or "default"
        settings = _profile_settings(env, aws_profile)
        if profile is not None:
            _check_no_static_keys(env, profile)
            _check_sso_token(env, profile, settings)
    if profile is not None:
        _export_profile(env, profile)
    if passthrough:
        _log_region(env, aws_profile, settings)
    _process_profile = profile
    _prepared_for = cfg


def _profile_label(profile) -> str:
    return "no s3.profile" if profile is None else f"s3.profile {profile!r}"


def _check_same_profile(profile) -> None:
    if _process_profile is UNSET or _process_profile == profile:
        return
    raise RuntimeError(
        f"prytt: one AWS profile per process. This process started with "
        f"{_profile_label(_process_profile)}, now the config resolves to "
        f"{_profile_label(profile)}. "
        "Sail loads AWS config once per process, so it would keep the first identity. "
        "To fix: set the profile once for the whole process, before it starts (for example "
        "PRYTT_S3_PROFILE in the shell, in mise.toml [env] or in the CI job, not per test), "
        "or restart the process (or notebook kernel) to switch profiles."
    )


_STATIC_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")


def _check_no_static_keys(env, profile: str) -> None:
    found = " and ".join(name for name in _STATIC_KEYS if env.get(name))
    if found:
        raise ValueError(
            f"prytt config: {found} set in the environment with s3.profile {profile!r} "
            "and S3 passthrough in use: the AWS credential chain prefers the keys and "
            f"would ignore the profile. Unset {found}, or drop s3.profile "
            "(and PRYTT_S3_PROFILE)."
        )


def _export_profile(env, profile: str) -> None:
    shell = env.get("AWS_PROFILE")
    env["AWS_PROFILE"] = profile
    if shell and shell != profile:
        log("WARN", f"s3.profile {profile!r} overrides AWS_PROFILE={shell} from the environment")
    log("INFO", f"AWS_PROFILE={profile} (s3.profile)")


def _home(env):
    from pathlib import Path

    return Path(env["HOME"]) if env.get("HOME") else Path.home()


def _profile_settings(env, profile: str) -> dict:
    """``profile``'s settings in the AWS config file (``AWS_CONFIG_FILE``,
    else ``~/.aws/config``); ``{}`` if the file or profile is missing or
    can't be parsed."""
    import configparser

    config_file = env.get("AWS_CONFIG_FILE") or _home(env) / ".aws" / "config"
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(config_file, encoding="utf-8")
    except (configparser.Error, UnicodeDecodeError):
        return {}
    section = "default" if profile == "default" else f"profile {profile}"
    return dict(parser[section]) if parser.has_section(section) else {}


_REFRESH_KEYS = ("refreshToken", "clientId", "clientSecret", "registrationExpiresAt")


def _parse_utc(value):
    from datetime import UTC, datetime

    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _sdk_can_refresh(token: dict, now) -> bool:
    """Whether the AWS SDK refreshes this expired ``sso_session`` token itself.

    Both SDKs need all four of ``_REFRESH_KEYS`` and an unexpired client
    registration: the Rust SDK Sail uses (aws-config 1.13,
    ``sso/cache.rs`` ``CachedSsoToken::refreshable`` and ``sso/token.rs``
    ``SsoTokenProvider::resolve_token``: ``expired && !refreshable`` is
    ``ExpiredToken``; a missing ``registrationExpiresAt`` counts as expired)
    and botocore (``botocore.tokens.SSOTokenProvider._refresh_access_token``).
    Legacy ``sso_start_url`` profiles don't go through the token provider,
    so the caller only asks for ``sso_session`` profiles.
    """
    if not all(token.get(key) for key in _REFRESH_KEYS):
        return False
    try:
        return _parse_utc(token["registrationExpiresAt"]) > now
    except (TypeError, ValueError):
        return False


def _check_sso_token(env, profile: str, settings: dict) -> None:
    """Raise if ``profile`` is an SSO profile whose cached token is missing,
    or expired and not refreshable. Best effort: the cache layout belongs to
    the AWS CLI, so a cache prytt can't find, read or parse only logs a
    ``WARN``.

    Layout, as botocore (which the AWS CLI v2 shares) writes it and the Rust
    SDK reads it (aws-config ``sso/cache.rs`` ``cached_token_path``):
    ``<home>/.aws/sso/cache/<sha1 hex of the sso_session name, or the
    legacy sso_start_url>.json``, JSON with ``expiresAt`` as an ISO-8601
    UTC timestamp.
    """
    import hashlib
    import json
    from datetime import UTC, datetime

    session = settings.get("sso_session")
    key = session or settings.get("sso_start_url")
    if not key:
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
    try:
        with open(cache_file, encoding="utf-8") as fh:
            text = fh.read()
    except FileNotFoundError:
        raise RuntimeError(
            f"prytt: no SSO token for AWS profile {profile!r} (s3.profile); {login}"
        ) from None
    except OSError as exc:  # permission denied, a directory, ...: the SDK may still read it
        log(
            "WARN",
            f"can't read the SSO token cache {cache_file.as_posix()} for AWS profile "
            f"{profile!r} ({type(exc).__name__}: {exc}); skipping the SSO token check",
        )
        return
    try:
        token = json.loads(text)
        expires_at = _parse_utc(token["expiresAt"])
    except (ValueError, TypeError, KeyError) as exc:
        log(
            "WARN",
            f"can't read the SSO token cache {cache_file.as_posix()} for AWS profile "
            f"{profile!r} ({type(exc).__name__}: {exc}); skipping the SSO token check",
        )
        return
    now = datetime.now(UTC)
    if expires_at > now:
        return
    if session and _sdk_can_refresh(token, now):
        log(
            "WARN",
            f"the SSO token for AWS profile {profile!r} (s3.profile) expired at "
            f"{expires_at.isoformat()}; the AWS SDK will try to refresh it "
            f"(if S3 access fails, {login})",
        )
        return
    raise RuntimeError(
        f"prytt: the SSO token for AWS profile {profile!r} (s3.profile) expired at "
        f"{expires_at.isoformat()}; {login}"
    )


def _log_region(env, aws_profile: str, settings: dict) -> None:
    """For the reader only: Sail 0.7.3 discovers each plain ``s3://``
    bucket's region itself and uses a configured one only with a custom
    endpoint. Same order as Sail's sources for one: object_store
    ``AmazonS3Builder::from_env`` (``AWS_REGION`` wins over
    ``AWS_DEFAULT_REGION``), then aws-config's default region chain
    (``EnvironmentVariableRegionProvider``: ``AWS_REGION``, then
    ``AWS_DEFAULT_REGION``; then the profile's ``region``)."""
    for var in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        if env.get(var):
            log("INFO", f"S3 passthrough: region {env[var]} ({var})")
            return
    region = settings.get("region")
    if region:
        log("INFO", f"S3 passthrough: region {region} (AWS profile {aws_profile})")
    else:
        log("INFO", "S3 passthrough: no region set; bucket region auto-discovered by Sail")
