"""AWS profile selection (``s3.profile`` / ``PRYTT_S3_PROFILE``) at session start.

Sessions here start on the shared in-process Sail server (pytest plugin
``spark``). No test reaches S3: a profile without static keys is only ever
used with no passthrough table to register, so Sail never loads AWS config.
"""

import json
import os

import pytest

from prytt import aws, config

AWS_VARS = (
    "AWS_PROFILE",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)


@pytest.fixture(autouse=True)
def project(monkeypatch, tmp_path, spark):
    """cwd is a project dir, no AWS env, an empty AWS config file, and a fresh
    one-profile-per-process record (restored afterwards, so the profile a test
    picks never constrains the next one)."""
    for name in (
        "CI",
        "PRYTT_STRICT",
        "PRYTT_LOG_LEVEL",
        "PRYTT_S3_MODE",
        "PRYTT_S3_PROFILE",
        *AWS_VARS,
    ):
        monkeypatch.delenv(name, raising=False)
    aws_config = tmp_path / "aws-config"
    aws_config.write_text("")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(aws_config))
    monkeypatch.setattr(aws, "_process_profile", aws.UNSET)
    monkeypatch.chdir(tmp_path)
    config.reset()
    yield tmp_path
    config.reset()


def configure(project, monkeypatch, toml="", **env):
    (project / "prytt.toml").write_text(toml)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    config.reset()


@pytest.fixture
def start_session():
    """Start a new session (``builder.create()``); every one is stopped afterwards."""
    from pyspark.sql import SparkSession

    sessions = []

    def start():
        sessions.append(SparkSession.builder.create())
        return sessions[-1]

    yield start
    for session in sessions:
        session.stop()


@pytest.mark.parametrize("mode", ["local", "passthrough"])
def test_profile_is_exported_and_logged_in_every_mode(
    project, monkeypatch, start_session, capsys, mode
):
    configure(project, monkeypatch, f'[s3]\nmode = "{mode}"\nprofile = "team-sso"\n')

    start_session()

    assert os.environ["AWS_PROFILE"] == "team-sso"
    assert "INFO AWS_PROFILE=team-sso" in capsys.readouterr().err


def test_a_different_shell_profile_is_overridden_with_a_warn_naming_both(
    project, monkeypatch, start_session, capsys
):
    configure(project, monkeypatch, '[s3]\nprofile = "team-sso"\n', AWS_PROFILE="personal")

    start_session()

    assert os.environ["AWS_PROFILE"] == "team-sso"
    warns = [line for line in capsys.readouterr().err.splitlines() if " WARN " in line]
    assert len(warns) == 1
    assert "personal" in warns[0] and "team-sso" in warns[0]


def test_the_same_shell_profile_is_not_warned_about(project, monkeypatch, start_session, capsys):
    configure(project, monkeypatch, '[s3]\nprofile = "team-sso"\n', AWS_PROFILE="team-sso")

    start_session()

    assert " WARN " not in capsys.readouterr().err


def test_without_a_profile_aws_env_is_untouched(project, monkeypatch, start_session, capsys):
    shell = {
        "AWS_PROFILE": "personal",
        "AWS_REGION": "eu-west-1",
        "AWS_ACCESS_KEY_ID": "prytt-test",
        "AWS_SECRET_ACCESS_KEY": "prytt-test",
    }
    configure(project, monkeypatch, '[s3]\nmode = "passthrough"\n', **shell)

    start_session()

    assert {name: os.environ.get(name) for name in AWS_VARS} == {
        **dict.fromkeys(AWS_VARS),
        **shell,
    }
    assert "AWS_PROFILE" not in capsys.readouterr().err


PASSTHROUGH_TABLE = """
[catalog."ref_db.prices"]
path = "s3://ref/prices/"
format = "json"
mode = "passthrough"
"""


@pytest.mark.parametrize(
    "toml",
    [
        '[s3]\nmode = "passthrough"\nprofile = "team-sso"\n',
        '[s3]\nprofile = "team-sso"\n' + PASSTHROUGH_TABLE,
    ],
    ids=["global", "per-table"],
)
@pytest.mark.parametrize("key", ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"])
def test_static_keys_with_a_profile_and_passthrough_in_use_raise(
    project, monkeypatch, start_session, toml, key
):
    configure(project, monkeypatch, toml, **{key: "prytt-test"})

    with pytest.raises(ValueError, match=rf"{key}.*team-sso") as info:
        start_session()

    assert f"Unset {key}" in str(info.value)
    assert "drop s3.profile" in str(info.value)
    assert "AWS_PROFILE" not in os.environ  # raised before exporting anything


def test_static_keys_with_a_profile_in_local_mode_are_fine(project, monkeypatch, start_session):
    configure(
        project,
        monkeypatch,
        '[s3]\nprofile = "team-sso"\n',
        AWS_ACCESS_KEY_ID="prytt-test",
        AWS_SECRET_ACCESS_KEY="prytt-test",
    )

    start_session()

    assert os.environ["AWS_PROFILE"] == "team-sso"


@pytest.fixture
def no_real_aws(monkeypatch):
    """A passthrough table is read at registration: fake keys and a closed
    local port make Sail fail fast instead of reaching AWS."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prytt-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prytt-test")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


def region_lines(err):
    return [line for line in err.splitlines() if "region" in line]


@pytest.mark.parametrize(
    "toml", ['[s3]\nmode = "passthrough"\n', PASSTHROUGH_TABLE], ids=["global", "per-table"]
)
def test_passthrough_without_a_region_starts_and_logs_auto_discovery(
    project, monkeypatch, start_session, no_real_aws, capsys, toml
):
    configure(project, monkeypatch, toml)

    start_session()

    lines = region_lines(capsys.readouterr().err)
    assert len(lines) == 1
    assert "INFO" in lines[0] and "auto-discovered by Sail" in lines[0]


def test_region_comes_from_the_configured_profile_not_the_shell_one(
    project, monkeypatch, start_session, capsys
):
    (project / "aws-config").write_text(
        "[profile personal]\nregion = us-east-1\n[profile team-sso]\nregion = af-south-1\n"
    )
    configure(
        project,
        monkeypatch,
        '[s3]\nmode = "passthrough"\nprofile = "team-sso"\n',
        AWS_PROFILE="personal",
    )

    start_session()

    lines = region_lines(capsys.readouterr().err)
    assert len(lines) == 1
    assert "INFO" in lines[0] and "af-south-1" in lines[0] and "team-sso" in lines[0]


def test_region_from_the_env_wins_and_is_logged(project, monkeypatch, start_session, capsys):
    (project / "aws-config").write_text("[profile team-sso]\nregion = af-south-1\n")
    configure(
        project,
        monkeypatch,
        '[s3]\nmode = "passthrough"\nprofile = "team-sso"\n',
        AWS_REGION="eu-west-1",
    )

    start_session()

    lines = region_lines(capsys.readouterr().err)
    assert len(lines) == 1
    assert "eu-west-1" in lines[0] and "AWS_REGION" in lines[0]


def test_a_different_profile_in_a_later_session_raises(project, monkeypatch, start_session):
    configure(project, monkeypatch, '[s3]\nprofile = "team-sso"\n')
    start_session()

    configure(project, monkeypatch, PRYTT_S3_PROFILE="other")
    with pytest.raises(RuntimeError, match=r"one AWS profile per process.*team-sso.*other"):
        start_session()

    assert os.environ["AWS_PROFILE"] == "team-sso"


def test_the_one_profile_error_says_how_to_fix_it(project, monkeypatch, start_session):
    configure(project, monkeypatch)
    start_session()

    configure(project, monkeypatch, PRYTT_S3_PROFILE="team-sso")
    with pytest.raises(RuntimeError) as excinfo:
        start_session()

    message = str(excinfo.value)
    assert "no s3.profile" in message
    assert "PRYTT_S3_PROFILE" in message and "mise.toml" in message and "CI" in message
    assert "restart" in message and "kernel" in message


def test_the_same_profile_in_a_later_session_is_fine(project, monkeypatch, start_session):
    configure(project, monkeypatch, '[s3]\nprofile = "team-sso"\n')
    start_session()

    configure(project, monkeypatch, PRYTT_S3_PROFILE="team-sso")
    start_session()


def test_local_mode_logs_no_region(project, monkeypatch, start_session, capsys):
    configure(project, monkeypatch, '[s3]\nprofile = "team-sso"\n')

    start_session()

    assert region_lines(capsys.readouterr().err) == []


SSO_SESSION_PROFILE = """
[profile team-sso]
sso_session = corp
sso_account_id = 111122223333
sso_role_name = ReadOnly
[sso-session corp]
sso_start_url = https://corp.awsapps.com/start
sso_region = eu-west-1
"""

LEGACY_SSO_PROFILE = """
[profile team-sso]
sso_start_url = https://legacy.awsapps.com/start
sso_region = eu-west-1
sso_account_id = 111122223333
sso_role_name = ReadOnly
"""

# The AWS CLI names a token's cache file by the SHA1 hex of the sso-session
# name, or of the start URL for a legacy profile. Literals (from `shasum`),
# so the tests don't re-derive the key the way prytt does.
CACHE_FILES = {
    "sso-session": "ee0bfd2552fbd840c02cc48b6e823320543c450f.json",  # "corp"
    "legacy": "79e435d7a515078e81c9dffc35f38d5687ebd3a7.json",  # the start URL
}
SSO_PROFILES = pytest.mark.parametrize(
    ("aws_config", "kind"),
    [(SSO_SESSION_PROFILE, "sso-session"), (LEGACY_SSO_PROFILE, "legacy")],
    ids=["sso-session", "legacy"],
)
SSO_PASSTHROUGH = '[s3]\nmode = "passthrough"\nprofile = "team-sso"\n'


@pytest.fixture
def sso_home(project, monkeypatch):
    """A tmp ``HOME`` with an empty AWS CLI SSO cache dir."""
    home = project / "home"
    (home / ".aws" / "sso" / "cache").mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    return home


def cache_file(home, kind):
    return home / ".aws" / "sso" / "cache" / CACHE_FILES[kind]


def write_token(home, kind, expires_at):
    cache_file(home, kind).write_text(
        json.dumps({"accessToken": "prytt-test", "expiresAt": expires_at})
    )


@SSO_PROFILES
def test_an_sso_profile_with_an_expired_token_raises_with_the_login_command(
    project, monkeypatch, start_session, sso_home, aws_config, kind
):
    (project / "aws-config").write_text(aws_config)
    write_token(sso_home, kind, "2020-01-01T00:00:00Z")
    configure(project, monkeypatch, SSO_PASSTHROUGH)

    with pytest.raises(RuntimeError, match="expired") as info:
        start_session()

    assert "run: aws sso login --profile team-sso" in str(info.value)


@SSO_PROFILES
def test_an_sso_profile_with_an_unexpired_token_starts(
    project, monkeypatch, start_session, sso_home, capsys, aws_config, kind
):
    (project / "aws-config").write_text(aws_config)
    write_token(sso_home, kind, "2999-01-01T00:00:00Z")
    configure(project, monkeypatch, SSO_PASSTHROUGH)

    start_session()

    assert " WARN " not in capsys.readouterr().err


@SSO_PROFILES
def test_an_sso_profile_without_a_cached_token_raises_with_the_login_command(
    project, monkeypatch, start_session, sso_home, aws_config, kind
):
    (project / "aws-config").write_text(aws_config)
    configure(project, monkeypatch, SSO_PASSTHROUGH)

    with pytest.raises(RuntimeError, match="no SSO token") as info:
        start_session()

    assert "run: aws sso login --profile team-sso" in str(info.value)


def test_a_token_for_another_sso_session_does_not_count(
    project, monkeypatch, start_session, sso_home
):
    (project / "aws-config").write_text(SSO_SESSION_PROFILE)
    write_token(sso_home, "legacy", "2999-01-01T00:00:00Z")
    configure(project, monkeypatch, SSO_PASSTHROUGH)

    with pytest.raises(RuntimeError, match="aws sso login --profile team-sso"):
        start_session()


@pytest.mark.parametrize(
    "content",
    ["{not json", '{"accessToken": "prytt-test"}', '{"expiresAt": "next tuesday"}'],
    ids=["corrupt", "no-expiry", "bad-expiry"],
)
def test_an_unreadable_cached_token_warns_and_the_session_starts(
    project, monkeypatch, start_session, sso_home, capsys, content
):
    (project / "aws-config").write_text(SSO_SESSION_PROFILE)
    cache_file(sso_home, "sso-session").write_text(content)
    configure(project, monkeypatch, SSO_PASSTHROUGH)

    start_session()

    warns = [line for line in capsys.readouterr().err.splitlines() if " WARN " in line]
    assert len(warns) == 1
    assert "SSO token" in warns[0] and "team-sso" in warns[0]


def test_no_sso_cache_dir_warns_and_the_session_starts(project, monkeypatch, start_session, capsys):
    monkeypatch.setenv("HOME", str(project / "empty-home"))
    (project / "aws-config").write_text(SSO_SESSION_PROFILE)
    configure(project, monkeypatch, SSO_PASSTHROUGH)

    start_session()

    warns = [line for line in capsys.readouterr().err.splitlines() if " WARN " in line]
    assert len(warns) == 1
    assert "SSO token cache" in warns[0] and "aws sso login --profile team-sso" in warns[0]


@pytest.mark.parametrize(
    ("aws_config", "toml"),
    [
        ("[profile team-sso]\nregion = eu-west-1\n", SSO_PASSTHROUGH),
        (SSO_SESSION_PROFILE, '[s3]\nprofile = "team-sso"\n'),
    ],
    ids=["non-sso-profile", "local-mode"],
)
def test_no_sso_check_for_a_non_sso_profile_or_without_passthrough(
    project, monkeypatch, start_session, sso_home, capsys, aws_config, toml
):
    """The cache is empty, so any check would raise."""
    (project / "aws-config").write_text(aws_config)
    configure(project, monkeypatch, toml)

    start_session()

    assert " WARN " not in capsys.readouterr().err
