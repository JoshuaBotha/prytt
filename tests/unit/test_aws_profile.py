"""AWS profile selection (``s3.profile`` / ``PRYTT_S3_PROFILE``) at session start.

Sessions here start on the shared in-process Sail server (pytest plugin
``spark``). No test reaches S3: a profile without static keys is only ever
used with no passthrough table to register, so Sail never loads AWS config.
"""

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


def test_the_same_profile_in_a_later_session_is_fine(project, monkeypatch, start_session):
    configure(project, monkeypatch, '[s3]\nprofile = "team-sso"\n')
    start_session()

    configure(project, monkeypatch, PRYTT_S3_PROFILE="team-sso")
    start_session()


def test_local_mode_logs_no_region(project, monkeypatch, start_session, capsys):
    configure(project, monkeypatch, '[s3]\nprofile = "team-sso"\n')

    start_session()

    assert region_lines(capsys.readouterr().err) == []
