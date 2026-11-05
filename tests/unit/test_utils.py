"""awsglue.utils.getResolvedOptions: real Glue argument parsing,
plus local defaults from config/env and a forgiving missing-option mode."""

import pytest

from awsglue.utils import GlueArgumentError, getResolvedOptions
from prytt import config


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, tmp_path):
    for name in (
        "CI",
        "PRYTT_STRICT",
        "PRYTT_LOG_LEVEL",
        "PRYTT_ARG_JOB_NAME",
        "PRYTT_ARG_source_path",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    config.reset()
    yield
    config.reset()


def strict(monkeypatch):
    monkeypatch.setenv("PRYTT_STRICT", "1")
    config.reset()


def test_space_separated_option_is_resolved():
    argv = ["script.py", "--JOB_NAME", "my-job"]

    assert getResolvedOptions(argv, ["JOB_NAME"]) == {"JOB_NAME": "my-job"}


def test_equals_form_is_resolved():
    argv = ["script.py", "--JOB_NAME=my-job", "--target=s3://b/out/"]

    assert getResolvedOptions(argv, ["JOB_NAME", "target"]) == {
        "JOB_NAME": "my-job",
        "target": "s3://b/out/",
    }


def test_extra_args_are_ignored_and_not_returned():
    argv = [
        "script.py",
        "--JOB_NAME",
        "j",
        "--job-bookmark-option",
        "job-bookmark-enable",
        "--enable-metrics",
        "positional",
    ]

    assert getResolvedOptions(argv, ["JOB_NAME"]) == {"JOB_NAME": "j"}


def test_first_arg_is_the_script_and_is_skipped():
    argv = ["--JOB_NAME", "x", "--JOB_NAME", "y"]

    assert getResolvedOptions(argv, ["JOB_NAME"]) == {"JOB_NAME": "y"}


def test_missing_option_returns_none_and_warns(capsys):
    argv = ["script.py", "--JOB_NAME", "j"]

    assert getResolvedOptions(argv, ["JOB_NAME", "source", "target"]) == {
        "JOB_NAME": "j",
        "source": None,
        "target": None,
    }
    err = capsys.readouterr().err
    assert "WARN" in err
    assert "--source" in err and "--target" in err


def test_missing_option_raises_in_strict_mode_like_glue(monkeypatch):
    strict(monkeypatch)
    argv = ["script.py", "--JOB_NAME", "j"]

    with pytest.raises(GlueArgumentError) as exc:
        getResolvedOptions(argv, ["JOB_NAME", "source", "target"])

    assert str(exc.value) == "the following arguments are required: --source, --target"


def test_option_falls_back_to_prytt_arg_env_var(monkeypatch, capsys):
    monkeypatch.setenv("PRYTT_ARG_JOB_NAME", "env-job")
    config.reset()

    assert getResolvedOptions(["script.py"], ["JOB_NAME"]) == {"JOB_NAME": "env-job"}
    assert "--JOB_NAME" in capsys.readouterr().err


def test_option_falls_back_to_config_args(tmp_path):
    (tmp_path / "prytt.toml").write_text(
        '[args]\nJOB_NAME = "cfg-job"\nsource_path = "s3://b/in/"\n'
    )
    config.reset()

    assert getResolvedOptions(["script.py"], ["JOB_NAME", "source-path"]) == {
        "JOB_NAME": "cfg-job",
        "source_path": "s3://b/in/",
    }


def test_command_line_beats_config_and_env(monkeypatch, tmp_path):
    (tmp_path / "prytt.toml").write_text('[args]\nJOB_NAME = "cfg-job"\n')
    monkeypatch.setenv("PRYTT_ARG_JOB_NAME", "env-job")
    config.reset()

    assert getResolvedOptions(["script.py", "--JOB_NAME", "cli"], ["JOB_NAME"]) == {
        "JOB_NAME": "cli"
    }


def test_config_args_satisfy_strict_mode(monkeypatch):
    monkeypatch.setenv("PRYTT_ARG_JOB_NAME", "env-job")
    strict(monkeypatch)

    assert getResolvedOptions(["script.py"], ["JOB_NAME"]) == {"JOB_NAME": "env-job"}


def test_hyphenated_option_is_keyed_by_argparse_dest():
    argv = ["script.py", "--source-path", "s3://b/in/"]

    assert getResolvedOptions(argv, ["source-path"]) == {"source_path": "s3://b/in/"}


def test_temp_dir_is_optional_and_not_warned(capsys):
    assert getResolvedOptions(["script.py"], ["TempDir"]) == {"TempDir": None}
    assert "WARN" not in capsys.readouterr().err


def test_redshift_temp_dir_falls_back_to_temp_dir():
    argv = ["script.py", "--TempDir", "s3://b/tmp/"]

    assert getResolvedOptions(argv, ["TempDir", "RedshiftTempDir"]) == {
        "TempDir": "s3://b/tmp/",
        "RedshiftTempDir": "s3://b/tmp/",
    }
