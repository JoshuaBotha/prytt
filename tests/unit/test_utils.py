"""awsglue.utils.getResolvedOptions (spec §9): real Glue argument parsing."""

import pytest

from awsglue.utils import GlueArgumentError, getResolvedOptions


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
        "--JOB_NAME", "j",
        "--job-bookmark-option", "job-bookmark-enable",
        "--enable-metrics",
        "positional",
    ]

    assert getResolvedOptions(argv, ["JOB_NAME"]) == {"JOB_NAME": "j"}


def test_first_arg_is_the_script_and_is_skipped():
    argv = ["--JOB_NAME", "x", "--JOB_NAME", "y"]

    assert getResolvedOptions(argv, ["JOB_NAME"]) == {"JOB_NAME": "y"}


def test_missing_option_raises_glue_argument_error_naming_it():
    argv = ["script.py", "--JOB_NAME", "j"]

    with pytest.raises(GlueArgumentError) as exc:
        getResolvedOptions(argv, ["JOB_NAME", "source", "target"])

    assert str(exc.value) == "the following arguments are required: --source, --target"


def test_missing_job_name_has_no_hidden_default():
    with pytest.raises(GlueArgumentError, match="--JOB_NAME"):
        getResolvedOptions(["script.py"], ["JOB_NAME"])


def test_hyphenated_option_is_keyed_by_argparse_dest():
    argv = ["script.py", "--source-path", "s3://b/in/"]

    assert getResolvedOptions(argv, ["source-path"]) == {"source_path": "s3://b/in/"}


def test_temp_dir_is_optional():
    assert getResolvedOptions(["script.py"], ["TempDir"]) == {"TempDir": None}


def test_redshift_temp_dir_falls_back_to_temp_dir():
    argv = ["script.py", "--TempDir", "s3://b/tmp/"]

    assert getResolvedOptions(argv, ["TempDir", "RedshiftTempDir"]) == {
        "TempDir": "s3://b/tmp/",
        "RedshiftTempDir": "s3://b/tmp/",
    }
