"""awsglue.job.Job (spec §9): init/commit log only; bookmarks are a no-op."""

import pytest

from prytt import config
from prytt.stub import GlueLocalUnsupported


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    config.reset()
    yield
    config.reset()


def strict(monkeypatch):
    monkeypatch.setenv("PRYTT_STRICT", "1")
    config.reset()


def test_init_logs_job_name(capsys):
    from awsglue.job import Job

    Job(object()).init("my-job", {"JOB_NAME": "my-job"})

    assert capsys.readouterr().err == "[prytt] INFO Job.init('my-job'), bookmarks off\n"


def test_commit_logs(capsys):
    from awsglue.job import Job

    job = Job(object())
    job.init("my-job", {})
    capsys.readouterr()

    job.commit()

    assert capsys.readouterr().err == "[prytt] INFO Job.commit('my-job')\n"


def test_init_and_commit_work_in_strict_mode(monkeypatch):
    from awsglue.job import Job

    strict(monkeypatch)
    job = Job(object())
    job.init("my-job", {})
    job.commit()


def test_args_default_to_empty(capsys):
    from awsglue.job import Job

    Job(object()).init("j")

    assert "Job.init('j')" in capsys.readouterr().err


def test_other_methods_hit_catch_all(capsys):
    from awsglue.job import Job

    job = Job(object())

    assert job.isInitialized() is job
    assert capsys.readouterr().err == "[prytt] STUB Job.isInitialized()\n"


def test_other_methods_raise_in_strict_mode(monkeypatch):
    from awsglue.job import Job

    strict(monkeypatch)

    with pytest.raises(GlueLocalUnsupported):
        Job(object()).isInitialized()
