"""prytt.stub: [prytt] logging, the LogAny catch-all and strict mode."""

import pytest

from prytt import config
from prytt.stub import GlueLocalUnsupported, LogAny, log


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    config.reset()
    yield
    config.reset()


def set_env(monkeypatch, **values):
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    config.reset()


class Job(LogAny):
    def init(self, name, args):
        return "real"


def test_unknown_method_logs_stub_line_and_returns_self(capsys):
    job = Job()

    assert job.commit() is job

    assert capsys.readouterr().err == "[prytt] STUB Job.commit()\n"


def test_stub_line_shows_call_arguments(capsys):
    Job().purge_s3_path("s3://bucket/tmp/", options={"retentionPeriod": 0})

    assert capsys.readouterr().err == (
        "[prytt] STUB Job.purge_s3_path('s3://bucket/tmp/', options={'retentionPeriod': 0})\n"
    )


def test_unknown_calls_chain(capsys):
    job = Job()

    assert job.setLogLevel("WARN").addPyFile("x.py") is job
    assert capsys.readouterr().err.splitlines() == [
        "[prytt] STUB Job.setLogLevel('WARN')",
        "[prytt] STUB Job.addPyFile('x.py')",
    ]


def test_real_methods_are_untouched(capsys):
    assert Job().init("t", {}) == "real"
    assert capsys.readouterr().err == ""


def test_strict_mode_raises_instead_of_logging(monkeypatch, capsys):
    set_env(monkeypatch, PRYTT_STRICT="1")

    with pytest.raises(GlueLocalUnsupported, match=r"Job\.commit\(\).*PRYTT_STRICT=0"):
        Job().commit()


def test_ci_makes_catch_all_strict_by_default(monkeypatch):
    set_env(monkeypatch, CI="true")

    with pytest.raises(GlueLocalUnsupported):
        Job().commit()


def test_ci_with_explicit_non_strict_logs_and_continues(monkeypatch, capsys):
    set_env(monkeypatch, CI="true", PRYTT_STRICT="0")
    job = Job()

    assert job.commit() is job
    assert "STUB Job.commit()" in capsys.readouterr().err


def test_strict_mode_still_allows_attribute_lookup(monkeypatch):
    # Only the call raises, so hasattr()/getattr() probes keep working.
    set_env(monkeypatch, PRYTT_STRICT="1")

    assert callable(Job().anything)


def test_glue_local_unsupported_is_a_not_implemented_error():
    assert issubclass(GlueLocalUnsupported, NotImplementedError)


def test_dunder_lookups_are_not_intercepted():
    job = Job()

    assert not hasattr(job, "__iter__")
    assert not hasattr(job, "__array__")


def test_log_tags_are_aligned_like_the_spec_examples(capsys):
    log("MAP", "s3://raw-bucket/orders/ -> .prytt/s3/raw-bucket/orders/")
    log("WARN", "Filter.apply: not implemented, frame passed through unchanged")
    log("INFO", "sail at sc://127.0.0.1:50051")

    assert capsys.readouterr().err.splitlines() == [
        "[prytt] MAP  s3://raw-bucket/orders/ -> .prytt/s3/raw-bucket/orders/",
        "[prytt] WARN Filter.apply: not implemented, frame passed through unchanged",
        "[prytt] INFO sail at sc://127.0.0.1:50051",
    ]


def test_unknown_tag_is_rejected():
    with pytest.raises(ValueError):
        log("NOTE", "x")


def test_log_level_from_config_filters_lines(monkeypatch, capsys):
    set_env(monkeypatch, PRYTT_LOG_LEVEL="WARN")

    log("INFO", "hidden")
    Job().commit()
    log("WARN", "shown")

    assert capsys.readouterr().err == "[prytt] WARN shown\n"


def test_output_is_not_duplicated_through_root_logger(capsys):
    import logging

    root_handler = logging.StreamHandler()  # stderr, as basicConfig would add
    logging.getLogger().addHandler(root_handler)
    try:
        log("INFO", "once")
    finally:
        logging.getLogger().removeHandler(root_handler)

    assert capsys.readouterr().err == "[prytt] INFO once\n"


def test_a_non_raising_log_any_logs_even_in_strict_mode(monkeypatch, capsys):
    class Jvm(LogAny):
        raises_in_strict = False

    set_env(monkeypatch, PRYTT_STRICT="1")
    jvm = Jvm()

    assert jvm.hadoopConfiguration().set("k", "v") is jvm
    assert capsys.readouterr().err.splitlines() == [
        "[prytt] STUB Jvm.hadoopConfiguration()",
        "[prytt] STUB Jvm.set('k', 'v')",
    ]
