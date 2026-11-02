"""awsgluedq.transforms.EvaluateDataQuality stub (spec §9)."""

import pytest

from prytt import config

RULESET = 'Rules = [ ColumnCount > 0, IsComplete "id" ]'


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    config.reset()
    yield
    config.reset()


def test_apply_returns_the_same_frame_and_logs_ruleset_name(capsys):
    from awsgluedq.transforms import EvaluateDataQuality

    frame = object()

    result = EvaluateDataQuality.apply(
        frame=frame,
        ruleset=RULESET,
        publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1"},
    )

    assert result is frame
    assert capsys.readouterr().err == (
        "[prytt] WARN EvaluateDataQuality skipped: ruleset "
        "'EvaluateDataQuality_node1' not evaluated locally\n"
    )


def test_apply_without_evaluation_context_names_the_ruleset_text(capsys):
    from awsgluedq.transforms import EvaluateDataQuality

    frame = object()

    assert EvaluateDataQuality.apply(frame, RULESET) is frame
    assert capsys.readouterr().err == (
        "[prytt] WARN EvaluateDataQuality skipped: ruleset "
        "'Rules = [ ColumnCount > 0, IsComplete \"id\" ]' not evaluated locally\n"
    )


def test_process_rows_goes_through_catch_all(capsys):
    from awsgluedq.transforms import EvaluateDataQuality

    dq = EvaluateDataQuality()

    assert dq.process_rows(frame="f", ruleset="r") is dq
    assert capsys.readouterr().err == (
        "[prytt] STUB EvaluateDataQuality.process_rows(frame='f', ruleset='r')\n"
    )


def test_import_does_not_pull_in_pyspark():
    import subprocess
    import sys

    code = "import sys, awsgluedq.transforms; print('pyspark' in sys.modules)"
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "False"
