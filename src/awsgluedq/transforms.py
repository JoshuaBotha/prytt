"""``awsgluedq.transforms`` stub (spec §9).

Glue Studio scripts insert ``EvaluateDataQuality``. Locally no rules run:
``apply`` logs a WARN naming the ruleset and returns the frame unchanged.
Other methods (``process_rows`` etc.) go through the ``LogAny`` catch-all.
Must not import pyspark: the frame can be any object.
"""

from prytt.stub import LogAny, log


def _ruleset_name(ruleset, publishing_options) -> str:
    context = (publishing_options or {}).get("dataQualityEvaluationContext")
    if context:
        return str(context)
    text = " ".join(str(ruleset or "").split())
    return text if len(text) <= 60 else text[:57] + "..."


class EvaluateDataQuality(LogAny):
    @classmethod
    def apply(cls, frame, ruleset=None, publishing_options=None, **kwargs):
        name = _ruleset_name(ruleset, publishing_options)
        log(
            "WARN",
            f"EvaluateDataQuality skipped: ruleset {name!r} not evaluated locally",
        )
        return frame
