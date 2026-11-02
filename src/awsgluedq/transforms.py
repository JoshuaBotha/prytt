"""``awsgluedq.transforms`` stub.

Glue Studio scripts insert ``EvaluateDataQuality``. Locally no rules run:

- ``EvaluateDataQuality.apply(frame, ruleset, publishing_options)``: logs a
  WARN naming the ruleset and returns the frame unchanged.
- ``EvaluateDataQuality().process_rows(frame, ruleset, publishing_options,
  additional_options)``: logs a WARN and returns a ``DynamicFrameCollection``
  whose ``originalData`` and ``rowLevelOutcomes`` are both the input frame
  (no outcome columns are added), so scripts that select from it keep going.
  Never raises in strict mode.
- Other methods go through the ``LogAny`` catch-all.

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

    def process_rows(self, frame, ruleset=None, publishing_options=None, additional_options=None, **kwargs):
        from awsglue.dynamicframe import DynamicFrameCollection

        name = _ruleset_name(ruleset, publishing_options)
        log(
            "WARN",
            f"EvaluateDataQuality.process_rows skipped: ruleset {name!r} not evaluated locally",
        )
        frames = {"originalData": frame, "rowLevelOutcomes": frame}
        return DynamicFrameCollection(frames, getattr(frame, "glue_ctx", None))
