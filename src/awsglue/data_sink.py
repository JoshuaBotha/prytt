"""``awsglue.data_sink`` stub (spec §5). ``DataSink`` (from
``GlueContext.getSink``) is a ``LogAny``: every call logs ``STUB`` and returns
the sink. Use ``write_dynamic_frame.from_options`` for real writes.
"""

from prytt.stub import LogAny

__all__ = ["DataSink"]


class DataSink(LogAny):
    def __init__(self, j_sink=None, sql_ctx=None):
        self._sql_ctx = sql_ctx
