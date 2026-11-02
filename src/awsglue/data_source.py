"""``awsglue.data_source`` stub (spec §5). ``DataSource`` (from
``GlueContext.getSource``) is a ``LogAny``: every call logs ``STUB`` and returns
the source. Use ``create_dynamic_frame.from_options`` for real reads.
"""

from prytt.stub import LogAny

__all__ = ["DataSource"]


class DataSource(LogAny):
    def __init__(self, j_source=None, sql_ctx=None, name=None):
        self._sql_ctx = sql_ctx
        self.name = name
