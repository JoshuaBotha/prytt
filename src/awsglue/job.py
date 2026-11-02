"""``awsglue.job`` shim (spec §9). Bookmarks are a no-op locally.

Interface:

- ``Job(glue_context)``: keeps the context; touches nothing.
- ``Job.init(job_name, args={})``: logs ``INFO Job.init('<name>'), bookmarks off``.
- ``Job.commit()``: logs ``INFO Job.commit('<name>')``.
- Any other method (``isInitialized``, ``id``, ...) goes through the ``LogAny``
  catch-all: logs ``STUB`` and returns the job, or raises in strict mode.
  ``init`` and ``commit`` are real, so they never raise in strict mode.
"""

from prytt.stub import LogAny, log

__all__ = ["Job"]


class Job(LogAny):
    def __init__(self, glue_context):
        self._glue_context = glue_context
        self._job_name = None

    def init(self, job_name, args=None):
        self._job_name = job_name
        log("INFO", f"Job.init({job_name!r}), bookmarks off")

    def commit(self):
        log("INFO", f"Job.commit({self._job_name!r})")
