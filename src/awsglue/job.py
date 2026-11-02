"""``awsglue.job`` shim (spec §9). Bookmarks are a no-op locally.

Interface:

- ``Job(glue_context)``: keeps the context; touches nothing.
- ``Job.init(job_name, args={})``: logs ``INFO Job.init('<name>'), bookmarks off``.
- ``Job.commit()``: logs ``INFO Job.commit('<name>')``.
- ``Job.isInitialized()``: ``True`` after ``init``, else ``False``; logs ``INFO``.
- Any other method (``id``, ...) goes through the ``LogAny`` catch-all: logs
  ``STUB`` and returns the job, or raises in strict mode. ``init``,
  ``commit`` and ``isInitialized`` are real, so they never raise in strict mode.
"""

from prytt.stub import LogAny, log

__all__ = ["Job"]


class Job(LogAny):
    def __init__(self, glue_context):
        self._glue_context = glue_context
        self._job_name = None
        self._initialized = False

    def init(self, job_name, args=None):
        self._job_name = job_name
        self._initialized = True
        log("INFO", f"Job.init({job_name!r}), bookmarks off")

    def commit(self):
        log("INFO", f"Job.commit({self._job_name!r})")

    def isInitialized(self):
        log("INFO", f"Job.isInitialized() -> {self._initialized}")
        return self._initialized
