"""Shared logging, the ``LogAny`` catch-all and strict mode (spec §9, §12).

Interface:

- ``log(tag, message)``: write ``[prytt] <TAG> <message>`` to stderr through
  the ``prytt`` logger. Tags: ``STUB`` (catch-all hit), ``MAP`` (path or
  catalog rewrite), ``WARN`` (known gap, data passed through), ``INFO``
  (server start, versions). ``WARN`` logs at WARNING, the rest at INFO;
  ``log_level`` from config filters them.
- ``stub_log(owner, name, args, kwargs, strict=True)``: log
  ``STUB Owner.name(args)``, or in strict mode raise ``GlueLocalUnsupported``
  instead. ``strict=False`` always logs (never raises).
- ``LogAny``: base class. Calling any method a subclass does not define goes
  through ``stub_log`` and returns ``self``, so chains keep going. Dunder
  lookups are not intercepted (they raise ``AttributeError`` as usual). A
  subclass that sets ``raises_in_strict = False`` never raises (for stand-ins
  whose every call is a known no-op, e.g. ``sc._jsc`` chains).
- ``first_line(exc) -> str``: the first line of an exception's message (or
  its type name), for one-line ``WARN`` logs.
- ``GlueLocalUnsupported``: raised for calls prytt cannot fake (strict-mode
  gaps, RDD APIs). Subclass of ``NotImplementedError``.
- ``get_logger()``: the configured ``prytt`` logger, for anything else.
"""

import logging
import reprlib
import sys

from prytt import config

TAGS = ("STUB", "MAP", "WARN", "INFO")
_LEVELS = {"WARN": logging.WARNING}


class GlueLocalUnsupported(NotImplementedError):
    """A Glue/Spark feature prytt does not support locally."""


class _StderrHandler(logging.StreamHandler):
    """Writes to whatever ``sys.stderr`` is at emit time (works with capture)."""

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, value):
        pass


_logger = logging.getLogger("prytt")
_configured_for = None  # the Config whose log_level was last applied


def get_logger() -> logging.Logger:
    global _configured_for
    if not _logger.handlers:
        handler = _StderrHandler()
        handler.setFormatter(logging.Formatter("[prytt] %(message)s"))
        _logger.addHandler(handler)
        _logger.propagate = False
    cfg = config.get()
    if cfg is not _configured_for:
        _logger.setLevel(cfg.log_level)
        _configured_for = cfg
    return _logger


def log(tag: str, message: str) -> None:
    if tag not in TAGS:
        raise ValueError(f"unknown prytt log tag {tag!r}; use one of {TAGS}")
    get_logger().log(_LEVELS.get(tag, logging.INFO), "%-4s %s", tag, message)


_repr = reprlib.Repr()
_repr.maxstring = 80
_repr.maxother = 80


def _call_text(owner: str, name: str, args, kwargs) -> str:
    parts = [_repr.repr(a) for a in args]
    parts += [f"{k}={_repr.repr(v)}" for k, v in kwargs.items()]
    return f"{owner}.{name}({', '.join(parts)})"


def stub_log(owner: str, name: str, args=(), kwargs=None, strict: bool = True) -> None:
    call = _call_text(owner, name, args, kwargs or {})
    if strict and config.get().strict:
        raise GlueLocalUnsupported(
            f"{call} is not implemented in prytt (strict mode; "
            "set PRYTT_STRICT=0 to log and continue)"
        )
    log("STUB", call)


def first_line(exc) -> str:
    return (str(exc).strip().splitlines() or [type(exc).__name__])[0]


class LogAny:
    raises_in_strict = True

    def __getattr__(self, name):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)

        def _noop(*args, **kwargs):
            stub_log(type(self).__name__, name, args, kwargs, strict=self.raises_in_strict)
            return self

        return _noop
