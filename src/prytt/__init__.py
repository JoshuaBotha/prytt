"""prytt: run AWS Glue job scripts locally on Sail.

PYTEST_DONT_REWRITE: ``prytt.pth`` imports this package at interpreter start,
before pytest can install its assertion-rewriting hook for our ``pytest11``
plugin; this marker silences pytest's "already imported" warning.
"""


def __getattr__(name):
    # Lazy: this package is imported at every interpreter start (prytt.pth),
    # and prytt.s3lock pulls in logging and config.
    if name == "PryttS3WriteBlocked":
        from prytt.s3lock import PryttS3WriteBlocked

        return PryttS3WriteBlocked
    raise AttributeError(f"module 'prytt' has no attribute {name!r}")
