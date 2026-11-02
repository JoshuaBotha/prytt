"""prytt: run AWS Glue job scripts locally on Sail.

PYTEST_DONT_REWRITE: ``prytt.pth`` imports this package at interpreter start,
before pytest can install its assertion-rewriting hook for our ``pytest11``
plugin; this marker silences pytest's "already imported" warning.
"""
