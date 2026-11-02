"""Imported from a .pth at interpreter start. Must stay cheap: stdlib only."""
import os, sys

if not os.environ.get("PRYTT_DISABLE"):
    from prytt_spike._hooks import install
    install()
