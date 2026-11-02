"""prytt's ``awsglue`` shim (spec §9). Exports ``DynamicFrame`` like the real package.

Importing ``awsglue`` does not import pyspark or start Sail.
"""

from awsglue.dynamicframe import DynamicFrame

__all__ = ["DynamicFrame"]
