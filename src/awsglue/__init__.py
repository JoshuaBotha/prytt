"""prytt's ``awsglue`` shim. Exports ``DynamicFrame`` like the real package.

Importing ``awsglue`` does not import pyspark or start Sail.
"""

from awsglue.dynamicframe import DynamicFrame

__all__ = ["DynamicFrame"]
