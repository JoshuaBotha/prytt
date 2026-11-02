# T4 GlueContext + DynamicFrame + reader/writer + S3 mapping

Status: ready-for-agent
Blocked by: 02

Spec: §9 GlueContext, DynamicFrame, DynamicFrameCollection, format mapping; §10 S3; §11 s3.* keys.

## Scope
- `prytt/paths.py`: s3/s3a/s3n → `<s3.root>/bucket/key` (MAP log), passthrough mode (region check, WARN best-effort), local dir creation on write. Leave a seam for T5 catalog resolution.
- `awsglue/context.py` GlueContext: spark_session, create_dynamic_frame/create_data_frame/write_dynamic_frame/write_data_frame `.from_options` (s3); `.from_catalog` may raise unsupported/placeholder until T5; get_logger; SparkSession delegation; catch-all.
- `awsglue/dynamicframe.py`: DynamicFrame per §9 table (filter/map v1 = WARN pass-through), DynamicFrameCollection.
- `awsglue/__init__.py` exports DynamicFrame; `awsglue/gluetypes.py` minimal; other stub modules (data_sink, data_source, ...).

## Acceptance
- Tests: JSON/CSV read from mapped s3 path, parquet write append + partitionKeys, format/option mapping, orc raises, DynamicFrame real methods, resolveChoice cast.

