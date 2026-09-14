import dagster as dg

from lakehouse_pipeline.defs.bootstrap import staging_toolchain_check
from lakehouse_pipeline.defs.ingestion import ingest_export_job

defs = dg.Definitions(assets=[staging_toolchain_check], jobs=[ingest_export_job])
