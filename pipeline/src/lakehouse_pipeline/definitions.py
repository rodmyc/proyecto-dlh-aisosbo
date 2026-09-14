import dagster as dg

from lakehouse_pipeline.defs.bootstrap import staging_toolchain_check

defs = dg.Definitions(assets=[staging_toolchain_check])
