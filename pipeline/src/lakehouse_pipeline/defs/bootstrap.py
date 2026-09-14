from pathlib import Path

import dagster as dg
import polars as pl


@dg.asset(group_name="bootstrap", kinds={"polars", "parquet"})
def staging_toolchain_check(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Materialize a tiny Parquet file to verify the local ELT toolchain."""
    staging_dir = Path(dg.EnvVar("STAGING_PATH").get_value("../data/staging"))
    staging_dir.mkdir(parents=True, exist_ok=True)
    destination = staging_dir / "toolchain_check.parquet"

    frame = pl.DataFrame(
        {
            "component": ["dagster", "polars", "parquet"],
            "status": ["ready", "ready", "ready"],
        }
    )
    frame.write_parquet(destination)

    context.log.info("Wrote toolchain check to %s", destination.resolve())
    return dg.MaterializeResult(
        metadata={
            "path": dg.MetadataValue.path(str(destination.resolve())),
            "rows": frame.height,
        }
    )
