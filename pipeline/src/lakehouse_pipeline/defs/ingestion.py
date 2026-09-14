import dagster as dg

from lakehouse_pipeline.ingestion import SUPPORTED_DATASETS, ingest_file


@dg.op(
    config_schema={
        "dataset": dg.Field(str, description="Logical dataset selected in the upload UI."),
        "file_path": dg.Field(
            str,
            description="Path relative to INGESTION_ROOT; absolute paths are rejected.",
        ),
    },
    retry_policy=dg.RetryPolicy(max_retries=1, delay=30),
    tags={"kind": "polars,postgres,parquet"},
)
def ingest_export(context: dg.OpExecutionContext) -> dict[str, object]:
    dataset = context.op_config["dataset"]
    if dataset not in SUPPORTED_DATASETS:
        raise dg.Failure(
            f"Unsupported dataset {dataset!r}. Expected one of {sorted(SUPPORTED_DATASETS)}"
        )

    result = ingest_file(dataset=dataset, file_path=context.op_config["file_path"])
    context.add_output_metadata(
        {
            "batch_id": result.batch_id,
            "dataset": result.dataset,
            "source_filename": result.source_filename,
            "status": result.status,
            "rows_read": result.rows_read,
            "rows_inserted": result.rows_inserted,
            "rows_updated": result.rows_updated,
            "rows_unchanged": result.rows_unchanged,
            "rows_rejected": result.rows_rejected,
            "staging_path": result.staging_path or "already staged",
        }
    )
    return result.to_dict()


@dg.job(description="Validate, stage and UPSERT one Salesforce or historical export.")
def ingest_export_job() -> None:
    ingest_export()
