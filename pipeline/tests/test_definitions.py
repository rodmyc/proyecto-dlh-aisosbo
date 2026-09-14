from dagster import materialize

from lakehouse_pipeline.definitions import defs
from lakehouse_pipeline.defs.bootstrap import staging_toolchain_check


def test_staging_toolchain_check(tmp_path, monkeypatch):
    monkeypatch.setenv("STAGING_PATH", str(tmp_path))

    result = materialize([staging_toolchain_check])

    assert result.success
    assert (tmp_path / "toolchain_check.parquet").is_file()


def test_ingestion_job_is_registered():
    assert defs.get_job_def("ingest_export_job").name == "ingest_export_job"
