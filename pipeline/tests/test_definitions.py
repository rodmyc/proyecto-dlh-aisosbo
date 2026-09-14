from dagster import materialize

from lakehouse_pipeline.defs.bootstrap import staging_toolchain_check


def test_staging_toolchain_check(tmp_path, monkeypatch):
    monkeypatch.setenv("STAGING_PATH", str(tmp_path))

    result = materialize([staging_toolchain_check])

    assert result.success
    assert (tmp_path / "toolchain_check.parquet").is_file()
