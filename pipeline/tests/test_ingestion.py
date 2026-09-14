from pathlib import Path
from uuid import uuid4

import polars as pl
import pytest

from lakehouse_pipeline.ingestion import (
    opportunity_status_group,
    resolve_source_path,
    scan_source,
    validate_frame,
    validate_source,
    write_staging_parquet,
)


@pytest.mark.parametrize(
    ("stage_name", "expected"),
    [
        ("Cobrada", "cobrada"),
        ("Perdida", "perdida"),
        ("Closed Lost", "perdida"),
        ("Rechazada", "perdida"),
        ("Comprometida", "pendiente"),
        ("Prospecting", "otro"),
    ],
)
def test_opportunity_status_group(stage_name, expected):
    assert opportunity_status_group(stage_name) == expected


def test_validate_frame_accepts_unique_case_insensitive_id():
    frame = pl.DataFrame({"ID": ["001", "002"], "NAME": ["A", "B"]})

    assert validate_frame("contacts", frame) == "ID"


def test_validate_frame_rejects_duplicate_ids():
    frame = pl.DataFrame({"ID": ["001", "001"]})

    with pytest.raises(ValueError, match="duplicate"):
        validate_frame("accounts", frame)


def test_resolve_source_path_rejects_path_traversal(tmp_path, monkeypatch):
    ingestion_root = tmp_path / "incoming"
    ingestion_root.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("ID\n001\n", encoding="utf-8")
    monkeypatch.setenv("INGESTION_ROOT", str(ingestion_root))

    with pytest.raises(ValueError, match="outside INGESTION_ROOT"):
        resolve_source_path(str(Path("..") / "outside.csv"))


def test_lazy_csv_validation_and_staging(tmp_path, monkeypatch):
    source = tmp_path / "contacts.csv"
    source.write_text("ID,NAME\n001,Ana\n002,Luis\n", encoding="utf-8")
    staging_root = tmp_path / "staging"
    monkeypatch.setenv("STAGING_PATH", str(staging_root))

    lazy_frame = scan_source(source)
    key_column, row_count = validate_source("contacts", lazy_frame)
    batch_id = uuid4()
    destination = write_staging_parquet("contacts", batch_id, lazy_frame)

    assert key_column == "ID"
    assert row_count == 2
    assert destination == staging_root / "contacts" / f"{batch_id}.parquet"
    assert pl.read_parquet(destination).to_dicts() == [
        {"ID": "001", "NAME": "Ana"},
        {"ID": "002", "NAME": "Luis"},
    ]
