from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import polars as pl
import psycopg

SUPPORTED_DATASETS = {
    "campaigns",
    "contacts",
    "accounts",
    "recurring_donations",
    "opportunities",
    "historical_donations",
}

GENERIC_SALESFORCE_DATASETS = SUPPORTED_DATASETS - {
    "opportunities",
    "historical_donations",
}


@dataclass(frozen=True)
class IngestionResult:
    batch_id: str
    dataset: str
    source_filename: str
    staging_path: str | None
    status: str
    rows_read: int
    rows_inserted: int
    rows_updated: int
    rows_unchanged: int
    rows_rejected: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def postgres_connection_options() -> dict[str, Any]:
    return {
        "host": os.environ["DAGSTER_PG_HOST"],
        "port": int(os.getenv("DAGSTER_PG_PORT", "5432")),
        "dbname": os.environ["DAGSTER_PG_DB"],
        "user": os.environ["DAGSTER_PG_USER"],
        "password": os.environ["DAGSTER_PG_PASSWORD"],
        "connect_timeout": 10,
    }


def resolve_source_path(file_path: str) -> tuple[Path, Path]:
    ingestion_root = Path(os.getenv("INGESTION_ROOT", "../exportaciones")).resolve()
    candidate = (ingestion_root / file_path).resolve()
    if not candidate.is_relative_to(ingestion_root):
        raise ValueError("The requested file is outside INGESTION_ROOT")
    if not candidate.is_file():
        raise FileNotFoundError(f"Input file does not exist: {file_path}")
    return ingestion_root, candidate


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def scan_source(path: Path) -> pl.LazyFrame:
    if path.suffix.casefold() == ".csv":
        return pl.scan_csv(
            path,
            encoding="utf8",
            infer_schema=False,
            try_parse_dates=False,
            null_values=[""],
        )
    if path.suffix.casefold() == ".parquet":
        return pl.scan_parquet(path)
    raise ValueError("Only CSV and Parquet files are supported")


def validate_source(dataset: str, frame: pl.LazyFrame) -> tuple[str, int]:
    if dataset not in SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")
    columns = {column.casefold(): column for column in frame.collect_schema().names()}
    key_name = "id_donacion" if dataset == "historical_donations" else "id"
    if key_name not in columns:
        raise ValueError(f"Required key column is missing: {key_name}")
    if dataset == "opportunities" and "stagename" not in columns:
        raise ValueError("Required opportunity column is missing: STAGENAME")
    if dataset == "historical_donations" and "fecha_cobro" not in columns:
        raise ValueError("Required historical column is missing: fecha_cobro")

    key_column = columns[key_name]
    metrics = (
        frame.select(
            pl.len().alias("rows"),
            pl.col(key_column).null_count().alias("null_count"),
            pl.col(key_column).n_unique().alias("unique_count"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    row_count = metrics["rows"]
    null_count = metrics["null_count"]
    unique_count = metrics["unique_count"]
    if row_count == 0:
        raise ValueError("The input file contains no rows")
    if null_count:
        raise ValueError(f"The key column contains {null_count} null values")
    if unique_count != row_count:
        raise ValueError(f"The key column contains {row_count - unique_count} duplicate rows")
    return key_column, row_count


def validate_frame(dataset: str, frame: pl.DataFrame) -> str:
    key_column, _ = validate_source(dataset, frame.lazy())
    return key_column


def write_staging_parquet(dataset: str, batch_id: UUID, frame: pl.LazyFrame) -> Path:
    staging_root = Path(os.getenv("STAGING_PATH", "../data/staging")).resolve()
    destination = staging_root / dataset / f"{batch_id}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    frame.sink_parquet(
        destination,
        compression="zstd",
        statistics=True,
        row_group_size=100_000,
        engine="streaming",
    )
    return destination


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _payload_and_hash(row: dict[str, Any]) -> tuple[str, str]:
    payload = json.dumps(
        row,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )
    return payload, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _column_lookup(frame: pl.DataFrame) -> dict[str, str]:
    return {column.upper(): column for column in frame.columns}


def _value(row: dict[str, Any], lookup: dict[str, str], name: str) -> Any:
    column = lookup.get(name.upper())
    return row.get(column) if column else None


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def opportunity_status_group(stage_name: str) -> str:
    normalized = stage_name.strip().casefold()
    if normalized == "cobrada":
        return "cobrada"
    if normalized in {"perdida", "closed lost", "rechazada"}:
        return "perdida"
    if normalized == "comprometida":
        return "pendiente"
    return "otro"


def _start_batch(
    connection: psycopg.Connection[Any],
    *,
    dataset: str,
    source_filename: str,
    checksum: str,
    size_bytes: int,
) -> tuple[UUID, bool]:
    existing = connection.execute(
        """
        SELECT batch_id, status, rows_read, rows_inserted, rows_updated,
               rows_unchanged, rows_rejected
        FROM etl.ingestion_batch
        WHERE dataset = %s AND file_sha256 = %s
        """,
        (dataset, checksum),
    ).fetchone()
    if existing and existing[1] == "completed":
        return existing[0], True

    if existing:
        batch_id = existing[0]
        connection.execute(
            """
            UPDATE etl.ingestion_batch
            SET status = 'running', error_message = NULL, started_at = now(), completed_at = NULL
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
    else:
        batch_id = uuid4()
        connection.execute(
            """
            INSERT INTO etl.ingestion_batch (
                batch_id, dataset, source_filename, file_sha256, file_size_bytes, status
            ) VALUES (%s, %s, %s, %s, %s, 'running')
            """,
            (batch_id, dataset, source_filename, checksum, size_bytes),
        )
    connection.commit()
    return batch_id, False


def _completed_result(connection: psycopg.Connection[Any], batch_id: UUID) -> IngestionResult:
    row = connection.execute(
        """
        SELECT dataset, source_filename, status, rows_read, rows_inserted,
               rows_updated, rows_unchanged, rows_rejected
        FROM etl.ingestion_batch WHERE batch_id = %s
        """,
        (batch_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("Ingestion batch disappeared")
    return IngestionResult(
        batch_id=str(batch_id),
        dataset=row[0],
        source_filename=row[1],
        staging_path=None,
        status="skipped_duplicate" if row[2] == "completed" else row[2],
        rows_read=row[3],
        rows_inserted=row[4],
        rows_updated=row[5],
        rows_unchanged=row[6],
        rows_rejected=row[7],
    )


def _load_generic_salesforce(
    connection: psycopg.Connection[Any],
    dataset: str,
    frames: Iterable[pl.DataFrame],
    key_column: str,
    batch_id: UUID,
) -> tuple[int, int, int]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE stage_salesforce_record (
                salesforce_id text PRIMARY KEY,
                payload jsonb NOT NULL,
                record_hash char(64) NOT NULL
            ) ON COMMIT DROP
            """
        )
        with cursor.copy(
            "COPY stage_salesforce_record (salesforce_id, payload, record_hash) FROM STDIN"
        ) as copy:
            for frame in frames:
                for row in frame.iter_rows(named=True):
                    payload, record_hash = _payload_and_hash(row)
                    copy.write_row((str(row[key_column]), payload, record_hash))

        stats = cursor.execute(
            """
            SELECT
                count(*) FILTER (WHERE current.salesforce_id IS NULL),
                count(*) FILTER (
                    WHERE current.salesforce_id IS NOT NULL
                      AND current.record_hash IS DISTINCT FROM stage.record_hash
                ),
                count(*) FILTER (WHERE current.record_hash = stage.record_hash)
            FROM stage_salesforce_record AS stage
            LEFT JOIN raw.salesforce_record_current AS current
              ON current.object_type = %s
             AND current.salesforce_id = stage.salesforce_id
            """,
            (dataset,),
        ).fetchone()
        cursor.execute(
            """
            INSERT INTO raw.salesforce_record_current (
                object_type, salesforce_id, payload, record_hash,
                first_batch_id, last_batch_id
            )
            SELECT %s, salesforce_id, payload, record_hash, %s, %s
            FROM stage_salesforce_record
            ON CONFLICT (object_type, salesforce_id) DO UPDATE
            SET payload = EXCLUDED.payload,
                record_hash = EXCLUDED.record_hash,
                last_batch_id = EXCLUDED.last_batch_id,
                last_seen_at = now()
            WHERE raw.salesforce_record_current.record_hash IS DISTINCT FROM EXCLUDED.record_hash
            """,
            (dataset, batch_id, batch_id),
        )
    return stats or (0, 0, 0)


def _load_opportunities(
    connection: psycopg.Connection[Any],
    frames: Iterable[pl.DataFrame],
    key_column: str,
    batch_id: UUID,
) -> tuple[int, int, int]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE stage_opportunity (
                id_opportunity text PRIMARY KEY,
                id_contact text,
                id_account text,
                id_campaign text,
                id_recurring_donation text,
                amount numeric(18, 4),
                close_date date,
                collection_date date,
                accounting_date date,
                stage_name text NOT NULL,
                status_group text NOT NULL,
                is_closed boolean,
                payload jsonb NOT NULL,
                record_hash char(64) NOT NULL
            ) ON COMMIT DROP
            """
        )
        with cursor.copy(
            """
            COPY stage_opportunity (
                id_opportunity, id_contact, id_account, id_campaign,
                id_recurring_donation, amount, close_date, collection_date,
                accounting_date, stage_name, status_group, is_closed,
                payload, record_hash
            ) FROM STDIN
            """
        ) as copy:
            for frame in frames:
                lookup = _column_lookup(frame)
                for row in frame.iter_rows(named=True):
                    stage_name = str(_value(row, lookup, "STAGENAME") or "")
                    payload, record_hash = _payload_and_hash(row)
                    copy.write_row(
                        (
                            str(row[key_column]),
                            _value(row, lookup, "CONTACTID"),
                            _value(row, lookup, "ACCOUNTID"),
                            _value(row, lookup, "CAMPAIGNID"),
                            _value(row, lookup, "NPE03__RECURRING_DONATION__C"),
                            _as_decimal(_value(row, lookup, "AMOUNT")),
                            _as_date(_value(row, lookup, "CLOSEDATE")),
                            _as_date(_value(row, lookup, "FECHA_DE_COBRO__C")),
                            _as_date(_value(row, lookup, "FECHA_DE_REGISTRO_CONTABLE__C")),
                            stage_name,
                            opportunity_status_group(stage_name),
                            _value(row, lookup, "ISCLOSED"),
                            payload,
                            record_hash,
                        )
                    )

        stats = cursor.execute(
            """
            SELECT
                count(*) FILTER (WHERE current.id_opportunity IS NULL),
                count(*) FILTER (
                    WHERE current.id_opportunity IS NOT NULL
                      AND current.record_hash IS DISTINCT FROM stage.record_hash
                ),
                count(*) FILTER (WHERE current.record_hash = stage.record_hash)
            FROM stage_opportunity AS stage
            LEFT JOIN raw.salesforce_opportunity_current AS current USING (id_opportunity)
            """
        ).fetchone()
        cursor.execute(
            """
            UPDATE history.salesforce_opportunity_status AS status
            SET valid_to = now()
            FROM stage_opportunity AS stage
            WHERE status.id_opportunity = stage.id_opportunity
              AND status.valid_to IS NULL
              AND status.stage_name IS DISTINCT FROM stage.stage_name
            """
        )
        cursor.execute(
            """
            INSERT INTO history.salesforce_opportunity_status (
                id_opportunity, stage_name, status_group, amount,
                id_contact, id_account, close_date, batch_id
            )
            SELECT
                stage.id_opportunity, stage.stage_name, stage.status_group, stage.amount,
                stage.id_contact, stage.id_account, stage.close_date, %s
            FROM stage_opportunity AS stage
            LEFT JOIN raw.salesforce_opportunity_current AS current USING (id_opportunity)
            WHERE current.id_opportunity IS NULL
               OR current.stage_name IS DISTINCT FROM stage.stage_name
            """,
            (batch_id,),
        )
        cursor.execute(
            """
            INSERT INTO raw.salesforce_opportunity_current (
                id_opportunity, id_contact, id_account, id_campaign,
                id_recurring_donation, amount, close_date, collection_date,
                accounting_date, stage_name, status_group, is_closed,
                payload, record_hash, first_batch_id, last_batch_id
            )
            SELECT
                id_opportunity, id_contact, id_account, id_campaign,
                id_recurring_donation, amount, close_date, collection_date,
                accounting_date, stage_name, status_group, is_closed,
                payload, record_hash, %s, %s
            FROM stage_opportunity
            ON CONFLICT (id_opportunity) DO UPDATE
            SET id_contact = EXCLUDED.id_contact,
                id_account = EXCLUDED.id_account,
                id_campaign = EXCLUDED.id_campaign,
                id_recurring_donation = EXCLUDED.id_recurring_donation,
                amount = EXCLUDED.amount,
                close_date = EXCLUDED.close_date,
                collection_date = EXCLUDED.collection_date,
                accounting_date = EXCLUDED.accounting_date,
                stage_name = EXCLUDED.stage_name,
                status_group = EXCLUDED.status_group,
                is_closed = EXCLUDED.is_closed,
                payload = EXCLUDED.payload,
                record_hash = EXCLUDED.record_hash,
                last_batch_id = EXCLUDED.last_batch_id,
                last_seen_at = now()
            WHERE raw.salesforce_opportunity_current.record_hash
                IS DISTINCT FROM EXCLUDED.record_hash
            """,
            (batch_id, batch_id),
        )
    return stats or (0, 0, 0)


def _load_historical_donations(
    connection: psycopg.Connection[Any],
    frames: Iterable[pl.DataFrame],
    key_column: str,
    batch_id: UUID,
) -> tuple[int, int, int]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE stage_historical_donation (
                id_donation text PRIMARY KEY,
                id_salesforce text,
                amount numeric(18, 4),
                frequency text,
                collection_date date NOT NULL,
                person_number text,
                external_numbers jsonb,
                friend_id text,
                psn text,
                psn_c text,
                source_origin text,
                record_hash char(64) NOT NULL
            ) ON COMMIT DROP
            """
        )
        with cursor.copy(
            """
            COPY stage_historical_donation (
                id_donation, id_salesforce, amount, frequency, collection_date,
                person_number, external_numbers, friend_id, psn, psn_c,
                source_origin, record_hash
            ) FROM STDIN
            """
        ) as copy:
            for frame in frames:
                lookup = _column_lookup(frame)
                for row in frame.iter_rows(named=True):
                    _, record_hash = _payload_and_hash(row)
                    external_numbers = _value(row, lookup, "nr_ajeno")
                    copy.write_row(
                        (
                            str(row[key_column]),
                            _value(row, lookup, "id_salesforce"),
                            _as_decimal(_value(row, lookup, "importe")),
                            _value(row, lookup, "frecuencia"),
                            _as_date(_value(row, lookup, "fecha_cobro")),
                            _value(row, lookup, "nr_persona"),
                            json.dumps(external_numbers, ensure_ascii=False)
                            if external_numbers is not None
                            else None,
                            _value(row, lookup, "ID_Amigo"),
                            _value(row, lookup, "PSN"),
                            _value(row, lookup, "PSN_C"),
                            _value(row, lookup, "fuente_origen"),
                            record_hash,
                        )
                    )

        stats = cursor.execute(
            """
            SELECT
                count(*) FILTER (WHERE current.id_donation IS NULL),
                count(*) FILTER (
                    WHERE current.id_donation IS NOT NULL
                      AND current.record_hash IS DISTINCT FROM stage.record_hash
                ),
                count(*) FILTER (WHERE current.record_hash = stage.record_hash)
            FROM stage_historical_donation AS stage
            LEFT JOIN raw.historical_donation AS current USING (id_donation)
            """
        ).fetchone()
        cursor.execute(
            """
            INSERT INTO raw.historical_donation (
                id_donation, id_salesforce, amount, frequency, collection_date,
                person_number, external_numbers, friend_id, psn, psn_c,
                source_origin, record_hash, first_batch_id, last_batch_id
            )
            SELECT
                id_donation, id_salesforce, amount, frequency, collection_date,
                person_number, external_numbers, friend_id, psn, psn_c,
                source_origin, record_hash, %s, %s
            FROM stage_historical_donation
            ON CONFLICT (id_donation) DO UPDATE
            SET id_salesforce = EXCLUDED.id_salesforce,
                amount = EXCLUDED.amount,
                frequency = EXCLUDED.frequency,
                collection_date = EXCLUDED.collection_date,
                person_number = EXCLUDED.person_number,
                external_numbers = EXCLUDED.external_numbers,
                friend_id = EXCLUDED.friend_id,
                psn = EXCLUDED.psn,
                psn_c = EXCLUDED.psn_c,
                source_origin = EXCLUDED.source_origin,
                record_hash = EXCLUDED.record_hash,
                last_batch_id = EXCLUDED.last_batch_id,
                last_seen_at = now()
            WHERE raw.historical_donation.record_hash IS DISTINCT FROM EXCLUDED.record_hash
            """,
            (batch_id, batch_id),
        )
    return stats or (0, 0, 0)


def _mark_completed(
    connection: psycopg.Connection[Any],
    batch_id: UUID,
    *,
    rows_read: int,
    stats: tuple[int, int, int],
) -> None:
    connection.execute(
        """
        UPDATE etl.ingestion_batch
        SET status = 'completed', rows_read = %s, rows_inserted = %s,
            rows_updated = %s, rows_unchanged = %s, rows_rejected = 0,
            completed_at = now()
        WHERE batch_id = %s
        """,
        (rows_read, stats[0], stats[1], stats[2], batch_id),
    )


def _mark_failed(batch_id: UUID, error: Exception) -> None:
    with psycopg.connect(**postgres_connection_options()) as connection:
        connection.execute(
            """
            UPDATE etl.ingestion_batch
            SET status = 'failed', error_message = %s, completed_at = now()
            WHERE batch_id = %s
            """,
            (str(error)[:4000], batch_id),
        )


def ingest_file(dataset: str, file_path: str) -> IngestionResult:
    if dataset not in SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")
    ingestion_root, source = resolve_source_path(file_path)
    source_filename = source.relative_to(ingestion_root).as_posix()
    checksum = file_sha256(source)

    with psycopg.connect(**postgres_connection_options()) as connection:
        batch_id, already_completed = _start_batch(
            connection,
            dataset=dataset,
            source_filename=source_filename,
            checksum=checksum,
            size_bytes=source.stat().st_size,
        )
        if already_completed:
            return _completed_result(connection, batch_id)

    try:
        source_frame = scan_source(source)
        key_column, row_count = validate_source(dataset, source_frame)
        staging_path = write_staging_parquet(dataset, batch_id, source_frame)
        staging_batches = pl.scan_parquet(staging_path).collect_batches(
            chunk_size=50_000,
            engine="streaming",
        )

        with psycopg.connect(**postgres_connection_options()) as connection:
            if dataset in GENERIC_SALESFORCE_DATASETS:
                stats = _load_generic_salesforce(
                    connection,
                    dataset,
                    staging_batches,
                    key_column,
                    batch_id,
                )
            elif dataset == "opportunities":
                stats = _load_opportunities(connection, staging_batches, key_column, batch_id)
            else:
                stats = _load_historical_donations(
                    connection,
                    staging_batches,
                    key_column,
                    batch_id,
                )
            _mark_completed(connection, batch_id, rows_read=row_count, stats=stats)

        return IngestionResult(
            batch_id=str(batch_id),
            dataset=dataset,
            source_filename=source_filename,
            staging_path=str(staging_path),
            status="completed",
            rows_read=row_count,
            rows_inserted=stats[0],
            rows_updated=stats[1],
            rows_unchanged=stats[2],
            rows_rejected=0,
        )
    except Exception as error:
        _mark_failed(batch_id, error)
        raise
