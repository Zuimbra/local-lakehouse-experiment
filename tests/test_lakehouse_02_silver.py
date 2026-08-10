from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


PROJECT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse_02_silver import (  # noqa: E402
    BRONZE_TABLE_NAME,
    SilverLoadResult,
    validate_arrow_partition_column,
    discover_affected_partitions,
    get_lakehouse_paths,
    load_silver_data,
    normalize_batch_ids,
    silver_supports_incremental_update,
)


RAW_COLUMNS = (
    "DATA_SERVIDOR",
    "TM_STAMP",
    "TIPO_LOG",
    "MESS_TYPE",
    "REPT_TYPE",
    "PRT_VER",
    "S/N ou IMEI",
    "TERM_STATUS",
    "BAT_VOLT",
    "LOC_STATUS",
    "LAT",
    "LONT",
    "SPEED",
    "DIR",
    "INT_BATT",
    "ODO_TRIP",
    "ODO_TOTAL",
    "HORIMETER",
    "HDOP",
    "MCC",
    "MNC",
    "LAC",
    "CELL_ID",
    "RX_LEVEL",
    "SER_COUNT",
    "TX_TECH",
    "GRP_MSG",
    "IO_STATUS",
    "DRIVER_ID",
    "PASS_ID",
    "RPM",
    "TACHO_SPD",
    "TACHO_ODO",
    "TEMP_1",
    "TEMP_2",
    "TEMP_3",
    "TEMP_4",
)


def raw_record(**overrides) -> dict[str, object]:
    row = {column: "" for column in RAW_COLUMNS}
    row.update(overrides)
    return row


def add_metadata(
    row: dict[str, object],
    *,
    source_file: str,
    source_file_hash: str,
    row_id: str,
    batch_id: str,
    source_row_number: int = 1,
    ingestion_date: date = date(2026, 8, 10),
) -> dict[str, object]:
    row.update(
        {
            "source_file": source_file,
            "source_file_hash": source_file_hash,
            "source_row_number": source_row_number,
            "row_id": row_id,
            "batch_id": batch_id,
            "ingested_at": datetime(
                2026,
                8,
                10,
                13,
                0,
                tzinfo=timezone.utc,
            ),
            "ingestion_date": ingestion_date,
        }
    )
    return row


def telemetry_row(
    event_timestamp: str,
    *,
    batch_id: str,
    row_id: str,
    source_file: str,
) -> dict[str, object]:
    return add_metadata(
        raw_record(
            DATA_SERVIDOR=event_timestamp,
            TM_STAMP=event_timestamp,
            TIPO_LOG="TRACKER",
            MESS_TYPE="T2",
            REPT_TYPE="1",
            PRT_VER="1",
            **{
                "S/N ou IMEI": "M123456789",
                "TERM_STATUS": "OK",
                "BAT_VOLT": "12.5",
                "LOC_STATUS": "A",
                "LAT": "-3.7319",
                "LONT": "-38.5267",
                "SPEED": "42",
                "DIR": "180",
                "INT_BATT": "4.1",
                "ODO_TRIP": "10",
                "ODO_TOTAL": "1000",
                "HORIMETER": "20",
                "HDOP": "1.2",
                "SER_COUNT": "10",
            },
        ),
        source_file=source_file,
        source_file_hash=f"hash-{row_id}",
        row_id=row_id,
        batch_id=batch_id,
    )


def identity_row(
    event_timestamp: str,
    *,
    batch_id: str,
    row_id: str,
) -> dict[str, object]:
    return add_metadata(
        raw_record(
            DATA_SERVIDOR=event_timestamp,
            TM_STAMP=event_timestamp,
            TIPO_LOG="TRACKER",
            MESS_TYPE="T1",
            REPT_TYPE="1",
            PRT_VER="1",
            **{
                "S/N ou IMEI": "M123456789",
                "BAT_VOLT": "89550500000000000001",
                "LOC_STATUS": "IDENTITY",
                "LAT": "724001234567890",
                "LONT": "359881234567890",
            },
        ),
        source_file="identity.csv",
        source_file_hash=f"hash-{row_id}",
        row_id=row_id,
        batch_id=batch_id,
    )


def invalid_timestamp_row(
    *,
    batch_id: str,
    row_id: str,
) -> dict[str, object]:
    return add_metadata(
        raw_record(
            DATA_SERVIDOR="",
            TM_STAMP="invalid",
            TIPO_LOG="TRACKER",
            MESS_TYPE="T2",
            REPT_TYPE="1",
            PRT_VER="1",
            **{
                "S/N ou IMEI": "M123456789",
            },
        ),
        source_file="invalid.csv",
        source_file_hash=f"hash-{row_id}",
        row_id=row_id,
        batch_id=batch_id,
    )


def bronze_path(project_dir: Path) -> Path:
    return (
        project_dir
        / "data"
        / "lakehouse"
        / "01_bronze"
        / BRONZE_TABLE_NAME
    )


def write_bronze(
    project_dir: Path,
    rows: list[dict[str, object]],
    *,
    mode: str = "overwrite",
) -> None:
    path = bronze_path(project_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    write_deltalake(
        path,
        pd.DataFrame(rows),
        mode=mode,
        schema_mode=(
            "merge"
            if mode == "append"
            else "overwrite"
        ),
        partition_by=(
            ["ingestion_date"]
            if mode == "overwrite"
            else None
        ),
    )


class SilverSprint7Tests(unittest.TestCase):
    def test_empty_arrow_partition_preserves_string_type(self) -> None:
        table = pa.table(
            {
                "event_date": pa.array(
                    [],
                    type=pa.string(),
                ),
                "log_type": pa.array(
                    [],
                    type=pa.string(),
                ),
            }
        )

        # Não deve gerar exceção.
        validate_arrow_partition_column(
            table,
            "event_date",
        )

        self.assertTrue(
            pa.types.is_string(
                table.schema.field(
                    "log_type"
                ).type
            )
        )

    def test_normalize_batch_ids(self) -> None:
        self.assertEqual(
            normalize_batch_ids(
                [" b ", "a", "a", ""]
            ),
            ("a", "b"),
        )

    def test_full_rebuild_migrates_partition_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)

            write_bronze(
                project_dir,
                [
                    telemetry_row(
                        "2026-08-10 10:00:00",
                        batch_id="batch-1",
                        row_id="row-1",
                        source_file="a.csv",
                    )
                ],
            )

            result = load_silver_data(
                project_dir=project_dir
            )

            self.assertIsInstance(
                result,
                SilverLoadResult,
            )
            self.assertEqual(result.mode, "FULL")

            paths = get_lakehouse_paths(project_dir)
            telemetry = DeltaTable(
                str(paths["telemetry"])
            ).to_pandas()

            self.assertEqual(
                telemetry["event_date"].tolist(),
                ["2026-08-10"],
            )
            self.assertTrue(
                silver_supports_incremental_update(
                    paths
                )
            )

    def test_incremental_late_file_rebuilds_only_affected_date(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)

            write_bronze(
                project_dir,
                [
                    telemetry_row(
                        "2026-08-10 10:00:00",
                        batch_id="batch-1",
                        row_id="row-1",
                        source_file="a.csv",
                    ),
                    telemetry_row(
                        "2026-08-11 10:00:00",
                        batch_id="batch-1",
                        row_id="row-2",
                        source_file="a.csv",
                    ),
                ],
            )

            load_silver_data(project_dir=project_dir)

            paths = get_lakehouse_paths(project_dir)
            telemetry_table = DeltaTable(
                str(paths["telemetry"])
            )
            version_before = telemetry_table.version()

            write_bronze(
                project_dir,
                [
                    telemetry_row(
                        "2026-08-10 11:00:00",
                        batch_id="batch-2",
                        row_id="row-3",
                        source_file="late.csv",
                    )
                ],
                mode="append",
            )

            result = load_silver_data(
                project_dir=project_dir,
                batch_ids={"batch-2"},
            )

            self.assertEqual(
                result.mode,
                "INCREMENTAL",
            )
            self.assertEqual(
                result.affected_event_dates,
                ("2026-08-10",),
            )

            telemetry_table = DeltaTable(
                str(paths["telemetry"])
            )
            telemetry = telemetry_table.to_pandas()

            august_10 = telemetry.loc[
                telemetry["event_date"]
                == "2026-08-10"
            ]
            august_11 = telemetry.loc[
                telemetry["event_date"]
                == "2026-08-11"
            ]

            self.assertEqual(len(august_10), 2)
            self.assertEqual(len(august_11), 1)
            self.assertGreater(
                telemetry_table.version(),
                version_before,
            )

    def test_incremental_identity_batch_updates_identity_partition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)

            write_bronze(
                project_dir,
                [
                    identity_row(
                        "2026-08-10 10:00:00",
                        batch_id="batch-1",
                        row_id="identity-1",
                    )
                ],
            )
            load_silver_data(project_dir=project_dir)

            write_bronze(
                project_dir,
                [
                    identity_row(
                        "2026-08-10 11:00:00",
                        batch_id="batch-2",
                        row_id="identity-2",
                    )
                ],
                mode="append",
            )

            result = load_silver_data(
                project_dir=project_dir,
                batch_ids={"batch-2"},
            )

            identity = DeltaTable(
                str(
                    get_lakehouse_paths(
                        project_dir
                    )["identity"]
                )
            ).to_pandas()

            self.assertEqual(result.mode, "INCREMENTAL")
            self.assertEqual(len(identity), 2)

    def test_empty_rejected_schema_accepts_first_real_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)

            # Primeiro rebuild: rejected_logs será criada vazia.
            write_bronze(
                project_dir,
                [
                    telemetry_row(
                        "2026-08-10 10:00:00",
                        batch_id="batch-1",
                        row_id="telemetry-1",
                        source_file="a.csv",
                    )
                ],
            )

            load_silver_data(project_dir=project_dir)

            rejected_path = get_lakehouse_paths(
                project_dir
            )["rejected"]

            empty_rejected = DeltaTable(
                str(rejected_path)
            )

            field_types = {
                field.name: str(
                    getattr(
                        field.type,
                        "type",
                        field.type,
                    )
                ).lower()
                for field in empty_rejected.schema().fields
            }

            # O schema vazio precisa preservar os tipos SQL.
            self.assertEqual(
                field_types["log_type"],
                "string",
            )

            # Depois chega o primeiro rejeitado real.
            write_bronze(
                project_dir,
                [
                    invalid_timestamp_row(
                        batch_id="batch-2",
                        row_id="invalid-1",
                    )
                ],
                mode="append",
            )

            result = load_silver_data(
                project_dir=project_dir,
                batch_ids={"batch-2"},
            )

            rejected = DeltaTable(
                str(rejected_path)
            ).to_pandas()

            self.assertEqual(
                result.mode,
                "INCREMENTAL",
            )
            self.assertEqual(len(rejected), 1)
            self.assertEqual(
                rejected.iloc[0]["log_type"],
                "TRACKER",
            )

    def test_unknown_rejection_partition_is_reprocessed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)

            write_bronze(
                project_dir,
                [
                    telemetry_row(
                        "2026-08-10 10:00:00",
                        batch_id="batch-1",
                        row_id="telemetry-1",
                        source_file="a.csv",
                    )
                ],
            )
            load_silver_data(project_dir=project_dir)

            write_bronze(
                project_dir,
                [
                    invalid_timestamp_row(
                        batch_id="batch-2",
                        row_id="invalid-1",
                    )
                ],
                mode="append",
            )

            result = load_silver_data(
                project_dir=project_dir,
                batch_ids={"batch-2"},
            )

            rejected = DeltaTable(
                str(
                    get_lakehouse_paths(
                        project_dir
                    )["rejected"]
                )
            ).to_pandas()

            self.assertIn(
                "unknown",
                result.affected_rejection_dates,
            )
            self.assertEqual(len(rejected), 1)
            self.assertEqual(
                rejected.iloc[0]["rejection_date"],
                "unknown",
            )

    def test_unknown_batch_is_noop(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)

            write_bronze(
                project_dir,
                [
                    telemetry_row(
                        "2026-08-10 10:00:00",
                        batch_id="batch-1",
                        row_id="row-1",
                        source_file="a.csv",
                    )
                ],
            )
            load_silver_data(project_dir=project_dir)

            result = load_silver_data(
                project_dir=project_dir,
                batch_ids={"does-not-exist"},
            )

            self.assertEqual(result.mode, "NOOP")
            self.assertEqual(
                result.telemetry_rows_written,
                0,
            )


if __name__ == "__main__":
    unittest.main()