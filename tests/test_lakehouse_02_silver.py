from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable, write_deltalake


PROJECT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse_02_silver import (  # noqa: E402
    BRONZE_METADATA_COLUMNS,
    BRONZE_TABLE_NAME,
    get_delta_column_names,
    get_lakehouse_paths,
    load_silver_data,
    validate_bronze_metadata,
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


def empty_raw_record() -> dict[str, object]:
    return {
        column: ""
        for column in RAW_COLUMNS
    }


def build_bronze_dataframe() -> pd.DataFrame:
    telemetry = empty_raw_record()
    telemetry.update(
        {
            "DATA_SERVIDOR": "2026-08-10 10:00:01",
            "TM_STAMP": "2026-08-10 10:00:00",
            "TIPO_LOG": "TRACKER",
            "MESS_TYPE": "T2",
            "REPT_TYPE": "1",
            "PRT_VER": "1",
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
            "RPM": "1500",
            "source_file": "telemetry.csv",
            "source_file_hash": "hash-telemetry",
            "source_row_number": 1,
            "row_id": "row-telemetry",
            "batch_id": "batch-1",
            "ingested_at": datetime(
                2026,
                8,
                10,
                13,
                0,
                tzinfo=timezone.utc,
            ),
            "ingestion_date": date(2026, 8, 10),
        }
    )

    identity = empty_raw_record()
    identity.update(
        {
            "DATA_SERVIDOR": "2026-08-10 10:05:01",
            "TM_STAMP": "2026-08-10 10:05:00",
            "TIPO_LOG": "TRACKER",
            "MESS_TYPE": "T1",
            "REPT_TYPE": "1",
            "PRT_VER": "1",
            "S/N ou IMEI": "M123456789",
            "BAT_VOLT": "89550500000000000001",
            "LOC_STATUS": "IDENTITY",
            "LAT": "724001234567890",
            "LONT": "359881234567890",
            "source_file": "identity.csv",
            "source_file_hash": "hash-identity",
            "source_row_number": 1,
            "row_id": "row-identity",
            "batch_id": "batch-1",
            "ingested_at": datetime(
                2026,
                8,
                10,
                13,
                1,
                tzinfo=timezone.utc,
            ),
            "ingestion_date": date(2026, 8, 10),
        }
    )

    rejected = empty_raw_record()
    rejected.update(
        {
            "DATA_SERVIDOR": "",
            "TM_STAMP": "",
            "TIPO_LOG": "TRACKER",
            "MESS_TYPE": "T2",
            "REPT_TYPE": "1",
            "PRT_VER": "1",
            "S/N ou IMEI": "M123456789",
            "source_file": "invalid.csv",
            "source_file_hash": "hash-invalid",
            "source_row_number": 1,
            "row_id": "row-invalid",
            "batch_id": "batch-1",
            "ingested_at": datetime(
                2026,
                8,
                10,
                13,
                2,
                tzinfo=timezone.utc,
            ),
            "ingestion_date": date(2026, 8, 10),
        }
    )

    return pd.DataFrame(
        [telemetry, identity, rejected]
    )


def create_test_bronze(
    project_dir: Path,
) -> Path:
    bronze_path = (
        project_dir
        / "data"
        / "lakehouse"
        / "01_bronze"
        / BRONZE_TABLE_NAME
    )
    bronze_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    write_deltalake(
        bronze_path,
        build_bronze_dataframe(),
        mode="overwrite",
        partition_by=["ingestion_date"],
    )

    return bronze_path


class SilverSprint6Tests(unittest.TestCase):
    def test_paths_use_consolidated_bronze(self) -> None:
        paths = get_lakehouse_paths(Path("project"))

        self.assertEqual(
            paths["bronze"],
            (
                Path("project")
                / "data"
                / "lakehouse"
                / "01_bronze"
                / "tracker_logs"
            ),
        )

    def test_delta_column_names(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)
            bronze_path = create_test_bronze(project_dir)

            table = DeltaTable(str(bronze_path))
            columns = get_delta_column_names(table)

            self.assertIn("row_id", columns)
            self.assertIn("source_file", columns)
            self.assertIn("batch_id", columns)

    def test_bronze_metadata_validation_accepts_sprint5(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)
            bronze_path = create_test_bronze(project_dir)

            table = DeltaTable(str(bronze_path))

            # Não deve gerar exceção.
            validate_bronze_metadata(table)

    def test_bronze_metadata_validation_rejects_old_bronze(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            table_path = Path(temporary_directory) / "old_bronze"

            old_dataframe = pd.DataFrame(
                [
                    {
                        column: ""
                        for column in RAW_COLUMNS
                    }
                ]
            )

            write_deltalake(
                table_path,
                old_dataframe,
                mode="overwrite",
            )

            table = DeltaTable(str(table_path))

            with self.assertRaises(ValueError):
                validate_bronze_metadata(table)

    def test_silver_reads_multiple_files_and_preserves_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)
            create_test_bronze(project_dir)

            load_silver_data(project_dir=project_dir)

            paths = get_lakehouse_paths(project_dir)

            telemetry = DeltaTable(
                str(paths["telemetry"])
            ).to_pandas()

            identity = DeltaTable(
                str(paths["identity"])
            ).to_pandas()

            rejected = DeltaTable(
                str(paths["rejected"])
            ).to_pandas()

            self.assertEqual(len(telemetry), 1)
            self.assertEqual(len(identity), 1)
            self.assertEqual(len(rejected), 1)

            for dataframe in (
                telemetry,
                identity,
                rejected,
            ):
                for column in BRONZE_METADATA_COLUMNS:
                    self.assertIn(
                        column,
                        dataframe.columns,
                    )

            self.assertEqual(
                telemetry.iloc[0]["source_file"],
                "telemetry.csv",
            )
            self.assertEqual(
                telemetry.iloc[0]["row_id"],
                "row-telemetry",
            )

            self.assertEqual(
                identity.iloc[0]["source_file"],
                "identity.csv",
            )
            self.assertEqual(
                identity.iloc[0]["row_id"],
                "row-identity",
            )

            self.assertEqual(
                rejected.iloc[0]["source_file"],
                "invalid.csv",
            )
            self.assertEqual(
                rejected.iloc[0]["row_id"],
                "row-invalid",
            )
            self.assertEqual(
                rejected.iloc[0]["rejection_reason"],
                "MISSING_OR_INVALID_TIMESTAMP",
            )

    def test_silver_rebuild_migrates_existing_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)
            create_test_bronze(project_dir)

            paths = get_lakehouse_paths(project_dir)
            paths["silver"].mkdir(
                parents=True,
                exist_ok=True,
            )

            # Simula tabela Silver anterior à Sprint 6.
            old_telemetry = pd.DataFrame(
                {
                    # Simula a representação temporal produzida pelo
                    # fluxo DuckDB -> Pandas usado pela Silver real.
                    "event_date": pd.to_datetime(
                        ["2026-08-09"]
                    ),
                    "source_file": ["old.csv"],
                }
            )

            write_deltalake(
                paths["telemetry"],
                old_telemetry,
                mode="overwrite",
                partition_by=["event_date"],
            )

            load_silver_data(project_dir=project_dir)

            migrated = DeltaTable(
                str(paths["telemetry"])
            ).to_pandas()

            self.assertIn(
                "source_file_hash",
                migrated.columns,
            )
            self.assertIn(
                "row_id",
                migrated.columns,
            )
            self.assertNotIn(
                "old.csv",
                migrated["source_file"].tolist(),
            )


if __name__ == "__main__":
    unittest.main()