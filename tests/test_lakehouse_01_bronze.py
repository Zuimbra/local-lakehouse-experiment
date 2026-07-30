import sys
import tempfile
import unittest
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse_01_bronze import (  # noqa: E402
    EXPECTED_COLUMNS,
    METADATA_COLUMNS,
    calculate_file_hash,
    calculate_row_id,
    create_raw_directories,
    discover_input_files,
    move_file_to_quarantine,
    prepare_bronze_dataframe,
    validate_input_file,
    write_current_bronze_table,
)


def build_valid_dataframe() -> pd.DataFrame:
    """
    Cria um registro mínimo com o schema estrutural esperado.
    """
    return pd.DataFrame(
        [
            {
                column: ""
                for column in EXPECTED_COLUMNS
            }
        ]
    )


class BronzeSprint3Tests(unittest.TestCase):
    def test_create_raw_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)

            (
                raw_path,
                inbox_path,
                archive_path,
                quarantine_path,
            ) = create_raw_directories(project_dir)

            self.assertEqual(
                raw_path,
                project_dir / "data" / "raw",
            )
            self.assertTrue(inbox_path.is_dir())
            self.assertTrue(archive_path.is_dir())
            self.assertTrue(quarantine_path.is_dir())

    def test_discover_only_csv_files_in_sorted_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            inbox_path = Path(temporary_directory)

            (inbox_path / "b.csv").write_text(
                "value\n2\n",
                encoding="utf-8",
            )
            (inbox_path / "A.CSV").write_text(
                "value\n1\n",
                encoding="utf-8",
            )
            (inbox_path / "notes.txt").write_text(
                "ignore",
                encoding="utf-8",
            )
            (inbox_path / "fake.csv").mkdir()

            discovered_files = discover_input_files(inbox_path)

            self.assertEqual(
                [file_path.name for file_path in discovered_files],
                ["A.CSV", "b.csv"],
            )

    def test_valid_csv_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = (
                Path(temporary_directory)
                / "valid.csv"
            )
            build_valid_dataframe().to_csv(
                file_path,
                index=False,
            )

            result = validate_input_file(file_path)

            self.assertTrue(result.is_valid)
            self.assertEqual(result.row_count, 1)
            self.assertEqual(result.missing_columns, ())
            self.assertIsNotNone(result.dataframe)

    def test_extra_columns_are_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = (
                Path(temporary_directory)
                / "extra_columns.csv"
            )
            dataframe = build_valid_dataframe()
            dataframe["EXTRA_COLUMN"] = "value"
            dataframe.to_csv(
                file_path,
                index=False,
            )

            result = validate_input_file(file_path)

            self.assertTrue(result.is_valid)

    def test_missing_column_rejects_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = (
                Path(temporary_directory)
                / "missing_column.csv"
            )
            dataframe = build_valid_dataframe().drop(
                columns=["TM_STAMP"]
            )
            dataframe.to_csv(
                file_path,
                index=False,
            )

            result = validate_input_file(file_path)

            self.assertFalse(result.is_valid)
            self.assertIn(
                "TM_STAMP",
                result.missing_columns,
            )
            self.assertIsNone(result.dataframe)

    def test_empty_file_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = (
                Path(temporary_directory)
                / "empty.csv"
            )
            file_path.write_bytes(b"")

            result = validate_input_file(file_path)

            self.assertFalse(result.is_valid)
            self.assertEqual(
                result.error_message,
                "O arquivo está vazio.",
            )

    def test_invalid_file_is_moved_to_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root_path = Path(temporary_directory)
            inbox_path = root_path / "inbox"
            quarantine_path = root_path / "quarantine"

            inbox_path.mkdir()
            quarantine_path.mkdir()

            file_path = inbox_path / "invalid.csv"
            pd.DataFrame(
                [{"DATA_SERVIDOR": "2026-07-01"}]
            ).to_csv(
                file_path,
                index=False,
            )

            result = validate_input_file(file_path)
            destination = move_file_to_quarantine(
                validation_result=result,
                quarantine_path=quarantine_path,
            )

            self.assertFalse(file_path.exists())
            self.assertTrue(destination.is_file())
            self.assertTrue(
                destination.with_suffix(
                    destination.suffix + ".error.txt"
                ).is_file()
            )


    def test_reserved_metadata_column_rejects_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = (
                Path(temporary_directory)
                / "reserved_metadata.csv"
            )
            dataframe = build_valid_dataframe()
            dataframe["source_file"] = "forged.csv"
            dataframe.to_csv(
                file_path,
                index=False,
            )

            result = validate_input_file(file_path)

            self.assertFalse(result.is_valid)
            self.assertEqual(
                result.reserved_columns,
                ("source_file",),
            )

    def test_file_hash_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = Path(temporary_directory) / "data.csv"
            file_content = b"column\nvalue\n"
            file_path.write_bytes(file_content)

            first_hash = calculate_file_hash(file_path)
            second_hash = calculate_file_hash(file_path)

            self.assertEqual(first_hash, second_hash)
            self.assertEqual(
                first_hash,
                sha256(file_content).hexdigest(),
            )

    def test_changed_file_changes_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = Path(temporary_directory) / "data.csv"
            file_path.write_text(
                "column\nfirst\n",
                encoding="utf-8",
            )
            first_hash = calculate_file_hash(file_path)

            file_path.write_text(
                "column\nsecond\n",
                encoding="utf-8",
            )
            second_hash = calculate_file_hash(file_path)

            self.assertNotEqual(first_hash, second_hash)

    def test_row_id_is_deterministic(self) -> None:
        first_row_id = calculate_row_id(
            source_file_hash="abc123",
            source_row_number=1,
        )
        second_row_id = calculate_row_id(
            source_file_hash="abc123",
            source_row_number=1,
        )
        another_row_id = calculate_row_id(
            source_file_hash="abc123",
            source_row_number=2,
        )

        self.assertEqual(first_row_id, second_row_id)
        self.assertNotEqual(first_row_id, another_row_id)

    def test_prepare_bronze_dataframe_adds_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = Path(temporary_directory) / "valid.csv"
            dataframe = pd.concat(
                [
                    build_valid_dataframe(),
                    build_valid_dataframe(),
                ],
                ignore_index=True,
            )
            dataframe.to_csv(
                file_path,
                index=False,
            )

            validation_result = validate_input_file(file_path)
            fixed_ingested_at = datetime(
                2026,
                7,
                30,
                17,
                30,
                tzinfo=timezone.utc,
            )

            prepared = prepare_bronze_dataframe(
                validation_result=validation_result,
                batch_id="batch-test-123",
                ingested_at=fixed_ingested_at,
            )

            for metadata_column in METADATA_COLUMNS:
                self.assertIn(
                    metadata_column,
                    prepared.columns,
                )

            self.assertEqual(
                prepared["source_file"].tolist(),
                ["valid.csv", "valid.csv"],
            )
            self.assertEqual(
                prepared["source_row_number"].tolist(),
                [1, 2],
            )
            self.assertEqual(
                prepared["batch_id"].tolist(),
                ["batch-test-123", "batch-test-123"],
            )
            self.assertEqual(
                prepared["ingestion_date"].tolist(),
                ["2026-07-30", "2026-07-30"],
            )
            self.assertNotEqual(
                prepared.loc[0, "row_id"],
                prepared.loc[1, "row_id"],
            )

    def test_row_id_does_not_depend_on_batch_id(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = Path(temporary_directory) / "valid.csv"
            build_valid_dataframe().to_csv(
                file_path,
                index=False,
            )
            validation_result = validate_input_file(file_path)

            first_batch = prepare_bronze_dataframe(
                validation_result=validation_result,
                batch_id="batch-one",
            )
            second_batch = prepare_bronze_dataframe(
                validation_result=validation_result,
                batch_id="batch-two",
            )

            self.assertEqual(
                first_batch.loc[0, "row_id"],
                second_batch.loc[0, "row_id"],
            )
            self.assertNotEqual(
                first_batch.loc[0, "batch_id"],
                second_batch.loc[0, "batch_id"],
            )

    def test_write_delta_table_persists_metadata(self) -> None:
        try:
            from deltalake import DeltaTable
        except ImportError:
            self.skipTest("deltalake não está instalado.")

        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)
            source_path = project_dir / "valid.csv"
            build_valid_dataframe().to_csv(
                source_path,
                index=False,
            )
            validation_result = validate_input_file(source_path)

            bronze_path = write_current_bronze_table(
                project_dir=project_dir,
                validation_result=validation_result,
                batch_id="batch-integration-test",
                ingested_at=datetime(
                    2026,
                    7,
                    30,
                    17,
                    30,
                    tzinfo=timezone.utc,
                ),
            )

            persisted = DeltaTable(
                str(bronze_path)
            ).to_pandas()

            for metadata_column in METADATA_COLUMNS:
                self.assertIn(
                    metadata_column,
                    persisted.columns,
                )

            self.assertEqual(
                persisted.loc[0, "batch_id"],
                "batch-integration-test",
            )


if __name__ == "__main__":
    unittest.main()