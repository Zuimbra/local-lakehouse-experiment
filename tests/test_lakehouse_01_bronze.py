import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse_01_bronze import (  # noqa: E402
    EXPECTED_COLUMNS,
    create_raw_directories,
    discover_input_files,
    move_file_to_quarantine,
    validate_input_file,
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


class BronzeSprint2Tests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()