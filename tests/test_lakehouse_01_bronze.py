import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse_01_bronze import (  # noqa: E402
    create_raw_directories,
    discover_input_files,
)


class BronzeFileDiscoveryTests(unittest.TestCase):
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

    def test_empty_inbox_returns_empty_list(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            inbox_path = Path(temporary_directory)

            discovered_files = discover_input_files(inbox_path)

            self.assertEqual(discovered_files, [])


if __name__ == "__main__":
    unittest.main()