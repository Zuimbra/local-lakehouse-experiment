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
    CONTROL_STATUSES,
    EXPECTED_COLUMNS,
    LEGACY_SOURCE_FILE,
    METADATA_COLUMNS,
    add_legacy_root_file_if_needed,
    append_control_event,
    calculate_file_hash,
    calculate_row_id,
    create_control_event,
    create_raw_directories,
    discover_input_files,
    get_control_table_path,
    load_successful_file_hashes,
    move_file_to_quarantine,
    prepare_bronze_dataframe,
    should_skip_file_hash,
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


class BronzeSprint4Tests(unittest.TestCase):
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

    def test_write_delta_table_migrates_old_schema(self) -> None:
        """
        Confirma que uma Bronze antiga, sem metadados, pode ser
        sobrescrita pela versão da Sprint 3 com o novo schema.
        """
        try:
            from deltalake import DeltaTable, write_deltalake
        except ImportError:
            self.skipTest("deltalake não está instalado.")

        with tempfile.TemporaryDirectory() as temporary_directory:
            project_dir = Path(temporary_directory)
            source_path = project_dir / "valid.csv"

            old_dataframe = build_valid_dataframe()
            old_dataframe.to_csv(
                source_path,
                index=False,
            )

            bronze_path = (
                project_dir
                / "data"
                / "lakehouse"
                / "01_bronze"
                / source_path.stem
            )
            bronze_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            # Simula a Delta Table criada antes da Sprint 3:
            # contém somente as 37 colunas originais.
            write_deltalake(
                bronze_path,
                old_dataframe,
                mode="overwrite",
            )

            old_persisted = DeltaTable(
                str(bronze_path)
            ).to_pandas()

            self.assertEqual(
                len(old_persisted.columns),
                len(EXPECTED_COLUMNS),
            )

            validation_result = validate_input_file(source_path)

            write_current_bronze_table(
                project_dir=project_dir,
                validation_result=validation_result,
                batch_id="batch-schema-migration-test",
                ingested_at=datetime(
                    2026,
                    7,
                    30,
                    17,
                    30,
                    tzinfo=timezone.utc,
                ),
            )

            migrated = DeltaTable(
                str(bronze_path)
            ).to_pandas()

            self.assertEqual(
                len(migrated.columns),
                len(EXPECTED_COLUMNS) + len(METADATA_COLUMNS),
            )

            for metadata_column in METADATA_COLUMNS:
                self.assertIn(
                    metadata_column,
                    migrated.columns,
                )


    def test_control_table_path(self) -> None:
        project_dir = Path("project")

        control_path = get_control_table_path(project_dir)

        self.assertEqual(
            control_path,
            (
                project_dir
                / "data"
                / "lakehouse"
                / "00_control"
                / "ingestion_files"
            ),
        )

    def test_processing_event_has_no_finished_at(self) -> None:
        started_at = datetime(
            2026,
            7,
            30,
            17,
            30,
            tzinfo=timezone.utc,
        )

        event = create_control_event(
            batch_id="batch-123",
            source_file="logs.csv",
            source_file_hash="hash-123",
            status="processing",
            started_at=started_at,
            recorded_at=started_at,
        )

        self.assertEqual(event.status, "PROCESSING")
        self.assertEqual(event.stage, "BRONZE")
        self.assertIsNone(event.finished_at)
        self.assertEqual(event.started_at, started_at)

    def test_final_event_receives_finished_at(self) -> None:
        started_at = datetime(
            2026,
            7,
            30,
            17,
            30,
            tzinfo=timezone.utc,
        )
        recorded_at = datetime(
            2026,
            7,
            30,
            17,
            31,
            tzinfo=timezone.utc,
        )

        event = create_control_event(
            batch_id="batch-123",
            source_file="logs.csv",
            source_file_hash="hash-123",
            status="SUCCESS",
            started_at=started_at,
            row_count=10,
            inserted_row_count=10,
            duplicate_row_count=0,
            recorded_at=recorded_at,
        )

        self.assertEqual(event.finished_at, recorded_at)
        self.assertEqual(event.row_count, 10)
        self.assertEqual(event.inserted_row_count, 10)

    def test_invalid_control_status_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            create_control_event(
                batch_id="batch-123",
                source_file="logs.csv",
                source_file_hash="hash-123",
                status="UNKNOWN",
                started_at=datetime.now(timezone.utc),
            )

        self.assertEqual(
            CONTROL_STATUSES,
            ("PROCESSING", "SUCCESS", "FAILED", "SKIPPED"),
        )

    def test_negative_control_count_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            create_control_event(
                batch_id="batch-123",
                source_file="logs.csv",
                source_file_hash="hash-123",
                status="SUCCESS",
                started_at=datetime.now(timezone.utc),
                row_count=-1,
            )

    def test_only_successful_hash_is_skipped(self) -> None:
        successful_hashes = {"successful-hash"}

        self.assertTrue(
            should_skip_file_hash(
                "successful-hash",
                successful_hashes,
            )
        )
        self.assertFalse(
            should_skip_file_hash(
                "failed-hash",
                successful_hashes,
            )
        )

    def test_add_legacy_root_file_when_inbox_does_not_have_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            raw_path = Path(temporary_directory)
            legacy_path = raw_path / LEGACY_SOURCE_FILE
            legacy_path.write_text(
                "column\\nvalue\\n",
                encoding="utf-8",
            )

            files = add_legacy_root_file_if_needed(
                input_files=[],
                raw_path=raw_path,
            )

            self.assertEqual(files, [legacy_path])

    def test_inbox_legacy_file_has_priority_over_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_path = root / "raw"
            inbox_path = raw_path / "inbox"
            inbox_path.mkdir(parents=True)

            root_legacy = raw_path / LEGACY_SOURCE_FILE
            inbox_legacy = inbox_path / LEGACY_SOURCE_FILE
            root_legacy.write_text("root", encoding="utf-8")
            inbox_legacy.write_text("inbox", encoding="utf-8")

            files = add_legacy_root_file_if_needed(
                input_files=[inbox_legacy],
                raw_path=raw_path,
            )

            self.assertEqual(files, [inbox_legacy])

    def test_control_table_appends_history_and_loads_success_hashes(self) -> None:
        try:
            from deltalake import DeltaTable
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("deltalake/pyarrow não estão instalados.")

        with tempfile.TemporaryDirectory() as temporary_directory:
            control_path = (
                Path(temporary_directory)
                / "ingestion_files"
            )
            started_at = datetime(
                2026,
                7,
                30,
                17,
                30,
                tzinfo=timezone.utc,
            )

            processing_event = create_control_event(
                batch_id="batch-123",
                source_file="logs.csv",
                source_file_hash="successful-hash",
                status="PROCESSING",
                started_at=started_at,
                recorded_at=started_at,
            )
            success_event = create_control_event(
                batch_id="batch-123",
                source_file="logs.csv",
                source_file_hash="successful-hash",
                status="SUCCESS",
                started_at=started_at,
                finished_at=datetime(
                    2026,
                    7,
                    30,
                    17,
                    31,
                    tzinfo=timezone.utc,
                ),
                row_count=10,
                inserted_row_count=10,
                duplicate_row_count=0,
            )
            failed_event = create_control_event(
                batch_id="batch-456",
                source_file="bad.csv",
                source_file_hash="failed-hash",
                status="FAILED",
                started_at=started_at,
                error_message="schema inválido",
            )

            append_control_event(control_path, processing_event)
            append_control_event(control_path, success_event)
            append_control_event(control_path, failed_event)

            persisted = DeltaTable(
                str(control_path)
            ).to_pandas()

            self.assertEqual(len(persisted), 3)
            self.assertEqual(
                set(persisted["status"].tolist()),
                {"PROCESSING", "SUCCESS", "FAILED"},
            )
            self.assertEqual(
                load_successful_file_hashes(control_path),
                {"successful-hash"},
            )


if __name__ == "__main__":
    unittest.main()