from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse_01_bronze import BronzeLoadResult  # noqa: E402
from lakehouse_02_silver import SilverLoadResult  # noqa: E402
from lakehouse_03_gold import GoldLoadResult  # noqa: E402
from lakehouse_pipeline import run_pipeline  # noqa: E402


class PipelineSprint8Tests(unittest.TestCase):
    @patch("lakehouse_pipeline.load_gold_data")
    @patch("lakehouse_pipeline.load_silver_data")
    @patch("lakehouse_pipeline.load_bronze_data")
    def test_no_new_bronze_skips_downstream(
        self,
        bronze_mock,
        silver_mock,
        gold_mock,
    ) -> None:
        bronze_mock.return_value = BronzeLoadResult(
            execution_batch_id="batch-1",
            batch_ids=(),
            has_new_data=False,
            inserted_row_count=0,
            source_files=(),
            validation_results=(),
        )

        result = run_pipeline()

        self.assertEqual(result.status, "NOOP")
        silver_mock.assert_not_called()
        gold_mock.assert_not_called()

    @patch("lakehouse_pipeline.load_gold_data")
    @patch("lakehouse_pipeline.load_silver_data")
    @patch("lakehouse_pipeline.load_bronze_data")
    def test_pipeline_passes_batch_and_dates(
        self,
        bronze_mock,
        silver_mock,
        gold_mock,
    ) -> None:
        bronze_mock.return_value = BronzeLoadResult(
            execution_batch_id="batch-2",
            batch_ids=("batch-2",),
            has_new_data=True,
            inserted_row_count=10,
            source_files=("new.csv",),
            validation_results=(),
        )

        silver_mock.return_value = SilverLoadResult(
            mode="INCREMENTAL",
            batch_ids=("batch-2",),
            affected_event_dates=("2026-08-10",),
            affected_rejection_dates=(
                "2026-08-10",
                "unknown",
            ),
            telemetry_rows_written=8,
            identity_rows_written=1,
            rejected_rows_written=1,
        )

        gold_mock.return_value = GoldLoadResult(
            mode="INCREMENTAL",
            affected_event_dates=("2026-08-10",),
            affected_rejection_dates=(
                "2026-08-10",
                "unknown",
            ),
            affected_devices=("123",),
            dim_device_rows_written=1,
            last_position_rows_written=1,
            route_points_rows_written=8,
            daily_summary_rows_written=1,
            quality_summary_rows_written=2,
        )

        result = run_pipeline()

        silver_mock.assert_called_once_with(
            batch_ids=("batch-2",),
        )
        gold_mock.assert_called_once_with(
            affected_event_dates=("2026-08-10",),
            affected_rejection_dates=(
                "2026-08-10",
                "unknown",
            ),
        )

        self.assertEqual(result.status, "SUCCESS")


if __name__ == "__main__":
    unittest.main()