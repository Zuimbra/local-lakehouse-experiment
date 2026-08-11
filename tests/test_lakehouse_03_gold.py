from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


PROJECT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse_03_gold import (  # noqa: E402
    get_lakehouse_paths,
    gold_supports_incremental_update,
    normalize_partition_values,
    replace_partitions,
)


class GoldSprint8Tests(unittest.TestCase):
    def test_normalize_partition_values(self) -> None:
        self.assertEqual(
            normalize_partition_values(
                ["2026-08-11", " 2026-08-10 ", "2026-08-10"]
            ),
            ("2026-08-10", "2026-08-11"),
        )

    def test_paths(self) -> None:
        paths = get_lakehouse_paths(Path("project"))

        self.assertEqual(
            paths["route_points"],
            Path("project")
            / "data"
            / "lakehouse"
            / "03_gold"
            / "device_route_points",
        )

    def test_missing_gold_is_not_incremental_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            paths = get_lakehouse_paths(
                Path(temporary_directory)
            )
            self.assertFalse(
                gold_supports_incremental_update(
                    paths
                )
            )

    def test_replace_partition_keeps_other_partition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            table_path = (
                Path(temporary_directory)
                / "route_points"
            )

            initial = pa.table(
                {
                    "event_date": pa.array(
                        [
                            "2026-08-10",
                            "2026-08-11",
                        ],
                        type=pa.string(),
                    ),
                    "value": pa.array(
                        [1, 2],
                        type=pa.int64(),
                    ),
                }
            )

            write_deltalake(
                table_path,
                initial,
                mode="overwrite",
                partition_by=["event_date"],
            )

            replacement = pa.table(
                {
                    "event_date": pa.array(
                        ["2026-08-10"],
                        type=pa.string(),
                    ),
                    "value": pa.array(
                        [99],
                        type=pa.int64(),
                    ),
                }
            )

            replace_partitions(
                table_path,
                replacement,
                partition_by="event_date",
                requested_partitions=(
                    "2026-08-10",
                ),
            )

            persisted = DeltaTable(
                str(table_path)
            ).to_pandas()

            values = {
                row.event_date: row.value
                for row in persisted.itertuples()
            }

            self.assertEqual(
                values["2026-08-10"],
                99,
            )
            self.assertEqual(
                values["2026-08-11"],
                2,
            )

    def test_empty_replacement_deletes_stale_partition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            table_path = (
                Path(temporary_directory)
                / "quality"
            )

            initial = pa.table(
                {
                    "metric_date": pa.array(
                        [
                            "2026-08-10",
                            "unknown",
                        ],
                        type=pa.string(),
                    ),
                    "value": pa.array(
                        [1, 2],
                        type=pa.int64(),
                    ),
                }
            )

            write_deltalake(
                table_path,
                initial,
                mode="overwrite",
                partition_by=["metric_date"],
            )

            empty = pa.table(
                {
                    "metric_date": pa.array(
                        [],
                        type=pa.string(),
                    ),
                    "value": pa.array(
                        [],
                        type=pa.int64(),
                    ),
                }
            )

            replace_partitions(
                table_path,
                empty,
                partition_by="metric_date",
                requested_partitions=("unknown",),
            )

            persisted = DeltaTable(
                str(table_path)
            ).to_pandas()

            self.assertNotIn(
                "unknown",
                persisted["metric_date"].tolist(),
            )
            self.assertIn(
                "2026-08-10",
                persisted["metric_date"].tolist(),
            )


if __name__ == "__main__":
    unittest.main()