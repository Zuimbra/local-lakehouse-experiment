from __future__ import annotations

from dataclasses import dataclass

from lakehouse_01_bronze import (
    BronzeLoadResult,
    load_bronze_data,
)
from lakehouse_02_silver import (
    SilverLoadResult,
    load_silver_data,
)
from lakehouse_03_gold import (
    GoldLoadResult,
    load_gold_data,
)


@dataclass(frozen=True)
class PipelineResult:
    status: str
    bronze: BronzeLoadResult
    silver: SilverLoadResult | None
    gold: GoldLoadResult | None


def run_pipeline() -> PipelineResult:
    """
    Executa Bronze → Silver → Gold transmitindo apenas o contexto
    necessário para o processamento incremental.
    """
    print("[Lakehouse][Pipeline] Starting Bronze...")
    bronze_result = load_bronze_data()

    if not bronze_result.has_new_data:
        print(
            "[Lakehouse][Pipeline] "
            "No new Bronze rows. Silver and Gold will not run."
        )
        return PipelineResult(
            status="NOOP",
            bronze=bronze_result,
            silver=None,
            gold=None,
        )

    print(
        "[Lakehouse][Pipeline] Starting Silver "
        f"for batches={bronze_result.batch_ids}..."
    )

    silver_result = load_silver_data(
        batch_ids=bronze_result.batch_ids,
    )

    if silver_result.mode == "NOOP":
        print(
            "[Lakehouse][Pipeline] "
            "Silver reported NOOP. Gold will not run."
        )
        return PipelineResult(
            status="SILVER_NOOP",
            bronze=bronze_result,
            silver=silver_result,
            gold=None,
        )

    print(
        "[Lakehouse][Pipeline] Starting Gold "
        f"for dates={silver_result.affected_event_dates}..."
    )

    gold_result = load_gold_data(
        affected_event_dates=(
            silver_result.affected_event_dates
        ),
        affected_rejection_dates=(
            silver_result.affected_rejection_dates
        ),
    )

    print(
        "[Lakehouse][Pipeline] Complete "
        f"| Silver={silver_result.mode} "
        f"| Gold={gold_result.mode}"
    )

    return PipelineResult(
        status="SUCCESS",
        bronze=bronze_result,
        silver=silver_result,
        gold=gold_result,
    )


if __name__ == "__main__":
    run_pipeline()