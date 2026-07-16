from pathlib import Path

import pandas as pd
from deltalake import write_deltalake


def load_bronze_data():
    project_dir = Path(__file__).resolve().parent.parent

    raw_path = (
        project_dir
        / "data"
        / "raw"
        / "logs_rastreador_2026-07-01.csv"
    )

    bronze_path = (
        project_dir
        / "data"
        / "lakehouse"
        / "01_bronze"
        / "logs_rastreador_2026-07-01"
    )

    print(f"[LAKEHOUSE]Loading data from: {raw_path}")

    if not raw_path.exists():
        raise FileNotFoundError(
            f"The file {raw_path} does not exist."
        )

    try:
        df = pd.read_csv(raw_path)

        bronze_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        print(f"[Lakehouse] Writing Delta Table to: {bronze_path}")
        write_deltalake(
            bronze_path,
            df,
            mode="overwrite",
        )

        print(f"[Lakehouse] Data successfully saved to: {bronze_path}")

    except Exception as e:
        raise RuntimeError(
            f"An error occurred while loading the data: {e}"
        ) from e


if __name__ == "__main__":
    load_bronze_data()