from pathlib import Path

import pandas as pd


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
        / "lake"
        / "01_bronze"
    )

    print(f"[LAKE]Loading data from: {raw_path}")

    if not raw_path.exists():
        raise FileNotFoundError(
            f"The file {raw_path} does not exist."
        )

    try:
        df = pd.read_csv(raw_path)

        bronze_path.mkdir(parents=True, exist_ok=True)

        output_path = (
            bronze_path
            / "logs_rastreador_2026-07-01.parquet"
        )

        df.to_parquet(output_path, index=False)

        print(f"[Lake] Rows loaded: {len(df)}")
        print(f"[Lake] Data successfully saved to: {output_path}")

    except Exception as e:
        raise RuntimeError(
            f"An error occurred while loading the data: {e}"
        ) from e


if __name__ == "__main__":
    load_bronze_data()