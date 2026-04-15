"""ETL pipeline for preparing retail sales data for analytics."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = BASE_DIR / "data" / "raw_sales.csv"
DEFAULT_OUTPUT_PATH = BASE_DIR / "data" / "cleaned_data.csv"
REQUIRED_COLUMNS = {
    "Order Date",
    "Sales",
    "Profit",
    "Region",
    "Category",
    "Sub-Category",
    "Customer Segment",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger(__name__)


def load_data(file_path: str | Path) -> pd.DataFrame:
    """Load raw retail sales data from a CSV file."""
    source_path = Path(file_path)
    LOGGER.info("Loading data from %s", source_path)

    if not source_path.exists():
        raise FileNotFoundError(f"Input data file not found: {source_path}")

    dataframe = pd.read_csv(source_path)
    missing_columns = REQUIRED_COLUMNS.difference(dataframe.columns)
    if missing_columns:
        missing_list = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns: {missing_list}")

    return dataframe


def clean_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Clean input data by handling nulls, types, and duplicate records."""
    cleaned_df = dataframe.copy()

    cleaned_df = cleaned_df.drop_duplicates().reset_index(drop=True)

    cleaned_df["Order Date"] = pd.to_datetime(
        cleaned_df["Order Date"],
        errors="coerce",
    )

    numeric_columns = ["Sales", "Profit"]
    for column_name in numeric_columns:
        cleaned_df[column_name] = pd.to_numeric(
            cleaned_df[column_name],
            errors="coerce",
        )

    text_columns = ["Region", "Category", "Sub-Category", "Customer Segment"]
    for column_name in text_columns:
        cleaned_df[column_name] = (
            cleaned_df[column_name]
            .astype("string")
            .str.strip()
            .replace({"": pd.NA})
            .fillna("Unknown")
        )

    cleaned_df["Sales"] = cleaned_df["Sales"].fillna(cleaned_df["Sales"].median())
    cleaned_df["Profit"] = cleaned_df["Profit"].fillna(0)
    cleaned_df = cleaned_df.dropna(subset=["Order Date"]).reset_index(drop=True)

    return cleaned_df


def transform_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Create analytics-ready features from the cleaned dataset."""
    transformed_df = dataframe.copy()

    transformed_df["Month"] = transformed_df["Order Date"].dt.strftime("%Y-%m")
    transformed_df["Profit Margin"] = np.where(
        transformed_df["Sales"].ne(0),
        transformed_df["Profit"] / transformed_df["Sales"],
        0,
    )
    transformed_df["Profit Margin"] = transformed_df["Profit Margin"].round(4)

    return transformed_df


def save_data(dataframe: pd.DataFrame, output_path: str | Path) -> Path:
    """Persist the transformed dataset to disk as a CSV file."""
    destination_path = Path(output_path)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(destination_path, index=False)
    LOGGER.info("Saved cleaned data to %s", destination_path)
    return destination_path


def run_etl(
    input_path: str | Path = DEFAULT_INPUT_PATH,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
) -> pd.DataFrame:
    """Execute the full ETL flow and return the transformed dataset."""
    raw_df = load_data(input_path)
    cleaned_df = clean_data(raw_df)
    transformed_df = transform_data(cleaned_df)
    save_data(transformed_df, output_path)
    return transformed_df


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the ETL pipeline."""
    parser = argparse.ArgumentParser(description="Run retail sales ETL pipeline.")
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT_PATH),
        help="Path to the input CSV file.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Path to save the cleaned CSV file.",
    )
    return parser.parse_args()


def main() -> Optional[int]:
    """Run ETL from the command line."""
    args = parse_args()
    try:
        run_etl(args.input, args.output)
        LOGGER.info("ETL pipeline completed successfully.")
        return 0
    except Exception as exc:
        LOGGER.exception("ETL pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
