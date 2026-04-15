"""Analytics module for generating Power BI-ready summary outputs."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = BASE_DIR / "data" / "cleaned_data.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "dashboard"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger(__name__)


def load_cleaned_data(file_path: str | Path) -> pd.DataFrame:
    """Load the cleaned dataset used for analytics outputs."""
    source_path = Path(file_path)
    LOGGER.info("Loading cleaned dataset from %s", source_path)

    if not source_path.exists():
        raise FileNotFoundError(f"Cleaned data file not found: {source_path}")

    dataframe = pd.read_csv(source_path, parse_dates=["Order Date"])
    return dataframe


def _resolve_product_column(dataframe: pd.DataFrame) -> str:
    """Return the most suitable column to represent products in analysis."""
    if "Product" in dataframe.columns:
        return "Product"
    if "Sub-Category" in dataframe.columns:
        return "Sub-Category"
    raise ValueError("No suitable product-level column found for top product analysis.")


def top_products_by_sales(dataframe: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """Calculate the top-selling products or sub-categories by sales."""
    product_column = _resolve_product_column(dataframe)
    summary_df = (
        dataframe.groupby(product_column, as_index=False)["Sales"]
        .sum()
        .sort_values("Sales", ascending=False)
        .head(top_n)
        .rename(columns={product_column: "Product"})
    )
    return summary_df


def region_wise_total_sales(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Aggregate total sales by region."""
    summary_df = (
        dataframe.groupby("Region", as_index=False)["Sales"]
        .sum()
        .sort_values("Sales", ascending=False)
    )
    return summary_df


def monthly_sales_trends(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Generate a monthly sales trend table."""
    if "Month" not in dataframe.columns:
        dataframe = dataframe.copy()
        dataframe["Month"] = dataframe["Order Date"].dt.strftime("%Y-%m")

    summary_df = (
        dataframe.groupby("Month", as_index=False)["Sales"]
        .sum()
        .sort_values("Month")
    )
    return summary_df


def save_outputs(output_tables: Dict[str, pd.DataFrame], output_dir: str | Path) -> None:
    """Save analytics outputs as CSV files for downstream dashboarding."""
    destination_dir = Path(output_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)

    for file_name, dataframe in output_tables.items():
        output_path = destination_dir / file_name
        dataframe.to_csv(output_path, index=False)
        LOGGER.info("Saved analysis output to %s", output_path)


def run_analysis(
    input_path: str | Path = DEFAULT_INPUT_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> Dict[str, pd.DataFrame]:
    """Run the full analytics flow and return generated summary tables."""
    cleaned_df = load_cleaned_data(input_path)

    outputs = {
        "top_5_products_by_sales.csv": top_products_by_sales(cleaned_df),
        "region_wise_total_sales.csv": region_wise_total_sales(cleaned_df),
        "monthly_sales_trends.csv": monthly_sales_trends(cleaned_df),
    }
    save_outputs(outputs, output_dir)
    return outputs


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the analytics module."""
    parser = argparse.ArgumentParser(description="Run retail sales analysis module.")
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT_PATH),
        help="Path to the cleaned CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory to save analysis outputs.",
    )
    return parser.parse_args()


def main() -> Optional[int]:
    """Run analytics generation from the command line."""
    args = parse_args()
    try:
        run_analysis(args.input, args.output_dir)
        LOGGER.info("Analysis module completed successfully.")
        return 0
    except Exception as exc:
        LOGGER.exception("Analysis module failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
