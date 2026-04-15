"""Automation entry point for refreshing the retail analytics pipeline."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from analysis import run_analysis
from etl import DEFAULT_INPUT_PATH, DEFAULT_OUTPUT_PATH, run_etl


BASE_DIR = Path(__file__).resolve().parents[1]
LOG_FILE_PATH = BASE_DIR / "refresh.log"
DASHBOARD_DIR = BASE_DIR / "dashboard"


def configure_logger() -> logging.Logger:
    """Configure a rotating logger for refresh runs."""
    logger = logging.getLogger("retailpulse.refresh")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=1_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def run_refresh() -> None:
    """Run the ETL and analysis pipeline in sequence."""
    logger = configure_logger()
    logger.info("RetailPulse refresh started.")

    cleaned_df = run_etl(DEFAULT_INPUT_PATH, DEFAULT_OUTPUT_PATH)
    logger.info("ETL completed with %s records.", len(cleaned_df))

    output_tables = run_analysis(DEFAULT_OUTPUT_PATH, DASHBOARD_DIR)
    logger.info(
        "Analysis completed and generated %s dashboard dataset(s).",
        len(output_tables),
    )
    logger.info("RetailPulse refresh finished successfully.")


def main() -> Optional[int]:
    """Execute the scheduled refresh workflow."""
    try:
        run_refresh()
        return 0
    except Exception as exc:
        logger = configure_logger()
        logger.exception("RetailPulse refresh failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
