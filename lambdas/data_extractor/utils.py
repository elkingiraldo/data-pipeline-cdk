"""Utility functions for Lambda."""

import logging
import os
from datetime import datetime, UTC
from typing import Dict


def setup_logging(level: str = None) -> logging.Logger:
    """
    Setup logging configuration.

    Args:
        level: Logging level

    Returns:
        Configured logger
    """
    log_level = level or os.environ.get("LOG_LEVEL", "INFO")

    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger()

    # Reduce noise from boto3
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    return logger


def get_partition_path() -> str:
    """
    Get S3 partition path based on current date.

    Returns:
        Partition path string (year=YYYY/month=MM/day=DD)
    """
    now = datetime.now(UTC)
    return f"year={now.year}/month={now.month:02d}/day={now.day:02d}"


def validate_environment() -> Dict[str, str]:
    """
    Validate required environment variables.

    Returns:
        Dictionary of environment variables

    Raises:
        ValueError: If required variables are missing
    """
    required_vars = ["DATA_BUCKET_NAME"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    return {
        "DATA_BUCKET_NAME": os.environ["DATA_BUCKET_NAME"],
        "API_ENDPOINT": os.environ.get("API_ENDPOINT", "https://jsonplaceholder.typicode.com/users"),
        "OUTPUT_FORMAT": os.environ.get("OUTPUT_FORMAT", "parquet"),
        "LOG_LEVEL": os.environ.get("LOG_LEVEL", "INFO")
    }
