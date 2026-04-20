"""Parquet storage with time-based partitioning."""
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
import logging
import uuid

logger = logging.getLogger(__name__)


def write_parquet(
    rows: List[Dict[str, Any]],
    base_path: str,
    snapshot_time: datetime,
) -> str:
    """
    Write normalized rows to partitioned Parquet file.

    Partitions by year, month, day, and hour based on snapshot_time.
    Each run gets a unique filename to prevent collisions.

    Args:
        rows: List of normalized row dictionaries.
        base_path: Base directory for storage (e.g., 'data/raw').
        snapshot_time: Timestamp to use for partition directory structure.

    Returns:
        Full path to the written Parquet file.

    Raises:
        Exception: If write fails.
    """
    if not rows:
        logger.warning("No rows to write to Parquet")
        return ""

    # Create partition directories
    year = snapshot_time.year
    month = snapshot_time.month
    day = snapshot_time.day
    hour = snapshot_time.hour

    partition_dir = (
        Path(base_path)
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"day={day:02d}"
        / f"hour={hour:02d}"
    )

    partition_dir.mkdir(parents=True, exist_ok=True)

    # Create unique filename
    timestamp_str = snapshot_time.strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    filename = f"snapshot_{timestamp_str}_{unique_id}.parquet"
    filepath = partition_dir / filename

    try:
        # Convert to DataFrame
        df = pd.DataFrame(rows)

        # Write with snappy compression
        df.to_parquet(
            filepath,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        logger.info(
            f"Successfully wrote {len(rows)} rows to {filepath}"
        )
        return str(filepath)

    except Exception as e:
        logger.error(f"Failed to write Parquet file {filepath}: {e}")
        raise


def read_sample_parquet(path: str, limit: int = 10) -> pd.DataFrame:
    """
    Read a Parquet file and return a sample.

    Args:
        path: Path to Parquet file.
        limit: Number of rows to return.

    Returns:
        DataFrame with sampled rows.
    """
    try:
        df = pd.read_parquet(path)
        return df.head(limit)
    except Exception as e:
        logger.error(f"Failed to read Parquet file {path}: {e}")
        raise
