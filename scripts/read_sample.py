"""Helper script to read and display sample data from collected Parquet files."""
import argparse
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_all_parquet_files(base_path: str, limit: int = 10) -> pd.DataFrame:
    """
    Recursively read all Parquet files from partitioned directory structure.

    Args:
        base_path: Root data directory (e.g., 'data/raw').
        limit: Maximum rows to display.

    Returns:
        Concatenated DataFrame from all Parquet files.
    """
    base_dir = Path(base_path)

    if not base_dir.exists():
        logger.warning(f"Directory does not exist: {base_path}")
        return pd.DataFrame()

    # Find all parquet files
    parquet_files = list(base_dir.rglob("*.parquet"))

    if not parquet_files:
        logger.warning(f"No Parquet files found in {base_path}")
        return pd.DataFrame()

    logger.info(f"Found {len(parquet_files)} Parquet files")

    # Read all files
    dfs = []
    for filepath in sorted(parquet_files, reverse=True):
        try:
            df = pd.read_parquet(filepath)
            dfs.append(df)
            logger.debug(f"Loaded {filepath}: {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to read {filepath}: {e}")

    if not dfs:
        logger.warning("No data could be loaded from Parquet files")
        return pd.DataFrame()

    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded total {len(combined_df)} rows from all files")

    return combined_df.head(limit)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Read and display sample aircraft data from Parquet files"
    )
    parser.add_argument(
        "--path",
        default="data/raw",
        help="Root path to Parquet data (default: data/raw)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of rows to display (default: 10)",
    )

    args = parser.parse_args()

    # Load data
    df = read_all_parquet_files(args.path, limit=args.limit)

    if df.empty:
        print("No data available")
        return

    # Display info
    print(f"\n{'='*80}")
    print(f"Dataset shape: {df.shape[0]} rows, {df.shape[1]} columns")
    print(f"{'='*80}\n")

    print("Columns:")
    print(df.dtypes)
    print(f"\n{'='*80}\n")

    print(f"Sample data (first {min(len(df), args.limit)} rows):")
    print(df.to_string())
    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()
