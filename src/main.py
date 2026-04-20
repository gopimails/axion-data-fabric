"""Main orchestration entry point for OpenSky data ingestion."""
import logging
from datetime import datetime, timezone
from src.config import Config, SWITZERLAND_BBOX
from src.auth import get_opensky_token
from src.client import fetch_states
from src.normalize import normalize_states
from src.storage import write_parquet

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def ingest_once() -> tuple[bool, str]:
    """
    Execute a single ingestion cycle: fetch, normalize, and store.

    Returns:
        Tuple of (success: bool, output_path: str)
    """
    try:
        logger.info("Starting OpenSky data ingestion cycle")

        # Get auth token if credentials are configured
        token = None
        if Config.has_auth():
            try:
                token = get_opensky_token()
                logger.info("Using authenticated OpenSky API")
            except Exception as e:
                logger.error(f"Failed to get auth token: {e}")
                logger.info("Falling back to unauthenticated API")
                token = None

        # Fetch aircraft states
        logger.info(f"Fetching states for Switzerland bounding box: {SWITZERLAND_BBOX}")
        api_response = fetch_states(bounding_box=SWITZERLAND_BBOX, token=token)

        # Normalize response
        rows = normalize_states(api_response)
        if not rows:
            logger.warning("No aircraft states found in response")
            return False, ""

        # Get snapshot time for partitioning
        snapshot_time = datetime.fromtimestamp(api_response["time"], tz=timezone.utc)

        # Write to Parquet
        output_path = write_parquet(
            rows=rows,
            base_path=str(Config.get_storage_path()),
            snapshot_time=snapshot_time,
        )

        logger.info(f"Ingestion complete: {len(rows)} rows written to {output_path}")
        return True, output_path

    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        return False, ""


if __name__ == "__main__":
    success, output_path = ingest_once()
    if success:
        logger.info(f"✓ Data ingestion successful: {output_path}")
        exit(0)
    else:
        logger.error("✗ Data ingestion failed")
        exit(1)
