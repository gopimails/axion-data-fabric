"""Configuration and environment loading for OpenSky data pipeline."""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


# Switzerland bounding box coordinates
SWITZERLAND_BBOX = {
    "lamin": 45.8,
    "lomin": 5.9,
    "lamax": 47.9,
    "lomax": 10.5,
}


class Config:
    """Central configuration object."""

    # OpenSky API credentials (OAuth v2, optional for free tier)
    CLIENT_ID: Optional[str] = os.getenv("CLIENT_ID")
    CLIENT_SECRET: Optional[str] = os.getenv("CLIENT_SECRET")

    # Storage path for parquet files
    STORAGE_PATH: str = os.getenv("STORAGE_PATH", "data/raw")

    # API configuration
    OPENSKY_API_BASE_URL: str = "https://opensky-network.org"
    OPENSKY_STATES_ENDPOINT: str = "/api/states/all"
    API_TIMEOUT_SECONDS: int = 10
    MAX_RETRIES: int = 3
    RETRY_BACKOFF_FACTOR: float = 2.0

    @classmethod
    def validate(cls) -> None:
        """Validate configuration and fail early if required values are missing."""
        # Create storage path if it doesn't exist
        Path(cls.STORAGE_PATH).mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_storage_path(cls) -> Path:
        """Get the storage path as a Path object."""
        return Path(cls.STORAGE_PATH)

    @classmethod
    def has_auth(cls) -> bool:
        """Check if authentication credentials are configured."""
        return cls.CLIENT_ID is not None and cls.CLIENT_SECRET is not None


# Validate config on import
Config.validate()
