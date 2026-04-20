"""OpenSky Network API client with retry logic."""
import requests
import time
import logging
from typing import Optional, Dict, Any
from src.config import Config

logger = logging.getLogger(__name__)


def fetch_states(
    bounding_box: Optional[Dict[str, float]] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch aircraft state vectors from OpenSky API.

    Args:
        bounding_box: Dictionary with keys lamin, lomin, lamax, lomax.
                      If None, fetches global states.
        token: Optional OAuth token for authenticated requests.

    Returns:
        Parsed JSON response as dict with 'time' and 'states' keys.

    Raises:
        Exception: If request fails after retries or on auth error.
    """
    url = f"{Config.OPENSKY_API_BASE_URL}{Config.OPENSKY_STATES_ENDPOINT}"
    params = {}

    if bounding_box:
        params.update(bounding_box)

    headers = {
        "User-Agent": "OpenSkyDataPipeline/1.0",
    }

    if token:
        headers["Authorization"] = f"Bearer {token}"

    attempt = 0
    last_error = None

    while attempt < Config.MAX_RETRIES:
        try:
            logger.debug(
                f"Fetching states (attempt {attempt + 1}/{Config.MAX_RETRIES}): {url}"
            )
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=Config.API_TIMEOUT_SECONDS,
            )

            # Handle authentication errors
            if response.status_code == 401:
                raise Exception("Authentication failed (401). Check credentials.")

            response.raise_for_status()
            result = response.json()
            logger.info(f"Successfully fetched states: {len(result.get('states', []))} aircraft")
            return result

        except requests.exceptions.Timeout as e:
            last_error = e
            logger.warning(f"Timeout on attempt {attempt + 1}: {e}")
        except requests.exceptions.ConnectionError as e:
            last_error = e
            logger.warning(f"Connection error on attempt {attempt + 1}: {e}")
        except requests.exceptions.HTTPError as e:
            if response.status_code >= 500:
                # Retry on 5xx errors
                last_error = e
                logger.warning(f"Server error {response.status_code} on attempt {attempt + 1}: {e}")
            else:
                # Don't retry on client errors (4xx)
                raise Exception(f"HTTP error {response.status_code}: {e}") from e
        except Exception as e:
            if "Authentication failed" in str(e):
                raise
            last_error = e
            logger.warning(f"Unexpected error on attempt {attempt + 1}: {e}")

        attempt += 1

        # Exponential backoff before retry
        if attempt < Config.MAX_RETRIES:
            wait_time = Config.RETRY_BACKOFF_FACTOR ** (attempt - 1)
            logger.debug(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)

    # All retries exhausted
    raise Exception(
        f"Failed to fetch OpenSky states after {Config.MAX_RETRIES} attempts: {last_error}"
    )
