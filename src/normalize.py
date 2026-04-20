"""Normalize OpenSky state vectors into tabular format."""
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


def _parse_state_vector(state_array: List[Any], snapshot_time_utc: datetime) -> Dict[str, Any]:
    """
    Convert OpenSky state array into a dict with named columns.

    OpenSky returns state vectors as arrays. This function unpacks them into explicit columns.

    State array indices (per OpenSky API docs):
    0: icao24
    1: callsign
    2: origin_country
    3: time_position
    4: last_contact
    5: longitude
    6: latitude
    7: baro_altitude
    8: on_ground
    9: velocity
    10: true_track
    11: vertical_rate
    12: geo_altitude
    13: squawk
    14: spi
    15: position_source
    16: category (optional in some responses)

    Args:
        state_array: List/tuple from OpenSky API state vectors.
        snapshot_time_utc: Timestamp of the API response.

    Returns:
        Dictionary with named columns.
    """
    row = {
        "snapshot_time_utc": snapshot_time_utc,
        "ingest_time_utc": datetime.now(timezone.utc),
        "icao24": state_array[0] if len(state_array) > 0 else None,
        "callsign": state_array[1] if len(state_array) > 1 else None,
        "origin_country": state_array[2] if len(state_array) > 2 else None,
        "time_position": state_array[3] if len(state_array) > 3 else None,
        "last_contact": state_array[4] if len(state_array) > 4 else None,
        "longitude": state_array[5] if len(state_array) > 5 else None,
        "latitude": state_array[6] if len(state_array) > 6 else None,
        "baro_altitude": state_array[7] if len(state_array) > 7 else None,
        "on_ground": state_array[8] if len(state_array) > 8 else None,
        "velocity": state_array[9] if len(state_array) > 9 else None,
        "true_track": state_array[10] if len(state_array) > 10 else None,
        "vertical_rate": state_array[11] if len(state_array) > 11 else None,
        "geo_altitude": state_array[12] if len(state_array) > 12 else None,
        "squawk": state_array[13] if len(state_array) > 13 else None,
        "spi": state_array[14] if len(state_array) > 14 else None,
        "position_source": state_array[15] if len(state_array) > 15 else None,
    }
    return row


def normalize_states(api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert OpenSky API response into list of normalized row dicts.

    Args:
        api_response: JSON response from /api/states/all endpoint.
                      Expected keys: 'time' (int, unix timestamp), 'states' (list of arrays).

    Returns:
        List of dictionaries, one per aircraft state.
        Empty list if no states in response.
    """
    if not api_response or "states" not in api_response:
        logger.warning("API response missing 'states' key or response is None")
        return []

    states = api_response.get("states", [])
    api_time = api_response.get("time")

    if not states:
        logger.info("No aircraft states in API response")
        return []

    # Convert unix timestamp to datetime
    try:
        snapshot_time_utc = datetime.fromtimestamp(api_time, tz=timezone.utc)
    except (TypeError, ValueError) as e:
        logger.error(f"Invalid timestamp in API response: {api_time} - {e}")
        snapshot_time_utc = datetime.now(timezone.utc)

    rows = []
    for state_array in states:
        try:
            row = _parse_state_vector(state_array, snapshot_time_utc)
            rows.append(row)
        except Exception as e:
            logger.warning(f"Failed to parse state vector: {e}")
            continue

    logger.info(f"Normalized {len(rows)} aircraft states from API response")
    return rows
