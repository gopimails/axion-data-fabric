"""Authentication token management for OpenSky Network API."""
import requests
from typing import Optional, Dict
from src.config import Config
import logging

logger = logging.getLogger(__name__)

# OpenSky OAuth2 Token Endpoint
TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)


class AuthTokenManager:
    """Manages OpenSky API authentication tokens."""

    _token: Optional[str] = None
    _token_cached: bool = False

    @classmethod
    def get_token(cls) -> Optional[str]:
        """
        Retrieve OpenSky API token using OAuth v2 client credentials flow.

        Returns:
            OAuth token string if credentials are configured, None otherwise.
            Token is cached in memory for the session.

        Raises:
            Exception: If authentication fails and credentials are configured.
        """
        if not Config.has_auth():
            return None

        # Return cached token if available
        if cls._token_cached:
            return cls._token

        try:
            logger.debug("Attempting to fetch OpenSky authentication token")
            data = {
                "grant_type": "client_credentials",
                "client_id": Config.CLIENT_ID,
                "client_secret": Config.CLIENT_SECRET,
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            
            response = requests.post(
                TOKEN_URL,
                data=data,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            
            payload: Dict = response.json()
            token = payload.get("access_token")
            if not token:
                raise RuntimeError(f"Token endpoint returned no access_token: {payload}")
            
            cls._token = token
            cls._token_cached = True
            logger.info("Successfully obtained OpenSky authentication token")
            return cls._token
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch OpenSky token: {e}")
            raise Exception(f"Authentication failed: {e}") from e

    @classmethod
    def reset(cls) -> None:
        """Clear cached token (useful for testing)."""
        cls._token = None
        cls._token_cached = False


def get_opensky_token() -> Optional[str]:
    """
    Convenience function to get OpenSky authentication token.

    Returns:
        Token string or None if no auth is configured.
    """
    return AuthTokenManager.get_token()
