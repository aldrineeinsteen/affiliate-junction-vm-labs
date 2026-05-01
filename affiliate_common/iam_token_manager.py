"""
IAM Token Manager for IBM Cloud watsonx.data SaaS Authentication

This module handles IBM Cloud IAM token lifecycle management for watsonx.data SaaS.
Tokens are cached and automatically refreshed before expiration.
"""

import os
import time
import logging
import requests
from threading import Lock
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class IAMTokenManager:
    """
    Manages IBM Cloud IAM tokens for watsonx.data SaaS authentication.
    
    Features:
    - Automatic token refresh before expiration
    - Thread-safe token caching
    - Configurable refresh buffer (default: 5 minutes before expiry)
    """
    
    IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"
    DEFAULT_REFRESH_BUFFER = 300  # Refresh 5 minutes before expiration
    
    def __init__(self, api_key: str, refresh_buffer: int = DEFAULT_REFRESH_BUFFER):
        """
        Initialize IAM token manager.
        
        Args:
            api_key: IBM Cloud API key
            refresh_buffer: Seconds before expiration to refresh token (default: 300)
        """
        self.api_key = api_key
        self.refresh_buffer = refresh_buffer
        self._token: Optional[str] = None
        self._token_expiry: float = 0
        self._lock = Lock()
        
    def get_token(self) -> str:
        """
        Get a valid IAM token, refreshing if necessary.
        
        Returns:
            Valid IAM Bearer token
            
        Raises:
            Exception: If token retrieval fails
        """
        with self._lock:
            current_time = time.time()
            
            # Check if we need to refresh the token
            if self._token is None or current_time >= (self._token_expiry - self.refresh_buffer):
                self._refresh_token()
                
            return self._token
    
    def _refresh_token(self) -> None:
        """
        Refresh the IAM token from IBM Cloud.
        
        Raises:
            Exception: If token refresh fails
        """
        try:
            logger.info("Refreshing IAM token...")
            
            response = requests.post(
                self.IAM_TOKEN_URL,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json"
                },
                data={
                    "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
                    "apikey": self.api_key
                },
                timeout=30
            )
            
            response.raise_for_status()
            token_data = response.json()
            
            self._token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
            self._token_expiry = time.time() + expires_in
            
            logger.info(f"IAM token refreshed successfully. Expires in {expires_in} seconds.")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to refresh IAM token: {e}")
            raise Exception(f"IAM token refresh failed: {e}")
        except (KeyError, ValueError) as e:
            logger.error(f"Invalid IAM token response: {e}")
            raise Exception(f"Invalid IAM token response: {e}")
    
    def invalidate(self) -> None:
        """Force token refresh on next get_token() call."""
        with self._lock:
            self._token = None
            self._token_expiry = 0
            logger.info("IAM token invalidated")


# Global token manager instance (lazy initialization)
_token_manager: Optional[IAMTokenManager] = None
_token_manager_lock = Lock()


def get_token_manager() -> IAMTokenManager:
    """
    Get or create the global IAM token manager instance.
    
    Uses PRESTO_PASSWD environment variable as the API key.
    
    Returns:
        Global IAMTokenManager instance
        
    Raises:
        ValueError: If PRESTO_PASSWD environment variable is not set
    """
    global _token_manager
    
    with _token_manager_lock:
        if _token_manager is None:
            api_key = os.getenv("PRESTO_PASSWD")
            if not api_key:
                raise ValueError("PRESTO_PASSWD environment variable must be set for IAM authentication")
            
            _token_manager = IAMTokenManager(api_key)
            logger.info("Global IAM token manager initialized")
            
        return _token_manager


def get_iam_token() -> str:
    """
    Convenience function to get a valid IAM token.
    
    Returns:
        Valid IAM Bearer token
        
    Raises:
        ValueError: If PRESTO_PASSWD is not set
        Exception: If token retrieval fails
    """
    return get_token_manager().get_token()

# Made with Bob
