"""
API Connection Manager
Creates and destroys API connections cleanly.
Supports use as a context manager (with statement) or manual open/close.
"""

import requests
from urllib3.exceptions import InsecureRequestWarning
import logging
import json
from json import JSONDecodeError
import time
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class API_Connection_Manager:
    """
    Manages an API connection lifecycle — create, use, destroy.

    Usage (context manager):
        with API_Connection_Manager('https://api.example.com', token='abc123') as api:
            data = api.get('/endpoint')

    Usage (manual):
        api = API_Connection_Manager('https://api.example.com', token='abc123')
        api.connect()
        data = api.get('/endpoint')
        api.disconnect()
    """

    def __init__(
        self,
        base_url: str,
        token: str = None,
        username: str = None,
        password: str = None,
        headers: Dict[str, str] = None,
        timeout: int = 30,
        verify_ssl: bool = True,
        retries: int = 3,
        retry_delay: float = 1.0,
    ):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.username = username
        self.password = password
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.retries = retries
        self.retry_delay = retry_delay

        self._session: Optional[requests.Session] = None
        self._connected = False

        # Merge any custom headers
        self._extra_headers = headers or {}

    # ─── Connection Lifecycle ─────────────────────────────────────────────────

    def connect(self) -> bool:
        """Create session and validate connection."""
        try:
            self._session = requests.Session()
            self._session.verify = self.verify_ssl

            if not self.verify_ssl:
                requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

            # Auth
            if self.token:
                self._session.headers.update({'Authorization': f'Bearer {self.token}'})
            elif self.username and self.password:
                self._session.auth = (self.username, self.password)

            # Extra headers
            if self._extra_headers:
                self._session.headers.update(self._extra_headers)

            self._connected = True
            logger.info(f"API connection established: {self.base_url}")
            return True

        except Exception as e:
            logger.error(f"API connection failed: {e}")
            self._connected = False
            return False

    def disconnect(self):
        """Close session and clean up."""
        if self._session:
            try:
                self._session.close()
                logger.info(f"API connection closed: {self.base_url}")
            except Exception as e:
                logger.error(f"Error closing API connection: {e}")
            finally:
                self._session = None
                self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected and self._session is not None

    # ─── Context Manager ──────────────────────────────────────────────────────

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False  # don't suppress exceptions

    # ─── HTTP Methods ─────────────────────────────────────────────────────────

    def _request(self, method: str, endpoint: str, **kwargs) -> Optional[requests.Response]:
        """Internal request handler with retry logic."""
        if not self.is_connected:
            raise RuntimeError("Not connected — call connect() first or use as context manager")

        url = f"{self.base_url}{endpoint}"
        last_error = None

        for attempt in range(1, self.retries + 1):
            try:
                response = self._session.request(
                    method, url, timeout=self.timeout, **kwargs
                )
                response.raise_for_status()
                return response

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error [{method} {url}]: {e}")
                return response  # return even on HTTP errors so caller can inspect

            except requests.exceptions.ConnectionError as e:
                last_error = e
                logger.warning(f"Connection error (attempt {attempt}/{self.retries}): {e}")
                if attempt < self.retries:
                    time.sleep(self.retry_delay)

            except requests.exceptions.Timeout:
                last_error = TimeoutError(f"Request timed out after {self.timeout}s")
                logger.warning(f"Timeout (attempt {attempt}/{self.retries}): {url}")
                if attempt < self.retries:
                    time.sleep(self.retry_delay)

            except Exception as e:
                logger.error(f"Unexpected error [{method} {url}]: {e}")
                raise

        logger.error(f"All {self.retries} attempts failed: {url}")
        raise last_error

    def get(self, endpoint: str, params: Dict = None, **kwargs) -> Optional[Dict[str, Any]]:
        """GET request — returns parsed JSON or None."""
        response = self._request('GET', endpoint, params=params, **kwargs)
        return response.json() if response else None

    def post(self, endpoint: str, data: Dict = None, payload: Dict = None, **kwargs) -> Optional[Dict[str, Any]]:
        """POST request — returns parsed JSON or None."""
        response = self._request('POST', endpoint, data=data, json=payload, **kwargs)
        if response is None:
            return response.status_code, response.text if response else False
        try:
            return response.status_code, response.json()
        except (json.JSONDecodeError, ValueError):
            return response.status_code, response.text

    def put(self, endpoint: str, payload: Dict = None, **kwargs) -> Optional[Dict[str, Any]]:
        """PUT request — returns parsed JSON, raw text, or True/False on empty body."""
        response = self._request('PUT', endpoint, json=payload, **kwargs)
        if response is None:
            return response.status_code, response.text if response else False
        try:
            return response.status_code, response.json()
        except (json.JSONDecodeError, ValueError):
            return response.status_code, response.text
        
    def patch(self, endpoint: str, payload: Dict = None, **kwargs) -> Optional[Dict[str, Any]]:
        """PATCH request — returns parsed JSON, raw text, or True/False on empty body."""
        response = self._request('PATCH', endpoint, json=payload, **kwargs)
        if response is None:
            return response.status_code, response.text if response else False
        try:
            return response.status_code, response.json()
        except (json.JSONDecodeError, ValueError):
            return response.status_code, response.text


    def delete(self, endpoint: str, **kwargs) -> bool:
        """DELETE request — returns True on success."""
        response = self._request('DELETE', endpoint, **kwargs)
        return response is not None and response.ok

    def raw(self, method: str, endpoint: str, **kwargs) -> Optional[requests.Response]:
        """Raw request — returns full Response object for custom handling."""
        return self._request(method, endpoint, **kwargs)
    
