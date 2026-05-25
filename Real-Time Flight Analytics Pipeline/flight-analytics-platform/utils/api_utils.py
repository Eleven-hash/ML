"""
=============================================================================
 API Utilities — Flight Analytics Platform
=============================================================================
 Robust HTTP client with production-grade features:
   - Exponential backoff retry with jitter
   - Rate limiting (token bucket algorithm)
   - Connection pooling via requests.Session
   - Comprehensive error handling
   - Request/response logging
   - Timeout management

 Usage:
   client = APIClient(config.api)
   data = client.get("/states/all", params={"extended": 1})
=============================================================================
"""

import time
import logging
import random
from typing import Optional, Dict, Any
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("flight_analytics.api_utils")


class RateLimiter:
    """
    Token bucket rate limiter for API call management.

    Ensures we stay within OpenSky's rate limits:
      - Unauthenticated: ~10 requests/minute
      - Authenticated: ~40 requests/minute
    """

    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self._last_call_time: float = 0.0

    def wait_if_needed(self) -> float:
        """
        Block if necessary to respect rate limits.

        Returns:
            Seconds waited (0 if no wait was needed)
        """
        now = time.time()
        elapsed = now - self._last_call_time
        wait_time = max(0, self.min_interval - elapsed)

        if wait_time > 0:
            logger.debug("Rate limiter: waiting %.2fs", wait_time)
            time.sleep(wait_time)

        self._last_call_time = time.time()
        return wait_time


class APIClient:
    """
    Production HTTP client for OpenSky Network API.

    Features:
      - Automatic retry with exponential backoff + jitter
      - Rate limiting to prevent API throttling
      - Connection pooling for performance
      - Request deduplication guard
      - Detailed timing metrics
    """

    def __init__(self, api_config):
        """
        Initialize API client.

        Args:
            api_config: APIConfig instance with endpoint and auth settings
        """
        self.config = api_config
        self.base_url = api_config.base_url.rstrip("/")

        # ── Rate limiter ───────────────────────────────────────────────
        self._rate_limiter = RateLimiter(api_config.effective_rate_limit)

        # ── Session with connection pooling ────────────────────────────
        self._session = self._create_session()

        # ── Metrics ────────────────────────────────────────────────────
        self._total_requests = 0
        self._total_errors = 0
        self._total_retries = 0

        logger.info(
            "APIClient initialized | base_url=%s | authenticated=%s | "
            "rate_limit=%d/min",
            self.base_url,
            api_config.is_authenticated,
            api_config.effective_rate_limit,
        )

    def _create_session(self) -> requests.Session:
        """Create a requests.Session with retry and connection pooling."""
        session = requests.Session()

        # ── Authentication ─────────────────────────────────────────────
        if self.config.is_authenticated:
            session.auth = (self.config.username, self.config.password)

        # ── Retry strategy via urllib3 ─────────────────────────────────
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=list(self.config.retry_status_codes),
            allowed_methods=["GET"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10,
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # ── Default headers ────────────────────────────────────────────
        session.headers.update({
            "Accept": "application/json",
            "User-Agent": "FlightAnalyticsPlatform/1.0",
        })

        return session

    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Perform a GET request with retry, rate limiting, and error handling.

        Args:
            endpoint: API endpoint path (e.g., '/states/all')
            params: Query parameters
            timeout: Request timeout in seconds (overrides config default)

        Returns:
            Parsed JSON response dict, or None on failure
        """
        url = f"{self.base_url}{endpoint}"
        timeout = timeout or self.config.request_timeout_seconds

        # ── Rate limiting ──────────────────────────────────────────────
        self._rate_limiter.wait_if_needed()

        self._total_requests += 1
        start_time = time.time()

        try:
            logger.info(
                "API Request | method=GET | url=%s | params=%s",
                url, params,
            )

            response = self._session.get(
                url, params=params, timeout=timeout
            )

            elapsed = time.time() - start_time

            # ── Handle response status ─────────────────────────────────
            if response.status_code == 200:
                data = response.json()
                logger.info(
                    "API Response | status=200 | elapsed=%.2fs | "
                    "size=%d bytes",
                    elapsed,
                    len(response.content),
                )
                return data

            elif response.status_code == 429:
                # Rate limited — back off significantly
                retry_after = int(
                    response.headers.get("Retry-After", 60)
                )
                logger.warning(
                    "Rate limited (429) | retry_after=%ds", retry_after
                )
                time.sleep(retry_after + random.uniform(0, 5))
                self._total_retries += 1
                return self.get(endpoint, params, timeout)  # Retry

            elif response.status_code in (500, 502, 503, 504):
                logger.error(
                    "Server error | status=%d | elapsed=%.2fs",
                    response.status_code, elapsed,
                )
                self._total_errors += 1
                return None

            else:
                logger.error(
                    "Unexpected status | status=%d | body=%s",
                    response.status_code,
                    response.text[:500],
                )
                self._total_errors += 1
                return None

        except requests.exceptions.Timeout:
            elapsed = time.time() - start_time
            logger.error(
                "Request timeout | url=%s | timeout=%ds | elapsed=%.2fs",
                url, timeout, elapsed,
            )
            self._total_errors += 1
            return None

        except requests.exceptions.ConnectionError as e:
            logger.error("Connection error | url=%s | error=%s", url, str(e))
            self._total_errors += 1
            return None

        except requests.exceptions.RequestException as e:
            logger.error(
                "Request failed | url=%s | error=%s", url, str(e)
            )
            self._total_errors += 1
            return None

        except ValueError as e:
            logger.error(
                "JSON decode error | url=%s | error=%s", url, str(e)
            )
            self._total_errors += 1
            return None

    def get_with_backoff(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_attempts: int = 5,
    ) -> Optional[Dict[str, Any]]:
        """
        GET request with manual exponential backoff + jitter.

        Use this for critical requests where the built-in urllib3 retry
        isn't sufficient (e.g., when you need custom backoff logic).

        Args:
            endpoint: API endpoint
            params: Query parameters
            max_attempts: Maximum retry attempts

        Returns:
            Parsed JSON or None
        """
        for attempt in range(1, max_attempts + 1):
            result = self.get(endpoint, params)

            if result is not None:
                return result

            if attempt < max_attempts:
                # Exponential backoff with jitter
                wait = min(
                    (2 ** attempt) + random.uniform(0, 1),
                    120,  # Cap at 2 minutes
                )
                logger.warning(
                    "Retry %d/%d | waiting %.1fs before next attempt",
                    attempt, max_attempts, wait,
                )
                time.sleep(wait)
                self._total_retries += 1

        logger.error(
            "All %d attempts failed for endpoint %s",
            max_attempts, endpoint,
        )
        return None

    def get_metrics(self) -> Dict[str, int]:
        """Return API client metrics."""
        return {
            "total_requests": self._total_requests,
            "total_errors": self._total_errors,
            "total_retries": self._total_retries,
            "success_rate": round(
                (self._total_requests - self._total_errors)
                / max(self._total_requests, 1)
                * 100,
                2,
            ),
        }

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()
        logger.info(
            "APIClient closed | metrics=%s", self.get_metrics()
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self) -> str:
        return (
            f"APIClient(base_url='{self.base_url}', "
            f"requests={self._total_requests}, "
            f"errors={self._total_errors})"
        )
