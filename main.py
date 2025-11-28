import logging
import time
from typing import Any, Dict, Optional

import requests
from requests import Response
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    Timeout,
    RequestException,
)

logger = logging.getLogger(__name__)


class TransientAPIError(Exception):
    """Raised when a transient error occurs after exhausting retries."""
    pass


class PermanentAPIError(Exception):
    """Raised when a permanent error occurs (e.g., 4xx)."""
    pass


def _is_transient_status(status_code: int) -> bool:
    # Retry on typical transient HTTP status codes.
    return status_code in {408, 429, 500, 502, 503, 504}


def fetch_user_details(
    user_id: str,
    *,
    base_url: str = "https://api.example.com",
    max_retries: int = 3,
    initial_backoff: float = 0.5,
    timeout: float | tuple[float, float] = (3.0, 5.0),  # (connect, read)
) -> Dict[str, Any]:
    """
    Fetch user details from the external API.

    :param user_id: The user identifier (non-empty string).
    :param base_url: Base URL for the API.
    :param max_retries: Maximum number of retries for transient failures.
    :param initial_backoff: Initial backoff delay in seconds.
    :param timeout: Timeout passed to requests (float or (connect, read)).
    :return: Parsed JSON document as a dictionary.
    :raises ValueError: If user_id is invalid.
    :raises PermanentAPIError: On non-retriable errors.
    :raises TransientAPIError: When transient errors persist after retries.
    """

    # Input validation for user_id
    if not isinstance(user_id, str) or not user_id.strip():
        raise ValueError("user_id must be a non-empty string")

    safe_user_id = user_id.strip()
    url = f"{base_url}/users/{safe_user_id}"

    attempt = 0
    backoff = initial_backoff

    while True:
        attempt += 1
        try:
            logger.info(
                "Fetching user details",
                extra={"event": "fetch_user_details_start", "user_id": safe_user_id, "attempt": attempt},
            )

            response: Response = requests.get(
                url,
                timeout=timeout,  # uses connect/read timeout per requests docs
            )
            status_code = response.status_code

            # Distinguish transient vs permanent HTTP errors
            if 200 <= status_code < 300:
                # Defensive JSON parsing
                try:
                    data: Any = response.json()
                except ValueError as exc:
                    logger.error(
                        "Failed to parse JSON response",
                        extra={
                            "event": "fetch_user_details_json_error",
                            "user_id": safe_user_id,
                            "attempt": attempt,
                            "status_code": status_code,
                        },
                    )
                    raise PermanentAPIError("Invalid JSON received from user service") from exc

                if not isinstance(data, dict):
                    logger.error(
                        "Unexpected JSON structure (expected object)",
                        extra={
                            "event": "fetch_user_details_unexpected_json_type",
                            "user_id": safe_user_id,
                            "attempt": attempt,
                            "status_code": status_code,
                            "json_type": type(data).__name__,
                        },
                    )
                    raise PermanentAPIError("Unexpected JSON structure from user service")

                logger.info(
                    "Successfully fetched user details",
                    extra={
                        "event": "fetch_user_details_success",
                        "user_id": safe_user_id,
                        "attempt": attempt,
                        "status_code": status_code,
                    },
                )
                return data

            # Non-2xx status
            if _is_transient_status(status_code):
                logger.warning(
                    "Transient HTTP error when fetching user details",
                    extra={
                        "event": "fetch_user_details_transient_http_error",
                        "user_id": safe_user_id,
                        "attempt": attempt,
                        "status_code": status_code,
                    },
                )
                if attempt > max_retries:
                    raise TransientAPIError(f"Transient HTTP error after {max_retries} retries")
            else:
                # Permanent error (e.g., 4xx other than 408/429)
                logger.error(
                    "Permanent HTTP error when fetching user details",
                    extra={
                        "event": "fetch_user_details_permanent_http_error",
                        "user_id": safe_user_id,
                        "attempt": attempt,
                        "status_code": status_code,
                    },
                )
                raise PermanentAPIError(f"Permanent HTTP error: {status_code}")

        except (ConnectionError, Timeout) as exc:
            # Treat network/timeout as transient
            logger.warning(
                "Transient network error when fetching user details",
                extra={
                    "event": "fetch_user_details_network_error",
                    "user_id": safe_user_id,
                    "attempt": attempt,
                    "error_type": type(exc).__name__,
                },
            )
            if attempt > max_retries:
                raise TransientAPIError(f"Network error after {max_retries} retries") from exc

        except HTTPError as exc:
            # HTTPError is often redundant with status_code handling, but keep defensive
            status_code: Optional[int] = getattr(exc.response, "status_code", None)
            if status_code is not None and _is_transient_status(status_code):
                logger.warning(
                    "Transient HTTPError when fetching user details",
                    extra={
                        "event": "fetch_user_details_http_error_transient",
                        "user_id": safe_user_id,
                        "attempt": attempt,
                        "status_code": status_code,
                    },
                )
                if attempt > max_retries:
                    raise TransientAPIError(f"Transient HTTP error after {max_retries} retries") from exc
            else:
                logger.error(
                    "Permanent HTTPError when fetching user details",
                    extra={
                        "event": "fetch_user_details_http_error_permanent",
                        "user_id": safe_user_id,
                        "attempt": attempt,
                        "status_code": status_code,
                    },
                )
                raise PermanentAPIError("Permanent HTTP error from user service") from exc

        except RequestException as exc:
            # Catch-all for requests-specific errors; treat as transient by default
            logger.warning(
                "Generic transient RequestException when fetching user details",
                extra={
                    "event": "fetch_user_details_generic_request_exception",
                    "user_id": safe_user_id,
                    "attempt": attempt,
                    "error_type": type(exc).__name__,
                },
            )
            if attempt > max_retries:
                raise TransientAPIError(f"Request error after {max_retries} retries") from exc

        # Exponential backoff with jitter for transient errors
        sleep_seconds = backoff
        logger.info(
            "Retrying fetch_user_details with backoff",
            extra={
                "event": "fetch_user_details_retry",
                "user_id": safe_user_id,
                "attempt": attempt,
                "sleep_seconds": sleep_seconds,
            },
        )
        time.sleep(sleep_seconds)
        backoff *= 2

