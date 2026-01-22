import asyncio
import logging
import random
import time
from typing import Any, Dict, Optional, Tuple

import httpx
import requests

from app.observability.metrics import record_upstream_request

logger = logging.getLogger(__name__)

_RETRY_STATUSES = {429, 503}
_MAX_ATTEMPTS = 5
_BASE_DELAY_SECONDS = 0.5
_MAX_DELAY_SECONDS = 8.0


class UpstreamHTTPError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


def build_full_url(base_url: str, path_or_url: str) -> str:
    trimmed = (path_or_url or "").strip()
    if not trimmed:
        return ""
    if trimmed.startswith("http://") or trimmed.startswith("https://"):
        return trimmed
    base = base_url.rstrip("/")
    if trimmed.startswith("/"):
        return f"{base}{trimmed}"
    return f"{base}/{trimmed.lstrip('/')}"


def request_json(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
    timeout: float = 30.0,
) -> Any:
    started = time.monotonic()
    try:
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        record_upstream_request("error", time.monotonic() - started)
        raise RuntimeError(f"Upstream request failed: {exc}") from exc
    record_upstream_request(str(response.status_code), time.monotonic() - started)
    response.raise_for_status()
    return response.json()


def _parse_retry_after(headers: httpx.Headers) -> Optional[float]:
    value = headers.get("Retry-After")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _compute_backoff(attempt: int, retry_after: Optional[float]) -> float:
    base = min(_MAX_DELAY_SECONDS, _BASE_DELAY_SECONDS * (2 ** attempt))
    jitter = random.uniform(0.0, base * 0.3)
    delay = base + jitter
    if retry_after is not None:
        delay = max(delay, retry_after)
    return delay


def _response_payload(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


async def async_request_json(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
) -> Tuple[Any, int]:
    attempt = 0
    while True:
        started = time.monotonic()
        try:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
            )
        except httpx.RequestError as exc:
            record_upstream_request("error", time.monotonic() - started)
            if attempt >= _MAX_ATTEMPTS - 1:
                raise RuntimeError(f"Upstream request failed: {exc}") from exc
            delay = _compute_backoff(attempt, None)
            logger.warning("Upstream request error, retrying", extra={"attempt": attempt + 1})
            await asyncio.sleep(delay)
            attempt += 1
            continue

        if response.status_code in _RETRY_STATUSES:
            record_upstream_request(str(response.status_code), time.monotonic() - started)
            if attempt >= _MAX_ATTEMPTS - 1:
                payload = _response_payload(response)
                raise UpstreamHTTPError(
                    response.status_code,
                    f"Upstream error {response.status_code}: {payload}",
                )
            retry_after = _parse_retry_after(response.headers)
            delay = _compute_backoff(attempt, retry_after)
            logger.warning(
                "Upstream throttle, retrying",
                extra={"status_code": response.status_code, "attempt": attempt + 1},
            )
            await asyncio.sleep(delay)
            attempt += 1
            continue

        if response.is_success:
            record_upstream_request(str(response.status_code), time.monotonic() - started)
            return _response_payload(response), response.status_code

        payload = _response_payload(response)
        record_upstream_request(str(response.status_code), time.monotonic() - started)
        raise UpstreamHTTPError(response.status_code, f"Upstream error {response.status_code}: {payload}")
