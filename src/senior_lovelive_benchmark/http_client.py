from __future__ import annotations

import time
from dataclasses import dataclass

import httpx


DEFAULT_HEADERS = {
    "User-Agent": "senior-lovelive-benchmark/0.1 (+https://www.eventernote.com/)",
    "Accept-Language": "ja,en;q=0.8",
}


@dataclass
class Fetcher:
    delay_seconds: float = 1.0
    timeout_seconds: float = 30.0
    max_retries: int = 2

    def __post_init__(self) -> None:
        self._last_request_at = 0.0
        self._client = httpx.Client(
            follow_redirects=True,
            timeout=self.timeout_seconds,
            headers=DEFAULT_HEADERS,
        )

    def close(self) -> None:
        self._client.close()

    def get_text(self, url: str, params: dict[str, str | int] | None = None) -> str:
        response = self._request("GET", url, params=params)
        response.encoding = response.encoding or "utf-8"
        return response.text

    def get_json(
        self,
        url: str,
        params: dict[str, str | int | None] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict:
        response = self._request("GET", url, params=params, headers=headers)
        return response.json()

    def post_json(
        self,
        url: str,
        json: dict,
        headers: dict[str, str] | None = None,
    ) -> dict:
        response = self._request("POST", url, json=json, headers=headers)
        return response.json()

    def _request(
        self,
        method: str,
        url: str,
        params: dict[str, str | int | None] | dict[str, str | int] | None = None,
        headers: dict[str, str] | None = None,
        json: dict | None = None,
    ) -> httpx.Response:
        response: httpx.Response | None = None
        for attempt in range(self.max_retries + 1):
            self._sleep_until_allowed()
            response = self._client.request(method, url, params=params, headers=headers, json=json)
            self._last_request_at = time.monotonic()
            if response.status_code not in {429, 500, 502, 503, 504} or attempt >= self.max_retries:
                response.raise_for_status()
                return response
            time.sleep(self._retry_delay(response, attempt))
        assert response is not None
        response.raise_for_status()
        return response

    def _sleep_until_allowed(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)

    def _retry_delay(self, response: httpx.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), self.delay_seconds)
            except ValueError:
                pass
        return self.delay_seconds * (2**attempt)
