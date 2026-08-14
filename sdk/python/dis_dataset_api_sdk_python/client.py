from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import requests

from .datasets import get_dataset
from .exceptions import ApiError, AuthenticationError, NotFoundError, ValidationError


class DatasetApiClient:
    # Endpoint methods are defined in dedicated files and attached here.
    get_dataset = get_dataset

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()

    def health(self) -> dict[str, Any]:
        """GET /health"""
        return self._request("GET", "/health")

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: Mapping[str, str | bytes] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=self.timeout,
                headers=headers,
            )
        except requests.RequestException as exc:
            raise ApiError(f"Request failed: {exc}") from exc

        if response.status_code in (401, 403):
            raise AuthenticationError(
                "Authentication failed", status_code=response.status_code
            )
        if response.status_code == 404:
            raise NotFoundError("Resource not found", status_code=response.status_code)
        if response.status_code in (400, 422):
            raise ValidationError(
                response.text or "Validation error", status_code=response.status_code
            )
        if response.status_code >= 500:
            raise ApiError("Server error", status_code=response.status_code)
        if response.status_code >= 400:
            raise ApiError(
                response.text or "Request failed", status_code=response.status_code
            )

        if not response.content:
            return {}
        return response.json()
