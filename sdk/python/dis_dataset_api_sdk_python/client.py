from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import requests

from .datasets import DatasetsAPI
from .editions import EditionsAPI
from .exceptions import ApiError, AuthenticationError, NotFoundError, ValidationError
from .protocols import (
    DatasetApiClientProtocol,
    DatasetsClientProtocol,
    EditionsClientProtocol,
    VersionsClientProtocol,
)
from .versions import VersionsAPI


class DatasetApiClient:
    datasets: DatasetsClientProtocol
    editions: EditionsClientProtocol
    versions: VersionsClientProtocol

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        session: requests.Session | None = None,
    ) -> None:
        if session is not None and not isinstance(session, requests.Session):
            raise TypeError("session must be an instance of requests.Session")

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.datasets = DatasetsAPI(self)
        self.editions = EditionsAPI(self)
        self.versions = VersionsAPI(self)

    def health(self) -> dict[str, Any]:
        """GET /health"""
        return self._request("GET", "/health").json()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: Mapping[str, str | bytes] | None = None,
    ) -> requests.Response:
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
                response.text or "Authentication failed",
                status_code=response.status_code,
            )

        if response.status_code == 404:
            raise NotFoundError(
                response.text or "Resource not found",
                status_code=response.status_code,
            )

        if response.status_code in (400, 422):
            raise ValidationError(
                response.text or "Validation error",
                status_code=response.status_code,
            )

        if response.status_code >= 500:
            raise ApiError(
                response.text or "Server error",
                status_code=response.status_code,
            )

        if response.status_code >= 400:
            raise ApiError(
                response.text or "Request failed",
                status_code=response.status_code,
            )

        return response


def create_client(
    base_url: str,
    timeout: float = 10.0,
    session: requests.Session | None = None,
) -> DatasetApiClientProtocol:
    return DatasetApiClient(base_url=base_url, timeout=timeout, session=session)
