from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from .models import Dataset


@runtime_checkable
class Headers(Protocol):
    Authorization: str | None
    CollectionID: str | None
    DownloadServiceToken: str | None
    IfMatch: str | None

    def to_http_headers(self) -> Mapping[str, str | bytes]: ...


class RequestingClient(Protocol):
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: Mapping[str, str | bytes] | None = None,
    ) -> dict[str, Any]: ...


@runtime_checkable
class DatasetsClientProtocol(Protocol):
    """Protocol for dataset-specific operations."""

    def get_dataset(
        self,
        dataset_id: str,
        headers: Headers | None = None,
    ) -> Dataset: ...



@runtime_checkable
class DatasetApiClientProtocol(Protocol):

    def health(self) -> dict[str, Any]: ...

    datasets: DatasetsClientProtocol
