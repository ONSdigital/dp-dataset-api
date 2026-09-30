from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

import requests

from .models import (
    Dataset,
    DatasetEditionsList,
    DatasetsList,
    Edition,
    EditionsList,
    Metadata,
    QueryParams,
    Version,
    VersionDimensionOptionsList,
    VersionDimensionsList,
    VersionsList,
)


@runtime_checkable
class Headers(Protocol):

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
    ) -> requests.Response: ...


@runtime_checkable
class DatasetsClientProtocol(Protocol):
    """Protocol for dataset-specific operations."""

    def get_dataset(
        self,
        dataset_id: str,
        headers: Headers | None = None,
    ) -> Dataset: ...

    def get_dataset_by_path(
        self,
        path: str,
        headers: Headers,
    ) -> Dataset: ...

    def get_dataset_editions(
        self,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[DatasetEditionsList, str | None]: ...

    def get_datasets(
        self,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[DatasetsList, str | None]: ...


@runtime_checkable
class EditionsClientProtocol(Protocol):
    """Protocol for edition-specific operations."""

    def get_edition(
        self,
        dataset_id: str,
        edition_id: str,
        headers: Headers | None = None,
    ) -> Edition: ...

    def get_editions(
        self,
        dataset_id: str,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[EditionsList, str | None]: ...


@runtime_checkable
class VersionsClientProtocol(Protocol):
    """Protocol for version-specific operations."""

    def get_version(
        self,
        dataset_id: str,
        edition_id: str,
        version_id: str,
        headers: Headers | None = None,
    ) -> Version: ...

    def get_version_metadata(
        self,
        dataset_id: str,
        edition_id: str,
        version_id: str,
        headers: Headers,
    ) -> Metadata: ...

    def get_versions(
        self,
        dataset_id: str,
        edition_id: str,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[VersionsList, str | None]: ...

    def get_versions_in_batches(
        self,
        dataset_id: str,
        edition_id: str,
        headers: Headers,
        batch_size: int,
        max_workers: int,
    ) -> tuple[VersionsList, str | None]: ...

    def get_versions_in_batches_with_query_params(
        self,
        dataset_id: str,
        edition_id: str,
        headers: Headers,
        req_limit: int,
        req_offset: int,
        batch_size: int,
        max_workers: int,
    ) -> tuple[VersionsList, str | None]: ...

    def get_version_dimensions(
        self,
        dataset_id: str,
        edition_id: str,
        version_id: str,
        headers: Headers,
    ) -> VersionDimensionsList: ...

    def get_version_dimension_options(
        self,
        dataset_id: str,
        edition_id: str,
        version_id: str,
        dimension_id: str,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[VersionDimensionOptionsList, str | None]: ...


@runtime_checkable
class DatasetApiClientProtocol(Protocol):
    def health(self) -> dict[str, Any]: ...

    datasets: DatasetsClientProtocol
    editions: EditionsClientProtocol
    versions: VersionsClientProtocol
