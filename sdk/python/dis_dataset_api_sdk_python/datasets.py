from __future__ import annotations

from .models import Dataset, DatasetEditionsList, DatasetsList, QueryParams
from .protocols import DatasetsClientProtocol, Headers, RequestingClient


class DatasetsAPI(DatasetsClientProtocol):
    def __init__(self, client: RequestingClient) -> None:
        self._client = client

    def get_dataset(
        self,
        dataset_id: str,
        headers: Headers | None = None,
    ) -> Dataset:
        request_headers = headers.to_http_headers() if headers else None
        response = self._client._request(
            "GET",
            f"/datasets/{dataset_id}",
            headers=request_headers,
        )
        payload = response.json() if response.content else {}

        if (
            request_headers
            and "Authorization" in request_headers
            and isinstance(payload.get("next"), dict)
        ):
            payload = payload["next"]

        return Dataset.model_validate(payload)

    def get_dataset_by_path(
        self,
        path: str,
        headers: Headers,
    ) -> Dataset:
        trimmed_path = path.strip("/")
        request_headers = headers.to_http_headers()
        response = self._client._request(
            "GET",
            f"/{trimmed_path}",
            headers=request_headers,
        )
        payload = response.json() if response.content else {}
        return Dataset.model_validate(payload)

    def get_dataset_editions(
        self,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[DatasetEditionsList, str | None]:
        path = "/dataset-editions"
        query: dict[str, str | int] = {}
        request_headers = headers.to_http_headers()

        if query_params is not None:
            try:
                query_params.validate_params()
            except ValueError as e:
                return DatasetEditionsList(), str(e)

            query["limit"] = query_params.limit if query_params.limit is not None else 0
            query["offset"] = (
                query_params.offset if query_params.offset is not None else 0
            )
            if query_params.state is not None:
                query["state"] = query_params.state

        response = self._client._request(
            "GET",
            path,
            params=query if query_params is not None else None,
            headers=request_headers,
        )
        payload = response.json() if response.content else {}

        return DatasetEditionsList.model_validate(payload), None

    def get_datasets(
        self,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[DatasetsList, str | None]:
        path = "/datasets"
        query: dict[str, str | int] = {}
        request_headers = headers.to_http_headers()

        if query_params is not None:
            try:
                query_params.validate_params()
            except ValueError as e:
                return DatasetsList(items=[], count=0, offset=0, limit=0, total_count=0), str(e)

            query["offset"] = (
                query_params.offset if query_params.offset is not None else 0
            )
            query["limit"] = query_params.limit if query_params.limit is not None else 0
            if query_params.is_based_on is not None:
                query["is_based_on"] = query_params.is_based_on

        response = self._client._request(
            "GET",
            path,
            params=query if query_params is not None else None,
            headers=request_headers,
        )
        payload = response.json() if response.content else {}

        return DatasetsList.model_validate(payload), None
