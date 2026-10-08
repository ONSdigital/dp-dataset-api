from __future__ import annotations

from .models import (
    Metadata,
    QueryParams,
    Version,
    VersionDimensionOptionsList,
    VersionDimensionsList,
    VersionsList,
)
from .protocols import Headers, RequestingClient, VersionsClientProtocol


class VersionsAPI(VersionsClientProtocol):
    def __init__(self, client: RequestingClient) -> None:
        self._client = client

    def _payload(self, response) -> dict:
        return response.json() if response.content else {}

    def get_version(
        self,
        dataset_id: str,
        edition_id: str,
        version_id: str,
        headers: Headers | None = None,
    ) -> Version:
        request_headers = headers.to_http_headers() if headers else None
        response = self._client._request(
            "GET",
            f"/datasets/{dataset_id}/editions/{edition_id}/versions/{version_id}",
            headers=request_headers,
        )
        payload = self._payload(response)

        if (
            request_headers
            and "Authorization" in request_headers
            and isinstance(payload.get("next"), dict)
        ):
            payload = payload["next"]

        return Version.model_validate(payload)

    def get_version_metadata(
        self,
        dataset_id: str,
        edition_id: str,
        version_id: str,
        headers: Headers,
    ) -> Metadata:
        response = self._client._request(
            "GET",
            f"/datasets/{dataset_id}/editions/{edition_id}/versions/{version_id}/metadata",
            headers=headers.to_http_headers(),
        )
        return Metadata.model_validate(self._payload(response))

    def get_versions(
        self,
        dataset_id: str,
        edition_id: str,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[VersionsList, str | None]:
        path = f"/datasets/{dataset_id}/editions/{edition_id}/versions"
        query: dict[str, str | int] = {}

        if query_params is not None:
            try:
                query_params.validate_params()
            except ValueError as e:
                return VersionsList(), str(e)

            query["limit"] = query_params.limit if query_params.limit is not None else 0
            query["offset"] = (
                query_params.offset if query_params.offset is not None else 0
            )

        response = self._client._request(
            "GET",
            path,
            params=query if query_params is not None else None,
            headers=headers.to_http_headers(),
        )

        return VersionsList.model_validate(self._payload(response)), None

    def get_versions_in_batches(
        self,
        dataset_id: str,
        edition_id: str,
        headers: Headers,
        batch_size: int,
        max_workers: int,
    ) -> tuple[VersionsList, str | None]:
        if batch_size <= 0:
            return VersionsList(), "batchSize must be a positive value"
        if max_workers <= 0:
            return VersionsList(), "maxWorkers must be a positive value"

        first_batch, err = self.get_versions(
            dataset_id,
            edition_id,
            headers,
            QueryParams(limit=batch_size, offset=0),
        )
        if err is not None:
            return VersionsList(), err

        total_count = first_batch.total_count
        result = VersionsList(
            items=[],
            count=total_count,
            offset=0,
            limit=0,
            total_count=total_count,
        )
        result.items.extend(first_batch.items)

        offset = batch_size
        while offset < total_count:
            batch, err = self.get_versions(
                dataset_id,
                edition_id,
                headers,
                QueryParams(limit=batch_size, offset=offset),
            )
            if err is not None:
                return VersionsList(), err
            result.items.extend(batch.items)
            offset += batch_size

        result.items = result.items[:total_count]
        return result, None

    def get_versions_in_batches_with_query_params(
        self,
        dataset_id: str,
        edition_id: str,
        headers: Headers,
        req_limit: int,
        req_offset: int,
        batch_size: int,
        max_workers: int,
    ) -> tuple[VersionsList, str | None]:
        if req_limit <= 0:
            return VersionsList(), "reqLimit must be a positive value"
        if req_offset < 0:
            return VersionsList(), "reqOffset must be greater than or equal to 0"
        if batch_size <= 0:
            return VersionsList(), "batchSize must be a positive value"
        if max_workers <= 0:
            return VersionsList(), "maxWorkers must be a positive value"

        first_batch, err = self.get_versions(
            dataset_id,
            edition_id,
            headers,
            QueryParams(limit=batch_size, offset=req_offset),
        )
        if err is not None:
            return VersionsList(), err

        total_count = first_batch.total_count
        if req_offset >= total_count:
            return (
                VersionsList(),
                f"request offset value greater than or equal to versions total count. versions total count: {total_count}, request offset value: {req_offset}",
            )

        count = min(req_limit, total_count - req_offset)
        result = VersionsList(
            items=[],
            count=count,
            offset=req_offset,
            limit=req_limit,
            total_count=total_count,
        )
        result.items.extend(first_batch.items)

        offset = req_offset + batch_size
        end_offset = req_offset + count
        while offset < end_offset:
            batch, err = self.get_versions(
                dataset_id,
                edition_id,
                headers,
                QueryParams(limit=batch_size, offset=offset),
            )
            if err is not None:
                return VersionsList(), err
            result.items.extend(batch.items)
            offset += batch_size

        result.items = result.items[:count]
        return result, None

    def get_version_dimensions(
        self,
        dataset_id: str,
        edition_id: str,
        version_id: str,
        headers: Headers,
    ) -> VersionDimensionsList:
        response = self._client._request(
            "GET",
            f"/datasets/{dataset_id}/editions/{edition_id}/versions/{version_id}/dimensions",
            headers=headers.to_http_headers(),
        )
        return VersionDimensionsList.model_validate(self._payload(response))

    def get_version_dimension_options(
        self,
        dataset_id: str,
        edition_id: str,
        version_id: str,
        dimension_id: str,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[VersionDimensionOptionsList, str | None]:
        path = f"/datasets/{dataset_id}/editions/{edition_id}/versions/{version_id}/dimensions/{dimension_id}/options"
        query: dict[str, str | int] = {}

        if query_params is not None:
            try:
                query_params.validate_params()
            except ValueError as e:
                return VersionDimensionOptionsList(), str(e)

            query["limit"] = query_params.limit if query_params.limit is not None else 0
            query["offset"] = (
                query_params.offset if query_params.offset is not None else 0
            )

        response = self._client._request(
            "GET",
            path,
            params=query if query_params is not None else None,
            headers=headers.to_http_headers(),
        )

        return VersionDimensionOptionsList.model_validate(self._payload(response)), None
