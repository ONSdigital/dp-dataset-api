from __future__ import annotations

from .models import Edition, EditionsList, QueryParams
from .protocols import EditionsClientProtocol, Headers, RequestingClient


class EditionsAPI(EditionsClientProtocol):
    def __init__(self, client: RequestingClient) -> None:
        self._client = client

    def get_edition(
        self,
        dataset_id: str,
        edition_id: str,
        headers: Headers | None = None,
    ) -> Edition:
        request_headers = headers.to_http_headers() if headers else None
        response = self._client._request(
            "GET",
            f"/datasets/{dataset_id}/editions/{edition_id}",
            headers=request_headers,
        )
        payload = response.json() if response.content else {}

        if (
            request_headers
            and "Authorization" in request_headers
            and isinstance(payload.get("next"), dict)
        ):
            payload = payload["next"]

        return Edition.model_validate(payload)

    def get_editions(
        self,
        dataset_id: str,
        headers: Headers,
        query_params: QueryParams | None = None,
    ) -> tuple[EditionsList, str | None]:
        path = f"/datasets/{dataset_id}/editions"
        query: dict[str, str | int] = {}
        request_headers = headers.to_http_headers()

        if query_params is not None:
            try:
                query_params.validate_params()
            except ValueError as e:
                return EditionsList(items=None, count=0, offset=0, limit=0, total_count=0), str(e)

            query["limit"] = query_params.limit if query_params.limit is not None else 0
            query["offset"] = (
                query_params.offset if query_params.offset is not None else 0
            )

        response = self._client._request(
            "GET",
            path,
            params=query if query_params is not None else None,
            headers=request_headers,
        )
        payload = response.json() if response.content else {}

        if "Authorization" in request_headers and isinstance(payload.get("items"), list):
            payload["items"] = [
                item["next"]
                if isinstance(item, dict) and isinstance(item.get("next"), dict)
                else item
                for item in payload["items"]
            ]

        return EditionsList.model_validate(payload), None
