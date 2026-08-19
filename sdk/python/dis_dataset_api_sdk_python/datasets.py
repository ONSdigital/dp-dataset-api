from __future__ import annotations

from .models import Dataset
from .protocols import DatasetsClientProtocol, Headers, RequestingClient


class DatasetsAPI(DatasetsClientProtocol):
    def __init__(self, client: RequestingClient) -> None:
        self._client = client

    def get_dataset(
        self,
        dataset_id: str,
        headers: Headers | None = None,
    ) -> Dataset:
        payload = self._client._request(
            "GET",
            f"/datasets/{dataset_id}",
            headers=headers.to_http_headers() if headers else None,
        )

        if headers and headers.Authorization and isinstance(payload.get("next"), dict):
            payload = payload["next"]

        return Dataset.model_validate(payload)
