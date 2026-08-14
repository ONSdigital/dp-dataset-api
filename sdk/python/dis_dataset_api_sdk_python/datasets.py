from __future__ import annotations

from .models import Dataset, Headers


def get_dataset(self, dataset_id: str, headers: Headers | None = None) -> Dataset:
    payload = self._request(
        "GET",
        f"/datasets/{dataset_id}",
        headers=headers.to_http_headers() if headers else None,
    )

    if headers and headers.Authorization and isinstance(payload.get("next"), dict):
        payload = payload["next"]

    return Dataset.model_validate(payload)
