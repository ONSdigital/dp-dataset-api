import unittest

from dis_dataset_api_sdk_python import (
    DatasetApiClientProtocol,
    DatasetsClientProtocol,
    Headers,
)
from dis_dataset_api_sdk_python.client import DatasetApiClient
from dis_dataset_api_sdk_python.exceptions import ApiError, NotFoundError
from dis_dataset_api_sdk_python.models import Dataset


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.content = b"{}" if payload is not None else b""
        self.text = str(self._payload)

    def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.last_kwargs: dict = {}

    def request(self, **kwargs):
        self.last_kwargs = kwargs
        return self.response


class FakeHeaders:
    def __init__(
        self,
        authorization: str | None = None,
        collection_id: str | None = None,
        download_service_token: str | None = None,
        if_match: str | None = None,
    ) -> None:
        self.Authorization = authorization
        self.CollectionID = collection_id
        self.DownloadServiceToken = download_service_token
        self.IfMatch = if_match

    def to_http_headers(self) -> dict[str, str]:
        return {"CollectionID": "collection-123", "IfMatch": "etag-1"}


class DatasetEndpointTests(unittest.TestCase):
    def test_get_dataset_returns_pydantic_model(self) -> None:
        session = FakeSession(FakeResponse(200, {"id": "abc", "title": "A dataset"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.datasets.get_dataset("abc")

        self.assertIsInstance(result, Dataset)
        self.assertEqual(result.id, "abc")

    def test_get_dataset_passes_header_mapping(self) -> None:
        session = FakeSession(FakeResponse(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        headers = FakeHeaders()
        client.datasets.get_dataset("abc", headers=headers)

        self.assertEqual(
            session.last_kwargs["headers"],
            {"CollectionID": "collection-123", "IfMatch": "etag-1"},
        )

    def test_get_dataset_404_raises_not_found(self) -> None:
        session = FakeSession(FakeResponse(404, {"error": "not found"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with self.assertRaises(NotFoundError):
            client.datasets.get_dataset("missing")

    def test_get_dataset_500_raises_api_error_with_status(self) -> None:
        session = FakeSession(FakeResponse(500, {"error": "server error"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with self.assertRaises(ApiError) as exc:
            client.datasets.get_dataset("abc")

        self.assertEqual(exc.exception.status_code, 500)

    def test_fake_headers_conform_to_headers_protocol(self) -> None:
        self.assertIsInstance(FakeHeaders(), Headers)

    def test_datasets_conforms_to_protocol(self) -> None:
        session = FakeSession(FakeResponse(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        self.assertIsInstance(client.datasets, DatasetsClientProtocol)

    def test_client_conforms_to_dataset_api_client_protocol(self) -> None:
        session = FakeSession(FakeResponse(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        self.assertIsInstance(client, DatasetApiClientProtocol)


if __name__ == "__main__":
    unittest.main()
