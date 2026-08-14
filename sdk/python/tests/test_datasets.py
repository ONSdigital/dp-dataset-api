import unittest

from dis_dataset_api_sdk_python.client import DatasetApiClient
from dis_dataset_api_sdk_python.exceptions import ApiError, NotFoundError
from dis_dataset_api_sdk_python.models import Dataset, Headers


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


class DatasetEndpointTests(unittest.TestCase):
    def test_get_dataset_returns_pydantic_model(self) -> None:
        session = FakeSession(FakeResponse(200, {"id": "abc", "title": "A dataset"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.get_dataset("abc")

        self.assertIsInstance(result, Dataset)
        self.assertEqual(result.id, "abc")

    def test_get_dataset_passes_header_mapping(self) -> None:
        session = FakeSession(FakeResponse(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        headers = Headers(CollectionID="collection-123", IfMatch="etag-1")
        client.get_dataset("abc", headers=headers)

        self.assertEqual(
            session.last_kwargs["headers"],
            {"CollectionID": "collection-123", "IfMatch": "etag-1"},
        )

    def test_get_dataset_404_raises_not_found(self) -> None:
        session = FakeSession(FakeResponse(404, {"error": "not found"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with self.assertRaises(NotFoundError):
            client.get_dataset("missing")

    def test_get_dataset_500_raises_api_error_with_status(self) -> None:
        session = FakeSession(FakeResponse(500, {"error": "server error"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with self.assertRaises(ApiError) as exc:
            client.get_dataset("abc")

        self.assertEqual(exc.exception.status_code, 500)


if __name__ == "__main__":
    unittest.main()
