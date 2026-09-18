import json
import unittest
from unittest.mock import Mock

import requests
from dis_dataset_api_sdk_python import (
    DatasetApiClientProtocol,
    DatasetsClientProtocol,
    Headers,
)
from dis_dataset_api_sdk_python.client import DatasetApiClient
from dis_dataset_api_sdk_python.exceptions import ApiError, NotFoundError
from dis_dataset_api_sdk_python.models import Dataset


def make_response(status_code: int, payload: dict | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(payload if payload is not None else {}).encode(
        "utf-8"
    )
    response.headers["Content-Type"] = "application/json"
    response.encoding = "utf-8"
    return response


def make_session(response: requests.Response) -> tuple[requests.Session, Mock]:
    session = requests.Session()
    request_mock = Mock(return_value=response)
    session.request = request_mock  # type: ignore[assignment]
    return session, request_mock


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
        session, _ = make_session(
            make_response(200, {"id": "abc", "title": "A dataset"})
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.datasets.get_dataset("abc")

        self.assertIsInstance(result, Dataset)
        self.assertEqual(result.id, "abc")

    def test_get_dataset_passes_header_mapping(self) -> None:
        session, request_mock = make_session(make_response(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        headers = FakeHeaders()
        client.datasets.get_dataset("abc", headers=headers)

        self.assertEqual(
            request_mock.call_args.kwargs["headers"],
            {"CollectionID": "collection-123", "IfMatch": "etag-1"},
        )

    def test_get_dataset_404_raises_not_found(self) -> None:
        session, _ = make_session(make_response(404, {"error": "not found"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with self.assertRaises(NotFoundError):
            client.datasets.get_dataset("missing")

    def test_get_dataset_500_raises_api_error_with_status(self) -> None:
        session, _ = make_session(make_response(500, {"error": "server error"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with self.assertRaises(ApiError) as exc:
            client.datasets.get_dataset("abc")

        self.assertEqual(exc.exception.status_code, 500)

    def test_fake_headers_conform_to_headers_protocol(self) -> None:
        self.assertIsInstance(FakeHeaders(), Headers)

    def test_datasets_conforms_to_protocol(self) -> None:
        session, _ = make_session(make_response(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        self.assertIsInstance(client.datasets, DatasetsClientProtocol)

    def test_client_conforms_to_dataset_api_client_protocol(self) -> None:
        session, _ = make_session(make_response(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        self.assertIsInstance(client, DatasetApiClientProtocol)


if __name__ == "__main__":
    unittest.main()
