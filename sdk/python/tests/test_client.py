import unittest

import requests

from dis_dataset_api_sdk_python import DatasetApiClientProtocol, create_client
from dis_dataset_api_sdk_python.client import DatasetApiClient


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.content = b"{}" if payload is not None else b""
        self.text = str(self._payload)

    def json(self) -> dict:
        return self._payload


class FakeSession(requests.Session):
    def __init__(self, response: FakeResponse) -> None:
        super().__init__()
        self.response = response
        self.last_kwargs: dict = {}

    def request(self, **kwargs):  # type: ignore[override]
        self.last_kwargs = kwargs
        return self.response


class DatasetApiClientTests(unittest.TestCase):
    def test_health_returns_dict(self) -> None:
        session = FakeSession(FakeResponse(200, {"status": "ok"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.health()

        self.assertEqual(result["status"], "ok")

    def test_create_client_returns_protocol_conforming_client(self) -> None:
        client = create_client("https://dp-dataset-api")

        self.assertIsInstance(client, DatasetApiClientProtocol)
        self.assertIsInstance(client, DatasetApiClient)

    def test_init_rejects_non_session_objects(self) -> None:
        invalid_session = object()

        with self.assertRaises(TypeError):
            DatasetApiClient(base_url="https://dp-dataset-api", session=invalid_session)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()