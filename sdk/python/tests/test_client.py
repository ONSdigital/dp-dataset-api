import unittest

from dis_dataset_api_sdk_python.client import DatasetApiClient


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


class DatasetApiClientTests(unittest.TestCase):
    def test_health_returns_dict(self) -> None:
        session = FakeSession(FakeResponse(200, {"status": "ok"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.health()

        self.assertEqual(result["status"], "ok")


if __name__ == "__main__":
    unittest.main()
