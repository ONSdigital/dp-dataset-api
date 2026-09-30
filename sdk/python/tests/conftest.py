"""Shared pytest fixtures for all tests."""
from __future__ import annotations

import json
from unittest.mock import Mock

import pytest
import requests


@pytest.fixture
def make_response():
    """Create a mock requests.Response with given status and payload."""

    def _make_response(status_code: int, payload: dict | None = None) -> requests.Response:
        response = requests.Response()
        response.status_code = status_code
        response._content = json.dumps(payload if payload is not None else {}).encode(
            "utf-8"
        )
        response.headers["Content-Type"] = "application/json"
        response.encoding = "utf-8"
        return response

    return _make_response


@pytest.fixture
def make_session(make_response):
    """Create a session with a mocked request method."""

    def _make_session(
        responses: requests.Response | list[requests.Response],
    ) -> tuple[requests.Session, Mock]:
        session = requests.Session()
        if isinstance(responses, list):
            request_mock = Mock(side_effect=responses)
        else:
            request_mock = Mock(return_value=responses)
        session.request = request_mock  # type: ignore[assignment]
        return session, request_mock

    return _make_session


@pytest.fixture
def fake_headers():
    """Create a fake headers object for testing."""

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
            headers = {"CollectionID": "collection-123", "IfMatch": "etag-1"}
            if self.Authorization is not None:
                headers["Authorization"] = self.Authorization
            return headers

    return FakeHeaders

