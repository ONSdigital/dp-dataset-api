"""Tests for the DatasetApiClient base client."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests

from dis_dataset_api_sdk_python import (
    ApiError,
    AuthenticationError,
    DatasetApiClient,
    DatasetApiClientProtocol,
    NotFoundError,
    ValidationError,
    create_client,
)


class TestClientHealth:
    """Tests for client.health() endpoint."""

    def test_health_returns_dict(self, make_session, make_response):
        """health() returns dict with status."""
        session, _ = make_session(make_response(200, {"status": "ok"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.health()

        assert isinstance(result, dict)
        assert result["status"] == "ok"

    def test_health_empty_response(self, make_session, make_response):
        """health() returns empty dict on empty response."""
        session, _ = make_session(make_response(200, None))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.health()

        assert result == {}


class TestClientErrors:
    """Tests for client._request() error handling."""

    def test_request_401_raises_authentication_error(self, make_session, make_response):
        """_request() raises AuthenticationError on 401."""
        session = make_session(make_response(401, {"error": "bad token"}))[0]
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(AuthenticationError) as exc_info:
            client._request("GET", "/datasets")

        assert exc_info.value.status_code == 401

    def test_request_403_raises_authentication_error(self, make_session, make_response):
        """_request() raises AuthenticationError on 403."""
        session = make_session(make_response(403, {"error": "forbidden"}))[0]
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(AuthenticationError) as exc_info:
            client._request("GET", "/datasets")

        assert exc_info.value.status_code == 403

    def test_request_404_raises_not_found(self, make_session, make_response):
        """_request() raises NotFoundError on 404."""
        session = make_session(make_response(404, {"error": "missing"}))[0]
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(NotFoundError) as exc_info:
            client._request("GET", "/datasets/missing")

        assert exc_info.value.status_code == 404

    def test_request_400_raises_validation_error(self, make_session, make_response):
        """_request() raises ValidationError on 400."""
        session = make_session(make_response(400, {"error": "invalid"}))[0]
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(ValidationError) as exc_info:
            client._request("GET", "/datasets")

        assert exc_info.value.status_code == 400

    def test_request_422_raises_validation_error(self, make_session, make_response):
        """_request() raises ValidationError on 422."""
        session = make_session(make_response(422, {"error": "unprocessable"}))[0]
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(ValidationError):
            client._request("GET", "/datasets")

    def test_request_500_raises_api_error_with_status(
        self, make_session, make_response
    ):
        """_request() raises ApiError with status_code on 500."""
        session = make_session(make_response(500, {"error": "server errored"}))[0]
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(ApiError) as exc_info:
            client._request("GET", "/datasets")

        assert exc_info.value.status_code == 500

    def test_request_502_raises_api_error_with_status(
        self, make_session, make_response
    ):
        """_request() raises ApiError with status_code on 502."""
        session = make_session(make_response(502, {"error": "bad gateway"}))[0]
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(ApiError) as exc_info:
            client._request("GET", "/datasets")

        assert exc_info.value.status_code == 502

    def test_request_429_raises_api_error(self, make_session, make_response):
        """_request() raises ApiError on 429."""
        session = make_session(make_response(429, {"error": "too many requests"}))[0]
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(ApiError):
            client._request("GET", "/datasets")

    def test_request_connection_error_raises_api_error(
        self, make_session, make_response
    ):
        """_request() raises ApiError on connection error."""
        session = make_session(make_response(200, {}))[0]
        session.request = Mock(
            side_effect=requests.ConnectionError("Connection failed")
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(ApiError):
            client._request("GET", "/datasets")


class TestClientProtocol:
    """Tests for protocol conformance."""

    def test_create_client_returns_protocol_conforming_client(self):
        """create_client returns DatasetApiClientProtocol."""
        client = create_client("https://dp-dataset-api")

        assert isinstance(client, DatasetApiClientProtocol)
        assert isinstance(client, DatasetApiClient)

    def test_init_rejects_non_session_objects(self):
        """DatasetApiClient.__init__ rejects non-Session objects."""
        invalid_session = object()

        with pytest.raises(TypeError):
            DatasetApiClient(base_url="https://dp-dataset-api", session=invalid_session)  # type: ignore[arg-type]

    def test_client_conforms_to_protocol(self, make_session, make_response):
        """DatasetApiClient conforms to DatasetApiClientProtocol."""
        session, _ = make_session(make_response(200, {"status": "ok"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        assert isinstance(client, DatasetApiClientProtocol)
