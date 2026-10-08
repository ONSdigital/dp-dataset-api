"""Tests for HttpHeaders model and header handling."""

from __future__ import annotations

from dis_dataset_api_sdk_python import Dataset, DatasetApiClient, Headers, HttpHeaders


class TestHttpHeaders:
    """Tests for HttpHeaders model."""

    def test_conform_to_headers_protocol(self):
        """HttpHeaders conforms to Headers protocol."""
        assert isinstance(HttpHeaders(), Headers)

    def test_maps_fields_to_expected_http_header_names(self):
        """HttpHeaders maps snake_case fields to HTTP header names."""
        headers = HttpHeaders(
            collection_id="collection-123",
            download_service_token="download-token",
            if_match="etag-1",
            authorization="Bearer example-auth-token",
        )

        assert headers.to_http_headers() == {
            "Collection-ID": "collection-123",
            "X-Download-Service-Token": "download-token",
            "If-Match": "etag-1",
            "Authorization": "Bearer example-auth-token",
        }

    def test_omits_none_values(self):
        """HttpHeaders omits None values from mapping."""
        headers = HttpHeaders(
            collection_id="collection-123",
            authorization="Bearer token",
        )

        result = headers.to_http_headers()

        assert "X-Download-Service-Token" not in result
        assert "If-Match" not in result
        assert result == {
            "Collection-ID": "collection-123",
            "Authorization": "Bearer token",
        }

    def test_empty_headers(self):
        """HttpHeaders with no values returns empty dict."""
        headers = HttpHeaders()

        assert headers.to_http_headers() == {}

    def test_pydantic_model_validation(self):
        """HttpHeaders is a valid Pydantic model."""
        headers = HttpHeaders(
            collection_id="col-1",
            authorization="Bearer token",
        )

        dumped = headers.model_dump()

        assert dumped["collection_id"] == "col-1"
        assert dumped["authorization"] == "Bearer token"
        assert dumped["if_match"] is None
        assert dumped["download_service_token"] is None


class TestHttpHeadersIntegration:
    """Integration tests for HttpHeaders with endpoints."""

    def test_dataset_endpoint_accepts_real_http_headers(
        self, make_session, make_response
    ):
        """Endpoints accept HttpHeaders and pass mapped headers correctly."""
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "next": {
                        "id": "abc",
                        "title": "A dataset",
                    }
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)
        headers = HttpHeaders(
            collection_id="collection-123",
            if_match="etag-1",
            authorization="Bearer example-auth-token",
        )

        result = client.datasets.get_dataset("abc", headers=headers)

        assert isinstance(result, Dataset)
        assert result.id == "abc"
        assert request_mock.call_args.kwargs["headers"] == {
            "Collection-ID": "collection-123",
            "If-Match": "etag-1",
            "Authorization": "Bearer example-auth-token",
        }
