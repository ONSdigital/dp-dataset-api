"""Tests for editions resource."""

from __future__ import annotations

from dis_dataset_api_sdk_python import DatasetApiClient
from dis_dataset_api_sdk_python.models import (
    Edition,
    EditionsList,
    QueryParams,
)


class TestGetEdition:
    """Tests for get_edition() endpoint."""

    def test_uses_next_document_when_authorized(
        self, make_session, make_response, fake_headers
    ):
        """get_edition() uses 'next' document when authorization header present."""
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "next": {
                        "edition": "2024",
                        "dataset_id": "abc",
                        "id": "edition-1",
                    }
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.editions.get_edition(
            "abc",
            "2024",
            headers=fake_headers(authorization="example-auth-token"),
        )

        assert isinstance(result, Edition)
        assert result.edition == "2024"
        assert result.dataset_id == "abc"
        assert request_mock.call_args.kwargs["headers"] == {
            "CollectionID": "collection-123",
            "IfMatch": "etag-1",
            "Authorization": "example-auth-token",
        }


class TestGetEditions:
    """Tests for get_editions() endpoint."""

    def test_passes_query_params_and_returns_model(
        self, make_session, make_response, fake_headers
    ):
        """get_editions() passes query params and returns model."""
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "items": [
                        {
                            "next": {
                                "edition": "2024",
                                "dataset_id": "abc",
                                "id": "edition-1",
                            }
                        }
                    ],
                    "count": 1,
                    "offset": 5,
                    "limit": 10,
                    "total_count": 1,
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.editions.get_editions(
            "abc",
            headers=fake_headers(authorization="example-auth-token"),
            query_params=QueryParams(limit=10, offset=5),
        )

        assert error is None
        assert isinstance(result, EditionsList)
        assert result.count == 1
        assert result.items is not None
        assert result.items[0].edition == "2024"
        assert request_mock.call_args.kwargs["params"] == {"limit": 10, "offset": 5}
        assert request_mock.call_args.kwargs["headers"] == {
            "CollectionID": "collection-123",
            "IfMatch": "etag-1",
            "Authorization": "example-auth-token",
        }

    def test_returns_validation_error_without_request(
        self, make_session, make_response, fake_headers
    ):
        """get_editions() returns error without making request on validation error."""
        session, request_mock = make_session(make_response(200, {}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.editions.get_editions(
            "abc",
            headers=fake_headers(),
            query_params=QueryParams(limit=-1),
        )

        assert error == "negative offsets or limits are not allowed"
        assert isinstance(result, EditionsList)
        assert result.items is None
        request_mock.assert_not_called()
