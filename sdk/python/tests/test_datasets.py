"""Tests for datasets resource."""

from __future__ import annotations

import pytest

from dis_dataset_api_sdk_python import (
    ApiError,
    Dataset,
    DatasetApiClient,
    DatasetApiClientProtocol,
    DatasetsClientProtocol,
    Headers,
    NotFoundError,
)
from dis_dataset_api_sdk_python.models import (
    DatasetEditionsList,
    DatasetsList,
    QueryParams,
)


class TestGetDataset:
    """Tests for get_dataset() endpoint."""

    def test_returns_pydantic_model(self, make_session, make_response):
        """get_dataset() returns Dataset model."""
        session, _ = make_session(
            make_response(200, {"id": "abc", "title": "A dataset"})
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.datasets.get_dataset("abc")

        assert isinstance(result, Dataset)
        assert result.id == "abc"

    def test_passes_header_mapping(self, make_session, make_response, fake_headers):
        """get_dataset() passes header mapping to request."""
        session, request_mock = make_session(make_response(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        headers = fake_headers()
        client.datasets.get_dataset("abc", headers=headers)

        assert request_mock.call_args.kwargs["headers"] == {
            "CollectionID": "collection-123",
            "IfMatch": "etag-1",
        }

    def test_404_raises_not_found(self, make_session, make_response):
        """get_dataset() raises NotFoundError on 404."""
        session, _ = make_session(make_response(404, {"error": "not found"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(NotFoundError):
            client.datasets.get_dataset("missing")

    def test_500_raises_api_error_with_status(self, make_session, make_response):
        """get_dataset() raises ApiError with status on 500."""
        session, _ = make_session(make_response(500, {"error": "server error"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(ApiError) as exc_info:
            client.datasets.get_dataset("abc")

        assert exc_info.value.status_code == 500


class TestGetDatasetByPath:
    """Tests for get_dataset_by_path() endpoint."""

    def test_trims_slashes_and_returns_model(
        self, make_session, make_response, fake_headers
    ):
        """get_dataset_by_path() trims slashes and returns Dataset model."""
        session, request_mock = make_session(
            make_response(200, {"id": "abc", "title": "A dataset"})
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.datasets.get_dataset_by_path(
            "/economy/gross-domestic-product/",
            headers=fake_headers(),
        )

        assert isinstance(result, Dataset)
        assert result.id == "abc"
        assert (
            request_mock.call_args.kwargs["url"]
            == "https://dp-dataset-api/economy/gross-domestic-product"
        )
        assert request_mock.call_args.kwargs["headers"] == {
            "CollectionID": "collection-123",
            "IfMatch": "etag-1",
        }


class TestGetDatasetEditions:
    """Tests for get_dataset_editions() endpoint."""

    def test_passes_query_params_and_returns_model(
        self, make_session, make_response, fake_headers
    ):
        """get_dataset_editions() passes query params and returns model."""
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "items": [{"dataset_id": "abc", "edition": "2024"}],
                    "count": 1,
                    "offset": 5,
                    "limit": 10,
                    "total_count": 1,
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.datasets.get_dataset_editions(
            headers=fake_headers(),
            query_params=QueryParams(limit=10, offset=5, state="published"),
        )

        assert error is None
        assert isinstance(result, DatasetEditionsList)
        assert result.count == 1
        assert request_mock.call_args.kwargs["params"] == {
            "limit": 10,
            "offset": 5,
            "state": "published",
        }

    def test_returns_validation_error_without_request(
        self, make_session, make_response, fake_headers
    ):
        """get_dataset_editions() returns error without making request on validation error."""
        session, request_mock = make_session(make_response(200, {}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.datasets.get_dataset_editions(
            headers=fake_headers(),
            query_params=QueryParams(limit=-1),
        )

        assert error == "negative offsets or limits are not allowed"
        assert isinstance(result, DatasetEditionsList)
        assert result.items is None
        request_mock.assert_not_called()


class TestGetDatasets:
    """Tests for get_datasets() endpoint."""

    def test_passes_query_params_and_returns_model(
        self, make_session, make_response, fake_headers
    ):
        """get_datasets() passes query params and returns model."""
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "items": [{"id": "abc"}],
                    "count": 1,
                    "offset": 2,
                    "limit": 10,
                    "total_count": 1,
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.datasets.get_datasets(
            headers=fake_headers(),
            query_params=QueryParams(limit=10, offset=2, is_based_on="source-id"),
        )

        assert error is None
        assert isinstance(result, DatasetsList)
        assert result.total_count == 1
        assert request_mock.call_args.kwargs["params"] == {
            "offset": 2,
            "limit": 10,
            "is_based_on": "source-id",
        }

    def test_returns_validation_error_without_request(
        self, make_session, make_response, fake_headers
    ):
        """get_datasets() returns error without making request on validation error."""
        session, request_mock = make_session(make_response(200, {}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.datasets.get_datasets(
            headers=fake_headers(),
            query_params=QueryParams(offset=-1),
        )

        assert error == "negative offsets or limits are not allowed"
        assert isinstance(result, DatasetsList)
        assert result.items == []
        assert result.count == 0
        assert result.offset == 0
        assert result.limit == 0
        assert result.total_count == 0
        request_mock.assert_not_called()


class TestDatasetsProtocol:
    """Tests for protocol conformance."""

    def test_fake_headers_conform_to_headers_protocol(self, fake_headers):
        """FakeHeaders conforms to Headers protocol."""
        assert isinstance(fake_headers(), Headers)

    def test_datasets_conforms_to_protocol(self, make_session, make_response):
        """client.datasets conforms to DatasetsClientProtocol."""
        session, _ = make_session(make_response(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        assert isinstance(client.datasets, DatasetsClientProtocol)

    def test_client_conforms_to_dataset_api_client_protocol(
        self, make_session, make_response
    ):
        """DatasetApiClient conforms to DatasetApiClientProtocol."""
        session, _ = make_session(make_response(200, {"id": "abc"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        assert isinstance(client, DatasetApiClientProtocol)
