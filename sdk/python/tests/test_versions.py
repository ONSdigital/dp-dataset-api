"""Tests for versions resource."""
from __future__ import annotations

import pytest

from dis_dataset_api_sdk_python import (
    DatasetApiClient,
    DatasetApiClientProtocol,
    Headers,
    NotFoundError,
    VersionsClientProtocol,
)
from dis_dataset_api_sdk_python.models import (
    Dimension,
    Metadata,
    PublicDimensionOption,
    QueryParams,
    Version,
    VersionDimensionOptionsList,
    VersionDimensionsList,
    VersionsList,
)


class TestGetVersion:
    """Tests for get_version() endpoint."""

    def test_uses_next_document_when_authorized(self, make_session, make_response, fake_headers):
        """get_version() uses 'next' document when authorization header present."""
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "next": {
                        "id": "1",
                        "dataset_id": "abc",
                        "edition": "2024",
                        "version": 1,
                    }
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.versions.get_version(
            "abc",
            "2024",
            "1",
            headers=fake_headers(authorization="example-auth-token"),
        )

        assert isinstance(result, Version)
        assert result.id == "1"
        assert result.dataset_id == "abc"
        assert result.edition == "2024"
        assert request_mock.call_args.kwargs["headers"] == {
            "CollectionID": "collection-123",
            "IfMatch": "etag-1",
            "Authorization": "example-auth-token",
        }


class TestGetVersionMetadata:
    """Tests for get_version_metadata() endpoint."""

    def test_returns_model(self, make_session, make_response, fake_headers):
        """get_version_metadata() returns Metadata model."""
        session, request_mock = make_session(
            make_response(200, {"edition": "2024", "version": 1, "id": "abc"})
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.versions.get_version_metadata(
            "abc",
            "2024",
            "1",
            headers=fake_headers(),
        )

        assert isinstance(result, Metadata)
        assert result.edition == "2024"
        assert result.version == 1
        assert (
            request_mock.call_args.kwargs["url"]
            == "https://dp-dataset-api/datasets/abc/editions/2024/versions/1/metadata"
        )


class TestGetVersions:
    """Tests for get_versions() endpoint."""

    def test_passes_query_params_and_returns_model(self, make_session, make_response, fake_headers):
        """get_versions() passes query params and returns model."""
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "items": [{"id": "1", "dataset_id": "abc"}],
                    "count": 1,
                    "offset": 2,
                    "limit": 10,
                    "total_count": 1,
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.versions.get_versions(
            "abc",
            "2024",
            headers=fake_headers(),
            query_params=QueryParams(limit=10, offset=2, is_based_on="ignored"),
        )

        assert error is None
        assert isinstance(result, VersionsList)
        assert result.count == 1
        assert request_mock.call_args.kwargs["params"] == {"limit": 10, "offset": 2}

    def test_returns_validation_error_without_request(self, make_session, make_response, fake_headers):
        """get_versions() returns error without making request on validation error."""
        session, request_mock = make_session(make_response(200, {}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.versions.get_versions(
            "abc",
            "2024",
            headers=fake_headers(),
            query_params=QueryParams(limit=-1),
        )

        assert error == "negative offsets or limits are not allowed"
        assert isinstance(result, VersionsList)
        assert result.items == []
        request_mock.assert_not_called()


class TestGetVersionsInBatches:
    """Tests for get_versions_in_batches() endpoint."""

    def test_accumulates_items(self, make_session, make_response, fake_headers):
        """get_versions_in_batches() accumulates items across batches."""
        session, request_mock = make_session(
            [
                make_response(
                    200,
                    {
                        "items": [{"id": "1"}],
                        "count": 1,
                        "offset": 0,
                        "limit": 1,
                        "total_count": 2,
                    },
                ),
                make_response(
                    200,
                    {
                        "items": [{"id": "2"}],
                        "count": 1,
                        "offset": 1,
                        "limit": 1,
                        "total_count": 2,
                    },
                ),
            ]
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.versions.get_versions_in_batches(
            "abc",
            "2024",
            headers=fake_headers(),
            batch_size=1,
            max_workers=1,
        )

        assert error is None
        assert isinstance(result, VersionsList)
        assert [item.id for item in result.items] == ["1", "2"]
        assert result.count == 2
        assert request_mock.call_args_list[0].kwargs["params"] == {"limit": 1, "offset": 0}
        assert request_mock.call_args_list[1].kwargs["params"] == {"limit": 1, "offset": 1}


class TestGetVersionsInBatchesWithQueryParams:
    """Tests for get_versions_in_batches_with_query_params() endpoint."""

    def test_accumulates_items(self, make_session, make_response, fake_headers):
        """get_versions_in_batches_with_query_params() accumulates items with query params."""
        session, request_mock = make_session(
            [
                make_response(
                    200,
                    {
                        "items": [{"id": "2"}, {"id": "3"}],
                        "count": 2,
                        "offset": 1,
                        "limit": 2,
                        "total_count": 6,
                    },
                ),
                make_response(
                    200,
                    {
                        "items": [{"id": "4"}, {"id": "5"}],
                        "count": 2,
                        "offset": 3,
                        "limit": 2,
                        "total_count": 6,
                    },
                ),
            ]
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.versions.get_versions_in_batches_with_query_params(
            "abc",
            "2024",
            headers=fake_headers(),
            req_limit=4,
            req_offset=1,
            batch_size=2,
            max_workers=1,
        )

        assert error is None
        assert isinstance(result, VersionsList)
        assert [item.id for item in result.items] == ["2", "3", "4", "5"]
        assert result.count == 4
        assert result.offset == 1
        assert result.limit == 4
        assert result.total_count == 6
        assert request_mock.call_args_list[0].kwargs["params"] == {"limit": 2, "offset": 1}
        assert request_mock.call_args_list[1].kwargs["params"] == {"limit": 2, "offset": 3}

    def test_returns_offset_error(self, make_session, make_response, fake_headers):
        """get_versions_in_batches_with_query_params() returns error for invalid offset."""
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "items": [],
                    "count": 0,
                    "offset": 6,
                    "limit": 3,
                    "total_count": 6,
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.versions.get_versions_in_batches_with_query_params(
            "abc",
            "2024",
            headers=fake_headers(),
            req_limit=6,
            req_offset=6,
            batch_size=3,
            max_workers=1,
        )

        assert (
            error
            == "request offset value greater than or equal to versions total count. versions total count: 6, request offset value: 6"
        )
        assert isinstance(result, VersionsList)
        assert result.items == []
        request_mock.assert_called_once()


class TestGetVersionDimensions:
    """Tests for get_version_dimensions() endpoint."""

    def test_returns_list(self, make_session, make_response, fake_headers):
        """get_version_dimensions() returns VersionDimensionsList."""
        expected = VersionDimensionsList(
            items=[
                Dimension(description="my 1st dimension", id="1"),
                Dimension(description="my 2nd dimension", id="2"),
            ]
        )
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "items": [
                        {
                            "description": "my 1st dimension",
                            "id": "1",
                        },
                        {
                            "description": "my 2nd dimension",
                            "id": "2",
                        },
                    ]
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result = client.versions.get_version_dimensions(
            "abc",
            "2024",
            "1",
            headers=fake_headers(),
        )

        assert isinstance(result, VersionDimensionsList)
        assert result == expected
        assert (
            request_mock.call_args.kwargs["url"]
            == "https://dp-dataset-api/datasets/abc/editions/2024/versions/1/dimensions"
        )

    def test_raises_not_found_for_404(self, make_session, make_response, fake_headers):
        """get_version_dimensions() raises NotFoundError on 404."""
        session, _ = make_session(make_response(404, {"errors": ["not found"]}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(NotFoundError):
            client.versions.get_version_dimensions(
                "abc",
                "2024",
                "1",
                headers=fake_headers(),
            )


class TestGetVersionDimensionOptions:
    """Tests for get_version_dimension_options() endpoint."""

    def test_without_query_params_sends_no_params(self, make_session, make_response, fake_headers):
        """get_version_dimension_options() sends no params when query_params is None."""
        session, request_mock = make_session(make_response(200, {"items": []}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.versions.get_version_dimension_options(
            "abc",
            "2024",
            "1",
            "id",
            headers=fake_headers(),
            query_params=None,
        )

        assert error is None
        assert isinstance(result, VersionDimensionOptionsList)
        assert result.items == []
        assert request_mock.call_args.kwargs["params"] is None

    def test_with_empty_query_params_sends_zero_limit_and_offset(
        self, make_session, make_response, fake_headers
    ):
        """get_version_dimension_options() sends zero limit/offset with empty QueryParams."""
        session, request_mock = make_session(make_response(200, {"items": []}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.versions.get_version_dimension_options(
            "abc",
            "2024",
            "1",
            "id",
            headers=fake_headers(),
            query_params=QueryParams(),
        )

        assert error is None
        assert isinstance(result, VersionDimensionOptionsList)
        assert result.items == []
        assert request_mock.call_args.kwargs["params"] == {"limit": 0, "offset": 0}

    def test_passes_query_params(self, make_session, make_response, fake_headers):
        """get_version_dimension_options() passes query params correctly."""
        expected = VersionDimensionOptionsList(
            items=[
                PublicDimensionOption(label="my 1st option"),
                PublicDimensionOption(label="my 2nd option"),
            ]
        )
        session, request_mock = make_session(
            make_response(
                200,
                {
                    "items": [
                        {
                            "label": "my 1st option",
                        },
                        {
                            "label": "my 2nd option",
                        },
                    ]
                },
            )
        )
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.versions.get_version_dimension_options(
            "abc",
            "2024",
            "1",
            "id",
            headers=fake_headers(authorization="example-auth-token"),
            query_params=QueryParams(limit=10, offset=5),
        )

        assert error is None
        assert isinstance(result, VersionDimensionOptionsList)
        assert result == expected
        assert request_mock.call_args.kwargs["params"] == {"limit": 10, "offset": 5}

    def test_raises_not_found_for_404(self, make_session, make_response, fake_headers):
        """get_version_dimension_options() raises NotFoundError on 404."""
        session, _ = make_session(make_response(404, {"errors": ["not found"]}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        with pytest.raises(NotFoundError):
            client.versions.get_version_dimension_options(
                "abc",
                "2024",
                "1",
                "id",
                headers=fake_headers(),
                query_params=QueryParams(),
            )

    def test_returns_validation_error_without_request(
        self, make_session, make_response, fake_headers
    ):
        """get_version_dimension_options() returns error without making request on validation error."""
        session, request_mock = make_session(make_response(200, {}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        result, error = client.versions.get_version_dimension_options(
            "abc",
            "2024",
            "1",
            "id",
            headers=fake_headers(),
            query_params=QueryParams(limit=-1),
        )

        assert error == "negative offsets or limits are not allowed"
        assert isinstance(result, VersionDimensionOptionsList)
        assert result.items == []
        request_mock.assert_not_called()


class TestVersionsProtocol:
    """Tests for protocol conformance."""

    def test_fake_headers_conform_to_headers_protocol(self, fake_headers):
        """FakeHeaders conforms to Headers protocol."""
        assert isinstance(fake_headers(), Headers)

    def test_versions_conform_to_protocol(self, make_session, make_response):
        """client.versions conforms to VersionsClientProtocol."""
        session, _ = make_session(make_response(200, {"id": "1"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        assert isinstance(client.versions, VersionsClientProtocol)

    def test_client_conforms_to_dataset_api_client_protocol(self, make_session, make_response):
        """DatasetApiClient conforms to DatasetApiClientProtocol."""
        session, _ = make_session(make_response(200, {"id": "1"}))
        client = DatasetApiClient(base_url="https://dp-dataset-api", session=session)

        assert isinstance(client, DatasetApiClientProtocol)
