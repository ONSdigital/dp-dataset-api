Feature: Static Dataset Versions DELETE API

    Background: We have static datasets for DELETE version testing
        Given I have these datasets:
            """
            [
                {
                    "id": "static-dataset-test",
                    "title": "Static dataset Test",
                    "state": "created",
                    "type": "static"
                },
                {
                    "id": "static-dataset-published",
                    "title": "static dataset with published version",
                    "state": "published",
                    "type": "static"
                },
                {
                    "id": "static-dataset-no-versions",
                    "title": "static dataset with no versions",
                    "state": "created",
                    "type": "static"
                },
                {
                    "id": "static-dataset-bad-version-download-url",
                    "title": "static dataset with bad version download_url",
                    "state": "created",
                    "type": "static"
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "static-version-approved",
                    "edition": "2024",
                    "edition_title": "2024 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-test"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-test/editions/2024",
                            "id": "2024"
                        }
                    },
                    "version": 1,
                    "release_date": "2024-01-01T09:00:00.000Z",
                    "state": "approved",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Published Dataset CSV",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/datasets/static-dataset-test/editions/2024/versions/1.csv",
                            "byte_size": 150000
                        }
                    ]
                },
                {
                    "id": "static-version-published",
                    "edition": "2025",
                    "edition_title": "2025 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-published"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-published/editions/2025",
                            "id": "2025"
                        }
                    },
                    "version": 1,
                    "release_date": "2024-01-01T09:00:00.000Z",
                    "state": "approved",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Published Dataset CSV",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/datasets/static-dataset-published/editions/2025/versions/1.csv",
                            "byte_size": 150000
                        }
                    ]
                },
                {
                    "id": "static-version-bad-download-url",
                    "edition": "January",
                    "edition_title": "January Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-bad-version-download-url"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-bad-version-download-url/editions/January",
                            "id": "January"
                        }
                    },
                    "version": 1,
                    "release_date": "2026-01-01T09:00:00.000Z",
                    "state": "associated",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Files API expected to fail",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/fail/to/delete.csv",
                            "byte_size": 150000
                        }
                    ]
                }
            ]
            """
    
     Scenario: DELETE static dataset with unpublished versions successfully
        Given private endpoints are enabled
        And I am an admin user
        When I DELETE "/datasets/static-dataset-test"
        Then the HTTP status code should be "204"
        And the dataset "static-dataset-test" should not exist
        And the static version "static-version-approved" should not exist 

    Scenario: DELETE rejects published static dataset from deletion
        Given private endpoints are enabled
        And I am an admin user
        When I DELETE "/datasets/static-dataset-published"
        Then the HTTP status code should be "403"
        And the dataset "static-dataset-published" should exist
        And the static version "static-version-published" should exist

    Scenario: DELETE /datasets/{id} fails due to bad files-api client response
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I DELETE "/datasets/static-dataset-bad-version-download-url"
        Then the HTTP status code should be "500"
        And the dataset "static-dataset-bad-version-download-url" should exist
        And the static version "static-version-bad-download-url" should exist

    Scenario: DELETE unpublished static dataset with no versions successfully
        Given private endpoints are enabled
        And I am an admin user
        When I DELETE "/datasets/static-dataset-no-versions"
        Then the HTTP status code should be "204"
        And the dataset "static-dataset-no-versions" should not exist