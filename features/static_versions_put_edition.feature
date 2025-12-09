Feature: Static Dataset Versions PUT edition ID and Title API
    Background: We have static datasets for PUT version testing
        Given I have these datasets:
            """
            [
                {
                    "id": "static-dataset-1",
                    "title": "static dataset with published version",
                    "state": "published",
                    "type": "static"
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "static-version-published",
                    "edition": "2024",
                    "edition_title": "2024 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-1"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-1/editions/2024",
                            "id": "2024"
                        },
                        "version": {
                            "href": "/datasets/static-dataset-1/editions/2024/versions/1",
                            "id": "1"
                        },
                        "self": {
                            "href": "/datasets/static-dataset-1/editions/2024/versions/1"
                        }
                    },
                    "version": 1,
                    "release_date": "2024-01-01T09:00:00.000Z",
                    "state": "published",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Published Dataset CSV",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/uuid/filename.csv",
                            "byte_size": 150000
                        }
                    ]
                },
                {
                    "id": "static-version-unpublished",
                    "edition": "2025",
                    "edition_title": "2025 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-1"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-1/editions/2025",
                            "id": "2025"
                        },
                        "version": {
                            "href": "/datasets/static-dataset-1/editions/2025/versions/1",
                            "id": "1"
                        },
                        "self": {
                            "href": "/datasets/static-dataset-1/editions/2025/versions/1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T09:00:00.000Z",
                    "state": "associated",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Unpublished Dataset CSV",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/uuid/filename.csv",
                            "byte_size": 150000
                        }
                    ]
                },
                {
                    "id": "static-version-published-2026",
                    "edition": "2026",
                    "edition_title": "2026 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-1"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-1/editions/2026",
                            "id": "2026"
                        },
                        "version": {
                            "href": "/datasets/static-dataset-1/editions/2026/versions/1",
                            "id": "1"
                        },
                        "self": {
                            "href": "/datasets/static-dataset-1/editions/2026/versions/1"
                        }
                    },
                    "version": 1,
                    "release_date": "2024-01-01T09:00:00.000Z",
                    "state": "published",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Published Dataset CSV",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/uuid/filename.csv",
                            "byte_size": 150000
                        }
                    ]
                },
                {
                    "id": "static-version-unpublished-2026",
                    "edition": "2026",
                    "edition_title": "2026 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-1"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-1/editions/2026",
                            "id": "2026"
                        },
                        "version": {
                            "href": "/datasets/static-dataset-1/editions/2026/versions/2",
                            "id": "2"
                        },
                        "self": {
                            "href": "/datasets/static-dataset-1/editions/2026/versions/2"
                        }
                    },
                    "version": 2,
                    "release_date": "2025-01-01T09:00:00.000Z",
                    "state": "associated",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Unpublished Dataset CSV",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/uuid/filename.csv",
                            "byte_size": 150000
                        }
                    ]
                }
            ]
            """

    Scenario: PUT fails when updating edition ID to duplicate value within same series
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-1/editions/2025/versions/1"
            """
            {
                "edition": "2024",
                "type": "static"
            }
            """
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            the edition already exists
            """

    Scenario: PUT fails when updating edition title to existing value within same series
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-1/editions/2024/versions/1"
            """
            {
                "edition_title": "2025 Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            the edition-title already exists
            """

    Scenario: PUT fails when both edition ID and title already exist within the same series
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-1/editions/2024/versions/1"
            """
            {
                "edition": "2025",
                "edition_title": "2025 Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            the edition already exists
            """

    Scenario: PUT succeeds when updating  edition title to unique value within the series
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-1/editions/2024/versions/1"
            """
            {
                "edition_title": "Unique 2024 Updated Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT succeeds when updating edition ID to unique value within the series
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-1/editions/2024/versions/1"
            """
            {
                "edition": "2024-updated-edition-id",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT succeeds when updating both edition ID and edition title to unique values within the series
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-1/editions/2026/versions/1"
            """
            {
                "edition": "2025-updated-edition-id",
                "edition_title": "Unique 2025 Updated Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT succeeds when updating release_date without changing edition ID or title
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-1/editions/2026/versions/1"
            """
            {
                "release_date": "2025-04-06T14:49:23.354Z",
                "edition": "20555",
                "edition_title": "2026 Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"