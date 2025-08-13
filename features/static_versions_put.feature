Feature: Static Dataset Versions PUT API

    Background: We have static datasets for PUT version testing
        Given I have a static dataset with version:
            """
            {
                "dataset": {
                    "id": "static-dataset-update",
                    "title": "Static Dataset for Updates",
                    "state": "associated",
                    "type": "static"
                },
                "edition": {
                    "edition": "2025",
                    "edition_title": "2025 Edition"
                },
                "version": {
                    "id": "static-version-update",
                    "edition": "2025",
                    "edition_title": "2025 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-update"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-update/editions/2025",
                            "id": "2025"
                        },
                        "self": {
                            "href": "/datasets/static-dataset-update/editions/2025/versions/1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T09:00:00.000Z",
                    "state": "associated",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "csv",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/datasets/static-dataset-update/editions/2025/versions/1.csv",
                            "byte_size": 125000
                        }
                    ]
                }
            }
            """

    Scenario: PUT updates static dataset version successfully
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "state": "approved",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"
        And I should receive the following JSON response:
            """
            {
                "dataset_id": "static-dataset-update",
                "distributions": [
                    {
                        "byte_size": 125000,
                        "download_url": "/downloads/datasets/static-dataset-update/editions/2025/versions/1.csv",
                        "format": "csv",
                        "media_type": "text/csv",
                        "title": "csv"
                    }
                ],
                "edition": "2025",
                "id": "static-version-update",
                "last_updated": "0001-01-01T00:00:00Z",
                "links": {
                    "dataset": {
                        "id": "static-dataset-update"
                    },
                    "edition": {
                        "href": "/datasets/static-dataset-update/editions/2025",
                        "id": "2025"
                    },
                    "self": {
                        "href": "/datasets/static-dataset-update/editions/2025/versions/1"
                    }
                },
                "release_date": "2025-01-01T09:00:00.000Z",
                "state": "approved",
                "type": "static"
            }
            """

    Scenario: PUT updates static dataset version with new data
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "release_date": "2025-03-01T09:00:00.000Z",
                "edition_title": "Updated 2025 Edition",
                "state": "approved",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT updates static dataset version distributions
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "distributions": [
                    {
                        "title": "updated csv",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/datasets/static-dataset-update/editions/2025/versions/1-updated.csv",
                        "byte_size": 150000
                    },
                    {
                        "title": "xlsx",
                        "format": "xlsx",
                        "media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "download_url": "/downloads/datasets/static-dataset-update/editions/2025/versions/1.xlsx",
                        "byte_size": 175000
                    }
                ],
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT updates static dataset version edition
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "edition": "2025-revised",
                "edition_title": "2025 Revised Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT fails for non-existent version
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/999"
            """
            {
                "state": "approved",
                "type": "static"
            }
            """
        Then the HTTP status code should be "404"

    Scenario: PUT fails for non-existent dataset
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/non-existent/editions/2025/versions/1"
            """
            {
                "state": "approved",
                "type": "static"
            }
            """
        Then the HTTP status code should be "404"

    Scenario: PUT fails when not authorised
        Given private endpoints are enabled
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "state": "approved",
                "type": "static"
            }
            """
        Then the HTTP status code should be "401"

    Scenario: PUT state endpoint updates successfully
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
            """
            {
                "state": "approved"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT state transitions from associated to approved
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
            """
            {
                "state": "approved"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT state transitions from approved to published
        Given I have a static dataset with version:
            """
            {
                "dataset": {
                    "id": "static-dataset-publish",
                    "title": "Static Dataset for Publishing",
                    "state": "associated",
                    "type": "static",
                    "links": {
                        "editions": {
                            "href": "/datasets/static-dataset-publish/editions"
                        },
                        "self": {
                            "href": "/datasets/static-dataset-publish"
                        }
                    }
                },
                "edition": {
                    "edition": "2025",
                    "edition_title": "2025 Edition"
                },
                "version": {
                    "id": "static-version-approved",
                    "edition": "2025",
                    "edition_title": "2025 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-publish"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-publish/editions/2025",
                            "id": "2025"
                        },
                        "self": {
                            "href": "/datasets/static-dataset-publish/editions/2025/versions/1"
                        },
                        "version": {
                            "href": "/datasets/static-dataset-publish/editions/2025/versions/1",
                            "id": "1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-02-01T09:00:00.000Z",
                    "state": "approved",
                    "type": "static"
                }
            }
            """
        And private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-publish/editions/2025/versions/1/state"
            """
            {
                "state": "published"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT state fails with invalid state
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
            """
            {
                "state": "invalid-state"
            }
            """
        Then the HTTP status code should be "400"

    Scenario: PUT state fails when not authorised
        Given private endpoints are enabled
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
            """
            {
                "state": "approved"
            }
            """
        Then the HTTP status code should be "401"