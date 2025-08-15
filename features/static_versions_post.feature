Feature: Static Dataset Versions POST API

    Background: We have static datasets for POST version testing
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
                    "id": "static-dataset-existing",
                    "title": "static dataset with published version",
                    "state": "associated",
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
                            "id": "static-dataset-existing"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-existing/editions/2024",
                            "id": "2024"
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
                            "download_url": "/downloads/datasets/static-dataset-existing/editions/2024/versions/1.csv",
                            "byte_size": 150000
                        }
                    ]
                }
            ]
            """

    Scenario: POST creates a new static dataset version successfully
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/static-dataset-test/editions/2024/versions"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "2024",
                "type": "static",
                "distributions": [
                    {
                        "title": "Full Dataset CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/datasets/static-dataset-test/editions/2024/versions/1.csv",
                        "byte_size": 100000
                    }
                ]
            }
            """
        Then the HTTP status code should be "201"

    Scenario: POST creates version 2 when version 1 is published
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/static-dataset-existing/editions/2024/versions"
            """
            {
                "release_date": "2024-06-01T09:00:00.000Z",
                "edition_title": "2024 Edition Updated",
                "type": "static",
                "distributions": [
                    {
                        "title": "Updated Dataset CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/datasets/static-dataset-existing/editions/2024/versions/2.csv",
                        "byte_size": 200000
                    }
                ]
            }
            """
        Then the HTTP status code should be "201"

    Scenario: POST fails with missing mandatory fields
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/static-dataset-test/editions/2024/versions"
            """
            {
                "type": "static"
            }
            """
        Then the HTTP status code should be "400"

    Scenario: POST fails for non-existent dataset
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/non-existent-dataset/editions/2024/versions"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "Test Edition",
                "type": "static",
                "distributions": [
                    {
                        "title": "Test CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/test.csv",
                        "byte_size": 100000
                    }
                ]
            }
            """
        Then the HTTP status code should be "404"

    Scenario: POST fails when unpublished version already exists
        Given I have these static versions:
            """
            [
                {
                    "id": "static-version-unpublished",
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
                    "state": "associated",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "csv",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/datasets/static-dataset-test/editions/2024/versions/1.csv",
                            "byte_size": 100000
                        }
                    ]
                }
            ]
            """
        And private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/static-dataset-test/editions/2024/versions"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "2024 Updated",
                "type": "static",
                "distributions": [
                    {
                        "title": "csv",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/datasets/static-dataset-test/editions/2024/versions/2.csv",
                        "byte_size": 120000
                    }
                ]
            }
            """
        Then the HTTP status code should be "400"

    Scenario: POST fails when not authorised
        Given private endpoints are enabled
        When I POST "/datasets/static-dataset-test/editions/2024/versions"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "Test Edition",
                "type": "static",
                "distributions": [
                    {
                        "title": "Test CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/test.csv",
                        "byte_size": 100000
                    }
                ]
            }
            """
        Then the HTTP status code should be "401"