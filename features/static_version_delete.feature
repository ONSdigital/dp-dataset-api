Feature: Static Dataset Version DELETE API

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
                }
            ]
            """
        
    Scenario: DELETE single static dataset version successfully
        Given private endpoints are enabled
        And detach dataset feature is enabled
        And I am identified as "admin@ons.gov.uk"
        And I am authorised
        When I DELETE "/datasets/static-dataset-test/editions/2024/versions/1"
        Then the HTTP status code should be "204"
        And the static version "static-version-approved" should not exist
        And the dataset "static-dataset-test" should exist