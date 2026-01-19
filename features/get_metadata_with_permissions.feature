Feature: Dataset API - Metadata Permissions

    Background:
        Given I have a static dataset with version:
            """
            {
                "dataset": {
                    "id": "static-test-dataset",
                    "title": "Published Static Title",
                    "description": "Public Description",
                    "state": "published",
                    "type": "static",
                    "license": "Open Government License v3.0"
                },
                "version": {
                    "id": "v1-approved",
                    "version": 1,
                    "edition": "time-series",
                    "state": "approved",
                    "type": "static",
                    "release_date": "2023-05-20",
                    "links": {
                        "dataset": {
                            "id": "static-test-dataset"
                        },
                        "edition": {
                            "id": "time-series"
                        },
                        "self": {
                            "href": "/datasets/static-test-dataset/editions/time-series/versions/1"
                        }
                    },
                    "distributions": [
                        {
                            "title": "Dataset CSV",
                            "format": "csv",
                            "download_url": "/files/data.csv"
                        }
                    ]
                }
            }
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions/1/metadata returns 200 for autharised viewer
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I have viewer access to the dataset edition "static-test-dataset/time-series"
        When I GET "/datasets/static-test-dataset/editions/time-series/versions/1/metadata"
        Then I should receive the following JSON response with status "200":
            """
            {
                "description": "Public Description",
                "distributions": [
                    {
                        "download_url": "/files/data.csv",
                        "format": "csv",
                        "title": "Dataset CSV"
                    }
                ],
                "edition": "time-series",
                "id": "static-test-dataset",
                "last_updated": "0001-01-01T00:00:00Z",
                "license": "Open Government License v3.0",
                "links": {
                    "self": {
                        "href": "/datasets/static-test-dataset/editions/time-series/versions/1/metadata"
                    },
                    "version": {
                        "href": "/datasets/static-test-dataset/editions/time-series/versions/1",
                        "id": "1"
                    },
                    "website_version": {
                        "href": "http://localhost:20000/datasets/static-test-dataset/editions/time-series/versions/1"
                    }
                },
                "release_date": "2023-05-20",
                "state": "approved",
                "title": "Published Static Title",
                "type": "static",
                "version": 1
            }
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions/1 returns 403 for unautharised viewer
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I don't have viewer access to the dataset edition "static-test-dataset/time-series"
        When I GET "/datasets/static-test-dataset/editions/time-series/versions/1/metadata"
        Then the HTTP status code should be "403"