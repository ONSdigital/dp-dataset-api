Feature: Dataset API - Versions Permissions

    Background:
        Given I have these datasets:
            """
            [
                {
                    "id": "test-dataset",
                    "state": "published",
                    "type": "static"
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "version-001",
                    "version": 1,
                    "edition": "2021",
                    "state": "published",
                    "type": "static",
                    "links": {
                        "dataset": {
                            "id": "test-dataset"
                        },
                        "edition": {
                            "id": "2021",
                            "href": "/datasets/test-dataset/editions/2021"
                        },
                        "self": {
                            "href": "/datasets/test-dataset/editions/2021/versions/1"
                        }
                    }
                },
                {
                    "id": "version-002",
                    "version": 2,
                    "edition": "2021",
                    "state": "associated",
                    "type": "static",
                    "links": {
                        "dataset": {
                            "id": "test-dataset"
                        },
                        "edition": {
                            "id": "2021",
                            "href": "/datasets/test-dataset/editions/2021"
                        },
                        "self": {
                            "href": "/datasets/test-dataset/editions/2021/versions/2"
                        }
                    }
                }
            ]
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions returns 200 for autharized viewer
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I have viewer access to the dataset edition "test-dataset/2021"
        When I GET "/datasets/test-dataset/editions/2021/versions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "dataset_id": "test-dataset",
                        "edition": "2021",
                        "id": "version-001",
                        "last_updated": "2021-01-01T00:00:00Z",
                        "links": {
                            "dataset": {
                                "id": "test-dataset"
                            },
                            "edition": {
                                "href": "/datasets/test-dataset/editions/2021",
                                "id": "2021"
                            },
                            "self": {
                                "href": "/datasets/test-dataset/editions/2021/versions/1"
                            }
                        },
                        "state": "published",
                        "type": "static",
                        "version": 1
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions returns 403 for unautharized viewer
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I don't have viewer access to the dataset edition "test-dataset/2021"
        When I GET "/datasets/test-dataset/editions/2021/versions"
        Then the HTTP status code should be "403"

    Scenario: GET /datasets/{id}/editions/{edition}/versions/1 returns 200 for autharized viewer
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I have viewer access to the dataset edition "test-dataset/2021"
        When I GET "/datasets/test-dataset/editions/2021/versions/1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "edition": "2021",
                "id": "version-001",
                "last_updated": "2021-01-01T00:00:00Z",
                "links": {
                    "dataset": {
                        "id": "test-dataset"
                    },
                    "edition": {
                        "href": "/datasets/test-dataset/editions/2021",
                        "id": "2021"
                    },
                    "self": {
                        "href": "/datasets/test-dataset/editions/2021/versions/1"
                    }
                },
                "state": "published",
                "type": "static",
                "version": 1
            }
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions/1 returns 403 for unautharized viewer
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I don't have viewer access to the dataset edition "test-dataset/2021"
        When I GET "/datasets/test-dataset/editions/2021/versions/1"
        Then the HTTP status code should be "403"
