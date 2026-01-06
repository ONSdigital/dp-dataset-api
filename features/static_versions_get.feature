Feature: Static versions GET /versions

    Background: We have static datasets, editions and versions for testing
        Given I have these datasets:
            """
            [
                {
                    "id": "test-static",
                    "state": "created",
                    "type": "static",
                    "links": {
                        "latest_version": {
                            "id": "1",
                            "href": "/datasets/test-static/editions/test-edition-static/versions/1"
                        }
                    }
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "test-static-version",
                    "version": 1,
                    "edition": "test-edition-static",
                    "edition_title": "Test Edition Static Title",
                    "links": {
                        "dataset": {
                            "id": "test-static"
                        },
                        "edition": {
                            "href": "/datasets/test-static/editions/test-edition-static",
                            "id": "test-edition-static"
                        },
                        "self": {
                            "href": "/datasets/test-static/editions/test-edition-static/versions/1"
                        }
                    },
                    "state": "created",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Distribution 1",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/uuid/filename.csv",
                            "byte_size": 100000
                        }
                    ]
                },
                {
                    "id": "test-static-version-approved",
                    "version": 1,
                    "edition": "test-edition-static-approved",
                    "edition_title": "Test Edition Static Approved Title",
                    "links": {
                        "dataset": {
                            "id": "test-static"
                        },
                        "edition": {
                            "href": "/datasets/test-static/editions/test-edition-static-approved",
                            "id": "test-edition-static-approved"
                        },
                        "self": {
                            "href": "/datasets/test-static/editions/test-edition-static-approved/versions/1"
                        }
                    },
                    "state": "approved",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Distribution 1",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/uuid/filename.csv",
                            "byte_size": 100000
                        }
                    ]
                }
            ]
            """

    Scenario: GET /datasets/test-static/editions/test-edition-static-approved/versions in private mode returns all versions
        Given private endpoints are enabled
        And I am an admin user
        When I GET "/datasets/test-static/editions/test-edition-static-approved/versions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "dataset_id": "test-static",
                        "id": "test-static-version-approved",
                        "last_updated": "2021-01-01T00:00:01Z",
                        "type": "static",
                        "version": 1,
                        "state": "approved",
                        "links": {
                            "dataset": {
                                "id": "test-static"
                            },
                            "edition": {
                                "href": "/datasets/test-static/editions/test-edition-static-approved",
                                "id": "test-edition-static-approved"
                            },
                            "self": {
                                "href": "/datasets/test-static/editions/test-edition-static-approved/versions/1"
                            }
                        },
                        "edition": "test-edition-static-approved",
                        "edition_title": "Test Edition Static Approved Title",
                        "distributions": [
                            {
                                "title": "Distribution 1",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "/uuid/filename.csv",
                                "byte_size": 100000
                            }
                        ]
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: Viewer with permission to read the dataset versions receives 200
        Given private endpoints are enabled
        Given I am a viewer user with permission
        And I have viewer access to the dataset "test-static/test-edition-static-approved"
        When I GET "/datasets/test-static/editions/test-edition-static-approved/versions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "dataset_id": "test-static",
                        "edition": "test-edition-static-approved",
                        "edition_title": "Test Edition Static Approved Title",
                        "id": "test-static-version-approved",
                        "last_updated": "2021-01-01T00:00:01Z",
                        "links": {
                            "dataset": {
                                "id": "test-static"
                            },
                            "edition": {
                                "href": "/datasets/test-static/editions/test-edition-static-approved",
                                "id": "test-edition-static-approved"
                            },
                            "self": {
                                "href": "/datasets/test-static/editions/test-edition-static-approved/versions/1"
                            }
                        },
                        "state": "approved",
                        "type": "static",
                        "version": 1,
                        "distributions": [
                            {
                                "title": "Distribution 1",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "/uuid/filename.csv",
                                "byte_size": 100000
                            }
                        ]
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: Viewer with no permission to read the dataset versions receives 403
        Given private endpoints are enabled
        And I am a viewer user without permission
        And I don't have viewer access to the dataset "test-static/test-edition-static-approved"
        When I GET "/datasets/static-dataset-3"
        Then the HTTP status code should be "403"