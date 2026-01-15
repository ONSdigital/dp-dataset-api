Feature: Dataset API

    Scenario: GET /datasets/{id}/editions with type static
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "published",
                    "type": "static"
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "population-estimates",
                    "edition": "2019",
                    "state": "published",
                    "version": 1,
                    "links": {
                        "dataset": {
                            "href": "/datasets/population-estimates",
                            "id": "population-estimates"
                        },
                        "edition": {
                            "href": "/datasets/population-estimates/editions/2019",
                            "id": "2019"
                        }
                    },
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
        When I GET "/datasets/population-estimates/editions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "edition": "2019",
                        "version": 1,
                        "state": "published",
                        "links": {
                            "dataset": {
                                "href": "/datasets/population-estimates",
                                "id": "population-estimates"
                            },
                            "latest_version": {
                                "href": "/datasets/population-estimates/editions/2019/versions/1",
                                "id": "1"
                            },
                            "self": {
                                "href": "/datasets/population-estimates/editions/2019",
                                "id": "2019"
                            },
                            "versions": {
                                "href": "/datasets/population-estimates/editions/2019/versions"
                            }
                        },
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

    Scenario: GET /datasets/{id}/editions/{edition_id} with type static
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "published",
                    "type": "static"
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "population-estimates",
                    "edition": "2019",
                    "version": 1,
                    "state": "published",
                    "links": {
                        "dataset": {
                            "href": "/datasets/population-estimates",
                            "id": "population-estimates"
                        },
                        "edition": {
                            "href": "/datasets/population-estimates/editions/2019",
                            "id": "2019"
                        }
                    },
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Distribution 1",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/files/uuid/filename.csv",
                            "byte_size": 100000
                        }
                    ]
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/2019"
        Then I should receive the following JSON response with status "200":
            """
            {
                "edition": "2019",
                "version": 1,
                "state": "published",
                "links": {
                    "dataset": {
                        "href": "/datasets/population-estimates",
                        "id": "population-estimates"
                    },
                    "latest_version": {
                        "href": "/datasets/population-estimates/editions/2019/versions/1",
                        "id": "1"
                    },
                    "self": {
                        "href": "/datasets/population-estimates/editions/2019",
                        "id": "2019"
                    },
                    "versions": {
                        "href": "/datasets/population-estimates/editions/2019/versions"
                    }
                },
                "distributions": [
                    {
                        "title": "Distribution 1",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/files/uuid/filename.csv",
                        "byte_size": 100000
                    }
                ]
            }
            """

    Scenario: Viewer with permission to read dataset edition receives 200
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I have viewer access to the dataset "population-estimates/2019"
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "published",
                    "type": "static"
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "population-estimates",
                    "edition": "2019",
                    "version": 1,
                    "state": "published",
                    "links": {
                        "dataset": {
                            "href": "/datasets/population-estimates",
                            "id": "population-estimates"
                        },
                        "edition": {
                            "href": "/datasets/population-estimates/editions/2019",
                            "id": "2019"
                        }
                    },
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Distribution 1",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/files/uuid/filename.csv",
                            "byte_size": 100000
                        }
                    ]
                },
                {
                    "id": "population-estimates",
                    "edition": "2019",
                    "version": 2,
                    "state": "associated",
                    "links": {
                        "dataset": {
                            "href": "/datasets/population-estimates",
                            "id": "population-estimates"
                        },
                        "edition": {
                            "href": "/datasets/population-estimates/editions/2019",
                            "id": "2019"
                        }
                    },
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Distribution 2",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/files/uuid/filename-v2.csv",
                            "byte_size": 200000
                        }
                    ]
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/2019"
        Then I should receive the following JSON response with status "200":
            """
            {
                "next": {
                    "edition": "2019",
                    "version": 2,
                    "state": "associated",
                    "links": {
                        "dataset": {
                            "href": "/datasets/population-estimates",
                            "id": "population-estimates"
                        },
                        "latest_version": {
                            "href": "/datasets/population-estimates/editions/2019/versions/2",
                            "id": "2"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/2019",
                            "id": "2019"
                        },
                        "versions": {
                            "href": "/datasets/population-estimates/editions/2019/versions"
                        }
                    },
                    "distributions": [
                        {
                            "title": "Distribution 2",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/files/uuid/filename-v2.csv",
                            "byte_size": 200000
                        }
                    ]
                }
            }
            """

    Scenario: Viewer without permission to read dataset edition receives 403
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I don't have viewer access to the dataset "population-estimates/2019"
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "published",
                    "type": "static"
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "population-estimates",
                    "edition": "2019",
                    "version": 1,
                    "state": "published",
                    "links": {
                        "dataset": {
                            "href": "/datasets/population-estimates",
                            "id": "population-estimates"
                        },
                        "edition": {
                            "href": "/datasets/population-estimates/editions/2019",
                            "id": "2019"
                        }
                    },
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Distribution 1",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/files/uuid/filename.csv",
                            "byte_size": 100000
                        }
                    ]
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/2019"
        Then the HTTP status code should be "403"
