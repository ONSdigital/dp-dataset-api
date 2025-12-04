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
                        "state": "published",
                        "links": {
                            "dataset": {
                                "href": "/datasets/population-estimates",
                                "id": "population-estimates"
                            },
                            "latest_version": {
                                "href": "/datasets/population-estimates/editions/2019/versions/population-estimates",
                                "id": "population-estimates"
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
                "state": "published",
                "links": {
                    "dataset": {
                        "href": "/datasets/population-estimates",
                        "id": "population-estimates"
                    },
                    "latest_version": {
                        "href": "/datasets/population-estimates/editions/2019/versions/population-estimates",
                        "id": "population-estimates"
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