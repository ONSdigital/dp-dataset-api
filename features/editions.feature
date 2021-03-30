Feature: Dataset API

    Scenario: GET /datasets/{id}/editions
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "published"
                }
            ]
            """
        And I have these editions:
            """
            [
                {
                    "id": "population-estimates",
                    "edition": "2019",
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
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
                        "id": "population-estimates",
                        "edition": "2019",
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            }
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET a dataset with two editions
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "published"
                }
            ]
            """
        And I have these editions:
            """
            [
                {
                    "id": "1",
                    "edition": "2019",
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                },
                {
                    "id": "2",
                    "edition": "time-series",
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "id": "1",
                        "edition": "2019",
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            }
                        }
                    },
                    {
                        "id": "2",
                        "edition": "time-series",
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            }
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """



