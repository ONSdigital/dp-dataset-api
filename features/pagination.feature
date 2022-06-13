Feature: Dataset Pagination
    Background:
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates"
                },
                {
                    "id": "income"
                },
                {
                    "id": "age"
                }
            ]
            """

    Scenario: Offset skips first result of datasets when set to 1
        When I GET "/datasets?offset=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "id": "income"
                    },
                    {
                        "id": "age"
                    }
                ],
                "limit": 20,
                "offset": 1,
                "total_count": 3
            }
            """

    Scenario: Results limited to 1 when limit set to 1
        When I GET "/datasets?offset=0&limit=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "id": "population-estimates"
                    }
                ],
                "limit": 1,
                "offset": 0,
                "total_count": 3
            }
            """
    Scenario: Second dataset returned when offset and limit set to 1
        When I GET "/datasets?offset=1&limit=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "id": "income"
                    }
                ],
                "limit": 1,
                "offset": 1,
                "total_count": 3
            }
            """

    Scenario: No datasets returned when  limit set to 0
        When I GET "/datasets?limit=0"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 0,
                "items": [],
                "limit": 0,
                "offset": 0,
                "total_count": 3
            }
            """

    Scenario: Empty list when offset greater than existing number of datasets
        When I GET "/datasets?offset=4&limit=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 0,
                "items": [],
                "limit": 1,
                "offset": 4,
                "total_count": 3
            }
            """

    Scenario: 400 error returned when limit set to greater than maximum limit
        When I GET "/datasets?offset=4&limit=1001"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """


    Scenario: 400 error returned when offset set to minus value
        When I GET "/datasets?offset=-1"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """

    Scenario: 400 error returned when limit set to minus value
        When I GET "/datasets?limit=-1"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """

    Scenario: Returning metadata when there are no datasets
        Given there are no datasets
        When I GET "/datasets?offset=1&limit=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 0,
                "items": [],
                "limit": 1,
                "offset": 1,
                "total_count": 0
            }
            """

    Scenario: GET a dataset with two editions with offset
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
        When I GET "/datasets/population-estimates/editions?offset=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
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
                "offset": 1,
                "total_count": 2
            }
            """

    Scenario: GET a dataset with two editions with limit
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
        When I GET "/datasets/population-estimates/editions?limit=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
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
                    }
                ],
                "limit": 1,
                "offset": 0,
                "total_count": 2
            }
            """
