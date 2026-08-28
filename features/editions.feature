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

    Scenario: GET /datasets/{id}/editions with URL rewriting enabled
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
        And URL rewriting is enabled
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
    
    Scenario: GET /datasets/{id}/editions/{edition_id}
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
        When I GET "/datasets/population-estimates/editions/2019"
        Then I should receive the following JSON response with status "200":
            """
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
            """

    Scenario: GET /datasets/{id}/editions/{edition_id} with URL rewriting enabled
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
        And URL rewriting is enabled
        When I GET "/datasets/population-estimates/editions/2019"
        Then I should receive the following JSON response with status "200":
            """
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
            """

    Scenario: GET /datasets/{id}/editions only returns published editions
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
                },
                {
                    "id": "3",
                    "edition": "2020",
                    "state": "associated",
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

    Scenario: GET /datasets/{id}/editions returns 404 for an unpublished dataset
        Given I have these datasets:
            """
            [
                {
                    "id": "unpublished-dataset",
                    "state": "associated"
                }
            ]
            """
        When I GET "/datasets/unpublished-dataset/editions"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            dataset not found
            """
    
    Scenario: GET /datasets/{id}/editions/{edition_id} returns 404 for an unpublished dataset
        Given I have these datasets:
            """
            [
                {
                    "id": "unpublished-dataset",
                    "state": "associated"
                }
            ]
            """
        When I GET "/datasets/unpublished-dataset/editions/unpublished-edition"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            dataset not found
            """

    Scenario: GET /datasets/{id}/editions/{edition_id} for a non-existent dataset returns 404
        When I GET "/datasets/non-existent-dataset/editions/january"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            dataset not found
            """

    Scenario: GET /datasets/{id}/editions/{edition_id} returns 404 for an unpublished edition
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
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/unpublished-edition"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            edition not found
            """

    Scenario: GET /datasets/{id}/editions/{edition_id} for a non-existent edition returns 404
        Given I have these datasets:
            """
                [
                    {
                        "id": "population-estimates",
                        "state": "published"
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/non-existent-edition"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            edition not found
            """
