Feature: Dataset API - Static Editions Admin Access

    Background:
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
                    "state": "associated",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                }
            ]
            """

    Scenario: GET /datasets/{id}/editions returns both published and associated editions for Admin
        Given private endpoints are enabled
        And I am an admin user
        When I GET "/datasets/population-estimates/editions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "id": "1",
                        "current": {
                            "id": "1",
                            "edition": "2019",
                            "state": "published",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                }
                            }
                        },
                        "next": {
                            "id": "1",
                            "edition": "2019",
                            "state": "published",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                }
                            }
                        }
                    },
                    {
                        "id": "2",
                        "current": {
                            "id": "2",
                            "edition": "time-series",
                            "state": "associated",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                }
                            }
                        },
                        "next": {
                            "id": "2",
                            "edition": "time-series",
                            "state": "associated",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                }
                            }
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """

    Scenario: GET /datasets/{id}/editions returns 200 for autharized viewer
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I have viewer access to the dataset "population-estimates"
        When I GET "/datasets/population-estimates/editions"
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
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET /datasets/{id}/editions returns 403 for unautharized viewer
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I don't have viewer access to the dataset "population-estimates"
        When I GET "/datasets/population-estimates/editions"
        Then the HTTP status code should be "403"


    Scenario: GET /datasets/{id}/editions/{edition} returns 200 for autharized viewer
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I have viewer access to the dataset "population-estimates/2019"
        When I GET "/datasets/population-estimates/editions/2019"
        Then I should receive the following JSON response with status "200":
            """
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
            """
    
    Scenario: GET /datasets/{id}/editions/{edition} returns 403 for unautharized viewer
        Given private endpoints are enabled
        And I am a viewer user without permission
        And I don't have viewer access to the dataset "population-estimates/2019"
        When I GET "/datasets/population-estimates/editions/2019"
        Then the HTTP status code should be "403"