Feature: Dataset API

    Background: we have instances
        Given I have these instances:
            """
            [
                {
                    "id": "test-item-1",
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                },
                {
                    "id": "test-item-2",
                    "state": "associated",
                    "links": {
                        "dataset": {
                            "id": "income"
                        }
                    }
                },
                {
                    "id": "test-item-3",
                    "state": "created",
                    "links": {
                        "dataset": {
                            "id": "income"
                        }
                    }
                }
            ]
            """

    Scenario: GET /instances in private mode returns all instances
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/instances"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 3,
                "items": [
                    {
                        "id": "test-item-3",
                        "import_tasks": null,
                        "last_updated": "0001-01-01T00:00:00Z",
                        "links": {
                            "dataset": {
                                "id": "income"
                            },
                            "job": null
                        },
                        "state": "created"
                    },
                    {
                        "id": "test-item-2",
                        "state": "associated",
                        "links": {
                            "dataset": {
                                "id": "income"
                            },
                            "job": null
                        },
                        "import_tasks": null,
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
                    {
                        "id": "test-item-1",
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "job": null
                        },
                        "import_tasks": null,
                        "last_updated": "0001-01-01T00:00:00Z"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 3
            }
            """

    Scenario: GET /instances in private mode with specified dataset returns instances for dataset
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/instances?dataset=population-estimates"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "id": "test-item-1",
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "job": null
                        },
                        "import_tasks": null,
                        "last_updated": "0001-01-01T00:00:00Z"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET /instances in private mode with specific state returns instances with specific state
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/instances?state=associated"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "id": "test-item-2",
                        "state": "associated",
                        "links": {
                            "dataset": {
                                "id": "income"
                            },
                            "job": null
                        },
                        "import_tasks": null,
                        "last_updated": "0001-01-01T00:00:00Z"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET /instances in private mode with specified state and dataset returns instance
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/instances?state=associated&dataset=income"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "id": "test-item-2",
                        "state": "associated",
                        "links": {
                            "dataset": {
                                "id": "income"
                            },
                            "job": null
                        },
                        "import_tasks": null,
                        "last_updated": "0001-01-01T00:00:00Z"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET /instances in private mode with state that doesnt match any instance returns error
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/instances?state=false"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            bad request - invalid filter state values: [false]
            """

    Scenario: GET /instances in private mode with dataset that doesnt match any instance returns empty list
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/instances?dataset=blah"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 0,
                "items": [],
                "limit": 20,
                "offset": 0,
                "total_count": 0
            }
            """

    Scenario: GET /instances in private mode with no auth returns not authorized
        Given private endpoints are enabled
        When I GET "/instances"
        Then the HTTP status code should be "401"