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
                },
                {
                    "id": "test-item-4",
                    "state": "created",
                    "links": {
                        "dataset": {
                            "id": "other"
                        }
                    }
                },
                {
                    "id": "test-item-5",
                    "state": "created",
                    "links": {
                        "dataset": {
                            "id": "other"
                        }
                    }
                },
                {
                    "id": "test-item-6",
                    "state": "created",
                    "links": {
                        "dataset": {
                            "id": "other"
                        }
                    },
                    "lowest_geography": "lowest_geo"
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
                "count": 6,
                "items": [
                    {
                        "id": "test-item-6",
                        "import_tasks": null,
                        "last_updated": "2021-01-01T00:00:05Z",
                        "links": {
                            "dataset": {
                                "id": "other"
                            },
                            "job": null
                        },
                        "state": "created",
                        "lowest_geography": "lowest_geo"
                    },
                    {
                        "id": "test-item-5",
                        "import_tasks": null,
                        "last_updated": "2021-01-01T00:00:04Z",
                        "links": {
                            "dataset": {
                                "id": "other"
                            },
                            "job": null
                        },
                        "state": "created"
                    },
                    {
                        "id": "test-item-4",
                        "import_tasks": null,
                        "last_updated": "2021-01-01T00:00:03Z",
                        "links": {
                            "dataset": {
                                "id": "other"
                            },
                            "job": null
                        },
                        "state": "created"
                    },
                    {
                        "id": "test-item-3",
                        "import_tasks": null,
                        "last_updated": "2021-01-01T00:00:02Z",
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
                        "last_updated": "2021-01-01T00:00:01Z"
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
                        "last_updated": "2021-01-01T00:00:00Z"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 6
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
                        "last_updated": "2021-01-01T00:00:00Z"
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
                        "last_updated": "2021-01-01T00:00:01Z"
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
                        "last_updated": "2021-01-01T00:00:01Z"
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

    Scenario: GET /instances/test-item-1 in private mode returns the correct instance
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/instances/test-item-1"
        Then I should receive the following JSON response with status "200":
            """
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
                "last_updated": "2021-01-01T00:00:00Z"
            }
            """

    Scenario: GET /instances/test-item-1 in private mode returns the correct instance
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/instances/test-item-1"
        Then I should receive the following JSON response with status "200":
            """
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
                "last_updated": "2021-01-01T00:00:00Z"
            }
            """

    Scenario: GET /instances/inexistent in private mode returns a notFound status code
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/instances/inexistent"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            instance not found
            """

    Scenario: GET /instances/test-item-1 in private mode with the wrong If-Match header value returns conflict
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        And I set the "If-Match" header to "wrongValue"
        When I GET "/instances/test-item-1"
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            instance does not match the expected eTag
            """
    Scenario: Updating instance with is_area_type explicitly false
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/instances"
        """
        {
            "id": "test-item-4",
            "dimensions":[
                {
                    "name": "foo",
                    "is_area_type": false,
                }
            ]
        }
        """

        Then the instance in the database for id "test-item-4" should be:
        """
        {
            "id": "test-item-4",
            "state": "created",
            "links": {
                "dataset": {
                    "id": "other"
                }
            },
            "dimensions":[
                {
                    "name": "foo",
                    "is_area_type": false
                }
            ]
        }
        """

    Scenario: Updating instance with quality statement fields
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/instances"
        """
        {
            "id": "test-item-5",
            "dimensions":[
                {
                    "name": "bar",
                    "quality_statement_text": "This is a quality statement",
                    "quality_statement_url": "www.ons.gov.uk/qualitystatement"
                }
            ]
        }
        """

        Then the instance in the database for id "test-item-5" should be:
        """
        {
            "id": "test-item-5",
            "state": "created",
            "links": {
                "dataset": {
                    "id": "other"
                }
            },
            "dimensions":[
                {
                    "name": "foo",
                    "quality_statement_text": "This is a quality statement",
                    "quality_statement_url": "www.ons.gov.uk/qualitystatement"
                }
            ]
        }
        """
