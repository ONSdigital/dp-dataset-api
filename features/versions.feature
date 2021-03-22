Feature: Dataset API

    Background: we have a dataset which has an edition with a variety of versions
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
                    "edition": "hello",
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                }
            ]
            """
        And I have these versions:
            """
            [
                {
                    "id": "test-item-1",
                    "version": 1,
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "self": {
                            "href": "someurl"
                        }
                    },
                    "edition": "hello"
                },
                {
                    "id": "test-item-2",
                    "version": 2,
                    "state": "associated",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "self": {
                            "href": "someurl"
                        }
                    },
                    "edition": "hello"
                },
                {
                    "id": "test-item-3",
                    "version": 3,
                    "state": "created",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "self": {
                            "href": "someurl"
                        }
                    },
                    "edition": "hellov2"
                },
                {
                    "id": "test-item-4",
                    "version": 4,
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "self": {
                            "href": "someurl"
                        }
                    },
                    "edition": "hello"
                }
            ]
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions in public mode returns published versions
        When I GET "/datasets/population-estimates/editions/hello/versions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "id": "test-item-4",
                        "version": 4,
                        "edition": "hello",
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "someurl"
                            }
                        }
                    },
                    {
                        "id": "test-item-1",
                        "version": 1,
                        "edition": "hello",
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "someurl"
                            }
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions in private mode returns all versions
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/datasets/population-estimates/editions/hello/versions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 3,
                "items": [
                    {
                        "id": "test-item-4",
                        "version": 4,
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "someurl"
                            }
                        },
                        "edition": "hello"
                    },
                    {
                        "id": "test-item-2",
                        "version": 2,
                        "state": "associated",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "someurl"
                            }
                        },
                        "edition": "hello"
                    },
                    {
                        "id": "test-item-1",
                        "version": 1,
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "someurl"
                            }
                        },
                        "edition": "hello"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 3
            }
            """