Feature: Dataset API

    Background: we have a dataset which has an edition with a variety of versions and a dimension option
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
                }
            ]
            """
        And I have these dimensions:
            """
            [
                {
                    "instance_id": "test-item-1",
                    "dimension": "geography",
                    "option": "K02000001"
                },
                {
                    "instance_id": "test-item-1",
                    "dimension": "geography",
                    "option": "K02000002"
                }
            ]
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version}/dimensions in public mode
        When I GET "/datasets/population-estimates/editions/hello/versions/1/dimensions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "name": "geography",
                        "links": {
                            "code_list": {},
                            "options": {
                                "href": "http://localhost:22000/datasets/population-estimates/editions/hello/versions//dimensions/geography/options",
                                "id": "geography"
                            },
                            "version": {
                                "href": "http://localhost:22000/datasets/population-estimates/editions/hello/versions/"
                            }
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET version with no dimensions in private mode
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/datasets/population-estimates/editions/hello/versions/2/dimensions"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            dimensions not found
            """

    Scenario: GET version with invalid state in public mode
        When I GET "/datasets/population-estimates/editions/hello/versions/2/dimensions"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            version not found
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options in public mode
        When I GET "/datasets/population-estimates/editions/hello/versions/1/dimensions/geography/options"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "dimension": "geography",
                        "label": "",
                        "option": "K02000001",
                        "links": {
                            "code": {},
                            "code_list": {},
                            "version": {
                                "href": "http://localhost:22000/datasets/population-estimates/editions/hello/versions/1",
                                "id": "1"
                            }
                        }
                    },
                    {
                        "dimension": "geography",
                        "label": "",
                        "option": "K02000002",
                        "links": {
                            "code": {},
                            "code_list": {},
                            "version": {
                                "href": "http://localhost:22000/datasets/population-estimates/editions/hello/versions/1",
                                "id": "1"
                            }
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """

    Scenario: GET dimensions with no options in private mode
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/datasets/population-estimates/editions/hello/versions/2/dimensions/age/options"
        Then the HTTP status code should be "200"
        And I should receive the following JSON response:
            """
            {
                "count": 0,
                "items": [],
                "limit": 20,
                "offset": 0,
                "total_count": 0
            }
            """