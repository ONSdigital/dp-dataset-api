Feature: Dataset API

    Background: we have a dataset which has an edition with a vareiety of versions and a dimension option
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
                }
            ]
            """
        And I have these dimensions:
            """
            [
                {
                    "instance_id": "test-item-1",
                    "name": "geography"
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
                        "links": {
                            "code_list": {},
                            "options": {
                                "href": "http://localhost:22000/datasets/population-estimates/editions/hello/versions//dimensions//options"
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

