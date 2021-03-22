Feature: Dataset API

    Scenario: GET /datasets/{id}/editions/{edition}/versions
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
                    "id": "population-estimates",
                    "version": 1,
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "version": {
                            "href": "someurl"
                        }
                    },
                    "edition": "hello"
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/hello/versions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "id": "population-estimates",
                        "version": 1,
                        "state": "published"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """