Feature: Dataset API

    Scenario: GET /datasets
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates"
                }
            ]
            """
        When I GET "/datasets"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count":1,
                "items": [
                    {
                        "id": "population-estimates"
                    }
                ],
                "limit":20, 
                "offset":0, 
                "total_count":1
            }
            """

    Scenario: GET a specific dataset
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates"
                },
                {
                    "id": "income-by-age"
                }
            ]
            """
        When I GET "/datasets/income-by-age"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "income-by-age"
            }
            """
