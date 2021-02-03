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
                "items": [
                    {
                        "id": "population-estimates"
                    }
                ]
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
