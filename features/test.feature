Feature: Dataset API

    Scenario: GET /datasets
        Given I have these datasets:
            """
            [
                {
                    "id": "DE3BC0B6-D6C4-4E20-917E-95D7EA8C91DC"
                }
            ]
            """
        When I GET "/datasets"
        Then I should receive the following JSON response with status "200":
            """
            {
                "items": [
                    {
                        "id": "DE3BC0B6-D6C4-4E20-917E-95D7EA8C91DC"
                    }
                ]
            }
            """
