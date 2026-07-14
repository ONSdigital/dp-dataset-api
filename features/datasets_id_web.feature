Feature: GET /datasets/{id} in web mode

    Background:
        Given I have these datasets:
            """
            [
                {
                    "id": "published-dataset",
                    "state": "published",
                    "title": "Published Dataset"
                },
                {
                    "id": "published-dataset-with-previous-series",
                    "state": "published",
                    "title": "Published Dataset with previous series ID",
                    "previous_series_id": ["old-dataset-id"]
                }
            ]
            """
        
    Scenario: Retrieving a published dataset returns 200
        When I GET "/datasets/published-dataset"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "published-dataset",
                "last_updated": "{{DYNAMIC_TIMESTAMP}}",
                "state": "published",
                "title": "Published Dataset"
            }
            """

    Scenario: Retrieving a published dataset with previous series returns 200 with the fields redacted
        When I GET "/datasets/published-dataset-with-previous-series"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "published-dataset-with-previous-series",
                "last_updated": "{{DYNAMIC_TIMESTAMP}}",
                "state": "published",
                "title": "Published Dataset with previous series ID"
            }
            """
    
    Scenario: Retrieving a non-existing dataset returns 404
        When I GET "/datasets/non-existing-dataset"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            dataset not found
            """