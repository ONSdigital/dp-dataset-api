Feature: Dataset Pagination
    Background: 
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates"
                },
                {
                    "id": "income"
                },
                {
                    "id": "age"
                }
            ]
            """
    
    Scenario: Offset skips first result of datasets when set to 1
        When I GET "/datasets?offset=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count":2,
                "items": [
                    {
                        "id": "income"
                    },
                    {
                        "id": "age"
                    }
                ],
                "limit":20, 
                "offset":1, 
                "total_count":3
            }
            """
    
    Scenario: Results limited to 1 when limit set to 1
        When I GET "/datasets?offset=0&limit=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count":1,
                "items": [
                    {
                        "id": "population-estimates"
                    }
                ],
                "limit":1, 
                "offset":0, 
                "total_count":3
            }
            """
    Scenario: Second dataset returned when offset and limit set to 1
        When I GET "/datasets?offset=1&limit=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count":1,
                "items": [
                    {
                        "id": "income"
                    }
                ],
                "limit":1, 
                "offset":1, 
                "total_count":3
            }
            """

    Scenario: Empty list when offset greater than existing number of datasets
        When I GET "/datasets?offset=4&limit=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count":0,
                "items": [],
                "limit":1, 
                "offset":4, 
                "total_count":3
            }
            """

    Scenario: 400 error returned when limit set to greater than maximum limit
        When I GET "/datasets?offset=4&limit=1001"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """


    Scenario: 400 error returned when offset set to minus value
        When I GET "/datasets?offset=-1"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """

    Scenario: 400 error returned when limit set to minus value
        When I GET "/datasets?limit=-1"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """
    
    Scenario: Returning metadata when there are no datasets
        Given there are no datasets
        When I GET "/datasets?offset=1&limit=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count":0,
                "items": [],
                "limit":1, 
                "offset":1, 
                "total_count":0
            }
            """
  