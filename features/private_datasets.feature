Feature: Private Dataset API

    Background:
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised

    Scenario: Successfully creating a new dataset document
        When I POST "/datasets/ageing-population-estimates"
            """
            {
                "title": "CID"
            }
            """
        Then the HTTP status code should be "201"
        And the document in the database for id "ageing-population-estimates" should be:
            """
            {
                "id": "ageing-population-estimates",
                "state": "created",
                "title": "CID",
                "type": "filterable"
            }
            """

    Scenario: A document with the same ID already exists in the database
        Given I have these datasets:
            """
            [
                {
                    "id": "ageing-population-estimates"
                }
            ]
            """
        When I POST "/datasets/ageing-population-estimates"
            """
            {
                "title": "Hello"
            }
            """
        Then the HTTP status code should be "403"
        And I should receive the following response:
            """
            forbidden - dataset already exists
            """