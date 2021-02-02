Feature: Private Dataset API

    Background:
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"


    Scenario:
        When I POST the following to "/datasets/E3BC0B6-D6C4-4E20-917E-95D7EA8C91DC":
            """
            {
                "title": "CID"
            }
            """
        Then the HTTP status code should be "201"
        And the document in the database for id "E3BC0B6-D6C4-4E20-917E-95D7EA8C91DC" should be:
            """
            {
                "id": "E3BC0B6-D6C4-4E20-917E-95D7EA8C91DC",
                "title": "CID",
                "state": "created",
                "filterable": true
            }
            """

    Scenario: A document with the same ID already exists in the database
        Given I have these datasets:
            """
            [
                {
                    "id": "helloworld"
                }
            ]
            """
        When I POST the following to "/datasets/helloworld":
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