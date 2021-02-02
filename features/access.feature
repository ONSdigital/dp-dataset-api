Feature: Access should not be given for private end-points unless identity is verified

    Background:
        Given private endpoints are enabled

    Scenario: Not being allowed access to a private end point when no id provided

        Given I am not identified
        And I am not authorised
        When I POST to "/datasets/E3BC0B6-D6C4-4E20-917E-95D7EA8C91DC" with body:
            """
            {
                "title": "CID"
            }
            """
        Then the HTTP status code should be "401"

    Scenario: Being allowed access to a private end point when id provided
        Given I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST to "/datasets/E3BC0B6-D6C4-4E20-917E-95D7EA8C91DC" with body:
            """
            {
                "title": "CID"
            }
            """
        Then the HTTP status code should be "201"
