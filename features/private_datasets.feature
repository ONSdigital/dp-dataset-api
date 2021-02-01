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
        And The data in the database for id "E3BC0B6-D6C4-4E20-917E-95D7EA8C91DC" should be:
        """
        {
            "id": "E3BC0B6-D6C4-4E20-917E-95D7EA8C91DC",
            "title": "CID",
            "state": "created"
        }
        """