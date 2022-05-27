Feature: Dataset API
    Background:
        Given I have these "public" datasets:
                """
                [
                  {
                    "id": "1",
                    "is_based_on": {
                      "type": "",
                      "id": "not-included"
                    }
                  },
                  {
                    "id": "unpublished-estimates",
                    "is_based_on": {
                      "type": "",
                      "id": "included"
                    }
                  }
                ]
                """
        And I have these "private" datasets:
        """
        [
          {
            "id": "2",
            "is_based_on": {
              "type": "",
              "id": "not-included"
            }
          },
          {
            "id": "3",
            "is_based_on": {
              "type": "",
              "id": "not-included"
            }
          }
        ]
        """

    Scenario: Get /datasets is_based_on is provided
        When I GET "/datasets?is_based_on=included"
        Then I should receive the following JSON response with status "200":
        """
        [
          {
            "id": "unpublished-estimates",
            "is_based_on": {
              "type": "",
              "id": "included"
            }
          }
        ]
        """
    Scenario: Get /datasets is_based_on is malformed
        Given I have these datasets:
        When I GET "/datasets?is_based_on="
        Then I should receive the following JSON response with status "400":
        """
        """
    Scenario: Get /datasets is_based_on returns nothing
        Given I have these datasets:
        When I GET "/datasets?is_based_on=not-exists"
        Then I should receive the following JSON response with status "404":
        """
        """
