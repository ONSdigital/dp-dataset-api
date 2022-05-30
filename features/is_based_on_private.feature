Feature: Private Dataset API
    Background:
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        And I have these "public" datasets:
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
                    "id": "2",
                    "is_based_on": {
                      "type": "",
                      "id": "not-included"
                    }
                  }
                ]
                """
        And I have these "private" datasets:
        """
        [
          {
            "id": "unpublished-estimates",
            "is_based_on": {
              "type": "",
              "id": "included"
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
    # Scenario: Get /datasets is_based_on is provided
    #     When I GET "/datasets?is_based_on=another" with is_based_on "test"
    #     Then I should receive the following JSON response with status "200":
    #     """
    #     [
    #       {
    #         "id": "unpublished-estimates",
    #         "is_based_on": {
    #           "type": "",
    #           "id": "included"
    #         }
    #       }
    #     ]
    #     """
    # Scenario: Get /datasets is_based_on is malformed
    #     When I GET "/datasets?is_based_on=" with is_based_on "test"
    #     Then I should receive the following JSON response with status "400":
    #     """
    #     """
    # Scenario: Get /datasets is_based_on returns 404
    #     When I GET "/datasets" with is_based_on "does not exist"
    #     Then I should receive the following JSON response with status "404":
    #     """
    #     """
