Feature: Get dataset in publishing mode

  Background:
    Given private endpoints are enabled
    And I am an admin user
    And I have realistic datasets:
      """
      [
        {
          "next": {
            "id": "unpublished-filterable-dataset",
            "state": "created",
            "title": "Unpublished Filterable Dataset",
            "type": "filterable"
          }
        }
      ]
      """

  Scenario: Get an unpublished dataset
    When I GET "/datasets/unpublished-filterable-dataset"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "unpublished-filterable-dataset",
        "next": {
          "id": "unpublished-filterable-dataset",
          "last_updated": "0001-01-01T00:00:00Z",
          "state": "created",
          "title": "Unpublished Filterable Dataset",
          "type": "filterable"
        }
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "READ" and resource "/datasets/unpublished-filterable-dataset" should be 1
