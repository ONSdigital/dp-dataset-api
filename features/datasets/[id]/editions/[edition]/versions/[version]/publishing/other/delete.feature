Feature: Delete version in publishing mode

  Background:
    Given private endpoints are enabled
    And I have these datasets:
      """
      [
        {
          "id": "non-static-dataset-no-versions",
          "title": "non-static dataset with no versions",
          "state": "created",
          "type": "filterable",
          "current": {
            "id": "non-static-dataset-no-versions",
            "title": "non-static dataset with no versions",
            "state": "created",
            "type": "filterable"
          },
          "next": {
            "id": "non-static-dataset-no-versions",
            "title": "non-static dataset with no versions - Updated Title",
            "state": "edition-confirmed",
            "type": "filterable"
          }
        }
      ]
      """

  Scenario: Delete a version with no authentication
    When I DELETE "/datasets/non-static-dataset-no-versions/editions/2025/versions/1"
    Then the HTTP status code should be "401"

  Scenario: Delete a version when ENABLE_DETACH_DATASET is disabled and ENABLE_DELETE_STATIC_VERSION is enabled
    Given I am an admin user
    And the "ENABLE_DETACH_DATASET" feature flag is "false"
    And the "ENABLE_DELETE_STATIC_VERSION" feature flag is "true"
    When I DELETE "/datasets/non-static-dataset-no-versions/editions/2025/versions/1"
    Then the HTTP status code should be "405"
    And I should receive the following response:
      """
      method not allowed
      """
