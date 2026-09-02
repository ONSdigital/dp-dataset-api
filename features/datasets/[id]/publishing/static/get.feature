Feature: Get a dataset

  Background:
    Given private endpoints are enabled
    And I have realistic datasets:
      """
      [
        {
          "next": {
            "id": "unpublished-static-dataset",
            "state": "created",
            "title": "Unpublished Static Dataset",
            "type": "static"
          }
        }
      ]
      """

  Scenario: Get a dataset as a viewer with permissions
    Given I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
    And I have viewer access to the dataset "unpublished-static-dataset"
    When I GET "/datasets/unpublished-static-dataset"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "unpublished-static-dataset",
        "next": {
          "id": "unpublished-static-dataset",
          "last_updated": "0001-01-01T00:00:00Z",
          "state": "created",
          "title": "Unpublished Static Dataset",
          "type": "static"
        }
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "READ" and resource "/datasets/unpublished-static-dataset" should be 1

  Scenario: Get a dataset as a viewer with no permissions
    Given I am a JWT user with email "viewer2@ons.gov.uk" and group "role-viewer-denied"
    When I GET "/datasets/unpublished-static-dataset"
    Then the HTTP status code should be "403"

  Scenario: Get a dataset that does not exist
    Given I am an admin user
    When I GET "/datasets/non-existing-dataset"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      dataset not found
      """

  Scenario: Get a dataset as a viewer with permissions using a previous series id
    Given I am a JWT user with email "viewer3@ons.gov.uk" and group "role-viewer-allowed"
    And I have realistic datasets:
      """
      [
        {
          "next": {
            "id": "static-series-b",
            "state": "created",
            "title": "Static Series B",
            "type": "static",
            "previous_series_id": [
              "static-series-a"
            ]
          }
        }
      ]
      """
    And I have viewer access to the dataset "static-series-a"
    When I GET "/datasets/static-series-b"
    Then the HTTP status code should be "200"
    And the total number of audit events should be 1
    And the number of events with action "READ" and resource "/datasets/static-series-b" should be 1

  Scenario: Get a dataset as a viewer with no permissions using a previous series id
    Given I am a JWT user with email "viewer4@ons.gov.uk" and group "role-viewer-allowed"
    And I have realistic datasets:
      """
      [
        {
          "next": {
            "id": "static-series-d",
            "state": "created",
            "title": "Static Series D",
            "type": "static",
            "previous_series_id": [
              "static-series-c"
            ]
          }
        }
      ]
      """
    And I have viewer access to the dataset "some-unrelated-dataset"
    When I GET "/datasets/static-series-d"
    Then the HTTP status code should be "403"
    And the total number of audit events should be 0
