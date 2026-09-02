Feature: Get dataset in web mode

  Background:
    Given I have these datasets:
      """
      [
        {
          "id": "published-dataset",
          "state": "published",
          "title": "Published Dataset"
        },
        {
          "id": "published-dataset-with-previous-series",
          "state": "published",
          "title": "Published Dataset with previous series ID",
          "previous_series_id": [
            "old-dataset-id"
          ],
          "is_migration": true
        }
      ]
      """

  Scenario: Get a published dataset
    When I GET "/datasets/published-dataset"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "published-dataset",
        "last_updated": "{{DYNAMIC_TIMESTAMP}}",
        "state": "published",
        "title": "Published Dataset"
      }
      """

  Scenario: Get a published dataset which has a previous series ID
    When I GET "/datasets/published-dataset-with-previous-series"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "published-dataset-with-previous-series",
        "last_updated": "{{DYNAMIC_TIMESTAMP}}",
        "state": "published",
        "title": "Published Dataset with previous series ID"
      }
      """

  Scenario: Get a dataset that does not exist
    When I GET "/datasets/non-existing-dataset"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dataset not found
      """

  Scenario: Get a published dataset with URL rewriting enabled
    Given URL rewriting is enabled
    When I GET "/datasets/published-dataset-with-previous-series"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "published-dataset-with-previous-series",
        "last_updated": "{{DYNAMIC_TIMESTAMP}}",
        "state": "published",
        "title": "Published Dataset with previous series ID"
      }
      """

  Scenario: Get an unpublished dataset
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "created"
        }
      ]
      """
    When I GET "/datasets/population-estimates"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dataset not found
      """
