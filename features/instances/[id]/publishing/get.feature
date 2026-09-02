Feature: Get instance in publishing mode

  Background:
    Given private endpoints are enabled
    And I am an admin user
    And I have these instances:
      """
      [
        {
          "id": "test-item-1",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """

  Scenario: Get an instance
    When I GET "/instances/test-item-1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "test-item-1",
        "state": "published",
        "links": {
          "dataset": {
            "id": "population-estimates"
          },
          "job": null
        },
        "import_tasks": null,
        "last_updated": "2021-01-01T00:00:00Z"
      }
      """

  Scenario: Get an instance that does not exist
    When I GET "/instances/inexistent"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            instance not found
      """

  Scenario: Get an instance with the wrong If-Match header value
    Given I set the "If-Match" header to "wrongValue"
    When I GET "/instances/test-item-1"
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
            instance does not match the expected eTag
      """
