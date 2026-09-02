Feature: Get a list of datasets using pagination

  Background:
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates"
        },
        {
          "id": "income"
        },
        {
          "id": "age"
        }
      ]
      """

  Scenario: Get a list of datasets with offset set to 1
    When I GET "/datasets?offset=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 2,
        "items": [
          {
            "id": "income",
            "last_updated": "0001-01-01T00:00:00Z"
          },
          {
            "id": "age",
            "last_updated": "0001-01-01T00:00:00Z"
          }
        ],
        "limit": 20,
        "offset": 1,
        "total_count": 3
      }
      """

  Scenario: Get a list of datasets with limit set to 1
    When I GET "/datasets?offset=0&limit=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "population-estimates",
            "last_updated": "0001-01-01T00:00:00Z"
          }
        ],
        "limit": 1,
        "offset": 0,
        "total_count": 3
      }
      """

  Scenario: Get a list of datasets with offset and limit set to 1
    When I GET "/datasets?offset=1&limit=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "income",
            "last_updated": "0001-01-01T00:00:00Z"
          }
        ],
        "limit": 1,
        "offset": 1,
        "total_count": 3
      }
      """

  Scenario: Get a list of datasets with limit set to 0
    When I GET "/datasets?limit=0"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 0,
        "items": [],
        "limit": 0,
        "offset": 0,
        "total_count": 3
      }
      """

  Scenario: Get a list of datasets with offset greater than existing number of datasets
    When I GET "/datasets?offset=4&limit=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 0,
        "items": [],
        "limit": 1,
        "offset": 4,
        "total_count": 3
      }
      """

  Scenario: Get a list of datasets with limit set to greater than maximum limit
    When I GET "/datasets?offset=4&limit=1001"
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid query parameter
      """

  Scenario: Get a list of datasets with offset set to minus value
    When I GET "/datasets?offset=-1"
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid query parameter
      """

  Scenario: Get a list of datasets with limit set to minus value
    When I GET "/datasets?limit=-1"
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid query parameter
      """

  Scenario: Get a list of datasets when there are no datasets
    Given there are no datasets
    When I GET "/datasets?offset=1&limit=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 0,
        "items": [],
        "limit": 1,
        "offset": 1,
        "total_count": 0
      }
      """
