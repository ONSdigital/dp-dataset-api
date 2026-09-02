Feature: List editions with pagination in web mode

  Scenario: Get a list of editions with offset set to 1
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published"
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "1",
          "edition": "2019",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        },
        {
          "id": "2",
          "edition": "time-series",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions?offset=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "2",
            "edition": "time-series",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          }
        ],
        "limit": 20,
        "offset": 1,
        "total_count": 2
      }
      """

  Scenario: Get a list of editions with limit set to 1
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published"
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "1",
          "edition": "2019",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        },
        {
          "id": "2",
          "edition": "time-series",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions?limit=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "1",
            "edition": "2019",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          }
        ],
        "limit": 1,
        "offset": 0,
        "total_count": 2
      }
      """
