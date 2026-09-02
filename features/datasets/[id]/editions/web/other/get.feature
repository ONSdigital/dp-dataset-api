Feature: List editions in web mode

  Scenario: List editions
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
          "id": "population-estimates",
          "edition": "2019",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "population-estimates",
            "edition": "2019",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """

  Scenario: List editions with URL rewriting enabled
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
          "id": "population-estimates",
          "edition": "2019",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    And URL rewriting is enabled
    When I GET "/datasets/population-estimates/editions"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "population-estimates",
            "edition": "2019",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """

  Scenario: List editions containing only published editions
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
        },
        {
          "id": "3",
          "edition": "2020",
          "state": "associated",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 2,
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
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 2
      }
      """

  Scenario: List editions for an unpublished dataset
    Given I have these datasets:
      """
      [
        {
          "id": "unpublished-dataset",
          "state": "associated"
        }
      ]
      """
    When I GET "/datasets/unpublished-dataset/editions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dataset not found
      """
