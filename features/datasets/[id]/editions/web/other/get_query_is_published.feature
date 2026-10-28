Feature: List editions in web mode

  Background:
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published",
          "type": "filterable"
        }
      ]
      """
    And I have these realistic editions:
      """
      [
        {
          "id": "published-edition",
          "current": {
            "edition": "published",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          },
          "next": {
            "edition": "published",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          }
        },
        {
          "id": "unpublished-edition",
          "next": {
            "edition": "unpublished",
            "state": "edition-confirmed",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          }
        }
      ]
      """

  Scenario: List only published editions when is_published is omitted
    When I GET "/datasets/population-estimates/editions"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "edition": "published",
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

  Scenario: List only published editions when is_published is true
    When I GET "/datasets/population-estimates/editions?is_published=true"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "edition": "published",
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

  Scenario: List only published editions when is_published is false
    When I GET "/datasets/population-estimates/editions?is_published=false"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "edition": "published",
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
