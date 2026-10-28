Feature: List static editions in web mode

  Background:
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published",
          "type": "static"
        }
      ]
      """
    And I have these static versions:
      """
      [
        {
          "id": "published-version-1",
          "edition": "published",
          "version": 1,
          "state": "published",
          "type": "static",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "published",
              "href": "/datasets/population-estimates/editions/published"
            }
          }
        },
        {
          "id": "unpublished-version-1",
          "edition": "unpublished",
          "version": 1,
          "state": "associated",
          "type": "static",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "unpublished",
              "href": "/datasets/population-estimates/editions/unpublished"
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
            "version": 1,
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "latest_version": {
                "id": "1",
                "href": "/datasets/population-estimates/editions/published/versions/1"
              },
              "self": {
                "id": "published",
                "href": "/datasets/population-estimates/editions/published"
              },
              "versions": {
                "href": "/datasets/population-estimates/editions/published/versions"
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
            "version": 1,
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "latest_version": {
                "id": "1",
                "href": "/datasets/population-estimates/editions/published/versions/1"
              },
              "self": {
                "id": "published",
                "href": "/datasets/population-estimates/editions/published"
              },
              "versions": {
                "href": "/datasets/population-estimates/editions/published/versions"
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
            "version": 1,
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "latest_version": {
                "id": "1",
                "href": "/datasets/population-estimates/editions/published/versions/1"
              },
              "self": {
                "id": "published",
                "href": "/datasets/population-estimates/editions/published"
              },
              "versions": {
                "href": "/datasets/population-estimates/editions/published/versions"
              }
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """
