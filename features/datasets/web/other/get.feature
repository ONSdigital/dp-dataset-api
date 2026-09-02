Feature: Get a list of datasets

  Scenario: Get a list of datasets
    Given I have realistic datasets:
      """
      [
        {
          "current": {
            "id": "population-estimates",
            "state": "published"
          },
          "next": {
            "id": "population-estimates",
            "state": "published"
          }
        },
        {
          "current": {
            "id": "income-by-age",
            "state": "published"
          },
          "next": {
            "id": "income-by-age",
            "state": "published"
          }
        },
        {
          "next": {
            "id": "cpih01",
            "state": "created"
          }
        }
      ]
      """
    When I GET "/datasets"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 2,
        "items": [
          {
            "id": "population-estimates",
            "last_updated": "0001-01-01T00:00:00Z",
            "state": "published"
          },
          {
            "id": "income-by-age",
            "last_updated": "0001-01-01T00:00:00Z",
            "state": "published"
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 2
      }
      """

  Scenario: Get a list of datasets containing redacted fields
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "previous_series_id": [
            "old-dataset-id"
          ],
          "is_migration": true
        }
      ]
      """
    When I GET "/datasets"
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
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """

  Scenario: Get a list of datasets containing redacted fields with URL rewriting enabled
    Given URL rewriting is enabled
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "previous_series_id": [
            "old-dataset-id"
          ],
          "is_migration": true
        }
      ]
      """
    When I GET "/datasets"
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
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """
