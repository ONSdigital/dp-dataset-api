Feature: List datasets in publishing mode

  Background:
    Given private endpoints are enabled

  Scenario: Get a list of datasets
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates"
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
            "next": {
              "id": "population-estimates",
              "last_updated": "0001-01-01T00:00:00Z"
            },
            "current": {
              "id": "population-estimates",
              "last_updated": "0001-01-01T00:00:00Z"
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """

  Scenario: Get a list of datasets as a publisher
    Given I am a publisher user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates"
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
            "next": {
              "id": "population-estimates",
              "last_updated": "0001-01-01T00:00:00Z"
            },
            "current": {
              "id": "population-estimates",
              "last_updated": "0001-01-01T00:00:00Z"
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """

  Scenario: Get a list of datasets with topics included
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "canonical_topic": "canonical-topic-ID",
          "subtopics": [
            "subtopic-ID"
          ]
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
            "next": {
              "id": "population-estimates",
              "canonical_topic": "canonical-topic-ID",
              "subtopics": [
                "subtopic-ID"
              ],
              "last_updated": "0001-01-01T00:00:00Z"
            },
            "current": {
              "id": "population-estimates",
              "canonical_topic": "canonical-topic-ID",
              "subtopics": [
                "subtopic-ID"
              ],
              "last_updated": "0001-01-01T00:00:00Z"
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """

  Scenario: Get a list of datasets containing is_migration
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
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
            "next": {
              "id": "population-estimates",
              "is_migration": true,
              "last_updated": "0001-01-01T00:00:00Z"
            },
            "current": {
              "id": "population-estimates",
              "is_migration": true,
              "last_updated": "0001-01-01T00:00:00Z"
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """
