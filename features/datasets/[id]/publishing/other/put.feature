Feature: Update dataset in publishing mode

  Background:
    Given private endpoints are enabled
    And I am an admin user

  Scenario: Update the survey field of a dataset
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates"
        }
      ]
      """
    When I PUT "/datasets/population-estimates"
      """
      {
        "survey": "mockSurvey"
      }
      """
    Then I should receive the following JSON response with status "200":
      """
      {
        "survey": "mockSurvey",
        "last_updated": "0001-01-01T00:00:00Z"
      }
      """
    And the document in the database for id "population-estimates" should be:
      """
      {
        "id": "population-estimates",
        "survey": "mockSurvey"
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/population-estimates" should be 1

  Scenario: Update the topic fields of a dataset
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates"
        }
      ]
      """
    When I PUT "/datasets/population-estimates"
      """
      {
        "canonical_topic": "canonical-topic-ID",
        "subtopics": [
          "subtopic-ID"
        ]
      }
      """
    Then I should receive the following JSON response with status "200":
      """
      {
        "canonical_topic": "canonical-topic-ID",
        "subtopics": [
          "subtopic-ID"
        ],
        "last_updated": "0001-01-01T00:00:00Z"
      }
      """
    And the document in the database for id "population-estimates" should be:
      """
      {
        "id": "population-estimates",
        "canonical_topic": "canonical-topic-ID",
        "subtopics": [
          "subtopic-ID"
        ]
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/population-estimates" should be 1

  Scenario: Add related content to a dataset
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates"
        }
      ]
      """
    When I PUT "/datasets/population-estimates"
      """
      {
        "related_content": [
          {
            "description": "Related content description",
            "href": "http://localhost:22000/datasets/123/relatedContent",
            "title": "Related content"
          }
        ]
      }
      """
    Then I should receive the following JSON response with status "200":
      """
      {
        "related_content": [
          {
            "description": "Related content description",
            "href": "http://localhost:22000/datasets/123/relatedContent",
            "title": "Related content"
          }
        ],
        "last_updated": "0001-01-01T00:00:00Z"
      }
      """
    And the document in the database for id "population-estimates" should be:
      """
      {
        "id": "population-estimates",
        "related_content": [
          {
            "description": "Related content description",
            "href": "http://localhost:22000/datasets/123/relatedContent",
            "title": "Related content"
          }
        ]
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/population-estimates" should be 1

  Scenario: Add is_migration to a dataset
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates"
        }
      ]
      """
    When I PUT "/datasets/population-estimates"
      """
      {
        "is_migration": false
      }
      """
    Then I should receive the following JSON response with status "200":
      """
      {
        "is_migration": false,
        "last_updated": "0001-01-01T00:00:00Z"
      }
      """
    And the document in the database for id "population-estimates" should be:
      """
      {
        "id": "population-estimates",
        "is_migration": false
      }
      """
