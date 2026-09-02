Feature: Update a dataset

  Scenario: Update a dataset
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
        ],
        "survey": "mockSurvey"
      }
      """
    Then the HTTP status code should be "405"
