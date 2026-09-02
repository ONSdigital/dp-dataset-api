Feature: Create a dataset

  Background:
    Given private endpoints are enabled

  Scenario: Create a dataset
    Given I am an admin user
    When I POST "/datasets/ageing-population-estimates"
      """
      {
        "state": "anything",
        "title": "CID",
        "type": "filterable"
      }
      """
    Then the HTTP status code should be "201"
    And the document in the database for id "ageing-population-estimates" should be:
      """
      {
        "id": "ageing-population-estimates",
        "state": "created",
        "title": "CID",
        "type": "filterable",
        "links": {
          "editions": {
            "href": "http://localhost:22000/datasets/ageing-population-estimates/editions"
          },
          "self": {
            "href": "http://localhost:22000/datasets/ageing-population-estimates"
          }
        }
      }
      """

  Scenario: Create a dataset as a publisher user
    Given I am a publisher user
    When I POST "/datasets/ageing-population-estimates"
      """
      {
        "state": "anything",
        "title": "CID",
        "type": "filterable"
      }
      """
    Then the HTTP status code should be "201"
    And the document in the database for id "ageing-population-estimates" should be:
      """
      {
        "id": "ageing-population-estimates",
        "state": "created",
        "title": "CID",
        "type": "filterable",
        "links": {
          "editions": {
            "href": "http://localhost:22000/datasets/ageing-population-estimates/editions"
          },
          "self": {
            "href": "http://localhost:22000/datasets/ageing-population-estimates"
          }
        }
      }
      """

  Scenario: Create a dataset that already exists
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "ageing-population-estimates"
        }
      ]
      """
    When I POST "/datasets/ageing-population-estimates"
      """
      {
        "title": "Hello"
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
      dataset already exists
      """
