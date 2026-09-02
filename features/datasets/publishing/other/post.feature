Feature: Create a new dataset

  Background:
    Given private endpoints are enabled
    And I am an admin user

  Scenario: Create a dataset
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "canonical_topic": "canonical-topic-ID",
        "subtopics": [
          "subtopic-ID"
        ],
        "state": "anything",
        "title": "CID",
        "type": "filterable",
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "next_release": "2016-04-04",
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ]
      }
      """
    Then the HTTP status code should be "201"
    And the document in the database for id "ageing-population-estimates" should be:
      """
      {
        "id": "ageing-population-estimates",
        "canonical_topic": "canonical-topic-ID",
        "subtopics": [
          "subtopic-ID"
        ],
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
        },
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "next_release": "2016-04-04",
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ]
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "CREATE" and resource "/datasets/ageing-population-estimates" should be 1

  Scenario: Create a new dataset with is_migration
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "state": "anything",
        "title": "CID",
        "type": "filterable",
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "next_release": "2016-04-04",
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "is_migration": true
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
        },
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "next_release": "2016-04-04",
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "is_migration": true
      }
      """
