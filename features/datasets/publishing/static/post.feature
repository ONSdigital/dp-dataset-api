Feature: Create a new dataset

  Background:
    Given private endpoints are enabled
    And I am an admin user

  Scenario: Create a dataset when a dataset with the same ID already exists
    Given I have these datasets:
      """
      [
        {
          "id": "ageing-population-estimates"
        }
      ]
      """
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "title": "title",
        "type": "static",
        "description": "description",
        "keywords": [
          "keyword"
        ],
        "next_release": "2016-04-04",
        "topics": [
          "topic"
        ],
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "license": "license"
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
      dataset already exists
      """

  Scenario: Create a dataset with missing dataset ID
    When I POST "/datasets"
      """
      {
        "title": "title",
        "type": "static",
        "state": "anything",
        "next_release": "2016-04-04",
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "topics": [
          "topic"
        ],
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "license": "license"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid fields: [ID]
      """

  Scenario: Create a dataset with missing dataset title
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "type": "static",
        "state": "anything",
        "next_release": "2016-04-04",
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "topics": [
          "topic"
        ],
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "license": "license"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid fields: [Title]
      """

  Scenario: Create a dataset with missing description
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "title": "title",
        "type": "static",
        "state": "anything",
        "next_release": "2016-04-04",
        "keywords": [
          "keyword"
        ],
        "topics": [
          "topic"
        ],
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "license": "license"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid fields: [Description]
      """

  Scenario: Create a dataset without keywords
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "title": "title",
        "type": "static",
        "state": "anything",
        "next_release": "2016-04-04",
        "description": "census",
        "topics": [
          "topic"
        ],
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "license": "license"
      }
      """
    Then the HTTP status code should be "201"

  Scenario: Create a dataset with missing next release
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "title": "title",
        "type": "static",
        "state": "anything",
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "topics": [
          "topic"
        ],
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "license": "license"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid fields: [NextRelease]
      """

  Scenario: Create a dataset with missing topics
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "title": "title",
        "type": "static",
        "state": "anything",
        "next_release": "2016-04-04",
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "license": "license"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid fields: [Topics]
      """

  Scenario: Create a dataset with missing contacts
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "title": "title",
        "type": "static",
        "state": "anything",
        "next_release": "2016-04-04",
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "topics": [
          "topic"
        ],
        "license": "license"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid fields: [Contacts]
      """

  Scenario: Create a dataset with missing license
    When I POST "/datasets"
      """
      {
        "id": "ageing-population-estimates",
        "title": "title",
        "type": "static",
        "state": "anything",
        "next_release": "2016-04-04",
        "description": "census",
        "keywords": [
          "keyword"
        ],
        "contacts": [
          {
            "email": "testing@hotmail.com",
            "name": "John Cox",
            "telephone": "01623 456789"
          }
        ],
        "topics": [
          "topic"
        ]
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid fields: [License]
      """

  Scenario: Create a dataset with spaces in the dataset ID
    When I POST "/datasets"
      """
      {
        "id": "test dataset id with spaces",
        "contacts": [
          {
            "email": "contact@ons.gov.uk",
            "name": "Expert Statistical Team",
            "telephone": "+44 1234 111111"
          }
        ],
        "description": "This dataset is for testing",
        "keywords": [
          "dataset"
        ],
        "license": "Open Government Licence v3.0",
        "next_release": "To be announced",
        "publishers": [
          {
            "name": "Office for National Statistics",
            "href": "https://www.ons.gov.uk"
          }
        ],
        "qmi": {
          "href": "https://www.ons.gov.uk/businessindustryandtrade/retailindustry/methodologies/retailsalesindexrsiqmi"
        },
        "title": "testing static title distributions 2",
        "topics": [
          "7779",
          "7755"
        ],
        "type": "static"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      spaces are not allowed in the ID field
      """
