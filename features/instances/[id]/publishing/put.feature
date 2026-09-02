Feature: Update instance in publishing mode

  Background:
    Given private endpoints are enabled
    And I have these instances:
      """
      [
        {
          "id": "test-item-1",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        },
        {
          "id": "test-item-2",
          "state": "associated",
          "links": {
            "dataset": {
              "id": "income"
            }
          }
        },
        {
          "id": "test-item-3",
          "state": "created",
          "links": {
            "dataset": {
              "id": "income"
            }
          }
        },
        {
          "id": "test-item-4",
          "state": "created",
          "links": {
            "dataset": {
              "id": "other"
            }
          }
        },
        {
          "id": "test-item-5",
          "state": "created",
          "links": {
            "dataset": {
              "id": "other"
            }
          }
        },
        {
          "id": "test-item-6",
          "state": "created",
          "links": {
            "dataset": {
              "id": "other"
            }
          },
          "lowest_geography": "lowest_geo"
        }
      ]
      """

  Scenario: Update an instance with is_area_type set to false
    Given I am an admin user
    When I PUT "/instances/test-item-4"
      """
      {
        "id": "test-item-4",
        "dimensions": [
          {
            "name": "foo",
            "is_area_type": false
          }
        ]
      }
      """
    Then the HTTP status code should be "200"
    And the instance in the database for id "test-item-4" should be:
      """
      {
        "id": "test-item-4",
        "state": "created",
        "links": {
          "dataset": {
            "id": "other"
          }
        },
        "dimensions": [
          {
            "name": "foo",
            "is_area_type": false
          }
        ]
      }
      """

  Scenario: Update an instance with quality statement fields
    Given I am an admin user
    When I PUT "/instances/test-item-5"
      """
      {
        "id": "test-item-5",
        "dimensions": [
          {
            "name": "bar",
            "quality_statement_text": "This is a quality statement",
            "quality_statement_url": "www.ons.gov.uk/qualitystatement"
          }
        ]
      }
      """
    Then the HTTP status code should be "200"
    And the instance in the database for id "test-item-5" should be:
      """
      {
        "id": "test-item-5",
        "state": "created",
        "links": {
          "dataset": {
            "id": "other"
          }
        },
        "dimensions": [
          {
            "name": "bar",
            "quality_statement_text": "This is a quality statement",
            "quality_statement_url": "www.ons.gov.uk/qualitystatement"
          }
        ]
      }
      """

  Scenario: Update an instance with quality statement fields as a publisher user
    Given I am a publisher user
    When I PUT "/instances/test-item-5"
      """
      {
        "id": "test-item-5",
        "dimensions": [
          {
            "name": "bar",
            "quality_statement_text": "This is a quality statement",
            "quality_statement_url": "www.ons.gov.uk/qualitystatement"
          }
        ]
      }
      """
    Then the HTTP status code should be "200"
    And the instance in the database for id "test-item-5" should be:
      """
      {
        "id": "test-item-5",
        "state": "created",
        "links": {
          "dataset": {
            "id": "other"
          }
        },
        "dimensions": [
          {
            "name": "bar",
            "quality_statement_text": "This is a quality statement",
            "quality_statement_url": "www.ons.gov.uk/qualitystatement"
          }
        ]
      }
      """
