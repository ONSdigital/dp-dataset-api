Feature: Get list of options for a dimension of an instance

  Background:
    Given private endpoints are enabled
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published"
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "population-estimates",
          "edition": "hello",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    And I have these versions:
      """
      [
        {
          "id": "test-item-1",
          "version": 1,
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/hello/versions/1"
            }
          },
          "edition": "hello"
        },
        {
          "id": "test-item-2",
          "version": 2,
          "state": "associated",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/hello/versions/2"
            }
          },
          "edition": "hello"
        },
        {
          "id": "test-item-3",
          "version": 3,
          "state": "associated",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/hello/versions/3"
            }
          },
          "edition": "hello"
        }
      ]
      """
    And I have these dimensions:
      """
      [
        {
          "instance_id": "test-item-1",
          "dimension": "geography",
          "option": "K02000001"
        },
        {
          "instance_id": "test-item-1",
          "dimension": "geography",
          "option": "K02000002"
        }
      ]
      """

  Scenario: List dimension options with no results
    Given I am an admin user
    When I GET "/datasets/population-estimates/editions/hello/versions/2/dimensions/age/options"
    Then the HTTP status code should be "200"
    And I should receive the following JSON response:
      """
      {
        "count": 0,
        "items": [],
        "limit": 20,
        "offset": 0,
        "total_count": 0
      }
      """

  Scenario: List dimension options with no results as a publisher user
    Given I am a publisher user
    When I GET "/datasets/population-estimates/editions/hello/versions/2/dimensions/age/options"
    Then the HTTP status code should be "200"
    And I should receive the following JSON response:
      """
      {
        "count": 0,
        "items": [],
        "limit": 20,
        "offset": 0,
        "total_count": 0
      }
      """
