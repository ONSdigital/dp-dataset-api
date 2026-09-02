Feature: Get a list of dimensions

  Background:
    Given private endpoints are enabled
    And I am an admin user
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

  Scenario: List dimensions for a version with no dimensions
    When I GET "/datasets/population-estimates/editions/hello/versions/2/dimensions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dimensions not found
      """
