Feature: Get list of options for a dimension of an instance

  Background:
    Given I have these datasets:
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

  Scenario: List dimension options
    When I GET "/datasets/population-estimates/editions/hello/versions/1/dimensions/geography/options"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 2,
        "items": [
          {
            "dimension": "geography",
            "label": "",
            "option": "K02000001",
            "links": {
              "code": {
              },
              "code_list": {
              },
              "version": {
                "href": "http://localhost:22000/datasets/population-estimates/editions/hello/versions/1",
                "id": "1"
              }
            }
          },
          {
            "dimension": "geography",
            "label": "",
            "option": "K02000002",
            "links": {
              "code": {
              },
              "code_list": {
              },
              "version": {
                "href": "http://localhost:22000/datasets/population-estimates/editions/hello/versions/1",
                "id": "1"
              }
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 2
      }
      """
