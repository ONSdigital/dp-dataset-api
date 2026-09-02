Feature: Get list of options for a dimension of an instance

  Background:
    Given private endpoints are enabled
    And I am an admin user
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
          },
          "dimensions": [
            {
              "href": "http://localhost:22400/code-lists/yyyy-qq",
              "id": "yyyy-qq",
              "name": "time"
            },
            {
              "href": "http://localhost:22400/code-lists/uk-only",
              "id": "uk-only",
              "name": "geography"
            }
          ]
        }
      ]
      """

    And I have these dimensions:
      """
      [
        {
          "instance_id": "test-item-1",
          "label": "2021 Q1",
          "links": {
            "code": {
              "href": "http://localhost:22400/code-lists/yyyy-qq/codes/2021-q1",
              "id": "2021-q1"
            },
            "code_list": {
              "href": "http://localhost:22400/code-lists/yyyy-qq",
              "id": "yyyy-qq"
            }
          },
          "dimension": "time",
          "option": "2021-q1",
          "node_id": "_e9ddf7a4-c72f-44e6-a23a-a02666b92139_time_2021-q1"
        },
        {
          "instance_id": "test-item-1",
          "label": "2021 Q2",
          "links": {
            "code": {
              "href": "http://localhost:22400/code-lists/yyyy-qq/codes/2021-q2",
              "id": "2021-q2"
            },
            "code_list": {
              "href": "http://localhost:22400/code-lists/yyyy-qq",
              "id": "yyyy-qq"
            }
          },
          "dimension": "time",
          "option": "2021-q2",
          "node_id": "_e9ddf7a4-c72f-44e6-a23a-a02666b92139_time_2021-q2"
        },
        {
          "instance_id": "test-item-1",
          "label": "2021 Q3",
          "links": {
            "code": {
              "href": "http://localhost:22400/code-lists/yyyy-qq/codes/2021-q3",
              "id": "2021-q3"
            },
            "code_list": {
              "href": "http://localhost:22400/code-lists/yyyy-qq",
              "id": "yyyy-qq"
            }
          },
          "dimension": "time",
          "option": "2021-q3",
          "node_id": "_e9ddf7a4-c72f-44e6-a23a-a02666b92139_time_2021-q3"
        },
        {
          "instance_id": "test-item-1",
          "label": "United Kingdom",
          "links": {
            "code": {
              "href": "http://localhost:22400/code-lists/uk-only/codes/K02000001",
              "id": "K02000001"
            },
            "code_list": {
              "href": "http://localhost:22400/code-lists/uk-only",
              "id": "uk-only"
            }
          },
          "dimension": "geography",
          "option": "K02000001",
          "node_id": "_7b48a92b-059f-4ff6-b96d-41858ffb4d3c_geography_K02000001"
        },
        {
          "instance_id": "test-item-2",
          "label": "2021 Q4",
          "links": {
          },
          "dimension": "time",
          "option": "2021-q4",
          "node_id": "_e9ddf7a4-c72f-44e6-a23a-a02666b92139_time_2021-q4"
        }
      ]
      """

  Scenario: List instance dimension options with pagination
    When I GET "/instances/test-item-1/dimensions/time/options?offset=1&limit=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          "2021-q2"
        ],
        "limit": 1,
        "offset": 1,
        "total_count": 3
      }
      """

  Scenario: List instance dimension options for an instance that does not exist
    When I GET "/instances/inexistent/dimensions/time/options"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            instance not found
      """

  Scenario: List instance dimension options for a dimension that does not exist
    When I GET "/instances/test-item-1/dimensions/inexistent/options"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dimension node not found
      """

  Scenario: List instance dimension options with the wrong If-Match header value
    Given I set the "If-Match" header to "wrongValue"
    When I GET "/instances/test-item-1/dimensions/time/options"
    Then the HTTP status code should be "409"
