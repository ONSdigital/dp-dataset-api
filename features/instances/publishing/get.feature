Feature: List and query instances

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

  Scenario: List all instances
    Given I am an admin user
    When I GET "/instances"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 6,
        "items": [
          {
            "id": "test-item-6",
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:05Z",
            "links": {
              "dataset": {
                "id": "other"
              },
              "job": null
            },
            "state": "created",
            "lowest_geography": "lowest_geo"
          },
          {
            "id": "test-item-5",
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:04Z",
            "links": {
              "dataset": {
                "id": "other"
              },
              "job": null
            },
            "state": "created"
          },
          {
            "id": "test-item-4",
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:03Z",
            "links": {
              "dataset": {
                "id": "other"
              },
              "job": null
            },
            "state": "created"
          },
          {
            "id": "test-item-3",
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:02Z",
            "links": {
              "dataset": {
                "id": "income"
              },
              "job": null
            },
            "state": "created"
          },
          {
            "id": "test-item-2",
            "state": "associated",
            "links": {
              "dataset": {
                "id": "income"
              },
              "job": null
            },
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:01Z"
          },
          {
            "id": "test-item-1",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "job": null
            },
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:00Z"
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 6
      }
      """

  Scenario: List all instances as a publisher
    Given I am a publisher user
    When I GET "/instances"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 6,
        "items": [
          {
            "id": "test-item-6",
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:05Z",
            "links": {
              "dataset": {
                "id": "other"
              },
              "job": null
            },
            "state": "created",
            "lowest_geography": "lowest_geo"
          },
          {
            "id": "test-item-5",
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:04Z",
            "links": {
              "dataset": {
                "id": "other"
              },
              "job": null
            },
            "state": "created"
          },
          {
            "id": "test-item-4",
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:03Z",
            "links": {
              "dataset": {
                "id": "other"
              },
              "job": null
            },
            "state": "created"
          },
          {
            "id": "test-item-3",
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:02Z",
            "links": {
              "dataset": {
                "id": "income"
              },
              "job": null
            },
            "state": "created"
          },
          {
            "id": "test-item-2",
            "state": "associated",
            "links": {
              "dataset": {
                "id": "income"
              },
              "job": null
            },
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:01Z"
          },
          {
            "id": "test-item-1",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "job": null
            },
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:00Z"
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 6
      }
      """

  Scenario: List instances for a specific dataset
    Given I am an admin user
    When I GET "/instances?dataset=population-estimates"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "test-item-1",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "job": null
            },
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:00Z"
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """

  Scenario: List instances for a specific state
    Given I am an admin user
    When I GET "/instances?state=associated"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "test-item-2",
            "state": "associated",
            "links": {
              "dataset": {
                "id": "income"
              },
              "job": null
            },
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:01Z"
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """

  Scenario: List instances for a specific state and dataset
    Given I am an admin user
    When I GET "/instances?state=associated&dataset=income"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "test-item-2",
            "state": "associated",
            "links": {
              "dataset": {
                "id": "income"
              },
              "job": null
            },
            "import_tasks": null,
            "last_updated": "2021-01-01T00:00:01Z"
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """

  Scenario: List instances for an invalid state
    Given I am an admin user
    When I GET "/instances?state=false"
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
            bad request - invalid filter state values: [false]
      """

  Scenario: List instances for a dataset that doesn't match any instance
    Given I am an admin user
    When I GET "/instances?dataset=blah"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 0,
        "items": [],
        "limit": 20,
        "offset": 0,
        "total_count": 0
      }
      """

  Scenario: List instances with no authentication
    When I GET "/instances"
    Then the HTTP status code should be "401"
