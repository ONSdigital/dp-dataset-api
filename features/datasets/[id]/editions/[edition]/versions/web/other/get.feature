Feature: List versions in web mode

  Background:
    Given I have these datasets:
      """
      [
        {
          "id": "test-cantabular-dataset-1",
          "type": "cantabular_flexible_table",
          "state": "edition-confirmed"
        },
        {
          "id": "test-cantabular-dataset-2",
          "type": "cantabular_flexible_table",
          "state": "associated",
          "links": {
            "latest_version": {
              "id": "1",
              "href": "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
            }
          }
        },
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
          "id": "test-edition-1",
          "edition": "hello",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        },
        {
          "id": "test-edition-2",
          "edition": "edition-with-no-versions",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        },
        {
          "id": "test-edition-3",
          "edition": "unpublished-edition",
          "state": "associated",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        },
        {
          "id": "hellov2",
          "edition": "hellov2",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        },
        {
          "id": "test-edition-cantabular-1",
          "edition": "2021",
          "state": "edition-confirmed",
          "type": "cantabular_flexible_table",
          "links": {
            "dataset": {
              "id": "test-cantabular-dataset-1"
            }
          }
        },
        {
          "id": "test-edition-cantabular-2",
          "edition": "2021",
          "state": "associated",
          "type": "cantabular_flexible_table",
          "links": {
            "dataset": {
              "id": "test-cantabular-dataset-2"
            },
            "latest_version": {
              "id": "1",
              "href": "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
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
          "state": "edition-confirmed",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/hellov2/versions/3"
            }
          },
          "edition": "hellov2"
        },
        {
          "id": "test-item-4",
          "version": 4,
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/hello/versions/4"
            }
          },
          "edition": "hello",
          "lowest_geography": "ltla"
        },
        {
          "id": "test-cantabular-version-1",
          "version": 1,
          "state": "edition-confirmed",
          "type": "cantabular_flexible_table",
          "links": {
            "dataset": {
              "id": "test-cantabular-dataset-1"
            },
            "self": {
              "href": "/datasets/test-cantabular-dataset-1/editions/2021/versions/1"
            }
          },
          "edition": "2021"
        },
        {
          "id": "test-cantabular-version-2",
          "version": 1,
          "state": "associated",
          "type": "cantabular_flexible_table",
          "links": {
            "dataset": {
              "id": "test-cantabular-dataset-2"
            },
            "self": {
              "href": "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
            }
          },
          "edition": "2021",
          "downloads": {
            "csv": {
              "public": "",
              "size": "1",
              "href": "/downloads/datasets/test-cantabular-dataset-2/editions/2021/versions/1.csv"
            }
          }
        }
      ]
      """

  Scenario: List versions with URL rewriting enabled
    Given URL rewriting is enabled
    And I set the "X-Forwarded-Host" header to "api.example.com"
    And I set the "X-Forwarded-Path-Prefix" header to "v1"
    When I GET "/datasets/population-estimates/editions/hello/versions"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 2,
        "items": [
          {
            "dataset_id": "population-estimates",
            "id": "test-item-4",
            "last_updated": "2021-01-01T00:00:03Z",
            "version": 4,
            "edition": "hello",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "self": {
                "href": "https://api.example.com/v1/datasets/population-estimates/editions/hello/versions/4"
              }
            },
            "lowest_geography": "ltla"
          },
          {
            "dataset_id": "population-estimates",
            "id": "test-item-1",
            "last_updated": "2021-01-01T00:00:00Z",
            "version": 1,
            "edition": "hello",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "self": {
                "href": "https://api.example.com/v1/datasets/population-estimates/editions/hello/versions/1"
              }
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 2
      }
      """

  Scenario: List versions
    When I GET "/datasets/population-estimates/editions/hello/versions"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 2,
        "items": [
          {
            "dataset_id": "population-estimates",
            "id": "test-item-4",
            "last_updated": "2021-01-01T00:00:03Z",
            "version": 4,
            "edition": "hello",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "self": {
                "href": "/datasets/population-estimates/editions/hello/versions/4"
              }
            },
            "lowest_geography": "ltla"
          },
          {
            "dataset_id": "population-estimates",
            "id": "test-item-1",
            "last_updated": "2021-01-01T00:00:00Z",
            "version": 1,
            "edition": "hello",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              },
              "self": {
                "href": "/datasets/population-estimates/editions/hello/versions/1"
              }
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 2
      }
      """

  Scenario: List versions for unpublished dataset
    When I GET "/datasets/test-cantabular-dataset-1/editions/hello/versions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      dataset not found
      """

  Scenario: List versions for a dataset that does not exist
    When I GET "/datasets/unknown-dataset/editions/hello/versions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      dataset not found
      """

  Scenario: List versions for unpublished edition
    When I GET "/datasets/population-estimates/editions/unpublished-edition/versions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      edition not found
      """

  Scenario: List versions for an edition that does not exist
    When I GET "/datasets/population-estimates/editions/unknown-edition/versions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      edition not found
      """

  Scenario: List versions for an edition with no versions
    When I GET "/datasets/population-estimates/editions/edition-with-no-versions/versions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      version not found
      """
