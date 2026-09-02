Feature: Get a version

  Background: we have a dataset which has an edition with a variety of versions
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

  Scenario: Get a version with URL rewriting enabled
    Given URL rewriting is enabled
    And I set the "X-Forwarded-Host" header to "api.example.com"
    And I set the "X-Forwarded-Path-Prefix" header to "v1"
    When I GET "/datasets/population-estimates/editions/hello/versions/4"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "test-item-4",
        "last_updated": "2021-01-01T00:00:03Z",
        "version": 4,
        "state": "published",
        "links": {
          "dataset": {
            "id": "population-estimates"
          },
          "self": {
            "href": "https://api.example.com/v1/datasets/population-estimates/editions/hello/versions/4"
          }
        },
        "edition": "hello",
        "lowest_geography": "ltla"
      }
      """
    And the response header "ETag" should be "etag-test-item-4"

  Scenario: Get a version
    When I GET "/datasets/population-estimates/editions/hello/versions/4"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "test-item-4",
        "last_updated": "2021-01-01T00:00:03Z",
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
      }
      """
    And the response header "ETag" should be "etag-test-item-4"

  Scenario: Get a version using a dataset ID that does not exist
    When I GET "/datasets/non-existent-dataset/editions/hello/versions/1"
    Then I should receive the following JSON response with status "404":
      """
      {
        "errors": [
          {
            "code": "dataset not found",
            "description": "dataset not found"
          }
        ]
      }
      """

  Scenario: Get a version using an edition ID that does not exist
    When I GET "/datasets/population-estimates/editions/non-existent-edition/versions/1"
    Then I should receive the following JSON response with status "404":
      """
      {
        "errors": [
          {
            "code": "edition not found",
            "description": "edition not found"
          }
        ]
      }
      """

  Scenario: Get a version using an invalid version ID
    When I GET "/datasets/population-estimates/editions/hello/versions/invalid-version"
    Then I should receive the following JSON response with status "400":
      """
      {
        "errors": [
          {
            "code": "invalid version requested",
            "description": "invalid version requested"
          }
        ]
      }
      """

  Scenario: Get a version using a version ID that does not exist
    When I GET "/datasets/population-estimates/editions/hello/versions/1000"
    Then I should receive the following JSON response with status "404":
      """
      {
        "errors": [
          {
            "code": "version not found",
            "description": "version not found"
          }
        ]
      }
      """
