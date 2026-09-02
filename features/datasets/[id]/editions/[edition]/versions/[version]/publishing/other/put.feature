Feature: Update a version

  Background:
    Given private endpoints are enabled
    And I have these datasets:
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

  Scenario: Update a version of a CMD dataset
    Given I am an admin user
    And I have a real kafka container with topic "filter-job-submitted"
    When I PUT "/datasets/population-estimates/editions/hellov2/versions/3"
      """
      {
        "instance_id": "test-item-3",
        "license": "ONS",
        "release_date": "2017-04-04",
        "state": "associated",
        "collection_id": "bla"
      }
      """
    And these generate downloads events are produced:
      | InstanceID  | DatasetID            | Edition | Version | FilterOutputID |
      | test-item-3 | population-estimates | hellov2 | 3       |                |
    Then I should receive the following JSON response with status "200":
      """
      {
        "collection_id": "bla",
        "dataset_id": "population-estimates",
        "id": "test-item-3",
        "last_updated": "0001-01-01T00:00:00Z",
        "links": {
          "dataset": {
            "id": "population-estimates"
          },
          "self": {
            "href": "/datasets/population-estimates/editions/hellov2/versions/3"
          }
        },
        "release_date": "2017-04-04",
        "edition": "hellov2",
        "state": "associated"
      }
      """

  Scenario: Update a version of a Cantabular dataset
    Given I am an admin user
    And I have a real kafka container with topic "cantabular-export-start"
    When I PUT "/datasets/test-cantabular-dataset-1/editions/2021/versions/1"
      """
      {
        "instance_id": "test-cantabular-version-1",
        "license": "ONS",
        "release_date": "2017-04-04",
        "state": "associated",
        "collection_id": "bla"
      }
      """
    And these cantabular generator downloads events are produced:
      | InstanceID                | DatasetID                 | Edition | Version | FilterOutputID | Dimensions |
      | test-cantabular-version-1 | test-cantabular-dataset-1 | 2021    | 1       |                | []         |
    Then I should receive the following JSON response with status "200":
      """
      {
        "collection_id": "bla",
        "dataset_id": "test-cantabular-dataset-1",
        "id": "test-cantabular-version-1",
        "last_updated": "0001-01-01T00:00:00Z",
        "links": {
          "dataset": {
            "id": "test-cantabular-dataset-1"
          },
          "self": {
            "href": "/datasets/test-cantabular-dataset-1/editions/2021/versions/1"
          }
        },
        "release_date": "2017-04-04",
        "edition": "2021",
        "state": "associated"
      }
      """

  Scenario: Update a published version of a Cantabular dataset
    Given I am an admin user
    And I have a real kafka container with topic "cantabular-export-start"
    And these versions need to be published:
      """
      [
        {
          "version_id": "test-cantabular-version-2",
          "version_number": "1"
        }
      ]
      """
    When I PUT "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
      """
      {
        "instance_id": "test-cantabular-version-2",
        "license": "ONS",
        "release_date": "2017-04-04",
        "state": "published",
        "collection_id": "bla",
        "links": {
          "version": {
            "id": "1",
            "href": "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
          }
        }
      }
      """
    And these cantabular generator downloads events are produced:
      | InstanceID                | DatasetID                 | Edition | Version | FilterOutputID | Dimensions |
      | test-cantabular-version-2 | test-cantabular-dataset-2 | 2021    | 1       |                | []         |
    Then I should receive the following JSON response with status "200":
      """
      {
        "dataset_id": "test-cantabular-dataset-2",
        "downloads": {
          "csv": {
            "href": "/downloads/datasets/test-cantabular-dataset-2/editions/2021/versions/1.csv",
            "size": "1"
          }
        },
        "id": "test-cantabular-version-2",
        "last_updated": "0001-01-01T00:00:00Z",
        "links": {
          "dataset": {
            "id": "test-cantabular-dataset-2"
          },
          "self": {
            "href": "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
          }
        },
        "release_date": "2017-04-04",
        "edition": "2021",
        "state": "published"
      }
      """

  Scenario: Update a published version of a Cantabular dataset as a publisher
    Given I am a publisher user
    And I have a real kafka container with topic "cantabular-export-start"
    And these versions need to be published:
      """
      [
        {
          "version_id": "test-cantabular-version-2",
          "version_number": "1"
        }
      ]
      """
    When I PUT "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
      """
      {
        "instance_id": "test-cantabular-version-2",
        "license": "ONS",
        "release_date": "2017-04-04",
        "state": "published",
        "collection_id": "bla",
        "links": {
          "version": {
            "id": "1",
            "href": "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
          }
        }
      }
      """
    And these cantabular generator downloads events are produced:
      | InstanceID                | DatasetID                 | Edition | Version | FilterOutputID | Dimensions |
      | test-cantabular-version-2 | test-cantabular-dataset-2 | 2021    | 1       |                | []         |
    Then I should receive the following JSON response with status "200":
      """
      {
        "dataset_id": "test-cantabular-dataset-2",
        "downloads": {
          "csv": {
            "href": "/downloads/datasets/test-cantabular-dataset-2/editions/2021/versions/1.csv",
            "size": "1"
          }
        },
        "id": "test-cantabular-version-2",
        "last_updated": "0001-01-01T00:00:00Z",
        "links": {
          "dataset": {
            "id": "test-cantabular-dataset-2"
          },
          "self": {
            "href": "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
          }
        },
        "release_date": "2017-04-04",
        "edition": "2021",
        "state": "published"
      }
      """

  Scenario: Update the edition ID of a Cantabular dataset
    Given I am an admin user
    When I PUT "/datasets/test-cantabular-dataset-1/editions/2021/versions/1"
      """
      {
        "edition": "2021-updated",
        "state": "edition-confirmed",
        "type": "cantabular_flexible_table"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
            unable to update edition-id, invalid dataset type
      """

  Scenario: Update the edition ID of a filterable dataset
    Given I am an admin user
    When I PUT "/datasets/population-estimates/editions/hello/versions/2"
      """
      {
        "edition": "hello-updated",
        "state": "associated"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
            unable to update edition-id, invalid dataset type
      """
