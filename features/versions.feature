Feature: Dataset API

  Background: we have a dataset which has an edition with a variety of versions
    Given I have these datasets:
            """
            [
                {
                    "id": "test-cantabular-dataset-1",
                    "type": "cantabular_table",
                    "state": "edition-confirmed"
                },
                {
                    "id": "test-cantabular-dataset-2",
                    "type": "cantabular_table",
                    "state": "associated",
                    "links": {
                      "latest_version": {
                        "id": "1",
                        "href": "someurl"
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
                    "type": "cantabular_table",
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
                    "type": "cantabular_table",
                    "links": {
                        "dataset": {
                            "id": "test-cantabular-dataset-2"
                        },
                        "latest_version": {
                            "id": "1",
                            "href": "someurl"
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
                            "href": "someurl"
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
                            "href": "someurl"
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
                            "href": "someurl"
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
                            "href": "someurl"
                        }
                    },
                    "edition": "hello",
                    "lowest_geography": "ltla"
                },
                {
                    "id": "test-cantabular-version-1",
                    "version": 1,
                    "state": "edition-confirmed",
                    "type": "cantabular_table",
                    "links": {
                        "dataset": {
                            "id": "test-cantabular-dataset-1"
                        },
                        "self": {
                            "href": "someurl"
                        }
                    },
                    "edition": "2021"
                },
                {
                    "id": "test-cantabular-version-2",
                    "version": 1,
                    "state": "associated",
                    "type": "cantabular_table",
                    "links": {
                        "dataset": {
                            "id": "test-cantabular-dataset-2"
                        },
                        "self": {
                            "href": "someurl"
                        }
                    },
                    "edition": "2021",
                    "downloads": {
                      "csv": {
                        "public": "",
                        "size": "1",
                        "href": "someurl"
                      }
                    }
                }
            ]
            """

  Scenario: GET /datasets/{id}/editions/{edition}/versions in public mode returns published versions
    When I GET "/datasets/population-estimates/editions/hello/versions" without a request host
    Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-4",
                        "version": 4,
                        "edition": "hello",
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "http://localhost:22000/someurl"
                            }
                        },
                        "lowest_geography": "ltla"
                    },
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-1",
                        "version": 1,
                        "edition": "hello",
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "http://localhost:22000/someurl"
                            }
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """


  Scenario: GET /datasets/{id}/editions/{edition}/versions in private mode returns all versions
    Given private endpoints are enabled
    And I am identified as "user@ons.gov.uk"
    And I am authorised
    When I GET "/datasets/population-estimates/editions/hello/versions" without a request host
    Then I should receive the following JSON response with status "200":
            """
            {
                "count": 3,
                "items": [
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-4",
                        "version": 4,
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "http://localhost:22000/someurl"
                            }
                        },
                        "edition": "hello",
                        "lowest_geography": "ltla"
                    },
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-2",
                        "version": 2,
                        "state": "associated",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "http://localhost:22000/someurl"
                            }
                        },
                        "edition": "hello"
                    },
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-1",
                        "version": 1,
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "http://localhost:22000/someurl"
                            }
                        },
                        "edition": "hello"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 3
            }
            """

  Scenario: GET versions for unknown dataset returns not found error
    When I GET "/datasets/unknown-dataset/editions/hello/versions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
            """
            dataset not found
            """

  Scenario: GET versions for unknown edition returns not found error
    When I GET "/datasets/population-estimates/editions/unknown/versions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
            """
            edition not found
            """

  Scenario: GET versions for edition with no versions returns not found error
    When I GET "/datasets/population-estimates/editions/edition-with-no-versions/versions"
    Then the HTTP status code should be "404"
    And I should receive the following response:
            """
            version not found
            """

  Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} in public mode returns the version
    When I GET "/datasets/population-estimates/editions/hello/versions/4" without a request host
    Then I should receive the following JSON response with status "200":
        """
        {
            "id": "test-item-4",
            "version": 4,
            "state": "published",
            "links": {
                "dataset": {
                    "id": "population-estimates"
                },
                "self": {
                    "href": "http://localhost:22000/someurl"
                }
            },
            "edition": "hello",
            "lowest_geography": "ltla"
        }
        """
    And the response header "ETag" should be "etag-test-item-4"

  Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} in private mode returns the version
    Given private endpoints are enabled
    And I am identified as "user@ons.gov.uk"
    And I am authorised
    When I GET "/datasets/population-estimates/editions/hello/versions/2" without a request host
    Then I should receive the following JSON response with status "200":
        """
        {
            "id": "test-item-2",
            "version": 2,
            "state": "associated",
            "links": {
                "dataset": {
                    "id": "population-estimates"
                },
                "self": {
                    "href": "http://localhost:22000/someurl"
                }
            },
            "edition": "hello"
        }
        """
    And the response header "ETag" should be "etag-test-item-2"

  Scenario: PUT versions for CMD dataset produces Kafka event and returns OK
    Given private endpoints are enabled
    And I am identified as "user@ons.gov.uk"
    And I am authorised
    And I have a real kafka container with topic "filter-job-submitted"
    When I PUT "/datasets/population-estimates/editions/hellov2/versions/3"
            """
            {
              "instance_id":"test-item-3",
              "license":"ONS",
              "release_date":"2017-04-04",
              "state":"associated",
              "collection_id":"bla"
            }
            """
    And these generate downloads events are produced:
      | InstanceID  | DatasetID            | Edition | Version | FilterOutputID |
      | test-item-3 | population-estimates | hellov2 | 3       |                |
    Then the HTTP status code should be "200"


  Scenario: PUT versions for Cantabular dataset produces Kafka event and returns OK
    Given private endpoints are enabled
    And I am identified as "user@ons.gov.uk"
    And I am authorised
    And I have a real kafka container with topic "cantabular-export-start"
    When I PUT "/datasets/test-cantabular-dataset-1/editions/2021/versions/1"
            """
            {
              "instance_id":"test-cantabular-version-1",
              "license":"ONS",
              "release_date":"2017-04-04",
              "state":"associated",
              "collection_id":"bla"
            }
            """
    And these cantabular generator downloads events are produced:
      | InstanceID                | DatasetID                 | Edition | Version |FilterOutputID| Dimensions |
      | test-cantabular-version-1 | test-cantabular-dataset-1 | 2021    | 1       |              | []         |
    Then the HTTP status code should be "200"


  Scenario: PUT published version for Cantabular dataset produces Kafka event and returns OK
    Given private endpoints are enabled
    And I am identified as "user@ons.gov.uk"
    And I am authorised
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
                  "href": "someurl"
                }
              }
            }
            """
    And these cantabular generator downloads events are produced:
      | InstanceID                | DatasetID                 | Edition | Version |FilterOutputID| Dimensions |
      | test-cantabular-version-2 | test-cantabular-dataset-2 | 2021    | 1       |              | []         |
    Then the HTTP status code should be "200"
