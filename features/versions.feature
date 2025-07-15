Feature: Dataset API

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
                },
                 {
                    "id": "test-static",
                    "state": "created",
                    "links": {
                      "latest_version": {
                        "id": "1",
                        "href": "/datasets/test-static/editions/test-edition-static/versions/1"
                      }
                    }
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
                },
                 {
                    "id": "test-edition-static",
                    "edition": "test-edition-static",
                    "state": "created",
                    "type":"static",
                    "links": {
                        "dataset": {
                            "id": "test-static"
                        },
                        "latest_version": {
                            "id": "1",
                            "href": "/datasets/test-static/editions/test-edition-static/versions/1"
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
                    "edition": "hello",
                    "distributions": [
                        {
                            "title": "Distribution 1",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/datasets/population-estimates/editions/hello/versions/1.csv",
                            "byte_size": 100000
                        }
                    ]
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
                    "edition": "hello",
                    "distributions": [
                        {
                            "title": "Distribution 2",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/datasets/population-estimates/editions/hello/versions/2.csv",
                            "byte_size": 100000
                        }
                    ]
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
                    "edition": "hellov2",
                    "distributions": [
                        {
                            "title": "Distribution 3",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/datasets/population-estimates/editions/hellov2/versions/3.csv",
                            "byte_size": 100000
                        }
                    ]
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
                    "lowest_geography": "ltla",
                    "distributions": [
                        {
                            "title": "Distribution 4",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/datasets/population-estimates/editions/hello/versions/4.csv",
                            "byte_size": 100000
                        }
                    ]
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
                    "edition": "2021",
                    "distributions": [
                        {
                            "title": "Distribution 1",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/datasets/test-cantabular-dataset-1/editions/2021/versions/1.csv",
                            "byte_size": 100000
                        }
                    ]
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
                    },
                    "distributions": [
                        {
                            "title": "Distribution 1",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/datasets/test-cantabular-dataset-2/editions/2021/versions/1.csv",
                            "byte_size": 100000
                        }
                    ]
                },
                {
                    "id": "test-static-version-1",
                    "version": 1,
                    "state": "created",
                    "type": "static",
                    "links": {
                        "dataset": {
                            "id": "test-static"
                        },
                        "self": {
                            "href": "/datasets/test-static/editions/test-edition-static/versions/1"
                        }
                    },
                    "edition": "test-edition-static",
                    "distributions": [
                        {
                            "title": "Distribution 1",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/datasets/test-static/editions/test-edition-static/versions/1.csv",
                            "byte_size": 100000
                        }
                    ]
                }
            ]
            """

  Scenario: GET /datasets/{id}/editions/{edition}/versions in public mode returns published versions
    And URL rewriting is enabled
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
                        "last_updated":"2021-01-01T00:00:03Z",
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
                        "lowest_geography": "ltla",
                        "distributions": [
                            {
                                "title": "Distribution 4",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "http://localhost:23600/downloads-new/datasets/population-estimates/editions/hello/versions/4.csv",
                                "byte_size": 100000
                            }
                        ]
                    },
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-1",
                        "last_updated":"2021-01-01T00:00:00Z",
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
                        },
                        "distributions": [
                            {
                                "title": "Distribution 1",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "http://localhost:23600/downloads-new/datasets/population-estimates/editions/hello/versions/1.csv",
                                "byte_size": 100000
                            }
                        ]
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """

  Scenario: GET /datasets/{id}/editions/{edition}/versions in public mode returns published versions
    When I GET "/datasets/population-estimates/editions/hello/versions"
    Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-4",
                        "last_updated":"2021-01-01T00:00:03Z",
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
                        "lowest_geography": "ltla",
                        "distributions": [
                            {
                                "title": "Distribution 4",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "/datasets/population-estimates/editions/hello/versions/4.csv",
                                "byte_size": 100000
                            }
                        ]
                    },
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-1",
                        "last_updated":"2021-01-01T00:00:00Z",
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
                        },
                        "distributions": [
                            {
                                "title": "Distribution 1",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "/datasets/population-estimates/editions/hello/versions/1.csv",
                                "byte_size": 100000
                            }
                        ]
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """

  Scenario: GET /datasets/{id}/editions/{edition}/versions in private mode returns all versions
    Given private endpoints are enabled
    And URL rewriting is enabled
    And I set the "X-Forwarded-Host" header to "api.example.com"
    And I set the "X-Forwarded-Path-Prefix" header to "v1"
    And I am identified as "user@ons.gov.uk"
    And I am authorised
    When I GET "/datasets/population-estimates/editions/hello/versions"
    Then I should receive the following JSON response with status "200":
            """
            {
                "count": 3,
                "items": [
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-4",
                        "last_updated":"2021-01-01T00:00:03Z",
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
                        "lowest_geography": "ltla",
                        "distributions": [
                            {
                                "title": "Distribution 4",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "http://localhost:23600/downloads-new/datasets/population-estimates/editions/hello/versions/4.csv",
                                "byte_size": 100000
                            }
                        ]
                    },
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-2",
                        "last_updated":"2021-01-01T00:00:01Z",
                        "version": 2,
                        "state": "associated",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "https://api.example.com/v1/datasets/population-estimates/editions/hello/versions/2"
                            }
                        },
                        "edition": "hello",
                        "distributions": [
                            {
                                "title": "Distribution 2",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "http://localhost:23600/downloads-new/datasets/population-estimates/editions/hello/versions/2.csv",
                                "byte_size": 100000
                            }
                        ]
                    },
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-1",
                        "last_updated":"2021-01-01T00:00:00Z",
                        "version": 1,
                        "state": "published",
                        "links": {
                            "dataset": {
                                "id": "population-estimates"
                            },
                            "self": {
                                "href": "https://api.example.com/v1/datasets/population-estimates/editions/hello/versions/1"
                            }
                        },
                        "edition": "hello",
                        "distributions": [
                            {
                                "title": "Distribution 1",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "http://localhost:23600/downloads-new/datasets/population-estimates/editions/hello/versions/1.csv",
                                "byte_size": 100000
                            }
                        ]
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 3
            }
            """

  Scenario: GET /datasets/{id}/editions/{edition}/versions in private mode returns all versions
    Given private endpoints are enabled
    And I am identified as "user@ons.gov.uk"
    And I am authorised
    When I GET "/datasets/population-estimates/editions/hello/versions"
    Then I should receive the following JSON response with status "200":
            """
            {
                "count": 3,
                "items": [
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-4",
                        "last_updated":"2021-01-01T00:00:03Z",
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
                        "lowest_geography": "ltla",
                        "distributions": [
                            {
                                "title": "Distribution 4",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "/datasets/population-estimates/editions/hello/versions/4.csv",
                                "byte_size": 100000
                            }
                        ]
                    },
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-2",
                        "last_updated":"2021-01-01T00:00:01Z",
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
                        "edition": "hello",
                        "distributions": [
                            {
                                "title": "Distribution 2",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "/datasets/population-estimates/editions/hello/versions/2.csv",
                                "byte_size": 100000
                            }
                        ]
                    },
                    {
                        "dataset_id": "population-estimates",
                        "id": "test-item-1",
                        "last_updated":"2021-01-01T00:00:00Z",
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
                        "edition": "hello",
                        "distributions": [
                            {
                                "title": "Distribution 1",
                                "format": "csv",
                                "media_type": "text/csv",
                                "download_url": "/datasets/population-estimates/editions/hello/versions/1.csv",
                                "byte_size": 100000
                            }
                        ]
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
    And URL rewriting is enabled
    And I set the "X-Forwarded-Host" header to "api.example.com"
    And I set the "X-Forwarded-Path-Prefix" header to "v1"
    When I GET "/datasets/population-estimates/editions/hello/versions/4"
    Then I should receive the following JSON response with status "200":
        """
        {
            "id": "test-item-4",
            "last_updated":"2021-01-01T00:00:03Z",
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
            "lowest_geography": "ltla",
            "distributions": [
                {
                    "title": "Distribution 4",
                    "format": "csv",
                    "media_type": "text/csv",
                    "download_url": "http://localhost:23600/downloads-new/datasets/population-estimates/editions/hello/versions/4.csv",
                    "byte_size": 100000
                }
            ]
        }
        """
    And the response header "ETag" should be "etag-test-item-4"

  Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} in public mode returns the version
    When I GET "/datasets/population-estimates/editions/hello/versions/4"
    Then I should receive the following JSON response with status "200":
        """
        {
            "id": "test-item-4",
            "last_updated":"2021-01-01T00:00:03Z",
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
            "lowest_geography": "ltla",
            "distributions": [
                {
                    "title": "Distribution 4",
                    "format": "csv",
                    "media_type": "text/csv",
                    "download_url": "/datasets/population-estimates/editions/hello/versions/4.csv",
                    "byte_size": 100000
                }
            ]
        }
        """
    And the response header "ETag" should be "etag-test-item-4"

  Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} in private mode returns the version
    Given private endpoints are enabled
    And URL rewriting is enabled
    And I set the "X-Forwarded-Host" header to "api.example.com"
    And I set the "X-Forwarded-Path-Prefix" header to "v1"
    And I am identified as "user@ons.gov.uk"
    And I am authorised
    When I GET "/datasets/population-estimates/editions/hello/versions/2"
    Then I should receive the following JSON response with status "200":
        """
        {
            "id": "test-item-2",
            "last_updated":"2021-01-01T00:00:01Z",
            "version": 2,
            "state": "associated",
            "links": {
                "dataset": {
                    "id": "population-estimates"
                },
                "self": {
                    "href": "https://api.example.com/v1/datasets/population-estimates/editions/hello/versions/2"
                }
            },
            "edition": "hello",
            "distributions": [
                {
                    "title": "Distribution 2",
                    "format": "csv",
                    "media_type": "text/csv",
                    "download_url": "http://localhost:23600/downloads-new/datasets/population-estimates/editions/hello/versions/2.csv",
                    "byte_size": 100000
                }
            ]
        }
        """
    And the response header "ETag" should be "etag-test-item-2"

  Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} in private mode returns the version
    Given private endpoints are enabled
    And I am identified as "user@ons.gov.uk"
    And I am authorised
    When I GET "/datasets/population-estimates/editions/hello/versions/2"
    Then I should receive the following JSON response with status "200":
        """
        {
            "id": "test-item-2",
            "last_updated":"2021-01-01T00:00:01Z",
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
            "edition": "hello",
            "distributions": [
                {
                    "title": "Distribution 2",
                    "format": "csv",
                    "media_type": "text/csv",
                    "download_url": "/datasets/population-estimates/editions/hello/versions/2.csv",
                    "byte_size": 100000
                }
            ]
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
                  "href": "/datasets/test-cantabular-dataset-2/editions/2021/versions/1"
                }
              }
            }
            """
    And these cantabular generator downloads events are produced:
      | InstanceID                | DatasetID                 | Edition | Version |FilterOutputID| Dimensions |
      | test-cantabular-version-2 | test-cantabular-dataset-2 | 2021    | 1       |              | []         |
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