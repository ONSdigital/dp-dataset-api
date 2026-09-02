Feature: Get metadata

  Background:
    Given private endpoints are enabled
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "canonical_topic": "canonical-topic-ID",
          "subtopics": [
            "subtopic-ID"
          ],
          "state": "associated"
        },
        {
          "id": "published-dataset",
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
          "edition": "2023",
          "state": "published",
          "links": {
            "dataset": {
              "id": "published-dataset"
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
          "state": "associated",
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
          "version": 1,
          "state": "associated",
          "links": {
            "dataset": {
              "id": "published-dataset"
            },
            "self": {
              "href": "/datasets/published-dataset/editions/2023/versions/1"
            }
          },
          "edition": "2023"
        },
        {
          "id": "test-item-3",
          "version": 2,
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/hello/versions/2"
            }
          },
          "edition": "hello"
        }
      ]
      """

  Scenario: Get metadata for a published version
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "title": "title",
          "description": "description",
          "canonical_topic": "canonical-topic-ID",
          "subtopics": [
            "subtopic-ID"
          ],
          "state": "published",
          "next_release": "2022",
          "contacts": [
            {
              "name": "name 1",
              "email": "name@example.com",
              "telephone": "01234 567890"
            }
          ],
          "publisher": {
            "name": "name",
            "type": "type"
          },
          "keywords": [
            "population",
            "estimates"
          ],
          "license": "license",
          "unit_of_measure": "people",
          "uri": "http://example.com/population-estimates",
          "theme": "population"
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "test-edition-1",
          "edition": "2023",
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
          "id": "test-version-1",
          "version": 1,
          "state": "published",
          "release_date": "2023-01-01T00:00:00.000Z",
          "dimensions": [
            {
              "name": "geography",
              "label": "label",
              "description": "description"
            },
            {
              "name": "age",
              "label": "label",
              "description": "description"
            }
          ],
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "2023"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/2023/versions/1"
            },
            "version": {
              "href": "/datasets/population-estimates/editions/2023/versions/1",
              "id": "1"
            }
          },
          "downloads": {
            "csv": {
              "href": "http://download-service/population-estimates.csv",
              "size": "1000"
            },
            "xlsx": {
              "href": "http://download-service/population-estimates.xlsx",
              "size": "2000"
            }
          },
          "edition": "2023"
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions/2023/versions/1/metadata"
    Then I should receive the following JSON response with status "200":
      """
      {
        "canonical_topic": "canonical-topic-ID",
        "subtopics": [
          "subtopic-ID"
        ],
        "contacts": [
          {
            "email": "name@example.com",
            "name": "name 1",
            "telephone": "01234 567890"
          }
        ],
        "description": "description",
        "dimensions": [
          {
            "description": "description",
            "label": "label",
            "links": {
              "code_list": {
              },
              "options": {
              },
              "version": {
              }
            },
            "name": "geography"
          },
          {
            "description": "description",
            "label": "label",
            "links": {
              "code_list": {
              },
              "options": {
              },
              "version": {
              }
            },
            "name": "age"
          }
        ],
        "distribution": [
          "json",
          "csv"
        ],
        "downloads": {
          "csv": {
            "href": "http://download-service/population-estimates.csv",
            "size": "1000"
          },
          "xlsx": {
            "href": "http://download-service/population-estimates.xlsx",
            "size": "2000"
          }
        },
        "edition": "2023",
        "id": "population-estimates",
        "keywords": [
          "population",
          "estimates"
        ],
        "last_updated": "0001-01-01T00:00:00Z",
        "license": "license",
        "links": {
          "self": {
            "href": "/datasets/population-estimates/editions/2023/versions/1/metadata"
          },
          "version": {
            "href": "/datasets/population-estimates/editions/2023/versions/1"
          },
          "website_version": {
            "href": "http://localhost:20000/datasets/population-estimates/editions/2023/versions/1"
          }
        },
        "next_release": "2022",
        "publisher": {
          "name": "name",
          "type": "type"
        },
        "release_date": "2023-01-01T00:00:00.000Z",
        "theme": "population",
        "title": "title",
        "unit_of_measure": "people",
        "uri": "http://example.com/population-estimates",
        "version": 1,
        "state": "published"
      }
      """

  Scenario: Get metadata for an unpublished version
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "title": "title",
          "description": "description",
          "state": "associated",
          "next_release": "2022",
          "contacts": [
            {
              "name": "name 1",
              "email": "name@example.com",
              "telephone": "01234 567890"
            }
          ]
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "test-edition-1",
          "edition": "2023",
          "state": "associated",
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
          "id": "test-version-1",
          "version": 1,
          "state": "associated",
          "release_date": "2023-01-01T00:00:00.000Z",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "2023"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/2023/versions/1"
            },
            "version": {
              "href": "/datasets/population-estimates/editions/2023/versions/1",
              "id": "1"
            }
          },
          "edition": "2023"
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions/2023/versions/1/metadata"
    Then I should receive the following JSON response with status "200":
      """
      {
        "contacts": [
          {
            "name": "name 1",
            "email": "name@example.com",
            "telephone": "01234 567890"
          }
        ],
        "description": "description",
        "distribution": [
          "json"
        ],
        "edition": "2023",
        "id": "population-estimates",
        "last_updated": "0001-01-01T00:00:00Z",
        "links": {
          "self": {
            "href": "/datasets/population-estimates/editions/2023/versions/1/metadata"
          },
          "version": {
            "href": "/datasets/population-estimates/editions/2023/versions/1"
          },
          "website_version": {
            "href": "http://localhost:20000/datasets/population-estimates/editions/2023/versions/1"
          }
        },
        "next_release": "2022",
        "release_date": "2023-01-01T00:00:00.000Z",
        "title": "title",
        "version": 1,
        "state": "associated"
      }
      """

  Scenario: Get metadata for an unpublished version as a publisher
    Given I am a publisher user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "title": "title",
          "description": "description",
          "state": "associated",
          "next_release": "2022",
          "contacts": [
            {
              "name": "name 1",
              "email": "name@example.com",
              "telephone": "01234 567890"
            }
          ]
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "test-edition-1",
          "edition": "2023",
          "state": "associated",
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
          "id": "test-version-1",
          "version": 1,
          "state": "associated",
          "release_date": "2023-01-01T00:00:00.000Z",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "2023"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/2023/versions/1"
            },
            "version": {
              "href": "/datasets/population-estimates/editions/2023/versions/1",
              "id": "1"
            }
          },
          "edition": "2023"
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions/2023/versions/1/metadata"
    Then I should receive the following JSON response with status "200":
      """
      {
        "contacts": [
          {
            "name": "name 1",
            "email": "name@example.com",
            "telephone": "01234 567890"
          }
        ],
        "description": "description",
        "distribution": [
          "json"
        ],
        "edition": "2023",
        "id": "population-estimates",
        "last_updated": "0001-01-01T00:00:00Z",
        "links": {
          "self": {
            "href": "/datasets/population-estimates/editions/2023/versions/1/metadata"
          },
          "version": {
            "href": "/datasets/population-estimates/editions/2023/versions/1"
          },
          "website_version": {
            "href": "http://localhost:20000/datasets/population-estimates/editions/2023/versions/1"
          }
        },
        "next_release": "2022",
        "release_date": "2023-01-01T00:00:00.000Z",
        "title": "title",
        "version": 1,
        "state": "associated"
      }
      """

  Scenario: Get metadata for a dataset that does not exist
    Given I am an admin user
    When I GET "/datasets/non-existent-dataset/editions/2023/versions/1/metadata"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dataset not found
      """

  Scenario: Get metadata for an edition that does not exist
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published"
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions/non-existent-edition/versions/1/metadata"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            edition not found
      """

  Scenario: Get metadata for a version that does not exist
    Given I am an admin user
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
          "id": "test-edition-1",
          "edition": "2023",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions/2023/versions/999/metadata"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            version not found
      """

  Scenario: Get metadata for a Cantabular Flexible Table dataset
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "cantabular-flexible-table",
          "title": "title",
          "description": "description",
          "state": "published",
          "type": "cantabular_flexible_table",
          "uri": "http://example.com/cantabular-flexible-table",
          "is_based_on": {
            "id": "cpih01",
            "@type": "cantabular_flexible_table"
          },
          "links": {
            "self": {
              "href": "/datasets/cantabular-flexible-table",
              "id": "cantabular-flexible-table"
            }
          }
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "cantabular-edition-1",
          "edition": "2023",
          "state": "published",
          "links": {
            "dataset": {
              "id": "cantabular-flexible-table"
            }
          }
        }
      ]
      """
    And I have these versions:
      """
      [
        {
          "id": "cantabular-version-1",
          "version": 1,
          "state": "published",
          "release_date": "2023-01-01T00:00:00.000Z",
          "dimensions": [
            {
              "name": "region",
              "label": "region",
              "description": "region"
            },
            {
              "name": "age",
              "label": "label",
              "description": "description"
            }
          ],
          "downloads": {
            "csv": {
              "href": "http://download-service/cantabular-data.csv",
              "size": "5000"
            }
          },
          "links": {
            "dataset": {
              "id": "cantabular-flexible-table"
            },
            "edition": {
              "id": "2023"
            },
            "self": {
              "href": "/datasets/cantabular-flexible-table/editions/2023/versions/1"
            },
            "version": {
              "href": "/datasets/cantabular-flexible-table/editions/2023/versions/1",
              "id": "1"
            }
          },
          "edition": "2023"
        }
      ]
      """
    When I GET "/datasets/cantabular-flexible-table/editions/2023/versions/1/metadata"
    Then I should receive the following JSON response with status "200":
      """
      {
        "dataset_links": {
          "self": {
            "href": "/datasets/cantabular-flexible-table",
            "id": "cantabular-flexible-table"
          }
        },
        "description": "description",
        "dimensions": [
          {
            "description": "region",
            "label": "region",
            "links": {
              "code_list": {
              },
              "options": {
              },
              "version": {
              }
            },
            "name": "region"
          },
          {
            "description": "description",
            "label": "label",
            "links": {
              "code_list": {
              },
              "options": {
              },
              "version": {
              }
            },
            "name": "age"
          }
        ],
        "distribution": [
          "json",
          "csv"
        ],
        "downloads": {
          "csv": {
            "href": "http://download-service/cantabular-data.csv",
            "size": "5000"
          }
        },
        "is_based_on": {
          "@id": "",
          "@type": "cantabular_flexible_table"
        },
        "last_updated": "0001-01-01T00:00:00Z",
        "release_date": "2023-01-01T00:00:00.000Z",
        "title": "title",
        "uri": "http://example.com/cantabular-flexible-table",
        "version": 1,
        "state": "published"
      }
      """
