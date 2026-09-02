Feature: Get version metadata in web mode

  Scenario: Get metadata for a Cantabular flexible table dataset
    Given I have these datasets:
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

  Scenario: Get metadata for a published Cantabular flexible table dataset
    Given I have these datasets:
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

  Scenario: Get metadata for an unpublished Cantabular flexible table dataset
    Given I have these datasets:
      """
      [
        {
          "id": "cantabular-flexible-table",
          "title": "title",
          "description": "description",
          "state": "associated",
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
          "state": "associated",
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
          "state": "associated",
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
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dataset not found
      """

  Scenario: Get metadata for an unpublished dataset
    Given I have these datasets:
      """
      [
        {
          "id": "unpublished-dataset",
          "state": "created"
        }
      ]
      """
    When I GET "/datasets/unpublished-dataset/editions/2023/versions/1/metadata"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dataset not found
      """

  Scenario: Get metadata for an unpublished edition
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
          "id": "unpublished-edition",
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
    When I GET "/datasets/population-estimates/editions/2023/versions/1/metadata"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            edition not found
      """

  Scenario: Get metadata for an unpublished version
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
          "id": "unpublished-version",
          "version": 1,
          "state": "created",
          "release_date": "2023-01-01T00:00:00.000Z",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "test-edition-1"
            },
            "self": {
              "href": "/datasets/population-estimates/editions/2023/versions/1"
            }
          }
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions/2023/versions/1/metadata"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            version not found
      """
