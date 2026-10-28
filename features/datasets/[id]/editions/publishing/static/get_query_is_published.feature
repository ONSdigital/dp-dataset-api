Feature: List static editions in publishing mode

  Background:
    Given private endpoints are enabled
    And I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published",
          "type": "static"
        }
      ]
      """
    And I have these static versions:
      """
      [
        {
          "id": "published-version-1",
          "edition": "published",
          "version": 1,
          "release_date": "2025-03-01T07:00:00.000Z",
          "state": "published",
          "type": "static",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "published",
              "href": "/datasets/population-estimates/editions/published"
            }
          }
        },
        {
          "id": "unpublished-version-1",
          "edition": "unpublished",
          "version": 1,
          "release_date": "2025-02-01T07:00:00.000Z",
          "state": "associated",
          "type": "static",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "unpublished",
              "href": "/datasets/population-estimates/editions/unpublished"
            }
          }
        },
        {
          "id": "mixed-version-1",
          "edition": "mixed",
          "version": 1,
          "release_date": "2025-01-01T07:00:00.000Z",
          "state": "published",
          "type": "static",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "mixed",
              "href": "/datasets/population-estimates/editions/mixed"
            }
          }
        },
        {
          "id": "mixed-version-2",
          "edition": "mixed",
          "version": 2,
          "release_date": "2025-01-02T07:00:00.000Z",
          "state": "associated",
          "type": "static",
          "links": {
            "dataset": {
              "id": "population-estimates"
            },
            "edition": {
              "id": "mixed",
              "href": "/datasets/population-estimates/editions/mixed"
            }
          }
        }
      ]
      """

  Scenario: List all editions when is_published is omitted
    When I GET "/datasets/population-estimates/editions"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 3,
        "items": [
          {
            "current": {
              "edition": "published",
              "version": 1,
              "release_date": "2025-03-01T07:00:00.000Z",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "1",
                  "href": "/datasets/population-estimates/editions/published/versions/1"
                },
                "self": {
                  "id": "published",
                  "href": "/datasets/population-estimates/editions/published"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/published/versions"
                }
              }
            },
            "next": {
              "edition": "published",
              "version": 1,
              "release_date": "2025-03-01T07:00:00.000Z",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "1",
                  "href": "/datasets/population-estimates/editions/published/versions/1"
                },
                "self": {
                  "id": "published",
                  "href": "/datasets/population-estimates/editions/published"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/published/versions"
                }
              }
            }
          },
          {
            "next": {
              "edition": "unpublished",
              "version": 1,
              "release_date": "2025-02-01T07:00:00.000Z",
              "state": "associated",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "1",
                  "href": "/datasets/population-estimates/editions/unpublished/versions/1"
                },
                "self": {
                  "id": "unpublished",
                  "href": "/datasets/population-estimates/editions/unpublished"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/unpublished/versions"
                }
              }
            }
          },
          {
            "current": {
              "edition": "mixed",
              "version": 1,
              "release_date": "2025-01-01T07:00:00.000Z",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "1",
                  "href": "/datasets/population-estimates/editions/mixed/versions/1"
                },
                "self": {
                  "id": "mixed",
                  "href": "/datasets/population-estimates/editions/mixed"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/mixed/versions"
                }
              }
            },
            "next": {
              "edition": "mixed",
              "version": 2,
              "release_date": "2025-01-02T07:00:00.000Z",
              "state": "associated",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "2",
                  "href": "/datasets/population-estimates/editions/mixed/versions/2"
                },
                "self": {
                  "id": "mixed",
                  "href": "/datasets/population-estimates/editions/mixed"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/mixed/versions"
                }
              }
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 3
      }
      """

  Scenario: List editions with a published version when is_published is true
    When I GET "/datasets/population-estimates/editions?is_published=true"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 2,
        "items": [
          {
            "current": {
              "edition": "published",
              "version": 1,
              "release_date": "2025-03-01T07:00:00.000Z",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "1",
                  "href": "/datasets/population-estimates/editions/published/versions/1"
                },
                "self": {
                  "id": "published",
                  "href": "/datasets/population-estimates/editions/published"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/published/versions"
                }
              }
            },
            "next": {
              "edition": "published",
              "version": 1,
              "release_date": "2025-03-01T07:00:00.000Z",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "1",
                  "href": "/datasets/population-estimates/editions/published/versions/1"
                },
                "self": {
                  "id": "published",
                  "href": "/datasets/population-estimates/editions/published"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/published/versions"
                }
              }
            }
          },
          {
            "current": {
              "edition": "mixed",
              "version": 1,
              "release_date": "2025-01-01T07:00:00.000Z",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "1",
                  "href": "/datasets/population-estimates/editions/mixed/versions/1"
                },
                "self": {
                  "id": "mixed",
                  "href": "/datasets/population-estimates/editions/mixed"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/mixed/versions"
                }
              }
            },
            "next": {
              "edition": "mixed",
              "version": 2,
              "release_date": "2025-01-02T07:00:00.000Z",
              "state": "associated",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "2",
                  "href": "/datasets/population-estimates/editions/mixed/versions/2"
                },
                "self": {
                  "id": "mixed",
                  "href": "/datasets/population-estimates/editions/mixed"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/mixed/versions"
                }
              }
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 2
      }
      """

  Scenario: Paginate published editions while preserving the filtered total count
    When I GET "/datasets/population-estimates/editions?is_published=true&limit=1&offset=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "current": {
              "edition": "mixed",
              "version": 1,
              "release_date": "2025-01-01T07:00:00.000Z",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "1",
                  "href": "/datasets/population-estimates/editions/mixed/versions/1"
                },
                "self": {
                  "id": "mixed",
                  "href": "/datasets/population-estimates/editions/mixed"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/mixed/versions"
                }
              }
            },
            "next": {
              "edition": "mixed",
              "version": 2,
              "release_date": "2025-01-02T07:00:00.000Z",
              "state": "associated",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "2",
                  "href": "/datasets/population-estimates/editions/mixed/versions/2"
                },
                "self": {
                  "id": "mixed",
                  "href": "/datasets/population-estimates/editions/mixed"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/mixed/versions"
                }
              }
            }
          }
        ],
        "limit": 1,
        "offset": 1,
        "total_count": 2
      }
      """

  Scenario: Preserve the unpublished edition total count when the offset exceeds the results
    When I GET "/datasets/population-estimates/editions?is_published=false&limit=1&offset=1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 0,
        "items": [],
        "limit": 1,
        "offset": 1,
        "total_count": 1
      }
      """

  Scenario: List editions with no published versions when is_published is false
    When I GET "/datasets/population-estimates/editions?is_published=false"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "next": {
              "edition": "unpublished",
              "version": 1,
              "release_date": "2025-02-01T07:00:00.000Z",
              "state": "associated",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                },
                "latest_version": {
                  "id": "1",
                  "href": "/datasets/population-estimates/editions/unpublished/versions/1"
                },
                "self": {
                  "id": "unpublished",
                  "href": "/datasets/population-estimates/editions/unpublished"
                },
                "versions": {
                  "href": "/datasets/population-estimates/editions/unpublished/versions"
                }
              }
            }
          }
        ],
        "limit": 20,
        "offset": 0,
        "total_count": 1
      }
      """
