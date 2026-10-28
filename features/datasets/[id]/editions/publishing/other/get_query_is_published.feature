Feature: List editions in publishing mode

  Background:
    Given private endpoints are enabled
    And I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published",
          "type": "filterable"
        }
      ]
      """
    And I have these realistic editions:
      """
      [
        {
          "id": "1",
          "current": {
            "edition": "published",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          },
          "next": {
            "edition": "published",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          }
        },
        {
          "id": "2",
          "next": {
            "edition": "unpublished",
            "state": "edition-confirmed",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          }
        },
        {
          "id": "3",
          "current": {
            "edition": "mixed",
            "state": "published",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
            }
          },
          "next": {
            "edition": "mixed",
            "state": "edition-confirmed",
            "links": {
              "dataset": {
                "id": "population-estimates"
              }
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
            "id": "1",
            "current": {
              "edition": "published",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                }
              }
            },
            "next": {
              "edition": "published",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                }
              }
            }
          },
          {
            "id": "2",
            "next": {
              "edition": "unpublished",
              "state": "edition-confirmed",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                }
              }
            }
          },
          {
            "id": "3",
            "current": {
              "edition": "mixed",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                }
              }
            },
            "next": {
              "edition": "mixed",
              "state": "edition-confirmed",
              "links": {
                "dataset": {
                  "id": "population-estimates"
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

  Scenario: List editions with a published current when is_published is true
    When I GET "/datasets/population-estimates/editions?is_published=true"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 2,
        "items": [
          {
            "id": "1",
            "current": {
              "edition": "published",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                }
              }
            },
            "next": {
              "edition": "published",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                }
              }
            }
          },
          {
            "id": "3",
            "current": {
              "edition": "mixed",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                }
              }
            },
            "next": {
              "edition": "mixed",
              "state": "edition-confirmed",
              "links": {
                "dataset": {
                  "id": "population-estimates"
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
            "id": "3",
            "current": {
              "edition": "mixed",
              "state": "published",
              "links": {
                "dataset": {
                  "id": "population-estimates"
                }
              }
            },
            "next": {
              "edition": "mixed",
              "state": "edition-confirmed",
              "links": {
                "dataset": {
                  "id": "population-estimates"
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

  Scenario: List editions with no published current when is_published is false
    When I GET "/datasets/population-estimates/editions?is_published=false"
    Then I should receive the following JSON response with status "200":
      """
      {
        "count": 1,
        "items": [
          {
            "id": "2",
            "next": {
              "edition": "unpublished",
              "state": "edition-confirmed",
              "links": {
                "dataset": {
                  "id": "population-estimates"
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

  Scenario: Reject an invalid is_published parameter
    When I GET "/datasets/population-estimates/editions?is_published=invalid"
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      invalid query parameter
      """
