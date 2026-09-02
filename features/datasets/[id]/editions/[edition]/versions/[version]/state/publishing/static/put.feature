Feature: Update static version state in publishing mode

  Background:
    Given private endpoints are enabled
    And I am an admin user
    And I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-dataset-update",
          "title": "Static Dataset for Updates",
          "state": "associated",
          "type": "static"
        },
        "version": {
          "id": "static-version-update",
          "edition": "2025",
          "edition_title": "2025 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-update"
            },
            "edition": {
              "href": "/datasets/static-dataset-update/editions/2025",
              "id": "2025"
            },
            "self": {
              "href": "/datasets/static-dataset-update/editions/2025/versions/1"
            }
          },
          "version": 1,
          "release_date": "2025-01-01T09:00:00.000Z",
          "state": "associated",
          "type": "static",
          "distributions": [
            {
              "title": "csv",
              "format": "csv",
              "media_type": "text/csv",
              "download_url": "/uuid/filename.csv",
              "byte_size": 125000
            }
          ]
        }
      }
      """

    And I have these static versions:
      """
      [
        {
          "id": "static-version-2024",
          "edition": "2024",
          "edition_title": "2024 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-update"
            },
            "edition": {
              "href": "/datasets/static-dataset-update/editions/2024",
              "id": "2024"
            },
            "self": {
              "href": "/datasets/static-dataset-update/editions/2024/versions/1"
            }
          },
          "version": 1,
          "release_date": "2024-01-01T09:00:00.000Z",
          "state": "associated",
          "type": "static",
          "distributions": [
            {
              "title": "csv",
              "format": "csv",
              "media_type": "text/csv",
              "download_url": "/uuid/filename2.csv",
              "byte_size": 125000
            }
          ]
        }
      ]
      """

    And I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-dataset-retry-publish",
          "title": "Static Dataset for retry publish",
          "state": "associated",
          "type": "static",
          "links": {
            "editions": {
              "href": "/datasets/static-dataset-retry-publish/editions"
            },
            "self": {
              "href": "/datasets/static-dataset-retry-publish"
            }
          },
          "topics": [
            "economy-topic-id"
          ]
        },
        "version": {
          "id": "static-version-publish-failed",
          "edition": "2026",
          "edition_title": "2026 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-retry-publish"
            },
            "edition": {
              "href": "/datasets/static-dataset-retry-publish/editions/2026",
              "id": "2026"
            },
            "self": {
              "href": "/datasets/static-dataset-retry-publish/editions/2026/versions/1"
            },
            "version": {
              "href": "/datasets/static-dataset-retry-publish/editions/2026/versions/1",
              "id": "1"
            },
            "web_page": {
              "href": "/economy/static-dataset-retry-publish/editions/2026/versions/1"
            }
          },
          "distributions": [
            {
              "title": "Full Dataset (CSV)",
              "byte_size": 4300000,
              "download_url": "testing/test-retry.csv",
              "format": "csv",
              "media_type": "text/csv"
            }
          ],
          "version": 1,
          "release_date": "2026-02-01T09:00:00.000Z",
          "state": "publish_failed",
          "type": "static"
        }
      }
      """

    And I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-dataset-publish-mark-fail",
          "title": "Static Dataset for publish mark failure",
          "state": "associated",
          "type": "static"
        },
        "version": {
          "id": "static-version-publish-mark-fail",
          "edition": "2027",
          "edition_title": "2027 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-publish-mark-fail"
            },
            "edition": {
              "href": "/datasets/static-dataset-publish-mark-fail/editions/2027",
              "id": "2027"
            },
            "self": {
              "href": "/datasets/static-dataset-publish-mark-fail/editions/2027/versions/1"
            },
            "version": {
              "href": "/datasets/static-dataset-publish-mark-fail/editions/2027/versions/1",
              "id": "1"
            },
            "web_page": {
              "href": "/economy/static-dataset-publish-mark-fail/editions/2027/versions/1"
            }
          },
          "distributions": [
            {
              "title": "Full Dataset (CSV)",
              "byte_size": 4300000,
              "download_url": "/fail/to/mark/published.csv",
              "format": "csv",
              "media_type": "text/csv"
            }
          ],
          "version": 1,
          "release_date": "2027-02-01T09:00:00.000Z",
          "state": "approved",
          "type": "static"
        }
      }
      """

  Scenario: Update version state from associated to approved
    Given cloudflare is enabled
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
      """
      {
        "state": "approved"
      }
      """
    Then the HTTP status code should be "200"
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset-update/editions/2025/versions/1/state" should be 1
    And there are no cloudflare purge calls

  Scenario: Update version state from associated to publish_failed
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
      """
      {
        "state": "publish_failed"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
            incorrect state, can be one of the following: edition-confirmed, associated, approved or published
      """

  Scenario: Update version state from publish_failed to published
    And cloudflare is enabled
    When I PUT "/datasets/static-dataset-retry-publish/editions/2026/versions/1/state"
      """
      {
        "state": "published"
      }
      """
    Then the HTTP status code should be "200"
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset-retry-publish/editions/2026/versions/1/state" should be 1

  Scenario: Update version state from approved to published but publish fails due to the Files API
    When I PUT "/datasets/static-dataset-publish-mark-fail/editions/2027/versions/1/state"
      """
      {
        "state": "published"
      }
      """
    Then the HTTP status code should be "500"
    And I should receive the following response:
      """
            internal error: internal error
      """
    And the static version "static-version-publish-mark-fail" should have state "publish_failed"

  Scenario: Update version state from approved to published
    Given I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-dataset-publish",
          "title": "Static Dataset for Publishing",
          "state": "associated",
          "type": "static",
          "links": {
            "editions": {
              "href": "/datasets/static-dataset-publish/editions"
            },
            "self": {
              "href": "/datasets/static-dataset-publish"
            }
          },
          "topics": [
            "economy-topic-id",
            "businessindustryandtrade-topic-id"
          ]
        },
        "version": {
          "id": "static-version-approved",
          "edition": "2025",
          "edition_title": "2025 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-publish"
            },
            "edition": {
              "href": "/datasets/static-dataset-publish/editions/2025",
              "id": "2025"
            },
            "self": {
              "href": "/datasets/static-dataset-publish/editions/2025/versions/1"
            },
            "version": {
              "href": "/datasets/static-dataset-publish/editions/2025/versions/1",
              "id": "1"
            },
            "web_page": {
              "href": "/economy/static-dataset-publish/editions/2025/versions/1"
            }
          },
          "distributions": [
            {
              "title": "Full Dataset (CSV)",
              "byte_size": 4300000,
              "download_url": "testing/test.csv",
              "format": "csv",
              "media_type": "text/csv"
            }
          ],
          "version": 1,
          "release_date": "2025-02-01T09:00:00.000Z",
          "state": "approved",
          "type": "static"
        }
      }
      """
    And cloudflare is enabled
    When I PUT "/datasets/static-dataset-publish/editions/2025/versions/1/state"
      """
      {
        "state": "published"
      }
      """
    Then the HTTP status code should be "200"
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset-publish/editions/2025/versions/1/state" should be 1
    And the following URL prefixes are purged by cloudflare:
      | http://localhost:20000/economy/datasets/static-dataset-publish                        |
      | http://localhost:20000/economy/datasets/static-dataset-publish/editions               |
      | http://localhost:20000/economy/datasets/static-dataset-publish/editions/2025/versions |
      | http://localhost:23200/v1/datasets/static-dataset-publish                             |
      | http://localhost:23200/v1/datasets/static-dataset-publish/editions                    |
      | http://localhost:23200/v1/datasets/static-dataset-publish/editions/2025/versions      |

  Scenario: Update version state from associated to published
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
      """
      {
        "state": "published"
      }
      """
    Then the HTTP status code should be "400"

  Scenario: Update version state from associated to approved but distribution file is missing
    And I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-dataset-missing-file",
          "title": "Static Dataset Missing File Test",
          "state": "associated",
          "type": "static"
        },
        "version": {
          "id": "static-version-missing-file",
          "edition": "2025",
          "edition_title": "2025 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-missing-file"
            },
            "edition": {
              "href": "/datasets/static-dataset-missing-file/editions/2025",
              "id": "2025"
            },
            "self": {
              "href": "/datasets/static-dataset-missing-file/editions/2025/versions/1"
            }
          },
          "version": 1,
          "release_date": "2025-01-01T09:00:00.000Z",
          "state": "associated",
          "type": "static",
          "distributions": [
            {
              "title": "Missing File (CSV)",
              "format": "csv",
              "download_url": "datasets/test-static-dataset/editions/test-edition/missing-file.csv"
            }
          ]
        }
      }
      """
    When I PUT "/datasets/static-dataset-missing-file/editions/2025/versions/1/state"
      """
      {
        "state": "approved"
      }
      """
    Then the HTTP status code should be "422"
    And I should receive the following response:
      """
                file metadata not found
      """

  Scenario: Update version state from associated to approved
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
      """
      {
        "state": "approved"
      }
      """
    Then the HTTP status code should be "200"

  Scenario: Update version state to an invalid state
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
      """
      {
        "state": "invalid-state"
      }
      """
    Then the HTTP status code should be "400"

  Scenario: Update version state when not authorised
    Given I am not authorised
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
      """
      {
        "state": "approved"
      }
      """
    Then the HTTP status code should be "401"
