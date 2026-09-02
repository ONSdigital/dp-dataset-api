Feature: Update a version

  Background:
    Given private endpoints are enabled
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

  Scenario: Update a version
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "state": "approved",
        "type": "static"
      }
      """
    Then the HTTP status code should be "200"
    And I should receive the following JSON response:
      """
      {
        "dataset_id": "static-dataset-update",
        "distributions": [
          {
            "byte_size": 125000,
            "download_url": "/uuid/filename.csv",
            "format": "csv",
            "media_type": "text/csv",
            "title": "csv"
          }
        ],
        "edition": "2025",
        "id": "static-version-update",
        "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
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
        "release_date": "2025-01-01T09:00:00.000Z",
        "state": "approved",
        "type": "static"
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset-update/editions/2025/versions/1" should be 1

  Scenario: Update a version as a publisher
    Given I am a publisher user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "state": "approved",
        "type": "static"
      }
      """
    Then the HTTP status code should be "200"
    And I should receive the following JSON response:
      """
      {
        "dataset_id": "static-dataset-update",
        "distributions": [
          {
            "byte_size": 125000,
            "download_url": "/uuid/filename.csv",
            "format": "csv",
            "media_type": "text/csv",
            "title": "csv"
          }
        ],
        "edition": "2025",
        "id": "static-version-update",
        "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
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
        "release_date": "2025-01-01T09:00:00.000Z",
        "state": "approved",
        "type": "static"
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset-update/editions/2025/versions/1" should be 1

  Scenario: Update a version's metadata
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "release_date": "2025-03-01T09:00:00.000Z",
        "edition_title": "Updated 2025 Edition",
        "quality_designation": "no-accreditation",
        "state": "approved",
        "type": "static"
      }
      """
    Then I should receive the following JSON response with status "200":
      """
      {
        "dataset_id": "static-dataset-update",
        "distributions": [
          {
            "byte_size": 125000,
            "download_url": "/uuid/filename.csv",
            "format": "csv",
            "media_type": "text/csv",
            "title": "csv"
          }
        ],
        "edition": "2025",
        "edition_title": "Updated 2025 Edition",
        "id": "static-version-update",
        "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
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
        "quality_designation": "no-accreditation",
        "release_date": "2025-03-01T09:00:00.000Z",
        "state": "approved",
        "type": "static"
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset-update/editions/2025/versions/1" should be 1

  Scenario: Update a version's distributions
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "distributions": [
          {
            "title": "updated csv",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/uuid/filename-updated.csv",
            "byte_size": 150000
          },
          {
            "title": "xlsx",
            "format": "xlsx",
            "media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "download_url": "/uuid/filename.xlsx",
            "byte_size": 175000
          }
        ],
        "type": "static"
      }
      """
    Then the HTTP status code should be "200"
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset-update/editions/2025/versions/1" should be 1

  Scenario: Update a version's edition
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "edition": "2025-revised",
        "edition_title": "2025 Revised Edition",
        "type": "static"
      }
      """
    Then the HTTP status code should be "200"
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset-update/editions/2025/versions/1" should be 1

  Scenario: Update a version that does not exist
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/999"
      """
      {
        "state": "approved",
        "type": "static"
      }
      """
    Then the HTTP status code should be "404"
    And the total number of audit events should be 0
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset-update/editions/2025/versions/999" should be 0

  Scenario: Update a version for a dataset that does not exist
    Given I am an admin user
    When I PUT "/datasets/non-existent/editions/2025/versions/1"
      """
      {
        "state": "approved",
        "type": "static"
      }
      """
    Then the HTTP status code should be "404"

  Scenario: Update a version without authorisation
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "state": "approved",
        "type": "static"
      }
      """
    Then the HTTP status code should be "401"

  Scenario: Update a version's edition to an existing edition
    Given I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-dataset-conflict",
          "title": "Static Dataset Conflict Test",
          "state": "associated",
          "type": "static"
        },
        "version": {
          "id": "static-version-conflict",
          "edition": "2025",
          "edition_title": "2025 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-conflict"
            },
            "edition": {
              "href": "/datasets/static-dataset-conflict/editions/2025",
              "id": "2025"
            },
            "self": {
              "href": "/datasets/static-dataset-conflict/editions/2025/versions/1"
            }
          },
          "version": 1,
          "release_date": "2025-01-01T09:00:00.000Z",
          "state": "associated",
          "type": "static"
        }
      }
      """
    And I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-dataset-conflict",
          "title": "Static Dataset Conflict Test",
          "state": "associated",
          "type": "static"
        },
        "version": {
          "id": "static-version-existing",
          "edition": "existing-edition",
          "edition_title": "Existing Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-conflict"
            },
            "edition": {
              "href": "/datasets/static-dataset-conflict/editions/existing-edition",
              "id": "existing-edition"
            },
            "self": {
              "href": "/datasets/static-dataset-conflict/editions/existing-edition/versions/1"
            }
          },
          "version": 1,
          "release_date": "2025-01-01T09:00:00.000Z",
          "state": "associated",
          "type": "static"
        }
      }
      """
    And I am an admin user
    When I PUT "/datasets/static-dataset-conflict/editions/2025/versions/1"
      """
      {
        "edition": "existing-edition",
        "type": "static"
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
            the edition already exists
      """

  Scenario: Update a version's edition
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "edition": "2025-new-edition",
        "edition_title": "2025 New Edition",
        "type": "static"
      }
      """
    Then the HTTP status code should be "200"

  Scenario: Update a version's edition to a unique value within the series
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "edition": "2026",
        "type": "static"
      }
      """
    Then the HTTP status code should be "200"

  Scenario: Update a version's edition title to an existing value within the same series
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "edition_title": "2024 Edition",
        "type": "static"
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
            the edition-title already exists
      """

  Scenario: Update a version's edition to an existing value within the same series
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "edition": "2024",
        "edition_title": "Different Title",
        "type": "static"
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
            the edition already exists
      """

  Scenario: Update a version's edition and title to unique values within the series
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "edition": "2026",
        "edition_title": "Unique 2026 Edition",
        "type": "static"
      }
      """
    Then the HTTP status code should be "200"

  Scenario: Update a version's edition and title to existing values within the same series
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "edition": "2024",
        "edition_title": "2024 Edition",
        "type": "static"
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
            the edition already exists
      """

  Scenario: Update a version's edition title to one that already exists but edition ID is unique
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "edition": "2026",
        "edition_title": "2024 Edition",
        "type": "static"
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
            the edition-title already exists
      """

  Scenario: Update a version's distributions with valid formats
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "distributions": [
          {
            "title": "Full Dataset (CSV)",
            "download_url": "/uuid/filename.csv",
            "byte_size": 4300000,
            "format": "csv"
          }
        ],
        "quality_designation": "accredited-official",
        "release_date": "2025-03-06T14:49:23.354Z",
        "type": "static",
        "edition": "2025",
        "dataset_id": "static-dataset-update",
        "usage_notes": [
          {
            "title": "This dataset",
            "note": "Please use it wisely"
          }
        ]
      }
      """
    Then the HTTP status code should be "200"

  Scenario: Update a version's distributions with a missing format field
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "distributions": [
          {
            "title": "Full Dataset (CSV)",
            "download_url": "/uuid/filename.csv",
            "byte_size": 4300000
          }
        ],
        "quality_designation": "accredited-official",
        "release_date": "2025-03-06T14:49:23.354Z",
        "type": "static",
        "edition": "march",
        "dataset_id": "test-static-dataset",
        "usage_notes": [
          {
            "title": "This dataset",
            "note": "Please use it wisely"
          }
        ]
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
            distributions[0].format field is missing
      """

  Scenario: Update a version's distributions with an invalid format field
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "distributions": [
          {
            "title": "Full Dataset (CSV)",
            "download_url": "/uuid/filename.csv",
            "byte_size": 4300000,
            "format": "INVALID"
          }
        ],
        "quality_designation": "accredited-official",
        "release_date": "2025-03-06T14:49:23.354Z",
        "type": "static",
        "edition": "march",
        "dataset_id": "test-static-dataset",
        "usage_notes": [
          {
            "title": "This dataset",
            "note": "Please use it wisely"
          }
        ]
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
            distributions[0].format field is invalid
      """

  Scenario: Update a version's edition field to one containing spaces
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "state": "approved",
        "type": "static",
        "edition": "edition id with spaces"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
            spaces are not allowed in the ID field
      """

  Scenario: Update a version's datasetID field to one containing spaces
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "state": "approved",
        "type": "static",
        "dataset_id": "dataset id with spaces"
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
            spaces are not allowed in the ID field
      """

  Scenario: Update a version to include the is_migration field
    Given I am an admin user
    When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
      """
      {
        "is_migration": true,
        "type": "static"
      }
      """
    Then I should receive the following JSON response with status "200":
      """
      {
        "dataset_id": "static-dataset-update",
        "distributions": [
          {
            "byte_size": 125000,
            "download_url": "/uuid/filename.csv",
            "format": "csv",
            "media_type": "text/csv",
            "title": "csv"
          }
        ],
        "edition": "2025",
        "id": "static-version-update",
        "is_migration": true,
        "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
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
        "release_date": "2025-01-01T09:00:00.000Z",
        "state": "associated",
        "type": "static"
      }
      """

  Scenario: Update a migrated version's edition ID
    Given I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "migrated-dataset",
          "title": "Migrated Dataset",
          "state": "associated",
          "type": "static"
        },
        "version": {
          "id": "migrated-version",
          "edition": "original-edition",
          "edition_title": "Original Edition",
          "links": {
            "dataset": {
              "id": "migrated-dataset"
            },
            "edition": {
              "href": "/datasets/migrated-dataset/editions/original-edition",
              "id": "original-edition"
            },
            "self": {
              "href": "/datasets/migrated-dataset/editions/original-edition/versions/1"
            }
          },
          "version": 1,
          "release_date": "2025-01-01T09:00:00.000Z",
          "state": "associated",
          "type": "static",
          "is_migration": true,
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
    And I am an admin user
    When I PUT "/datasets/migrated-dataset/editions/original-edition/versions/1"
      """
      {
        "edition": "changed-edition",
        "edition_title": "Changed Edition",
        "type": "static"
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
            cannot change the edition ID for a migrated edition
      """

  Scenario: Updating a version's edition ID should update all associated links
    Given I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "edition-change-dataset",
          "title": "Change edition in version link test",
          "state": "associated",
          "type": "static",
          "topics": [
            "businessindustryandtrade-topic-id"
          ]
        },
        "version": {
          "id": "static-version-webpage-link",
          "edition": "2025-links",
          "edition_title": "2025 Edition links",
          "links": {
            "dataset": {
              "href": "/datasets/edition-change-dataset",
              "id": "edition-change-dataset"
            },
            "edition": {
              "href": "/datasets/edition-change-dataset/editions/2025-links",
              "id": "2025-links"
            },
            "self": {
              "href": "/datasets/edition-change-dataset/editions/2025-links/versions/1"
            },
            "version": {
              "href": "/datasets/edition-change-dataset/editions/2025-links/versions/1",
              "id": "1"
            },
            "web_page": {
              "href": "/businessindustryandtrade/datasets/edition-change-dataset/editions/2025-links/versions/1"
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
    And I am an admin user
    When I PUT "/datasets/edition-change-dataset/editions/2025-links/versions/1"
      """
      {
        "edition": "2026-update",
        "edition_title": "2026 Edition",
        "type": "static",
        "state": "associated"
      }
      """
    Then I should receive the following JSON response with status "200":
      """
      {
        "dataset_id": "edition-change-dataset",
        "distributions": [
          {
            "byte_size": 125000,
            "download_url": "/uuid/filename.csv",
            "format": "csv",
            "media_type": "text/csv",
            "title": "csv"
          }
        ],
        "edition": "2026-update",
        "edition_title": "2026 Edition",
        "id": "static-version-webpage-link",
        "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
        "links": {
          "dataset": {
            "href": "/datasets/edition-change-dataset",
            "id": "edition-change-dataset"
          },
          "edition": {
            "href": "/datasets/edition-change-dataset/editions/2026-update",
            "id": "2026-update"
          },
          "self": {
            "href": "/datasets/edition-change-dataset/editions/2026-update/versions/1"
          },
          "web_page": {
            "href": "/businessindustryandtrade/datasets/edition-change-dataset/editions/2026-update/versions/1"
          }
        },
        "previous_edition_id": [
          "2025-links"
        ],
        "release_date": "2025-01-01T09:00:00.000Z",
        "state": "associated",
        "type": "static"
      }
      """
    And the dataset "edition-change-dataset" should have latest_version href "/datasets/edition-change-dataset/editions/2026-update/versions/1"

  Scenario: Updating a version's edition should save the previous edition ID
    Given I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "previous-edition-dataset",
          "title": "Previous edition saved",
          "state": "associated",
          "type": "static",
          "topics": [
            "businessindustryandtrade-topic-id"
          ]
        },
        "version": {
          "id": "static-dataset-previous-edition",
          "edition": "old-edition",
          "edition_title": "2025 Edition",
          "links": {
            "dataset": {
              "href": "/datasets/previous-edition-dataset",
              "id": "previous-edition-dataset"
            },
            "edition": {
              "href": "/datasets/previous-edition-dataset/editions/old-edition",
              "id": "old-edition"
            },
            "self": {
              "href": "/datasets/previous-edition-dataset/editions/old-edition/versions/1"
            },
            "version": {
              "href": "/datasets/previous-edition-dataset/editions/old-edition/versions/1",
              "id": "1"
            },
            "web_page": {
              "href": "/businessindustryandtrade/datasets/previous-edition-dataset/editions/old-edition/versions/1"
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
    And I am an admin user
    When I PUT "/datasets/previous-edition-dataset/editions/old-edition/versions/1"
      """
      {
        "edition": "new-edition",
        "edition_title": "2026 Edition",
        "state": "associated",
        "type": "static"
      }
      """
    Then I should receive the following JSON response with status "200":
      """
      {
        "dataset_id": "previous-edition-dataset",
        "distributions": [
          {
            "byte_size": 125000,
            "download_url": "/uuid/filename.csv",
            "format": "csv",
            "media_type": "text/csv",
            "title": "csv"
          }
        ],
        "edition": "new-edition",
        "edition_title": "2026 Edition",
        "id": "static-dataset-previous-edition",
        "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
        "links": {
          "dataset": {
            "href": "/datasets/previous-edition-dataset",
            "id": "previous-edition-dataset"
          },
          "edition": {
            "href": "/datasets/previous-edition-dataset/editions/new-edition",
            "id": "new-edition"
          },
          "self": {
            "href": "/datasets/previous-edition-dataset/editions/new-edition/versions/1"
          },
          "web_page": {
            "href": "/businessindustryandtrade/datasets/previous-edition-dataset/editions/new-edition/versions/1"
          }
        },
        "previous_edition_id": [
          "old-edition"
        ],
        "release_date": "2025-01-01T09:00:00.000Z",
        "state": "associated",
        "type": "static"
      }
      """
