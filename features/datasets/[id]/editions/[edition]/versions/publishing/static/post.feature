Feature: Create static version in publishing mode

  Background:
    Given private endpoints are enabled
    And I have these datasets:
      """
      [
        {
          "id": "static-dataset-test",
          "title": "Static dataset Test",
          "links": {
            "self": {
              "href": "http://localhost:22000/datasets/static-dataset-test",
              "id": "static-test-dataset"
            }
          },
          "state": "created",
          "type": "static"
        },
        {
          "id": "static-dataset-existing",
          "title": "static dataset with published version",
          "links": {
            "self": {
              "href": "http://localhost:22000/datasets/static-dataset-existing",
              "id": "static-test-existing"
            }
          },
          "state": "associated",
          "type": "static"
        }
      ]
      """
    And I have these static versions:
      """
      [
        {
          "id": "static-version-published",
          "edition": "2024",
          "edition_title": "2024 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-existing"
            },
            "edition": {
              "href": "/datasets/static-dataset-existing/editions/2024",
              "id": "2024"
            }
          },
          "version": 1,
          "release_date": "2024-01-01T09:00:00.000Z",
          "state": "published",
          "type": "static",
          "distributions": [
            {
              "title": "Published Dataset CSV",
              "format": "csv",
              "media_type": "text/csv",
              "download_url": "/uuid/filename.csv",
              "byte_size": 150000
            }
          ]
        }
      ]
      """

  Scenario: Create a version
    Given I am an admin user
    When I POST "/datasets/static-dataset-test/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024",
        "type": "static",
        "distributions": [
          {
            "title": "Full Dataset CSV",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/downloads/files/static-dataset-test/2024/1/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
    Then the HTTP status code should be "201"
    And the total number of audit events should be 1
    And the number of events with action "CREATE" and resource "/datasets/static-dataset-test/editions/2024/versions/1" should be 1

  Scenario: Create a version as a publisher user
    Given I am a publisher user
    When I POST "/datasets/static-dataset-test/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024",
        "type": "static",
        "distributions": [
          {
            "title": "Full Dataset CSV",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
    Then I should receive the following JSON response with status "201":
      """
      {
        "dataset_id": "static-dataset-test",
        "distributions": [
          {
            "byte_size": 100000,
            "download_url": "/uuid/filename.csv",
            "format": "csv",
            "media_type": "text/csv",
            "title": "Full Dataset CSV"
          }
        ],
        "edition": "2024",
        "edition_title": "2024",
        "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
        "links": {
          "dataset": {
            "href": "http://localhost:22000/datasets/static-dataset-test",
            "id": "static-dataset-test"
          },
          "edition": {
            "href": "http://localhost:22000/datasets/static-dataset-test/editions/2024",
            "id": "2024"
          },
          "self": {
            "href": "http://localhost:22000/datasets/static-dataset-test/editions/2024/versions/1"
          }
        },
        "release_date": "2024-12-01T09:00:00.000Z",
        "state": "associated",
        "type": "static",
        "version": 1
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "CREATE" and resource "/datasets/static-dataset-test/editions/2024/versions/1" should be 1

  Scenario: Create a version when a previous version is published
    Given I am an admin user
    When I POST "/datasets/static-dataset-existing/editions/2024/versions"
      """
      {
        "release_date": "2024-06-01T09:00:00.000Z",
        "edition_title": "2024 Edition Updated",
        "type": "static",
        "distributions": [
          {
            "title": "Updated Dataset CSV",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 200000
          }
        ]
      }
      """
    Then the HTTP status code should be "201"
    And the total number of audit events should be 1
    And the number of events with action "CREATE" and resource "/datasets/static-dataset-existing/editions/2024/versions/2" should be 1

  Scenario: Create a version with missing mandatory fields
    Given I am an admin user
    When I POST "/datasets/static-dataset-test/editions/2024/versions"
      """
      {
        "type": "static"
      }
      """
    Then the HTTP status code should be "400"

  Scenario: Create a version for a dataset that does not exist
    Given I am an admin user
    When I POST "/datasets/non-existent-dataset/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "Test Edition",
        "type": "static",
        "distributions": [
          {
            "title": "Test CSV",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
    Then the HTTP status code should be "404"

  Scenario: Create a version when an unpublished version already exists
    Given I am an admin user
    And I have these static versions:
      """
      [
        {
          "id": "static-version-unpublished",
          "edition": "2024",
          "edition_title": "2024 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-test"
            },
            "edition": {
              "href": "/datasets/static-dataset-test/editions/2024",
              "id": "2024"
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
              "download_url": "/uuid/filename.csv",
              "byte_size": 100000
            }
          ]
        }
      ]
      """
    When I POST "/datasets/static-dataset-test/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024 Updated",
        "type": "static",
        "distributions": [
          {
            "title": "csv",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 120000
          }
        ]
      }
      """
    Then the HTTP status code should be "400"

  Scenario: Create a version when not authorised
    When I POST "/datasets/static-dataset-test/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "Test Edition",
        "type": "static",
        "distributions": [
          {
            "title": "Test CSV",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
    Then the HTTP status code should be "401"

  Scenario: Create a version with an existing edition title
    Given I am an admin user
    When I POST "/datasets/static-dataset-existing/editions/2025/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024 Edition",
        "type": "static",
        "distributions": [
          {
            "title": "Full Dataset CSV",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following JSON response:
      """
      {
        "errors": [
          {
            "code": "ErrEditionTitleAlreadyExists",
            "description": "edition title already exists"
          }
        ]
      }
      """

  Scenario: Create a version with missing media_type in the distribution
    Given I am an admin user
    When I POST "/datasets/static-dataset-test/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024",
        "type": "static",
        "distributions": [
          {
            "title": "Full Dataset CSV",
            "format": "csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
    Then the HTTP status code should be "201"
    And the total number of audit events should be 1
    And the number of events with action "CREATE" and resource "/datasets/static-dataset-test/editions/2024/versions/1" should be 1

  Scenario: Create a version with missing distribution format field
    Given I am an admin user
    When I POST "/datasets/static-dataset-test/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024",
        "type": "static",
        "distributions": [
          {
            "title": "Full Dataset CSV",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following JSON response:
      """
      {
        "errors": [
          {
            "code": "ErrMissingParameters",
            "description": "distributions[0].format field is missing"
          }
        ]
      }
      """

  Scenario: Create a version with an invalid distribution format field
    Given I am an admin user
    When I POST "/datasets/static-dataset-test/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024",
        "type": "static",
        "distributions": [
          {
            "title": "Full Dataset CSV",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000,
            "format": "WRONG"
          }
        ]
      }
      """

    Then the HTTP status code should be "400"
    And I should receive the following JSON response:
      """
      {
        "errors": [
          {
            "code": "ErrMissingParameters",
            "description": "distributions[0].format field is invalid"
          }
        ]
      }
      """

  Scenario: Create a version with spaces in the edition ID
    Given I am an admin user
    When I POST "/datasets/static-dataset-test/editions/edition%201/versions"
      """
      {
        "distributions": [
          {
            "title": "Full Dataset (CSV)",
            "download_url": "https://download.ons.gov.uk/my-dataset-download.csv",
            "byte_size": 4300000,
            "format": "csv"
          },
          {
            "title": "Full Dataset (CSV)",
            "download_url": "https://download.ons.gov.uk/my-dataset-download.csv",
            "byte_size": 4300000,
            "format": "csv"
          },
          {
            "title": "Full Dataset (CSV)",
            "download_url": "https://download.ons.gov.uk/my-dataset-download.csv",
            "byte_size": 4300000,
            "format": "sdmx"
          }
        ],
        "quality_designation": "accredited-official",
        "release_date": "2025-03-06T14:49:23.354Z",
        "type": "static",
        "edition_title": "edition title of this editionss 5",
        "usage_notes": [
          {
            "title": "This dataset",
            "note": "Please use it wisely"
          }
        ]
      }
      """

    Then the HTTP status code should be "400"
    And I should receive the following JSON response:
      """
      {
        "errors": [
          {
            "code": "ErrSpacesNotAllowed",
            "description": "spaces are not allowed in the ID field"
          }
        ]
      }
      """

  Scenario: Create a version with is_migration set to true
    Given I am an admin user
    When I POST "/datasets/static-dataset-test/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024",
        "type": "static",
        "is_migration": true,
        "distributions": [
          {
            "title": "Full Dataset CSV",
            "format": "csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
    Then I should receive the following JSON response with status "201":
      """
      {
        "dataset_id": "static-dataset-test",
        "distributions": [
          {
            "byte_size": 100000,
            "download_url": "/uuid/filename.csv",
            "format": "csv",
            "media_type": "text/csv",
            "title": "Full Dataset CSV"
          }
        ],
        "edition": "2024",
        "edition_title": "2024",
        "is_migration": true,
        "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
        "links": {
          "dataset": {
            "href": "http://localhost:22000/datasets/static-dataset-test",
            "id": "static-dataset-test"
          },
          "edition": {
            "href": "http://localhost:22000/datasets/static-dataset-test/editions/2024",
            "id": "2024"
          },
          "self": {
            "href": "http://localhost:22000/datasets/static-dataset-test/editions/2024/versions/1"
          }
        },
        "release_date": "2024-12-01T09:00:00.000Z",
        "state": "associated",
        "type": "static",
        "version": 1
      }
      """

  Scenario: Create a version when the dataset has topics
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "static-dataset-topics-condensed",
          "title": "Static dataset with topics",
          "state": "created",
          "type": "static",
          "topics": [
            "economy-topic-id"
          ],
          "links": {
            "self": {
              "href": "http://localhost:22000/datasets/static-dataset-topics-condensed"
            }
          }
        }
      ]
      """
    When I POST "/datasets/static-dataset-topics-condensed/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024",
        "type": "static",
        "distributions": [
          {
            "title": "Full Dataset CSV",
            "format": "csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100
          }
        ]
      }
      """
    Then I should receive the following JSON response with status "201":
      """
      {
        "dataset_id": "static-dataset-topics-condensed",
        "distributions": [
          {
            "byte_size": 100,
            "download_url": "/uuid/filename.csv",
            "format": "csv",
            "media_type": "text/csv",
            "title": "Full Dataset CSV"
          }
        ],
        "edition": "2024",
        "edition_title": "2024",
        "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
        "links": {
          "dataset": {
            "href": "http://localhost:22000/datasets/static-dataset-topics-condensed",
            "id": "static-dataset-topics-condensed"
          },
          "edition": {
            "href": "http://localhost:22000/datasets/static-dataset-topics-condensed/editions/2024",
            "id": "2024"
          },
          "self": {
            "href": "http://localhost:22000/datasets/static-dataset-topics-condensed/editions/2024/versions/1"
          },
          "web_page": {
            "href": "economy/datasets/static-dataset-topics-condensed/editions/2024/versions/1"
          }
        },
        "release_date": "2024-12-01T09:00:00.000Z",
        "state": "associated",
        "type": "static",
        "version": 1
      }
      """
    And the response header "ETag" should not be empty

  Scenario: Create a version when the Topic API returns an error
    Given I am an admin user
    And I have these datasets:
      """
      [
        {
          "id": "static-dataset-unknown-topic-condensed",
          "title": "Static dataset with unknown topic",
          "state": "created",
          "type": "static",
          "topics": [
            "unknown-topic-id"
          ],
          "links": {
            "self": {
              "href": "http://localhost:22000/datasets/static-dataset-unknown-topic-condensed"
            }
          }
        }
      ]
      """
    When I POST "/datasets/static-dataset-unknown-topic-condensed/editions/2024/versions"
      """
      {
        "release_date": "2024-12-01T09:00:00.000Z",
        "edition_title": "2024",
        "type": "static",
        "distributions": [
          {
            "title": "Full Dataset CSV",
            "format": "csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100
          }
        ]
      }
      """
    Then the HTTP status code should be "500"
