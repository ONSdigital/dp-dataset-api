Feature: Delete a version

  Background:
    Given private endpoints are enabled
    And I have these datasets:
      """
      [
        {
          "id": "static-dataset-test",
          "title": "Static dataset Test",
          "state": "created",
          "type": "static",
          "current": {
            "id": "static-dataset-test",
            "title": "Static dataset Test",
            "state": "created",
            "type": "static"
          },
          "next": {
            "id": "static-dataset-test",
            "title": "Static dataset Test - Updated Title",
            "state": "edition-confirmed",
            "type": "static"
          }
        },
        {
          "id": "static-dataset-published",
          "title": "static dataset with published version",
          "state": "published",
          "type": "static"
        },
        {
          "id": "static-dataset-no-versions",
          "title": "static dataset with no versions",
          "state": "created",
          "type": "static"
        },
        {
          "id": "static-dataset-bad-version-download-url",
          "title": "static dataset with bad version download_url",
          "state": "created",
          "type": "static"
        }
      ]
      """
    And I have these static versions:
      """
      [
        {
          "id": "static-version-approved",
          "edition": "2024",
          "edition_title": "2024 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-test"
            },
            "edition": {
              "href": "/datasets/static-dataset-test/editions/2024",
              "id": "2024"
            },
            "version": {
              "href": "/datasets/static-dataset-test/editions/2024/versions/1",
              "id": "1"
            }
          },
          "version": 1,
          "release_date": "2024-01-01T09:00:00.000Z",
          "state": "approved",
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
        },
        {
          "id": "static-version-published",
          "edition": "2025",
          "edition_title": "2025 Edition",
          "links": {
            "dataset": {
              "id": "static-dataset-published"
            },
            "edition": {
              "href": "/datasets/static-dataset-published/editions/2025",
              "id": "2025"
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
        },
        {
          "id": "static-version-bad-download-url",
          "edition": "January",
          "edition_title": "January Edition Title",
          "links": {
            "dataset": {
              "id": "static-dataset-bad-version-download-url"
            },
            "edition": {
              "href": "/datasets/static-dataset-bad-version-download-url/editions/January",
              "id": "January"
            }
          },
          "version": 1,
          "release_date": "2026-01-01T09:00:00.000Z",
          "state": "associated",
          "type": "static",
          "distributions": [
            {
              "title": "Files API expected to fail",
              "format": "csv",
              "media_type": "text/csv",
              "download_url": "/fail/to/delete.csv",
              "byte_size": 150000
            }
          ]
        }
      ]
      """

  Scenario: Delete a version
    Given I am an admin user
    And the "ENABLE_DETACH_DATASET" feature flag is "false"
    And the "ENABLE_DELETE_STATIC_VERSION" feature flag is "true"
    When I DELETE "/datasets/static-dataset-test/editions/2024/versions/1"
    Then the HTTP status code should be "204"
    And the static version "static-version-approved" should not exist
    And the dataset "static-dataset-test" should exist
    And the dataset "static-dataset-test" should have next equal to current
    And the total number of audit events should be 1
    And the number of events with action "DELETE" and resource "/datasets/static-dataset-test/editions/2024/versions/1" should be 1

  Scenario: Delete a version as a publisher
    Given I am a publisher user
    And the "ENABLE_DETACH_DATASET" feature flag is "false"
    And the "ENABLE_DELETE_STATIC_VERSION" feature flag is "true"
    When I DELETE "/datasets/static-dataset-test/editions/2024/versions/1"
    Then the HTTP status code should be "204"
    And the static version "static-version-approved" should not exist
    And the dataset "static-dataset-test" should exist
    And the dataset "static-dataset-test" should have next equal to current
    And the total number of audit events should be 1
    And the number of events with action "DELETE" and resource "/datasets/static-dataset-test/editions/2024/versions/1" should be 1

  Scenario: Delete a version with an invalid version parameter
    Given I am an admin user
    And the "ENABLE_DETACH_DATASET" feature flag is "false"
    And the "ENABLE_DELETE_STATIC_VERSION" feature flag is "true"
    When I DELETE "/datasets/static-dataset-published/editions/2025/versions/invalid-version"
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
            invalid version requested
      """

  Scenario: Delete a version which is published
    Given I am an admin user
    And the "ENABLE_DETACH_DATASET" feature flag is "false"
    And the "ENABLE_DELETE_STATIC_VERSION" feature flag is "true"
    When I DELETE "/datasets/static-dataset-published/editions/2025/versions/1"
    Then the HTTP status code should be "403"
    And I should receive the following response:
      """
            a published version cannot be deleted
      """

  Scenario: Delete a version using a dataset ID that does not exist
    Given I am an admin user
    And the "ENABLE_DETACH_DATASET" feature flag is "false"
    And the "ENABLE_DELETE_STATIC_VERSION" feature flag is "true"
    When I DELETE "/datasets/static-dataset-non-existent/editions/2024/versions/1"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dataset not found
      """

  Scenario: Delete a version using an edition that does not exist
    Given I am an admin user
    And the "ENABLE_DETACH_DATASET" feature flag is "false"
    And the "ENABLE_DELETE_STATIC_VERSION" feature flag is "true"
    When I DELETE "/datasets/static-dataset-test/editions/non-existent-edition/versions/1"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            edition not found
      """

  Scenario: Delete a version that does not exist
    Given I am an admin user
    And the "ENABLE_DETACH_DATASET" feature flag is "false"
    And the "ENABLE_DELETE_STATIC_VERSION" feature flag is "true"
    When I DELETE "/datasets/static-dataset-test/editions/2024/versions/12"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            version not found
      """

  Scenario: Delete a version where the files-api client fails
    Given I am an admin user
    And the "ENABLE_DELETE_STATIC_VERSION" feature flag is "true"
    When I DELETE "/datasets/static-dataset-bad-version-download-url/editions/January/versions/1"
    Then the HTTP status code should be "500"
    And I should receive the following response:
      """
            internal error: failed to delete file at path: /fail/to/delete.csv
      """
