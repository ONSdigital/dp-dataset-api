Feature: Get static edition in web mode

  Background:
    Given I have these datasets:
      """
      [
        {
          "id": "static-dataset",
          "state": "published",
          "type": "static"
        }
      ]
      """
    And I have these static versions:
      """
      [
        {
          "id": "2024-version-1",
          "edition": "2024",
          "state": "published",
          "version": 1,
          "links": {
            "dataset": {
              "href": "/datasets/static-dataset",
              "id": "static-dataset"
            },
            "edition": {
              "href": "/datasets/static-dataset/editions/2024",
              "id": "2024"
            },
            "self": {
              "href": "/datasets/static-dataset/editions/2024/versions/1"
            },
            "version": {
              "href": "/datasets/static-dataset/editions/2024/versions/1"
            }
          },
          "type": "static",
          "distributions": [
            {
              "title": "Distribution 1",
              "format": "csv",
              "media_type": "text/csv",
              "download_url": "/uuid/filename.csv",
              "byte_size": 100000
            }
          ],
          "release_date": "2024-01-01T07:00:00.000Z"
        },
        {
          "id": "2024-version-2",
          "edition": "2024",
          "state": "published",
          "version": 2,
          "links": {
            "dataset": {
              "href": "/datasets/static-dataset",
              "id": "static-dataset"
            },
            "edition": {
              "href": "/datasets/static-dataset/editions/2024",
              "id": "2024"
            },
            "self": {
              "href": "/datasets/static-dataset/editions/2024/versions/2"
            },
            "version": {
              "href": "/datasets/static-dataset/editions/2024/versions/2"
            }
          },
          "type": "static",
          "distributions": [
            {
              "title": "Distribution 1",
              "format": "csv",
              "media_type": "text/csv",
              "download_url": "/uuid/filename.csv",
              "byte_size": 100000
            }
          ],
          "release_date": "2026-01-01T07:00:00.000Z"
        },
        {
          "id": "2025-version-1",
          "edition": "2025",
          "state": "published",
          "version": 1,
          "links": {
            "dataset": {
              "href": "/datasets/static-dataset",
              "id": "static-dataset"
            },
            "edition": {
              "href": "/datasets/static-dataset/editions/2025",
              "id": "2025"
            },
            "self": {
              "href": "/datasets/static-dataset/editions/2025/versions/1"
            },
            "version": {
              "href": "/datasets/static-dataset/editions/2025/versions/1"
            }
          },
          "type": "static",
          "distributions": [
            {
              "title": "Distribution 1",
              "format": "csv",
              "media_type": "text/csv",
              "download_url": "/uuid/filename.csv",
              "byte_size": 100000
            }
          ],
          "release_date": "2025-01-01T07:00:00.000Z"
        }
      ]
      """

  Scenario: Get an edition
    When I GET "/datasets/static-dataset/editions/2025"
    Then I should receive the following JSON response with status "200":
      """
      {
        "edition": "2025",
        "version": 1,
        "state": "published",
        "links": {
          "dataset": {
            "href": "/datasets/static-dataset",
            "id": "static-dataset"
          },
          "latest_version": {
            "href": "/datasets/static-dataset/editions/2025/versions/1",
            "id": "1"
          },
          "self": {
            "href": "/datasets/static-dataset/editions/2025",
            "id": "2025"
          },
          "versions": {
            "href": "/datasets/static-dataset/editions/2025/versions"
          }
        },
        "distributions": [
          {
            "title": "Distribution 1",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000
          }
        ],
        "release_date": "2025-01-01T07:00:00.000Z"
      }
      """
    And the total number of audit events should be 0
