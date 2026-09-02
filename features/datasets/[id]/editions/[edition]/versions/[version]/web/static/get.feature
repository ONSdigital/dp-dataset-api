Feature: Get static version in web mode

  Background:
    Given I have realistic datasets:
      """
      [
        {
          "next": {
            "id": "test-static",
            "state": "created",
            "type": "static",
            "links": {
              "latest_version": {
                "id": "1",
                "href": "/datasets/test-static/editions/test-edition-static/versions/1"
              }
            }
          },
          "current": {
            "id": "test-static",
            "state": "published",
            "type": "static",
            "links": {
              "latest_version": {
                "id": "1",
                "href": "/datasets/test-static/editions/test-edition-published/versions/1"
              }
            }
          }
        },
        {
          "next": {
            "id": "test-created-dataset",
            "state": "created",
            "type": "static",
            "links": {
              "latest_version": {
                "id": "1",
                "href": "/datasets/test-created-dataset/editions/test-edition-static/versions/1"
              }
            }
          },
          "current": null
        }
      ]
      """
    And I have these static versions:
      """
      [
        {
          "id": "test-static-version",
          "version": 1,
          "edition": "test-edition-static",
          "edition_title": "Test Edition Static Title",
          "links": {
            "dataset": {
              "id": "test-static"
            },
            "edition": {
              "href": "/datasets/test-static/editions/test-edition-static",
              "id": "test-edition-static"
            },
            "self": {
              "href": "/datasets/test-static/editions/test-edition-static/versions/1"
            },
            "web_page": {
              "href": "/economy/datasets/test-static/editions/test-edition-static/versions/1"
            }
          },
          "state": "associated",
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
          "is_migration": true
        },
        {
          "id": "test-static-version-approved",
          "version": 1,
          "edition": "test-edition-static-approved",
          "edition_title": "Test Edition Static Approved Title",
          "links": {
            "dataset": {
              "id": "test-static"
            },
            "edition": {
              "href": "/datasets/test-static/editions/test-edition-static-approved",
              "id": "test-edition-static-approved"
            },
            "self": {
              "href": "/datasets/test-static/editions/test-edition-static-approved/versions/1"
            },
            "web_page": {
              "href": "/economy/datasets/test-static/editions/test-edition-static-approved/versions/1"
            }
          },
          "state": "approved",
          "type": "static",
          "previous_edition_id": [
            "approved-old-edition-1",
            "approved-old-edition-2"
          ],
          "distributions": [
            {
              "title": "Distribution 1",
              "format": "csv",
              "media_type": "text/csv",
              "download_url": "/uuid/filename.csv",
              "byte_size": 100000
            }
          ]
        },
        {
          "id": "test-static-version-published",
          "version": 1,
          "edition": "test-edition-published",
          "edition_title": "Test Edition Published Title",
          "links": {
            "dataset": {
              "id": "test-static"
            },
            "edition": {
              "href": "/datasets/test-static/editions/test-edition-published",
              "id": "test-edition-published"
            },
            "self": {
              "href": "/datasets/test-static/editions/test-edition-published/versions/1"
            },
            "web_page": {
              "href": "/economy/datasets/test-static/editions/test-edition-published/versions/1"
            }
          },
          "state": "published",
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
          "previous_edition_id": [
            "old-edition-1",
            "old-edition-2"
          ]
        }
      ]
      """

  Scenario: Get a version
    When I GET "/datasets/test-static/editions/test-edition-published/versions/1"
    Then the HTTP status code should be "200"
    And the total number of audit events should be 0

  Scenario: Get a version of an unpublished dataset
    When I GET "/datasets/test-created-dataset/editions/test-edition-static/versions/1"
    Then I should receive the following JSON response with status "404":
      """
      {
        "errors": [
          {
            "code": "dataset not found",
            "description": "dataset not found"
          }
        ]
      }
      """

  Scenario: Get a version of an unpublished edition
    When I GET "/datasets/test-static/editions/test-edition-unpublished/versions/1"
    Then the HTTP status code should be "404"
    Then I should receive the following JSON response with status "404":
      """
      {
        "errors": [
          {
            "code": "edition not found",
            "description": "edition not found"
          }
        ]
      }
      """

  Scenario: Get a version using a previous edition ID
    And I am not authenticated
    When I GET "/datasets/test-static/editions/old-edition-1/versions/1"
    Then the HTTP status code should be "404"

  Scenario: Get a version that contains redacted fields
    When I GET "/datasets/test-static/editions/test-edition-published/versions/1"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "test-static-version-published",
        "last_updated": "2021-01-01T00:00:02Z",
        "type": "static",
        "version": 1,
        "state": "published",
        "links": {
          "dataset": {
            "id": "test-static"
          },
          "edition": {
            "href": "/datasets/test-static/editions/test-edition-published",
            "id": "test-edition-published"
          },
          "self": {
            "href": "/datasets/test-static/editions/test-edition-published/versions/1"
          },
          "web_page": {
            "href": "/economy/datasets/test-static/editions/test-edition-published/versions/1"
          }
        },
        "edition": "test-edition-published",
        "edition_title": "Test Edition Published Title",
        "distributions": [
          {
            "title": "Distribution 1",
            "format": "csv",
            "media_type": "text/csv",
            "download_url": "/uuid/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
