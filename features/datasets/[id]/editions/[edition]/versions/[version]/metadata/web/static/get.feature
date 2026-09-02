Feature: Get static version metadata in web mode

  Scenario: Get metadata for a published dataset
    Given I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-dataset",
          "title": "static title",
          "description": "static description",
          "state": "published",
          "type": "static",
          "next_release": "2023-12-01",
          "license": "license",
          "keywords": [
            "statistics",
            "population"
          ],
          "contacts": [
            {
              "name": "name",
              "email": "name@example.com",
              "telephone": "01234 567890"
            }
          ],
          "topics": [
            "economy",
            "demographics"
          ]
        },
        "version": {
          "version": 1,
          "state": "published",
          "release_date": "2023-01-15",
          "temporal": [
            {
              "frequency": "Monthly",
              "start_date": "2023-01-01",
              "end_date": "2023-01-31"
            }
          ],
          "links": {
            "dataset": {
              "href": "/datasets/static-dataset",
              "id": "static-dataset"
            },
            "edition": {
              "href": "/datasets/static-dataset/editions/time-series",
              "id": "time-series"
            },
            "self": {
              "href": "/datasets/static-dataset/editions/time-series/versions/1"
            },
            "version": {
              "href": "/datasets/static-dataset/editions/time-series/versions/1",
              "id": "1"
            },
            "web_page": {
              "href": "economy/datasets/static-dataset/editions/time-series/versions/1"
            }
          },
          "edition": "time-series",
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
      }
      """
    When I GET "/datasets/static-dataset/editions/time-series/versions/1/metadata"
    Then I should receive the following JSON response with status "200":
      """
      {
        "contacts": [
          {
            "name": "name",
            "email": "name@example.com",
            "telephone": "01234 567890"
          }
        ],
        "description": "static description",
        "keywords": [
          "statistics",
          "population"
        ],
        "last_updated": "0001-01-01T00:00:00Z",
        "license": "license",
        "next_release": "2023-12-01",
        "release_date": "2023-01-15",
        "title": "static title",
        "topics": [
          "economy",
          "demographics"
        ],
        "links": {
          "self": {
            "href": "/datasets/static-dataset/editions/time-series/versions/1/metadata"
          },
          "version": {
            "href": "/datasets/static-dataset/editions/time-series/versions/1",
            "id": "1"
          },
          "website_version": {
            "href": "economy/datasets/static-dataset/editions/time-series/versions/1"
          }
        },
        "edition": "time-series",
        "id": "static-dataset",
        "temporal": [
          {
            "frequency": "Monthly",
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
          }
        ],
        "type": "static",
        "version": 1,
        "state": "published",
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

  Scenario: Get metadata for an unpublished dataset
    Given I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-dataset",
          "title": "static title",
          "description": "static description",
          "state": "created",
          "type": "static",
          "next_release": "2023-12-01",
          "license": "license",
          "keywords": [
            "statistics",
            "population"
          ],
          "contacts": [
            {
              "name": "name",
              "email": "name@example.com",
              "telephone": "01234 567890"
            }
          ],
          "topics": [
            "economy",
            "demographics"
          ]
        },
        "version": {
          "version": 1,
          "state": "edition-confirmed",
          "release_date": "2023-01-15",
          "temporal": [
            {
              "frequency": "Monthly",
              "start_date": "2023-01-01",
              "end_date": "2023-01-31"
            }
          ],
          "links": {
            "dataset": {
              "href": "/datasets/static-dataset",
              "id": "static-dataset"
            },
            "edition": {
              "href": "/datasets/static-dataset/editions/time-series",
              "id": "time-series"
            },
            "self": {
              "href": "/datasets/static-dataset/editions/time-series/versions/1"
            },
            "version": {
              "href": "/datasets/static-dataset/editions/time-series/versions/1",
              "id": "1"
            },
            "web_page": {
              "href": "economy/datasets/static-dataset/editions/time-series/versions/1"
            }
          },
          "edition": "time-series",
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
      }
      """
    When I GET "/datasets/static-dataset/editions/time-series/versions/1/metadata"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
            dataset not found
      """

  Scenario: Get metadata containing redacted fields
    Given I have a static dataset with version:
      """
      {
        "dataset": {
          "id": "static-migrated-dataset-published",
          "title": "static migration published title",
          "description": "Migration Published Description",
          "state": "published",
          "type": "static",
          "license": "Open Government License v3.0"
        },
        "version": {
          "id": "v1-migration-published",
          "version": 1,
          "edition": "time-series",
          "state": "published",
          "type": "static",
          "release_date": "2023-05-20",
          "is_migration": true,
          "links": {
            "dataset": {
              "href": "/datasets/static-migrated-dataset-published",
              "id": "static-migrated-dataset-published"
            },
            "edition": {
              "href": "/datasets/static-migrated-dataset-published/editions/time-series",
              "id": "time-series"
            },
            "self": {
              "href": "/datasets/static-migrated-dataset-published/editions/time-series/versions/1"
            },
            "web_page": {
              "href": "economy/datasets/static-migrated-dataset-published/editions/time-series/versions/1"
            }
          },
          "distributions": [
            {
              "title": "Dataset CSV",
              "format": "csv",
              "download_url": "uuid/data.csv"
            }
          ]
        }
      }
      """
    When I GET "/datasets/static-migrated-dataset-published/editions/time-series/versions/1/metadata"
    Then I should receive the following JSON response with status "200":
      """
      {
        "description": "Migration Published Description",
        "distributions": [
          {
            "download_url": "uuid/data.csv",
            "format": "csv",
            "title": "Dataset CSV"
          }
        ],
        "edition": "time-series",
        "id": "static-migrated-dataset-published",
        "last_updated": "0001-01-01T00:00:00Z",
        "license": "Open Government License v3.0",
        "links": {
          "self": {
            "href": "/datasets/static-migrated-dataset-published/editions/time-series/versions/1/metadata"
          },
          "version": {
            "href": "/datasets/static-migrated-dataset-published/editions/time-series/versions/1",
            "id": "1"
          },
          "website_version": {
            "href": "economy/datasets/static-migrated-dataset-published/editions/time-series/versions/1"
          }
        },
        "release_date": "2023-05-20",
        "state": "published",
        "title": "static migration published title",
        "type": "static",
        "version": 1
      }
      """
