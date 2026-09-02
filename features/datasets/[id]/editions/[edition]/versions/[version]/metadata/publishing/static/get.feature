Feature: Get metadata

  Background:
    Given private endpoints are enabled
    And I am an admin user

  Scenario: Get metadata
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
    And the total number of audit events should be 1
    And the number of events with action "READ" and resource "/datasets/static-dataset/editions/time-series/versions/1/metadata" should be 1

  Scenario: Get metadata with URL rewriting enabled
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
    And URL rewriting is enabled
    And I set the "X-Forwarded-Host" header to "api.example.com"
    And I set the "X-Forwarded-Path-Prefix" header to "v1"
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
            "href": "https://api.example.com/v1/datasets/static-dataset/editions/time-series/versions/1/metadata"
          },
          "version": {
            "href": "https://api.example.com/v1/datasets/static-dataset/editions/time-series/versions/1",
            "id": "1"
          },
          "website_version": {
            "href": "http://localhost:20000/economy/datasets/static-dataset/editions/time-series/versions/1"
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
            "download_url": "http://localhost:23600/downloads/files/uuid/filename.csv",
            "byte_size": 100000
          }
        ]
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "READ" and resource "/datasets/static-dataset/editions/time-series/versions/1/metadata" should be 1

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
        "state": "edition-confirmed",
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
    And the total number of audit events should be 1
    And the number of events with action "READ" and resource "/datasets/static-dataset/editions/time-series/versions/1/metadata" should be 1
