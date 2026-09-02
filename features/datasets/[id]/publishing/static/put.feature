Feature: Update static dataset in publishing mode

  Background:
    Given private endpoints are enabled
    And I am an admin user
    And I have realistic datasets:
      """
      [
        {
          "next": {
            "id": "old-dataset-id",
            "type": "static",
            "title": "Original Title",
            "description": "A static dataset",
            "state": "created",
            "topics": [
              "old-topic",
              "topic-1"
            ],
            "links": {
              "self": {
                "href": "/datasets/old-dataset-id"
              },
              "editions": {
                "href": "/datasets/old-dataset-id/editions"
              },
              "latest_version": {
                "href": "/datasets/old-dataset-id/editions/2025/versions/1",
                "id": "1"
              }
            }
          }
        },
        {
          "next": {
            "id": "existing-dataset-id",
            "type": "static",
            "title": "Existing Dataset",
            "description": "An existing dataset",
            "state": "created",
            "topics": [
              "topic-2",
              "topic-3"
            ]
          }
        },
        {
          "current": {
            "id": "published-dataset-id",
            "state": "published",
            "type": "static",
            "title": "Published Dataset",
            "topics": [
              "old-topic",
              "topic-1"
            ]
          },
          "next": {
            "id": "published-dataset-id",
            "state": "published",
            "type": "static",
            "title": "Published Dataset",
            "topics": [
              "old-topic",
              "topic-1"
            ]
          }
        },
        {
          "next": {
            "id": "migrated-dataset-id",
            "state": "created",
            "type": "static",
            "title": "Migrated Dataset",
            "topics": [
              "old-topic",
              "topic-1"
            ],
            "is_migration": true
          }
        }
      ]
      """
    And I have these static versions:
      """
      [
        {
          "id": "version-1",
          "edition": "2025",
          "edition_title": "2025 Edition",
          "version": 1,
          "release_date": "2025-01-01T00:00:00Z",
          "state": "associated",
          "type": "static",
          "links": {
            "dataset": {
              "id": "old-dataset-id"
            },
            "edition": {
              "href": "/datasets/old-dataset-id/editions/2025",
              "id": "2025"
            },
            "version": {
              "href": "/datasets/old-dataset-id/editions/2025/versions/1"
            },
            "web_page": {
              "href": "/old-topic/datasets/old-dataset-id/editions/2025/versions/1"
            }
          }
        }
      ]
      """

  Scenario: Rename a dataset and change its canonical topic
    When I PUT "/datasets/old-dataset-id"
      """
      {
        "id": "new-dataset-id",
        "contacts": [
          {
            "name": "John Doe",
            "email": "john@example.com"
          }
        ],
        "type": "static",
        "title": "Original Title",
        "description": "A static dataset",
        "next_release": "2026-01-01T00:00:00Z",
        "license": "Open Government Licence v3.0",
        "keywords": [
          "economy",
          "prices"
        ],
        "topics": [
          "economy-topic-id",
          "topic-1"
        ]
      }
      """
    Then the HTTP status code should be "200"
    And the dataset "old-dataset-id" should not exist
    And the document in the database for id "new-dataset-id" should be:
      """
      {
        "id": "new-dataset-id",
        "contacts": [
          {
            "name": "John Doe",
            "email": "john@example.com"
          }
        ],
        "title": "Original Title",
        "description": "A static dataset",
        "license": "Open Government Licence v3.0",
        "next_release": "2026-01-01T00:00:00Z",
        "keywords": [
          "economy",
          "prices"
        ],
        "type": "static",
        "topics": [
          "economy-topic-id",
          "topic-1"
        ],
        "links": {
          "self": {
            "href": "/datasets/new-dataset-id"
          },
          "editions": {
            "href": "/datasets/new-dataset-id/editions"
          },
          "latest_version": {
            "href": "/datasets/new-dataset-id/editions/2025/versions/1",
            "id": "1"
          }
        },
        "previous_series_id": [
          "old-dataset-id"
        ]
      }
      """
    And the static version in the database for id "version-1" should be:
      """
      {
        "id": "version-1",
        "edition": "2025",
        "edition_title": "2025 Edition",
        "version": 1,
        "release_date": "2025-01-01T00:00:00Z",
        "state": "associated",
        "type": "static",
        "links": {
          "dataset": {
            "id": "new-dataset-id",
            "href": "/datasets/new-dataset-id"
          },
          "edition": {
            "href": "/datasets/new-dataset-id/editions/2025",
            "id": "2025"
          },
          "self": {
            "href": "/datasets/new-dataset-id/editions/2025/versions/1"
          },
          "version": {
            "href": "/datasets/new-dataset-id/editions/2025/versions/1",
            "id": "1"
          },
          "web_page": {
            "href": "economy/datasets/new-dataset-id/editions/2025/versions/1"
          }
        }
      }
      """

  Scenario: Change the title of a dataset
    Given I have these datasets:
      """
      [
        {
          "id": "static-dataset",
          "next": {
            "id": "static-dataset",
            "contacts": [
              {
                "name": "abc",
                "email": "abc@email.com"
              }
            ],
            "type": "static",
            "title": "example 1",
            "description": "some description",
            "license": "Open Government Licence v3.0",
            "topics": [
              "topic-0",
              "topic-1"
            ]
          }
        }
      ]
      """
    When I PUT "/datasets/static-dataset"
      """
      {
        "id": "static-dataset",
        "contacts": [
          {
            "name": "abc",
            "email": "abc@email.com"
          }
        ],
        "type": "static",
        "title": "new title",
        "description": "some description",
        "license": "Open Government Licence v3.0",
        "topics": [
          "topic-0",
          "topic-1"
        ]
      }
      """
    Then I should receive the following JSON response with status "200":
      """
      {
        "contacts": [
          {
            "name": "abc",
            "email": "abc@email.com"
          }
        ],
        "description": "some description",
        "license": "Open Government Licence v3.0",
        "title": "new title",
        "topics": [
          "topic-0",
          "topic-1"
        ],
        "id": "static-dataset",
        "last_updated": "0001-01-01T00:00:00Z"
      }
      """
    And the document in the database for id "static-dataset" should be:
      """
      {
        "id": "static-dataset",
        "contacts": [
          {
            "name": "abc",
            "email": "abc@email.com"
          }
        ],
        "title": "new title",
        "description": "some description",
        "license": "Open Government Licence v3.0",
        "next": {
          "topics": [
            "topic-0",
            "topic-1"
          ],
          "type": "static"
        }
      }
      """
    And the total number of audit events should be 1
    And the number of events with action "UPDATE" and resource "/datasets/static-dataset" should be 1

  Scenario: Update a dataset with spaces in the ID
    Given I have these datasets:
      """
      [
        {
          "id": "valid-dataset-id",
          "title": "Valid Dataset",
          "state": "created",
          "type": "static"
        }
      ]
      """
    When I PUT "/datasets/valid-dataset-id"
      """
      {
        "id": "test dataset id with spaces",
        "title": "Valid Dataset",
        "description": "Dataset description",
        "license": "Open Government Licence v3.0",
        "next_release": "2026-01-01",
        "keywords": [
          "keyword"
        ],
        "topics": [
          "topic-0"
        ],
        "contacts": [
          {
            "name": "Test",
            "email": "test@test.com"
          }
        ]
      }
      """
    Then the HTTP status code should be "400"
    And I should receive the following response:
      """
      spaces are not allowed in the ID field
      """

  Scenario: Rename a dataset to an existing ID
    When I PUT "/datasets/old-dataset-id"
      """
      {
        "id": "existing-dataset-id",
        "contacts": [
          {
            "name": "John Doe",
            "email": "john@example.com"
          }
        ],
        "type": "static",
        "title": "Dataset To Rename",
        "description": "A static dataset",
        "next_release": "2026-01-01T00:00:00Z",
        "license": "Open Government Licence v3.0",
        "keywords": [
          "economy",
          "prices"
        ],
        "topics": [
          "old-topic",
          "topic-1"
        ]
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
      dataset already exists
      """

  Scenario: Change the ID of a published dataset
    When I PUT "/datasets/published-dataset-id"
      """
      {
        "id": "new-dataset-id",
        "type": "static",
        "title": "Published Dataset",
        "description": "Published static dataset",
        "next_release": "2026-01-01T00:00:00Z",
        "contacts": [
          {
            "name": "John Doe",
            "email": "john@example.com"
          }
        ],
        "license": "Open Government Licence v3.0",
        "keywords": [
          "economy",
          "prices"
        ],
        "topics": [
          "old-topic",
          "topic-1"
        ]
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
      cannot change the dataset ID for a published dataset
      """

  Scenario: Change the ID of a migrated dataset
    When I PUT "/datasets/migrated-dataset-id"
      """
      {
        "id": "new-dataset-id",
        "type": "static",
        "title": "Migrated Dataset",
        "description": "Migrated static dataset",
        "next_release": "2026-01-01T00:00:00Z",
        "contacts": [
          {
            "name": "John Doe",
            "email": "john@example.com"
          }
        ],
        "license": "Open Government Licence v3.0",
        "keywords": [
          "economy",
          "prices"
        ],
        "topics": [
          "old-topic",
          "topic-1"
        ]
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
      cannot change the dataset ID for a migrated dataset
      """

  Scenario: Change the canonical topic of a published dataset
    When I PUT "/datasets/published-dataset-id"
      """
      {
        "id": "published-dataset-id",
        "type": "static",
        "title": "Published Dataset",
        "description": "Published static dataset",
        "next_release": "2026-01-01T00:00:00Z",
        "contacts": [
          {
            "name": "John Doe",
            "email": "john@example.com"
          }
        ],
        "license": "Open Government Licence v3.0",
        "keywords": [
          "economy",
          "prices"
        ],
        "topics": [
          "new-topic",
          "topic-1"
        ]
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
      canonical topic can't be changed once a series is published
      """

  Scenario: Change the canonical topic of a published dataset
    Given I have these datasets:
      """
      [
        {
          "id": "update-published-topic-test",
          "contacts": [
            {
              "email": "contact@ons.gov.uk",
              "name": "Expert Statistical Team",
              "telephone": "+44 1234 111111"
            }
          ],
          "description": "This dataset is for testing",
          "keywords": [
            "dataset"
          ],
          "license": "Open Government Licence v3.0",
          "next_release": "To be announced",
          "title": "Static Dataset for Updates",
          "state": "published",
          "topics": [
            "topic-1",
            "topic-2"
          ],
          "type": "static"
        }
      ]
      """
    When I PUT "/datasets/update-published-topic-test"
      """
      {
        "id": "update-published-topic-test",
        "contacts": [
          {
            "email": "contact@ons.gov.uk",
            "name": "Expert Statistical Team",
            "telephone": "+44 1234 111111"
          }
        ],
        "description": "This dataset is for testing",
        "keywords": [
          "dataset"
        ],
        "license": "Open Government Licence v3.0",
        "next_release": "To be announced",
        "title": "Static Dataset for Updates",
        "state": "published",
        "topics": [
          "updated-topic-1",
          "topic-2"
        ],
        "type": "static"
      }
      """
    Then the HTTP status code should be "409"
    And I should receive the following response:
      """
      canonical topic can't be changed once a series is published
      """
