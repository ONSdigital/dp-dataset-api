Feature: PUT /datasets/{id} for static datasets

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

    Scenario: Successfully rename unpublished static dataset and update its canonical topic
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

    Scenario: Cannot rename unpublished static dataset when new id already exists
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

    Scenario: Cannot change id of published static dataset
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

    Scenario: Cannot change canonical topic of published static dataset
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
