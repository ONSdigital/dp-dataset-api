Feature: Static Dataset Versions PUT API

    Background: We have static datasets for PUT version testing
        Given I have a static dataset with version:
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

    Scenario: PUT updates static dataset version successfully for an admin user
        Given private endpoints are enabled
        And I am an admin user
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
                "last_updated": "0001-01-01T00:00:00Z",
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

    Scenario: PUT updates static dataset version successfully for a publisher user
        Given private endpoints are enabled
        And I am a publisher user
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
                "last_updated": "0001-01-01T00:00:00Z",
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

    Scenario: PUT updates static dataset version with new data
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "release_date": "2025-03-01T09:00:00.000Z",
                "edition_title": "Updated 2025 Edition",
                "state": "approved",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT updates static dataset version distributions
        Given private endpoints are enabled
        And I am an admin user
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

    Scenario: PUT updates static dataset version edition
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "edition": "2025-revised",
                "edition_title": "2025 Revised Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT fails for non-existent version
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/999"
            """
            {
                "state": "approved",
                "type": "static"
            }
            """
        Then the HTTP status code should be "404"

    Scenario: PUT fails for non-existent dataset
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/non-existent/editions/2025/versions/1"
            """
            {
                "state": "approved",
                "type": "static"
            }
            """
        Then the HTTP status code should be "404"

    Scenario: PUT fails when not authorised
        Given private endpoints are enabled
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "state": "approved",
                "type": "static"
            }
            """
        Then the HTTP status code should be "401"

    Scenario: PUT state endpoint updates successfully
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
            """
            {
                "state": "approved"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT state transitions from associated to approved
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
            """
            {
                "state": "approved"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT state transitions from approved to published
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
                    }
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
                        }
                    },
                    "version": 1,
                    "release_date": "2025-02-01T09:00:00.000Z",
                    "state": "approved",
                    "type": "static"
                }
            }
            """
        And private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-publish/editions/2025/versions/1/state"
            """
            {
                "state": "published"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT state fails with invalid state transition from associated to published
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
            """
            {
                "state": "published"
            }
            """
        Then the HTTP status code should be "400"

    Scenario: PUT state fails with invalid state
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
            """
            {
                "state": "invalid-state"
            }
            """
        Then the HTTP status code should be "400"

    Scenario: PUT state fails when not authorised
        Given private endpoints are enabled
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1/state"
            """
            {
                "state": "approved"
            }
            """
        Then the HTTP status code should be "401"

    Scenario: PUT fails when updating edition-id to existing edition for static dataset
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
        And private endpoints are enabled
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

    Scenario: PUT succeeds when updating edition-id to new edition for static dataset
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "edition": "2025-new-edition",
                "edition_title": "2025 New Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT state handles idempotent transitions correctly
        Given I have a static dataset with version:
            """
            {
                "dataset": {
                    "id": "static-dataset-published",
                    "title": "Static Dataset Published Test",
                    "state": "published",
                    "type": "static"
                },
                "version": {
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
                        },
                        "self": {
                            "href": "/datasets/static-dataset-published/editions/2025/versions/1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T09:00:00.000Z",
                    "state": "published",
                    "type": "static"
                }
            }
            """
        And private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-published/editions/2025/versions/1/state"
            """
            {
                "state": "published"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT succeeds when updating edition ID to unique value within series
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "edition": "2026",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT fails when updating edition title to existing value within same series
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "edition_title": "2025 Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            the edition-title already exists
            """

    Scenario: PUT fails when updating edition ID to duplicate value within same series
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "edition": "2025",
                "edition_title": "Different Title",
                "type": "static"
            }
            """
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            the edition already exists
            """

    Scenario: PUT succeeds when updating both edition ID and title to unique values within the series
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "edition": "2026",
                "edition_title": "Unique 2026 Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "200"

    Scenario: PUT fails when both edition ID and title already exist within the same series
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "edition": "2025",
                "edition_title": "2025 Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            the edition already exists
            """

    Scenario: PUT fails when updating edition title to one that already exists but edition ID is unique
        Given private endpoints are enabled
        And I am an admin user
        When I PUT "/datasets/static-dataset-update/editions/2025/versions/1"
            """
            {
                "edition": "2026",
                "edition_title": "2025 Edition",
                "type": "static"
            }
            """
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            the edition-title already exists
            """

    Scenario: PUT succeeds when distributions contain valid formats
        Given private endpoints are enabled
        And I am an admin user
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
        Then the HTTP status code should be "200"

    Scenario: PUT fails when a distributions object is missing required field format
        Given private endpoints are enabled
        And I am an admin user
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

    Scenario: PUT fails when a distributions object is having inavalid field format
        Given private endpoints are enabled
        And I am an admin user
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
