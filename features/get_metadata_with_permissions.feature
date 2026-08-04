Feature: Dataset API - Metadata Permissions

    Background:
        Given I have a static dataset with version:
            """
            {
                "dataset": {
                    "id": "static-test-dataset",
                    "title": "static dataset title",
                    "description": "Public Description",
                    "state": "created",
                    "type": "static",
                    "license": "Open Government License v3.0"
                },
                "version": {
                    "id": "v1-approved",
                    "version": 1,
                    "edition": "time-series",
                    "state": "approved",
                    "type": "static",
                    "release_date": "2023-05-20",
                    "links": {
                        "dataset": {
                            "href": "/datasets/static-test-dataset",
                            "id": "static-test-dataset"
                        },
                        "edition": {
                            "href": "/datasets/static-test-dataset/editions/time-series",
                            "id": "time-series"
                        },
                        "self": {
                            "href": "/datasets/static-test-dataset/editions/time-series/versions/1"
                        },
                        "web_page": {
                            "href": "economy/datasets/static-test-dataset/editions/time-series/versions/1"
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
        And I have a static dataset with version:
            """
            {
                "dataset": {
                    "id": "static-migrated-dataset-unpublished",
                    "title": "static migration title",
                    "description": "Migration Description",
                    "state": "created",
                    "type": "static",
                    "license": "Open Government License v3.0"
                },
                "version": {
                    "id": "v1-migration-approved",
                    "version": 1,
                    "edition": "time-series",
                    "state": "approved",
                    "type": "static",
                    "release_date": "2023-05-20",
                    "is_migration": true,
                    "links": {
                        "dataset": {
                            "href": "/datasets/static-migrated-dataset-unpublished",
                            "id": "static-migrated-dataset-unpublished"
                        },
                        "edition": {
                            "href": "/datasets/static-migrated-dataset-unpublished/editions/time-series",
                            "id": "time-series"
                        },
                        "self": {
                            "href": "/datasets/static-migrated-dataset-unpublished/editions/time-series/versions/1"
                        },
                        "web_page": {
                            "href": "economy/datasets/static-migrated-dataset-unpublished/editions/time-series/versions/1"
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
        And I have a static dataset with version:
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

    Scenario: GET /datasets/{id}/editions/{edition}/versions/1/metadata returns 200 for an authorised viewer
        Given private endpoints are enabled
        And I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
        And I have viewer access to the dataset edition "static-test-dataset/time-series"
        When I GET "/datasets/static-test-dataset/editions/time-series/versions/1/metadata"
        Then I should receive the following JSON response with status "200":
            """
            {
                "description": "Public Description",
                "distributions": [
                    {
                        "download_url": "uuid/data.csv",
                        "format": "csv",
                        "title": "Dataset CSV"
                    }
                ],
                "edition": "time-series",
                "id": "static-test-dataset",
                "last_updated": "0001-01-01T00:00:00Z",
                "license": "Open Government License v3.0",
                "links": {
                    "self": {
                        "href": "/datasets/static-test-dataset/editions/time-series/versions/1/metadata"
                    },
                    "version": {
                        "href": "/datasets/static-test-dataset/editions/time-series/versions/1",
                        "id": "1"
                    },
                    "website_version": {
                        "href": "economy/datasets/static-test-dataset/editions/time-series/versions/1"
                    }
                },
                "release_date": "2023-05-20",
                "state": "approved",
                "title": "static dataset title",
                "type": "static",
                "version": 1
            }
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions/1/metadata returns is_migration for an authorised viewer
        Given private endpoints are enabled
        And I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
        And I have viewer access to the dataset edition "static-migrated-dataset-unpublished/time-series"
        When I GET "/datasets/static-migrated-dataset-unpublished/editions/time-series/versions/1/metadata"
        Then I should receive the following JSON response with status "200":
            """
            {
                "description": "Migration Description",
                "distributions": [
                    {
                        "download_url": "uuid/data.csv",
                        "format": "csv",
                        "title": "Dataset CSV"
                    }
                ],
                "edition": "time-series",
                "id": "static-migrated-dataset-unpublished",
                "is_migration": true,
                "last_updated": "0001-01-01T00:00:00Z",
                "license": "Open Government License v3.0",
                "links": {
                    "self": {
                        "href": "/datasets/static-migrated-dataset-unpublished/editions/time-series/versions/1/metadata"
                    },
                    "version": {
                        "href": "/datasets/static-migrated-dataset-unpublished/editions/time-series/versions/1",
                        "id": "1"
                    },
                    "website_version": {
                        "href": "economy/datasets/static-migrated-dataset-unpublished/editions/time-series/versions/1"
                    }
                },
                "release_date": "2023-05-20",
                "state": "approved",
                "title": "static migration title",
                "type": "static",
                "version": 1
            }
            """

    Scenario: GET /datasets/{id}/editions/{edition}/versions/1/metadata does not return is_migration for an unauthenticated user
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
    
    Scenario: GET /datasets/{id}/editions/{edition}/versions/1 returns 403 for an unauthorised viewer
        Given private endpoints are enabled
        And I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
        And I don't have viewer access to the dataset edition "static-test-dataset/time-series"
        When I GET "/datasets/static-test-dataset/editions/time-series/versions/1/metadata"
        Then the HTTP status code should be "403"
