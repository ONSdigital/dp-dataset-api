Feature: Static versions GET /versions

    Background: We have static datasets, editions and versions for testing
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
                    "previous_edition_id": ["approved-old-edition-1", "approved-old-edition-2"],
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
                    "previous_edition_id": ["old-edition-1", "old-edition-2"]
                }
            ]
            """

    Scenario: GET /datasets/test-static/editions/test-edition-static-approved/versions in private mode returns all versions
        Given private endpoints are enabled
        And I am an admin user
        When I GET "/datasets/test-static/editions/test-edition-static-approved/versions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "dataset_id": "test-static",
                        "id": "test-static-version-approved",
                        "last_updated":"2021-01-01T00:00:01Z",
                        "type":"static",
                        "version": 1,
                        "state": "approved",
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
                        "edition": "test-edition-static-approved",
                        "edition_title": "Test Edition Static Approved Title",
                        "previous_edition_id": ["approved-old-edition-1", "approved-old-edition-2"],
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
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET /datasets/test-static/editions/test-edition-static-approved/versions in private mode rewrites all links when URL rewriting is enabled
        Given private endpoints are enabled
        And URL rewriting is enabled
        And I set the "X-Forwarded-Host" header to "api.example.com"
        And I set the "X-Forwarded-Path-Prefix" header to "v1"
        And I am an admin user
        When I GET "/datasets/test-static/editions/test-edition-static-approved/versions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "dataset_id": "test-static",
                        "id": "test-static-version-approved",
                        "last_updated":"2021-01-01T00:00:01Z",
                        "type":"static",
                        "version": 1,
                        "state": "approved",
                        "links": {
                            "dataset": {
                                "id": "test-static"
                            },
                            "edition": {
                                "href": "https://api.example.com/v1/datasets/test-static/editions/test-edition-static-approved",
                                "id": "test-edition-static-approved"
                            },
                            "self": {
                                "href": "https://api.example.com/v1/datasets/test-static/editions/test-edition-static-approved/versions/1"
                            },
                            "web_page": {
                                "href": "http://localhost:20000/economy/datasets/test-static/editions/test-edition-static-approved/versions/1"
                            }
                        },
                        "edition": "test-edition-static-approved",
                        "edition_title": "Test Edition Static Approved Title",
                        "previous_edition_id": ["approved-old-edition-1", "approved-old-edition-2"],
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
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET /datasets/test-static/editions/test-edition-static-approved/versions/1 in private mode rewrites all links when URL rewriting is enabled
        Given private endpoints are enabled
        And URL rewriting is enabled
        And I set the "X-Forwarded-Host" header to "api.example.com"
        And I set the "X-Forwarded-Path-Prefix" header to "v1"
        And I am an admin user
        When I GET "/datasets/test-static/editions/test-edition-static-approved/versions/1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "test-static-version-approved",
                "last_updated":"2021-01-01T00:00:01Z",
                "type":"static",
                "version": 1,
                "state": "approved",
                "links": {
                    "dataset": {
                        "id": "test-static"
                    },
                    "edition": {
                        "href": "https://api.example.com/v1/datasets/test-static/editions/test-edition-static-approved",
                        "id": "test-edition-static-approved"
                    },
                    "self": {
                        "href": "https://api.example.com/v1/datasets/test-static/editions/test-edition-static-approved/versions/1"
                    },
                    "web_page": {
                        "href": "http://localhost:20000/economy/datasets/test-static/editions/test-edition-static-approved/versions/1"
                    }
                },
                "edition": "test-edition-static-approved",
                "edition_title": "Test Edition Static Approved Title",
                "previous_edition_id": ["approved-old-edition-1", "approved-old-edition-2"],
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

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} records audit event with authorised user
        Given private endpoints are enabled
        And I am a publisher user
        When I GET "/datasets/test-static/editions/test-edition-static-approved/versions/1"
        Then the HTTP status code should be "200"
        And the total number of audit events should be 1
        And the number of events with action "READ" and resource "/datasets/test-static/editions/test-edition-static-approved/versions/1" should be 1

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} does not record audit event without auth
        When I GET "/datasets/test-static/editions/test-edition-published/versions/1"
        Then the HTTP status code should be "200"
        And the total number of audit events should be 0

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} for an unpublished dataset returns 404
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

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} for an unpublished edition returns 404
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

    Scenario: Get dataset with created state returns 404 in web mode
        When I GET "/datasets/test-created-dataset"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            dataset not found
            """

    Scenario: Get dataset with created state returns dataset in private mode
        When private endpoints are enabled
        And I am an admin user
        When I GET "/datasets/test-static"
        Then the HTTP status code should be "200"

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} returns is_migration when set
        Given private endpoints are enabled
        And I am an admin user
        When I GET "/datasets/test-static/editions/test-edition-static/versions/1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "test-static-version",
                "is_migration": true,
                "last_updated": "2021-01-01T00:00:00Z",
                "type": "static",
                "version": 1,
                "state": "associated",
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
                "edition": "test-edition-static",
                "edition_title": "Test Edition Static Title",
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

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} returns previous edition names when present
        Given private endpoints are enabled
        And I am an admin user
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
                "previous_edition_id": ["old-edition-1", "old-edition-2"],
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

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} returns version when edition matches previous_edition_id and caller is authorised
        Given private endpoints are enabled
        And I am an admin user
        When I GET "/datasets/test-static/editions/old-edition-1/versions/1"
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
                "previous_edition_id": ["old-edition-1", "old-edition-2"],
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

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} returns 404 when edition matches previous_edition_id but caller is not authorised
        And I am not authenticated
        When I GET "/datasets/test-static/editions/old-edition-1/versions/1"
        Then the HTTP status code should be "404"

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} returns 404 when edition does not match direct edition or previous_edition_id for an authorised caller
        Given private endpoints are enabled
        And I am an admin user
        When I GET "/datasets/test-static/editions/non-existent-legacy-edition/versions/1"
        Then the HTTP status code should be "404"

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} does not return previous edition names to public
        And I am not authenticated
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

    Scenario: GET /datasets/{id}/editions/{edition}/versions/{version} returns version when edition matches previous_edition_id and viewer user is authorised
       Given private endpoints are enabled
       And I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
       And I have viewer access to the dataset edition "test-static/approved-old-edition-2"
       When I GET "/datasets/test-static/editions/test-edition-static-approved/versions/1"
       Then I should receive the following JSON response with status "200":
            """
            {
                "id": "test-static-version-approved",
                "last_updated": "2021-01-01T00:00:01Z",
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
                "previous_edition_id": ["approved-old-edition-1", "approved-old-edition-2"],
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