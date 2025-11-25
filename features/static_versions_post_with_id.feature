Feature: POST /datasets/{dataset_id}/editions/{edition}/versions/{version}

    Background: We have existing datasets, editions and versions
        Given I have these datasets:
            """
            [
                {
                    "id": "static-dataset-1",
                    "title": "static dataset with published version",
                    "state": "published",
                    "type": "static"
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "static-version-published",
                    "edition": "2024",
                    "edition_title": "2024 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-1"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-1/editions/2024",
                            "id": "2024"
                        },
                        "version": {
                            "href": "/datasets/static-dataset-1/editions/2024/versions/1",
                            "id": "1"
                        },
                        "self": {
                            "href": "/datasets/static-dataset-1/editions/2024/versions/1"
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
                            "download_url": "/static-dataset-1/2024/1/filename.csv",
                            "byte_size": 150000
                        }
                    ]
                },
                {
                    "id": "static-version-unpublished",
                    "edition": "2025",
                    "edition_title": "2025 Edition",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-1"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-1/editions/2025",
                            "id": "2025"
                        },
                        "version": {
                            "href": "/datasets/static-dataset-1/editions/2025/versions/2",
                            "id": "2025"
                        },
                        "self": {
                            "href": "/datasets/static-dataset-1/editions/2025/versions/2"
                        }
                    },
                    "version": 2,
                    "release_date": "2025-01-01T09:00:00.000Z",
                    "state": "associated",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "Unpublished Dataset CSV",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/files/static-dataset-1/2025/2/filename.csv",
                            "byte_size": 150000
                        }
                    ]
                }
            ]
            """
    
    Scenario: Successfully creating a new version
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/static-dataset-1/editions/2024/versions/2"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "2024",
                "distributions": [
                    {
                        "title": "Full Dataset CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/files/static-dataset-1/2024/2/filename.csv",
                        "byte_size": 100
                    }
                ],
                "type": "static"
            }
            """
        Then I should receive the following JSON response with status "201":
            """
            {
                "dataset_id": "static-dataset-1",
                "distributions": [
                    {
                    "byte_size": 100,
                    "download_url": "/static-dataset-1/2024/2/filename.csv",
                    "format": "csv",
                    "media_type": "text/csv",
                    "title": "Full Dataset CSV"
                    }
                ],
                "links": {
                        "dataset": {
                            "href": "http://localhost:22000/datasets/static-dataset-1",
                            "id": "static-dataset-1"
                        },
                        "edition": {
                            "href": "http://localhost:22000/datasets/static-dataset-1/editions/2024",
                            "id": "2024"
                        },
                        "self": {
                            "href": "http://localhost:22000/datasets/static-dataset-1/editions/2024/versions/2"
                        }
                    },
                "edition": "2024",
                "edition_title": "2024",
                "last_updated": "{{DYNAMIC_RECENT_TIMESTAMP}}",
                "release_date": "2024-12-01T09:00:00.000Z",
                "state": "associated",
                "type": "static",
                "version": 2
            }
            """
        And the response header "ETag" should not be empty

    Scenario: Request without Authorization header returns 401
        Given private endpoints are enabled
        When I POST "/datasets/static-dataset-1/editions/2024/versions/2"
        """
        {
            "release_date": "2024-12-01T09:00:00.000Z",
            "edition_title": "2024",
            "distributions": [
                {
                    "title": "Full Dataset CSV",
                    "format": "csv",
                    "media_type": "text/csv",
                    "download_url": "/downloads/files/static-dataset-1/2024/2/filename.csv",
                    "byte_size": 100
                }
            ],
            "type": "static"
        }
        """
        Then the HTTP status code should be "401"
    
    Scenario: Request with a dataset that doesn't exist returns 404
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/missing/editions/2024/versions/2"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "2024",
                "distributions": [
                    {
                        "title": "Full Dataset CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/files/static-dataset-1/2024/2/filename.csv",
                        "byte_size": 100
                    }
                ],
                "type": "static"
            }
            """
        Then I should receive the following JSON response with status "404":
        """
        {
            "errors": [
                {
                    "code": "ErrDatasetNotFound",
                    "description": "dataset not found"
                }
            ]
        }
        """
    
    Scenario: Request with an edition that doesn't exist returns 404
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/static-dataset-1/editions/0/versions/2"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "2024",
                "distributions": [
                    {
                        "title": "Full Dataset CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/files/static-dataset-1/2024/2/filename.csv",
                        "byte_size": 100
                    }
                ],
                "type": "static"
            }
            """
        Then I should receive the following JSON response with status "404":
        """
        {
            "errors": [
                {
                    "code": "ErrEditionNotFound",
                    "description": "edition not found"
                }
            ]
        }
        """
    
    Scenario: Request with a version that already exists returns 409
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/static-dataset-1/editions/2024/versions/1"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "2024",
                "distributions": [
                    {
                        "title": "Full Dataset CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/files/static-dataset-1/2024/1/filename.csv",
                        "byte_size": 100
                    }
                ],
                "type": "static"
            }
            """
        Then I should receive the following JSON response with status "409":
        """
        {
            "errors": [
                {
                    "code": "ErrVersionAlreadyExists",
                    "description": "version already exists"
                }
            ]
        }
        """
    
    Scenario: Request with an invalid version returns 400
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/static-dataset-1/editions/2024/versions/invalid"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "2024",
                "distributions": [
                    {
                        "title": "Full Dataset CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/files/static-dataset-1/2024/2/filename.csv",
                        "byte_size": 100
                    }
                ],
                "type": "static"
            }
            """
        Then I should receive the following JSON response with status "400":
        """
        {
            "errors": [
                {
                    "code": "ErrInvalidQueryParameter",
                    "description": "invalid query parameter: version"
                }
            ]
        }
        """

    Scenario: Request with a type that isn't static returns 400
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/static-dataset-1/editions/2024/versions/2"
            """
            {
                "release_date": "2024-12-01T09:00:00.000Z",
                "edition_title": "2024",
                "distributions": [
                    {
                        "title": "Full Dataset CSV",
                        "format": "csv",
                        "media_type": "text/csv",
                        "download_url": "/downloads/files/static-dataset-1/2024/2/filename.csv",
                        "byte_size": 100
                    }
                ],
                "type": "not valid"
            }
            """
        Then I should receive the following JSON response with status "400":
        """
        {
            "errors": [
                {
                    "code": "InvalidType",
                    "description": "version type should be static"
                }
            ]
        }
        """
    
    Scenario: Request with all mandatory fields missing returns 400
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I POST "/datasets/missing/editions/2024/versions/2"
            """
            {
                "type": "static"
            }
            """
        Then I should receive the following JSON response with status "400":
        """
        {
            "errors": [
                {
                    "code": "ErrMissingParameters",
                    "description": "missing properties in JSON: release_date"
                },
                {
                    "code": "ErrMissingParameters",
                    "description": "missing properties in JSON: distributions"
                },
                {
                    "code": "ErrMissingParameters",
                    "description": "missing properties in JSON: edition_title"
                }
            ]
        }
        """
