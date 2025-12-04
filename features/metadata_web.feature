Feature: Dataset API - metadata in WEB mode

Scenario: GET metadata for a published static dataset
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
                "keywords": ["statistics", "population"],
                "contacts": [
                    {
                        "name": "name",
                        "email": "name@example.com",
                        "telephone": "01234 567890"
                    }
                ],
                "topics": ["economy", "demographics"]
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
                        "id": "static-dataset"
                    },
                    "edition": {
                        "id": "time-series"
                    },
                    "self": {
                        "href": "/datasets/static-dataset/editions/time-series/versions/1"
                    },
                    "version": {
                        "href": "/datasets/static-dataset/editions/time-series/versions/1",
                        "id": "1"
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
            "keywords": ["statistics", "population"],
            "last_updated": "0001-01-01T00:00:00Z",
            "license": "license",
            "next_release": "2023-12-01",
            "release_date": "2023-01-15",
            "title": "static title",
            "topics": ["economy", "demographics"],
            "links": {
                "self": {
                    "href": "/datasets/static-dataset/editions/time-series/versions/1/metadata"
                },
                "version": {
                    "href": "/datasets/static-dataset/editions/time-series/versions/1",
                    "id": "1"
                },
                "website_version": {
                    "href": "http://localhost:20000/datasets/static-dataset/editions/time-series/versions/1"
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

Scenario: GET metadata for an unpublished static dataset
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
                "keywords": ["statistics", "population"],
                "contacts": [
                    {
                        "name": "name",
                        "email": "name@example.com",
                        "telephone": "01234 567890"
                    }
                ],
                "topics": ["economy", "demographics"]
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
                        "id": "static-dataset"
                    },
                    "edition": {
                        "id": "time-series"
                    },
                    "self": {
                        "href": "/datasets/static-dataset/editions/time-series/versions/1"
                    },
                    "version": {
                        "href": "/datasets/static-dataset/editions/time-series/versions/1",
                        "id": "1"
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

Scenario: GET metadata for Cantabular flexible table dataset
    Given I have these datasets:
        """
        [
            {
                "id": "cantabular-flexible-table",
                "title": "title",
                "description": "description",
                "state": "published",
                "type": "cantabular_flexible_table",
                "uri": "http://example.com/cantabular-flexible-table",
                "is_based_on": {
                    "id": "cpih01",
                    "@type": "cantabular_flexible_table"
                },
                "links": {
                    "self": {
                        "href": "/datasets/cantabular-flexible-table",
                        "id": "cantabular-flexible-table"
                    }
                }
            }
        ]
        """
    And I have these editions:
        """
        [
            {
                "id": "cantabular-edition-1",
                "edition": "2023",
                "state": "published",
                "links": {
                    "dataset": {
                        "id": "cantabular-flexible-table"
                    }
                }
            }
        ]
        """
    And I have these versions:
        """
        [
            {
                "id": "cantabular-version-1",
                "version": 1,
                "state": "published",
                "release_date": "2023-01-01T00:00:00.000Z",
                "dimensions": [
                    {
                        "name": "region",
                        "label": "region",
                        "description": "region"
                    },
                    {
                        "name": "age",
                        "label": "label",
                        "description": "description"
                    }
                ],
                "downloads": {
                    "csv": {
                        "href": "http://download-service/cantabular-data.csv",
                        "size": "5000"
                    }
                },
                "links": {
                    "dataset": {
                        "id": "cantabular-flexible-table"
                    },
                    "edition": {
                        "id": "2023"
                    },
                    "self": {
                        "href": "/datasets/cantabular-flexible-table/editions/2023/versions/1"
                    },
                    "version": {
                        "href": "/datasets/cantabular-flexible-table/editions/2023/versions/1",
                        "id": "1"
                    }
                },
                "edition": "2023"
            }
        ]
        """
    When I GET "/datasets/cantabular-flexible-table/editions/2023/versions/1/metadata"
    Then I should receive the following JSON response with status "200":
        """
        {
        "dataset_links": {
            "self": {
            "href": "/datasets/cantabular-flexible-table", 
            "id": "cantabular-flexible-table"
            }
        },
        "description": "description",
        "dimensions": [
            {
            "description": "region",
            "label": "region",
            "links": {
                "code_list": {},
                "options": {},
                "version": {}
            },
            "name": "region"
            },
            {
            "description": "description",
            "label": "label",
            "links": {
                "code_list": {},
                "options": {},
                "version": {}
            },
            "name": "age"
            }
        ],
        "distribution": ["json", "csv"],
        "downloads": {
            "csv": {
            "href": "http://download-service/cantabular-data.csv",
            "size": "5000"
            }
        },
        "is_based_on": {
            "@id": "",
            "@type": "cantabular_flexible_table"
        },
        "last_updated": "0001-01-01T00:00:00Z",
        "release_date": "2023-01-01T00:00:00.000Z",
        "title": "title",
        "uri": "http://example.com/cantabular-flexible-table",
        "version": 1,
        "state": "published"
        }
        """

Scenario: GET metadata for an unpublished Cantabular flexible table dataset
    Given I have these datasets:
        """
        [
            {
                "id": "cantabular-flexible-table",
                "title": "title",
                "description": "description",
                "state": "published",
                "type": "cantabular_flexible_table",
                "uri": "http://example.com/cantabular-flexible-table",
                "is_based_on": {
                    "id": "cpih01",
                    "@type": "cantabular_flexible_table"
                },
                "links": {
                    "self": {
                        "href": "/datasets/cantabular-flexible-table",
                        "id": "cantabular-flexible-table"
                    }
                }
            }
        ]
        """
    And I have these editions:
        """
        [
            {
                "id": "cantabular-edition-1",
                "edition": "2023",
                "state": "published",
                "links": {
                    "dataset": {
                        "id": "cantabular-flexible-table"
                    }
                }
            }
        ]
        """
    And I have these versions:
        """
        [
            {
                "id": "cantabular-version-1",
                "version": 1,
                "state": "published",
                "release_date": "2023-01-01T00:00:00.000Z",
                "dimensions": [
                    {
                        "name": "region",
                        "label": "region",
                        "description": "region"
                    },
                    {
                        "name": "age",
                        "label": "label",
                        "description": "description"
                    }
                ],
                "downloads": {
                    "csv": {
                        "href": "http://download-service/cantabular-data.csv",
                        "size": "5000"
                    }
                },
                "links": {
                    "dataset": {
                        "id": "cantabular-flexible-table"
                    },
                    "edition": {
                        "id": "2023"
                    },
                    "self": {
                        "href": "/datasets/cantabular-flexible-table/editions/2023/versions/1"
                    },
                    "version": {
                        "href": "/datasets/cantabular-flexible-table/editions/2023/versions/1",
                        "id": "1"
                    }
                },
                "edition": "2023"
            }
        ]
        """
    When I GET "/datasets/cantabular-flexible-table/editions/2023/versions/1/metadata"
    Then I should receive the following JSON response with status "200":
        """
            {
            "dataset_links": {
                "self": {
                "href": "/datasets/cantabular-flexible-table", 
                "id": "cantabular-flexible-table"
                }
            },
            "description": "description",
            "dimensions": [
                {
                "description": "region",
                "label": "region",
                "links": {
                    "code_list": {},
                    "options": {},
                    "version": {}
                },
                "name": "region"
                },
                {
                "description": "description",
                "label": "label",
                "links": {
                    "code_list": {},
                    "options": {},
                    "version": {}
                },
                "name": "age"
                }
            ],
            "distribution": ["json", "csv"],
            "downloads": {
                "csv": {
                "href": "http://download-service/cantabular-data.csv",
                "size": "5000"
                }
            },
            "is_based_on": {
                "@id": "",
                "@type": "cantabular_flexible_table"
            },
            "last_updated": "0001-01-01T00:00:00Z",
            "release_date": "2023-01-01T00:00:00.000Z",
            "title": "title",
            "uri": "http://example.com/cantabular-flexible-table",
            "version": 1,
            "state": "published"
            }
        """

Scenario: GET metadata for a Cantabular flexible table dataset
    Given I have these datasets:
        """
        [
            {
                "id": "cantabular-flexible-table",
                "title": "title",
                "description": "description",
                "state": "associated",
                "type": "cantabular_flexible_table",
                "uri": "http://example.com/cantabular-flexible-table",
                "is_based_on": {
                    "id": "cpih01",
                    "@type": "cantabular_flexible_table"
                },
                "links": {
                    "self": {
                        "href": "/datasets/cantabular-flexible-table",
                        "id": "cantabular-flexible-table"
                    }
                }
            }
        ]
        """
    And I have these editions:
        """
        [
            {
                "id": "cantabular-edition-1",
                "edition": "2023",
                "state": "associated",
                "links": {
                    "dataset": {
                        "id": "cantabular-flexible-table"
                    }
                }
            }
        ]
        """
    And I have these versions:
        """
        [
            {
                "id": "cantabular-version-1",
                "version": 1,
                "state": "associated",
                "release_date": "2023-01-01T00:00:00.000Z",
                "dimensions": [
                    {
                        "name": "region",
                        "label": "region",
                        "description": "region"
                    },
                    {
                        "name": "age",
                        "label": "label",
                        "description": "description"
                    }
                ],
                "downloads": {
                    "csv": {
                        "href": "http://download-service/cantabular-data.csv",
                        "size": "5000"
                    }
                },
                "links": {
                    "dataset": {
                        "id": "cantabular-flexible-table"
                    },
                    "edition": {
                        "id": "2023"
                    },
                    "self": {
                        "href": "/datasets/cantabular-flexible-table/editions/2023/versions/1"
                    },
                    "version": {
                        "href": "/datasets/cantabular-flexible-table/editions/2023/versions/1",
                        "id": "1"
                    }
                },
                "edition": "2023"
            }
        ]
        """
    When I GET "/datasets/cantabular-flexible-table/editions/2023/versions/1/metadata"
    Then the HTTP status code should be "404"