Feature: Dataset API - metadata

    Background:
        Given private endpoints are enabled
        And I am an admin user
        And I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "canonical_topic": "canonical-topic-ID",
                    "subtopics": ["subtopic-ID"],
                    "state": "associated"
                },
                {
                    "id": "published-dataset",
                    "state": "published"
                }
            ]
            """
        And I have these editions:
            """
            [
                {
                    "id": "test-edition-1",
                    "edition": "hello",
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                },
                {
                    "id": "test-edition-2",
                    "edition": "2023",
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "published-dataset"
                        }
                    }
                }
            ]
            """
        And I have these versions:
            """
            [
                {
                    "id": "test-item-1",
                    "version": 1,
                    "state": "associated",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/hello/versions/1"
                        }
                    },
                    "edition": "hello"
                },
                {
                    "id": "test-item-2",
                    "version": 1,
                    "state": "associated",
                    "links": {
                        "dataset": {
                            "id": "published-dataset"
                        },
                        "self": {
                            "href": "/datasets/published-dataset/editions/2023/versions/1"
                        }
                    },
                    "edition": "2023"
                },
                {
                    "id": "test-item-3",
                    "version": 2,
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/hello/versions/2"
                        }
                    },
                    "edition": "hello"
                }
            ]
            """

    Scenario: Successful PUT metadata with valid etag
        When I set the "If-Match" header to "etag-test-item-1"
        And I PUT "/datasets/population-estimates/editions/hello/versions/1/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "200"
        And the document in the database for id "population-estimates" should be:
            """
            {
                "id": "population-estimates",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "state": "associated",
                "title": "new title"          
            }
            """
        And the version in the database for id "test-item-1" should be:
            """
            {
                "id": "test-item-1",
                "version": 1,
                "state": "associated",
                "links": {
                    "dataset": {
                        "id": "population-estimates"
                    },
                    "self": {
                        "href": "/datasets/population-estimates/editions/hello/versions/1"
                    }
                },
                "edition": "hello",
                "release_date": "today"
            }
            """
        And the total number of audit events should be 1
        And the number of events with action "UPDATE" and resource "/datasets/population-estimates/editions/hello/versions/1/metadata" should be 1

    Scenario: Successful PUT metadata with valid etag for a publisher user
        When I set the "If-Match" header to "etag-test-item-1"
        And I am a publisher user
        And I PUT "/datasets/population-estimates/editions/hello/versions/1/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "200"
        And the document in the database for id "population-estimates" should be:
            """
            {
                "id": "population-estimates",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "state": "associated",
                "title": "new title"          
            }
            """
        And the version in the database for id "test-item-1" should be:
            """
            {
                "id": "test-item-1",
                "version": 1,
                "state": "associated",
                "links": {
                    "dataset": {
                        "id": "population-estimates"
                    },
                    "self": {
                        "href": "/datasets/population-estimates/editions/hello/versions/1"
                    }
                },
                "edition": "hello",
                "release_date": "today"
            }
            """
        And the total number of audit events should be 1
        And the number of events with action "UPDATE" and resource "/datasets/population-estimates/editions/hello/versions/1/metadata" should be 1


    Scenario: Successful PUT metadata with no etag
        When I PUT "/datasets/population-estimates/editions/hello/versions/1/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "200"
        And the document in the database for id "population-estimates" should be:
            """
            {
                "id": "population-estimates",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "state": "associated",
                "title": "new title"            
            }
            """
        And the version in the database for id "test-item-1" should be:
            """
            {
                "id": "test-item-1",
                "version": 1,
                "state": "associated",
                "links": {
                    "dataset": {
                        "id": "population-estimates"
                    },
                    "self": {
                        "href": "/datasets/population-estimates/editions/hello/versions/1"
                    }
                },
                "edition": "hello",
                "release_date": "today"
            }
            """
        And the total number of audit events should be 1
        And the number of events with action "UPDATE" and resource "/datasets/population-estimates/editions/hello/versions/1/metadata" should be 1

    Scenario: Successful PUT metadata with * etag
        When I set the "If-Match" header to "*"
        And I PUT "/datasets/population-estimates/editions/hello/versions/1/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "200"
        And the document in the database for id "population-estimates" should be:
            """
            {
                "id": "population-estimates",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "state": "associated",
                "title": "new title"
            }
            """
        And the version in the database for id "test-item-1" should be:
            """
            {
                "id": "test-item-1",
                "version": 1,
                "state": "associated",
                "links": {
                    "dataset": {
                        "id": "population-estimates"
                    },
                    "self": {
                        "href": "/datasets/population-estimates/editions/hello/versions/1"
                    }
                },
                "edition": "hello",
                "release_date": "today"
            }
            """
        And the total number of audit events should be 1
        And the number of events with action "UPDATE" and resource "/datasets/population-estimates/editions/hello/versions/1/metadata" should be 1

    Scenario: PUT metadata on a dataset that is published
        When I set the "If-Match" header to "etag-test-item-2"
        And I PUT "/datasets/published-dataset/editions/2023/versions/1/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "403"
        And the document in the database for id "published-dataset" should be:
            """
            {
                "id": "published-dataset",
                "state": "published"
            }
            """
        And the version in the database for id "test-item-2" should be:
            """
            {
                "id": "test-item-2",
                "version": 1,
                "state": "associated",
                "links": {
                    "dataset": {
                        "id": "published-dataset"
                    },
                    "self": {
                        "href": "/datasets/published-dataset/editions/2023/versions/1"
                    }
                },
                "edition": "2023"
            }
            """

    Scenario: PUT metadata on a version that is published
        When I set the "If-Match" header to "etag-test-item-3"
        And I PUT "/datasets/population-estimates/editions/hello/versions/2/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "403"
        And the document in the database for id "population-estimates" should be:
            """
            {
                "id": "population-estimates",
                "canonical_topic": "canonical-topic-ID",
                "subtopics": ["subtopic-ID"],
                "state": "associated"
            }
            """
        And the version in the database for id "test-item-3" should be:
            """
            {
                "id": "test-item-3",
                "version": 2,
                "state": "published",
                "links": {
                    "dataset": {
                        "id": "population-estimates"
                    },
                    "self": {
                        "href": "/datasets/population-estimates/editions/hello/versions/2"
                    }
                },
                "edition": "hello"
            }
            """

    Scenario: PUT metadata using wrong version etag
        When I set the "If-Match" header to "wrong-etag"
        And I PUT "/datasets/population-estimates/editions/hello/versions/1/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "409"
        And the document in the database for id "population-estimates" should be:
            """
            {
                "id": "population-estimates",
                "canonical_topic": "canonical-topic-ID",
                "subtopics": ["subtopic-ID"],
                "state": "associated"
            }
            """
        And the version in the database for id "test-item-1" should be:
            """
            {
                "id": "test-item-1",
                "version": 1,
                "state": "associated",
                "links": {
                    "dataset": {
                        "id": "population-estimates"
                    },
                    "self": {
                        "href": "/datasets/population-estimates/editions/hello/versions/1"
                    }
                },
                "edition": "hello"
            }
            """

    Scenario: PUT metadata using non-existent dataset
        When I set the "If-Match" header to "an-etag"
        And I PUT "/datasets/unknown/editions/hello/versions/1/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "404"

    Scenario: PUT metadata using non-existent edition
        When I set the "If-Match" header to "an-etag"
        And I PUT "/datasets/population-estimates/editions/unknown/versions/1/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "404"

    Scenario: PUT metadata using non-existent version
        When I set the "If-Match" header to "an-etag"
        And I PUT "/datasets/population-estimates/editions/hello/versions/985/metadata"
            """
            {
                "title": "new title",
                "canonical_topic": "new-canonical-topic-id",
                "subtopics": ["a", "b"],
                "release_date": "today"
            }
            """
        Then the HTTP status code should be "404"


    Scenario: Successfully GET metadata for a published version
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "title": "title",
                    "description": "description",
                    "canonical_topic": "canonical-topic-ID",
                    "subtopics": ["subtopic-ID"],
                    "state": "published",
                    "next_release": "2022",
                    "contacts": [
                        {
                            "name": "name 1",
                            "email": "name@example.com",
                            "telephone": "01234 567890"
                        }
                    ],
                    "publisher": {
                        "name": "name",
                        "type": "type"
                    },
                    "keywords": ["population", "estimates"],
                    "license": "license",
                    "unit_of_measure": "people",
                    "uri": "http://example.com/population-estimates",
                    "theme": "population"
                }
            ]
            """
        And I have these editions:
            """
            [
                {
                    "id": "test-edition-1",
                    "edition": "2023",
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                }
            ]
            """
        And I have these versions:
            """
            [
                {
                    "id": "test-version-1",
                    "version": 1,
                    "state": "published",
                    "release_date": "2023-01-01T00:00:00.000Z",
                    "dimensions": [
                        {
                            "name": "geography",
                            "label": "label",
                            "description": "description"
                        },
                        {
                            "name": "age",
                            "label": "label",
                            "description": "description"
                        }
                    ],
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "edition": {
                            "id": "2023"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/2023/versions/1"
                        },
                        "version": {
                            "href": "/datasets/population-estimates/editions/2023/versions/1",
                            "id": "1"
                        }
                    },
                    "downloads": {
                        "csv": {
                            "href": "http://download-service/population-estimates.csv",
                            "size": "1000"
                        },
                        "xlsx": {
                            "href": "http://download-service/population-estimates.xlsx",
                            "size": "2000"
                        }
                    },
                    "edition": "2023"
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/2023/versions/1/metadata"
        Then I should receive the following JSON response with status "200":
            """
            {
            "canonical_topic": "canonical-topic-ID",
            "subtopics": ["subtopic-ID"],
            "contacts": [
                {
                "email": "name@example.com",
                "name": "name 1",
                "telephone": "01234 567890"
                }
            ],
            "description": "description",
            "dimensions": [
                {
                "description": "description",
                "label": "label",
                "links": {
                    "code_list": {},
                    "options": {},
                    "version": {}
                },
                "name": "geography"
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
                "href": "http://download-service/population-estimates.csv",
                "size": "1000"
                },
                "xlsx": {
                "href": "http://download-service/population-estimates.xlsx",
                "size": "2000"
                }
            },
            "edition": "2023",
            "id": "population-estimates",
            "keywords": ["population", "estimates"],
            "last_updated": "0001-01-01T00:00:00Z",
            "license": "license",
            "links": {
                "self": {
                "href": "/datasets/population-estimates/editions/2023/versions/1/metadata"
                },
                "version": {
                "href": "/datasets/population-estimates/editions/2023/versions/1"
                },
                "website_version": {
                "href": "http://localhost:20000/datasets/population-estimates/editions/2023/versions/1"
                }
            },
            "next_release": "2022",
            "publisher": {
                "name": "name",
                "type": "type"
            },
            "release_date": "2023-01-01T00:00:00.000Z",
            "theme": "population",
            "title": "title",
            "unit_of_measure": "people",
            "uri": "http://example.com/population-estimates",
            "version": 1,
            "state": "published"
            }
            """

    Scenario: GET metadata for unpublished version when authorised for an admin user
        Given private endpoints are enabled
        And I am an admin user
        And I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "title": "title",
                    "description": "description",
                    "state": "associated",
                    "next_release": "2022",
                    "contacts": [
                        {
                            "name": "name 1",
                            "email": "name@example.com",
                            "telephone": "01234 567890"
                        }
                    ]
                }
            ]
            """
        And I have these editions:
            """
            [
                {
                    "id": "test-edition-1",
                    "edition": "2023",
                    "state": "associated",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                }
            ]
            """
        And I have these versions:
            """
            [
                {
                    "id": "test-version-1",
                    "version": 1,
                    "state": "associated",
                    "release_date": "2023-01-01T00:00:00.000Z",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "edition": {
                            "id": "2023"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/2023/versions/1"
                        },
                        "version": {
                            "href": "/datasets/population-estimates/editions/2023/versions/1",
                            "id": "1"
                        }
                    },
                    "edition": "2023"
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/2023/versions/1/metadata"
        Then I should receive the following JSON response with status "200":
            """
            {
            "contacts": [
                {
                "name": "name 1",
                "email": "name@example.com",
                "telephone": "01234 567890"
                }
            ],
            "description": "description",
            "distribution": ["json"],
            "edition": "2023",
            "id": "population-estimates",
            "last_updated": "0001-01-01T00:00:00Z",
            "links": {
                "self": {
                "href": "/datasets/population-estimates/editions/2023/versions/1/metadata"
                },
                "version": {
                "href": "/datasets/population-estimates/editions/2023/versions/1"
                },
                "website_version": {
                "href": "http://localhost:20000/datasets/population-estimates/editions/2023/versions/1"
                }
            },
            "next_release": "2022",
            "release_date": "2023-01-01T00:00:00.000Z",
            "title": "title",
            "version": 1,
            "state": "associated"
            }
            """

    Scenario: GET metadata for unpublished version when authorised for a publisher user
        Given private endpoints are enabled
        And I am a publisher user
        And I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "title": "title",
                    "description": "description",
                    "state": "associated",
                    "next_release": "2022",
                    "contacts": [
                        {
                            "name": "name 1",
                            "email": "name@example.com",
                            "telephone": "01234 567890"
                        }
                    ]
                }
            ]
            """
        And I have these editions:
            """
            [
                {
                    "id": "test-edition-1",
                    "edition": "2023",
                    "state": "associated",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                }
            ]
            """
        And I have these versions:
            """
            [
                {
                    "id": "test-version-1",
                    "version": 1,
                    "state": "associated",
                    "release_date": "2023-01-01T00:00:00.000Z",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "edition": {
                            "id": "2023"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/2023/versions/1"
                        },
                        "version": {
                            "href": "/datasets/population-estimates/editions/2023/versions/1",
                            "id": "1"
                        }
                    },
                    "edition": "2023"
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/2023/versions/1/metadata"
        Then I should receive the following JSON response with status "200":
            """
            {
            "contacts": [
                {
                "name": "name 1",
                "email": "name@example.com",
                "telephone": "01234 567890"
                }
            ],
            "description": "description",
            "distribution": ["json"],
            "edition": "2023",
            "id": "population-estimates",
            "last_updated": "0001-01-01T00:00:00Z",
            "links": {
                "self": {
                "href": "/datasets/population-estimates/editions/2023/versions/1/metadata"
                },
                "version": {
                "href": "/datasets/population-estimates/editions/2023/versions/1"
                },
                "website_version": {
                "href": "http://localhost:20000/datasets/population-estimates/editions/2023/versions/1"
                }
            },
            "next_release": "2022",
            "release_date": "2023-01-01T00:00:00.000Z",
            "title": "title",
            "version": 1,
            "state": "associated"
            }
            """

    Scenario: GET metadata for non-existent dataset returns 404
        When I GET "/datasets/non-existent-dataset/editions/2023/versions/1/metadata"
        Then the HTTP status code should be "404"

    Scenario: GET metadata for non-existent edition returns 404
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "published"
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/non-existent-edition/versions/1/metadata"
        Then the HTTP status code should be "404"

    Scenario: GET metadata for non-existent version returns 404
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "published"
                }
            ]
            """
        And I have these editions:
            """
            [
                {
                    "id": "test-edition-1",
                    "edition": "2023",
                    "state": "published",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        }
                    }
                }
            ]
            """
        When I GET "/datasets/population-estimates/editions/2023/versions/999/metadata"
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
            
Scenario: GET metadata for a static dataset
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
    And the total number of audit events should be 1
    And the number of events with action "READ" and resource "/datasets/static-dataset/editions/time-series/versions/1" should be 1

Scenario: GET metadata for a static dataset with URL rewriting enabled
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
            "keywords": ["statistics", "population"],
            "last_updated": "0001-01-01T00:00:00Z",
            "license": "license",
            "next_release": "2023-12-01",
            "release_date": "2023-01-15",
            "title": "static title",
            "topics": ["economy", "demographics"],
            "links": {
                "self": {
                    "href": "https://api.example.com/v1/datasets/static-dataset/editions/time-series/versions/1/metadata"
                },
                "version": {
                    "href": "https://api.example.com/v1/datasets/static-dataset/editions/time-series/versions/1",
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
                    "download_url": "http://localhost:23600/downloads/files/uuid/filename.csv",
                    "byte_size": 100000
                }
            ]
        }
        """
    And the total number of audit events should be 1
    And the number of events with action "READ" and resource "/datasets/static-dataset/editions/time-series/versions/1" should be 1

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
    And the number of events with action "READ" and resource "/datasets/static-dataset/editions/time-series/versions/1" should be 1
