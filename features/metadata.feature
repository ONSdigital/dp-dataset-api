Feature: Dataset API - metadata

    Background:
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
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
                "title": "new title",
                "themes": ["new-canonical-topic-id", "a", "b"]                
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
                "title": "new title",
                "themes": ["new-canonical-topic-id", "a", "b"]                
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
                "title": "new title",
                "themes": ["new-canonical-topic-id", "a", "b"]
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
