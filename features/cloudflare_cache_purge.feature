Feature: Cloudflare Cache Purging on Version Publication

    Scenario: Cloudflare cache is purged when version is published
        Given I have a static dataset with version:
            """
            {
                "dataset": {
                    "id": "cache-test-dataset",
                    "title": "Cache Test Dataset",
                    "state": "associated",
                    "type": "static",
                    "links": {
                        "editions": {
                            "href": "/datasets/cache-test-dataset/editions"
                        },
                        "self": {
                            "href": "/datasets/cache-test-dataset"
                        }
                    }
                },
                "version": {
                    "id": "cache-test-version",
                    "edition": "2025",
                    "edition_title": "2025 Edition",
                    "links": {
                        "dataset": {
                            "id": "cache-test-dataset"
                        },
                        "edition": {
                            "href": "/datasets/cache-test-dataset/editions/2025",
                            "id": "2025"
                        },
                        "self": {
                            "href": "/datasets/cache-test-dataset/editions/2025/versions/1"
                        },
                        "version": {
                            "href": "/datasets/cache-test-dataset/editions/2025/versions/1",
                            "id": "1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T09:00:00.000Z",
                    "state": "approved",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "csv",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/datasets/cache-test-dataset/editions/2025/versions/1.csv",
                            "byte_size": 125000
                        }
                    ]
                }
            }
            """
        And private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/cache-test-dataset/editions/2025/versions/1/state"
            """
            {
                "state": "published"
            }
            """
        Then the HTTP status code should be "200"
        And cloudflare cache purge should have been called for dataset "cache-test-dataset" and edition "2025"

    Scenario: Cloudflare cache is not purged when version transitions to non-published state
        Given I have a static dataset with version:
            """
            {
                "dataset": {
                    "id": "cache-test-non-publish",
                    "title": "Cache Test Non-Publish",
                    "state": "created",
                    "type": "static"
                },
                "version": {
                    "id": "cache-test-version-2",
                    "edition": "2025",
                    "edition_title": "2025 Edition",
                    "links": {
                        "dataset": {
                            "id": "cache-test-non-publish"
                        },
                        "edition": {
                            "href": "/datasets/cache-test-non-publish/editions/2025",
                            "id": "2025"
                        },
                        "self": {
                            "href": "/datasets/cache-test-non-publish/editions/2025/versions/1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T09:00:00.000Z",
                    "state": "created",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "csv",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/datasets/cache-test-non-publish/editions/2025/versions/1.csv",
                            "byte_size": 125000
                        }
                    ]
                }
            }
            """
        And private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I PUT "/datasets/cache-test-non-publish/editions/2025/versions/1/state"
            """
            {
                "state": "associated"
            }
            """
        Then the HTTP status code should be "200"
        And cloudflare cache purge should not have been called

    Scenario: Publication succeeds even if Cloudflare cache purge fails
        Given I have a static dataset with version:
            """
            {
                "dataset": {
                    "id": "cache-test-fail",
                    "title": "Cache Test Fail",
                    "state": "associated",
                    "type": "static",
                    "links": {
                        "editions": {
                            "href": "/datasets/cache-test-fail/editions"
                        },
                        "self": {
                            "href": "/datasets/cache-test-fail"
                        }
                    }
                },
                "version": {
                    "id": "cache-test-version-3",
                    "edition": "2025",
                    "edition_title": "2025 Edition",
                    "links": {
                        "dataset": {
                            "id": "cache-test-fail"
                        },
                        "edition": {
                            "href": "/datasets/cache-test-fail/editions/2025",
                            "id": "2025"
                        },
                        "self": {
                            "href": "/datasets/cache-test-fail/editions/2025/versions/1"
                        },
                        "version": {
                            "href": "/datasets/cache-test-fail/editions/2025/versions/1",
                            "id": "1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T09:00:00.000Z",
                    "state": "approved",
                    "type": "static",
                    "distributions": [
                        {
                            "title": "csv",
                            "format": "csv",
                            "media_type": "text/csv",
                            "download_url": "/downloads/datasets/cache-test-fail/editions/2025/versions/1.csv",
                            "byte_size": 125000
                        }
                    ]
                }
            }
            """
        And private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        And cloudflare cache purge is configured to fail
        When I PUT "/datasets/cache-test-fail/editions/2025/versions/1/state"
            """
            {
                "state": "published"
            }
            """
        Then the HTTP status code should be "200"
