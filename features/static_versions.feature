Feature: Dataset API

    Background:
        Given I have these datasets:
            """
            [
                {
                    "id": "static-dataset-1",
                    "title": "Static Dataset 1",
                    "state": "created",
                    "type": "static"
                },
                {
                    "id": "static-dataset-2",
                    "title": "Static Dataset 2",
                    "state": "created",
                    "type": "static"
                },
                {
                    "id": "static-dataset-3",
                    "title": "Static Dataset 3",
                    "state": "created",
                    "type": "static"
                },
                {
                    "id": "static-dataset-4",
                    "title": "Static Dataset 4",
                    "state": "created",
                    "type": "static"
                }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "test-version-id-1",
                    "edition": "January",
                    "edition_title": "January Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T07:00:00.000Z",
                    "state": "associated"
                },
                {
                    "id": "test-version-id-2",
                    "edition": "February",
                    "edition_title": "February Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-2"
                        }
                    },
                    "version": 2,
                    "release_date": "2025-02-01T07:00:00.000Z",
                    "state": "edition-confirmed"
                },
                {
                    "id": "test-version-id-3",
                    "edition": "March",
                    "edition_title": "March Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-3"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-03-01T07:00:00.000Z",
                    "state": "approved"
                },
                {
                    "id": "test-version-id-4",
                    "edition": "April",
                    "edition_title": "April Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-3"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-04-01T07:00:00.000Z",
                    "state": "approved"
                },
                {
                    "id": "test-version-id-5",
                    "edition": "May",
                    "edition_title": "May Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-4"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-05-01T07:00:00.000Z",
                    "state": "associated"
                },
                {
                    "id": "test-version-id-6",
                    "edition": "February",
                    "edition_title": "February Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-2"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-02-01T06:00:00.000Z",
                    "state": "published"
                }
            ]
            """
        
    Scenario: GET /dataset-editions returns all unpublished versions and returns 200
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/dataset-editions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 5,
                "items": [
                    {
                        "dataset_id": "static-dataset-4",
                        "edition": "May",
                        "edition_title": "May Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-4/editions/May/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-05-01T07:00:00.000Z",
                        "title": "Static Dataset 4"
                    },
                    {
                        "dataset_id": "static-dataset-3",
                        "edition": "April",
                        "edition_title": "April Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-3/editions/April/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-04-01T07:00:00.000Z",
                        "title": "Static Dataset 3"
                    },
                    {
                        "dataset_id": "static-dataset-3",
                        "edition": "March",
                        "edition_title": "March Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-3/editions/March/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-03-01T07:00:00.000Z",
                        "title": "Static Dataset 3"
                    },
                    {
                        "dataset_id": "static-dataset-2",
                        "edition": "February",
                        "edition_title": "February Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-2/editions/February/versions/2",
                            "id": "2"
                        },
                        "release_date": "2025-02-01T07:00:00.000Z",
                        "title": "Static Dataset 2"
                    },
                    {
                        "dataset_id": "static-dataset-1",
                        "edition": "January",
                        "edition_title": "January Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-1/editions/January/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-01-01T07:00:00.000Z",
                        "title": "Static Dataset 1"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 5
            }
            """

    Scenario: GET /dataset-editions?state=associated returns all associated versions and returns 200
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/dataset-editions?state=associated"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "dataset_id": "static-dataset-4",
                        "edition": "May",
                        "edition_title": "May Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-4/editions/May/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-05-01T07:00:00.000Z",
                        "title": "Static Dataset 4"
                    },
                    {
                        "dataset_id": "static-dataset-1",
                        "edition": "January",
                        "edition_title": "January Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-1/editions/January/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-01-01T07:00:00.000Z",
                        "title": "Static Dataset 1"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """

    Scenario: GET /dataset-editions?state=published returns 400
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/dataset-editions?state=published"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """
    
    Scenario: GET /dataset-editions returns 404 when there are no editions of static datasets
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        And there are no datasets
        When I GET "/dataset-editions"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            no editions were found
            """
    
    Scenario: GET /dataset-editions?state=associated returns 404 when there are no editions of static datasets that match the given state
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        And there are no datasets
        And I have these static versions:
            """
            [
                {
                    "id": "test-version-id-1",
                    "edition": "January",
                    "edition_title": "January Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T07:00:00.000Z",
                    "state": "edition-confirmed"
                }
            ]
            """

        When I GET "/dataset-editions?state=associated"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            no editions were found
            """
    
    Scenario: GET /dataset-editions returns 401 when user is not authorized
        Given private endpoints are enabled
        When I GET "/dataset-editions"
        Then the HTTP status code should be "401"