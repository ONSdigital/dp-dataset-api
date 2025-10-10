Feature: Dataset API

    Background:
        Given I have these datasets:
            """
            [
                {
                    "id": "static-dataset-1",
                    "title": "Static Dataset 1",
                    "description": "Static Dataset 1 Description",
                    "state": "created",
                    "type": "static"
                },
                {
                    "id": "static-dataset-2",
                    "title": "Static Dataset 2",
                    "description": "Static Dataset 2 Description",
                    "state": "created",
                    "type": "static"
                },
                {
                    "id": "static-dataset-3",
                    "title": "Static Dataset 3",
                    "description": "Static Dataset 3 Description",
                    "state": "created",
                    "type": "static"
                },
                {
                    "id": "static-dataset-4",
                    "title": "Static Dataset 4",
                    "description": "Static Dataset 4 Description",
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
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-1/editions/January",
                            "id": "January"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T07:00:00.000Z",
                    "state": "associated",
                    "type": "static"
                },
                {
                    "id": "test-version-id-2",
                    "edition": "February",
                    "edition_title": "February Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-2"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-2/editions/February",
                            "id": "February"
                        }
                    },
                    "version": 2,
                    "release_date": "2025-02-01T07:00:00.000Z",
                    "state": "edition-confirmed",
                    "type": "static"
                },
                {
                    "id": "test-version-id-3",
                    "edition": "March",
                    "edition_title": "March Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-3"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-3/editions/March",
                            "id": "March"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-03-01T07:00:00.000Z",
                    "state": "approved",
                    "type": "static"
                },
                {
                    "id": "test-version-id-4",
                    "edition": "April",
                    "edition_title": "April Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-3"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-3/editions/April",
                            "id": "April"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-04-01T07:00:00.000Z",
                    "state": "approved",
                    "type": "static"
                },
                {
                    "id": "test-version-id-5",
                    "edition": "May",
                    "edition_title": "May Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-4"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-4/editions/May",
                            "id": "May"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-05-01T07:00:00.000Z",
                    "state": "associated",
                    "type": "static"
                },
                {
                    "id": "test-version-id-6",
                    "edition": "February",
                    "edition_title": "February Edition Title",
                    "links": {
                        "dataset": {
                            "id": "static-dataset-2"
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-2/editions/February",
                            "id": "February"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-02-01T06:00:00.000Z",
                    "state": "published",
                    "type": "static"
                }
            ]
            """
        
    Scenario: GET /dataset-editions returns all versions and returns 200
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/dataset-editions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 6,
                "items": [
                     {
                         "dataset_id": "static-dataset-2",
                         "title": "Static Dataset 2",
                         "description": "Static Dataset 2 Description",
                         "edition": "February",
                         "edition_title": "February Edition Title",
                         "latest_version": {
                            "href": "/datasets/static-dataset-2/editions/February/versions/1",
                            "id": "1"
                        },
                         "release_date": "2025-02-01T06:00:00.000Z",
                         "state": "published"
                    },
                    {
                        "dataset_id": "static-dataset-4",
                        "title": "Static Dataset 4",
                        "description": "Static Dataset 4 Description",
                        "edition": "May",
                        "edition_title": "May Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-4/editions/May/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-05-01T07:00:00.000Z",
                        "state": "associated"
                    },
                    {
                        "dataset_id": "static-dataset-3",
                        "title": "Static Dataset 3",
                        "description": "Static Dataset 3 Description",
                        "edition": "April",
                        "edition_title": "April Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-3/editions/April/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-04-01T07:00:00.000Z",
                        "state": "approved"
                    },
                    {
                        "dataset_id": "static-dataset-3",
                        "title": "Static Dataset 3",
                        "description": "Static Dataset 3 Description",
                        "edition": "March",
                        "edition_title": "March Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-3/editions/March/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-03-01T07:00:00.000Z",
                        "state": "approved"
                    },
                    {
                        "dataset_id": "static-dataset-2",
                        "title": "Static Dataset 2",
                        "description": "Static Dataset 2 Description",
                        "edition": "February",
                        "edition_title": "February Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-2/editions/February/versions/2",
                            "id": "2"
                        },
                        "release_date": "2025-02-01T07:00:00.000Z",
                        "state": "edition-confirmed"
                    },
                    {
                        "dataset_id": "static-dataset-1",
                        "title": "Static Dataset 1",
                        "description": "Static Dataset 1 Description",
                        "edition": "January",
                        "edition_title": "January Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-1/editions/January/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-01-01T07:00:00.000Z",
                        "state": "associated"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 6
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
                        "title": "Static Dataset 4",
                        "description": "Static Dataset 4 Description",
                        "edition": "May",
                        "edition_title": "May Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-4/editions/May/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-05-01T07:00:00.000Z",
                        "state": "associated"
                    },
                    {
                        "dataset_id": "static-dataset-1",
                        "title": "Static Dataset 1",
                        "description": "Static Dataset 1 Description",
                        "edition": "January",
                        "edition_title": "January Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-1/editions/January/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-01-01T07:00:00.000Z",
                        "state": "associated"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """
    
    Scenario: GET /dataset-editions?published=true returns published versions only and returns 200
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/dataset-editions?published=true"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                     {
                         "dataset_id": "static-dataset-2",
                         "title": "Static Dataset 2",
                         "description": "Static Dataset 2 Description",
                         "edition": "February",
                         "edition_title": "February Edition Title",
                         "latest_version": {
                            "href": "/datasets/static-dataset-2/editions/February/versions/1",
                            "id": "1"
                        },
                         "release_date": "2025-02-01T06:00:00.000Z",
                         "state": "published"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """

    Scenario: GET /dataset-editions?published=false returns no published versions only and returns 200
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/dataset-editions?published=false"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 5,
                "items": [
                    {
                        "dataset_id": "static-dataset-4",
                        "title": "Static Dataset 4",
                        "description": "Static Dataset 4 Description",
                        "edition": "May",
                        "edition_title": "May Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-4/editions/May/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-05-01T07:00:00.000Z",
                        "state": "associated"
                    },
                    {
                        "dataset_id": "static-dataset-3",
                        "title": "Static Dataset 3",
                        "description": "Static Dataset 3 Description",
                        "edition": "April",
                        "edition_title": "April Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-3/editions/April/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-04-01T07:00:00.000Z",
                        "state": "approved"
                    },
                    {
                        "dataset_id": "static-dataset-3",
                        "title": "Static Dataset 3",
                        "description": "Static Dataset 3 Description",
                        "edition": "March",
                        "edition_title": "March Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-3/editions/March/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-03-01T07:00:00.000Z",
                        "state": "approved"
                    },
                    {
                        "dataset_id": "static-dataset-2",
                        "title": "Static Dataset 2",
                        "description": "Static Dataset 2 Description",
                        "edition": "February",
                        "edition_title": "February Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-2/editions/February/versions/2",
                            "id": "2"
                        },
                        "release_date": "2025-02-01T07:00:00.000Z",
                        "state": "edition-confirmed"
                    },
                    {
                        "dataset_id": "static-dataset-1",
                        "title": "Static Dataset 1",
                        "description": "Static Dataset 1 Description",
                        "edition": "January",
                        "edition_title": "January Edition Title",
                        "latest_version": {
                            "href": "/datasets/static-dataset-1/editions/January/versions/1",
                            "id": "1"
                        },
                        "release_date": "2025-01-01T07:00:00.000Z",
                        "state": "associated"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 5
            }
            """

    Scenario: GET /dataset-editions invalid published parameter
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/dataset-editions?published=123"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """
    
    Scenario: GET /dataset-editions with state and published parameters
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        When I GET "/dataset-editions?published=true&state=associated"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            cannot request state and published parameters at the same time
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
            no versions were found
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
                        },
                        "edition": {
                            "href": "/datasets/static-dataset-1/editions/January",
                            "id": "January"
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
            no versions were found
            """
    
    Scenario: GET /dataset-editions returns 401 when user is not authorized
        Given private endpoints are enabled
        When I GET "/dataset-editions"
        Then the HTTP status code should be "401"