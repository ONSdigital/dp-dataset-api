Feature: GET /datasets/{id} in publishing mode

    Background:
        Given I have realistic datasets:
            """
            [
                {
                    "next": {
                        "id": "unpublished-filterable-dataset",
                        "state": "created",
                        "title": "Unpublished Filterable Dataset",
                        "type": "filterable"
                    }
                },
                {
                    "next":{
                        "id": "unpublished-static-dataset",
                        "state": "created",
                        "title": "Unpublished Static Dataset",
                        "type": "static"
                    }
                }
            ]
            """
        And private endpoints are enabled
    
    Scenario: Retrieving an unpublished dataset returns 200 (non-static type)
        Given I am an admin user
        When I GET "/datasets/unpublished-filterable-dataset"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "unpublished-filterable-dataset",
                "next": {
                    "id": "unpublished-filterable-dataset",
                    "last_updated": "0001-01-01T00:00:00Z",
                    "state": "created",
                    "title": "Unpublished Filterable Dataset",
                    "type": "filterable"
                }
            }
            """
        And the total number of audit events should be 1
        And the number of events with action "READ" and resource "/datasets/unpublished-filterable-dataset" should be 1
    
    Scenario: Viewer with permission to read the dataset receives 200 (static type)
        Given I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
        And I have viewer access to the dataset "unpublished-static-dataset"
        When I GET "/datasets/unpublished-static-dataset"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "unpublished-static-dataset",
                "next": {
                    "id": "unpublished-static-dataset",
                    "last_updated": "0001-01-01T00:00:00Z",
                    "state": "created",
                    "title": "Unpublished Static Dataset",
                    "type": "static"
                }
            }
            """
        And the total number of audit events should be 1
        And the number of events with action "READ" and resource "/datasets/unpublished-static-dataset" should be 1

    Scenario: Viewer with no permission to read the dataset receives 403 (static type)
        Given I am a JWT user with email "viewer2@ons.gov.uk" and group "role-viewer-denied"
        When I GET "/datasets/unpublished-static-dataset"
        Then the HTTP status code should be "403"
    
    Scenario: Retrieving a non-existing dataset returns 404
        Given I am an admin user
        When I GET "/datasets/non-existing-dataset"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            dataset not found
            """
    
    Scenario: Renaming a static dataset preserves viewer access via previous series id
        Given I am an admin user
        And I have realistic datasets:
            """
            [
                {
                    "next": {
                        "id": "static-series-a",
                        "state": "created",
                        "title": "Static Series A",
                        "type": "static",
                        "topics": [
                            "shared-topic"
                        ]
                    }
                }
            ]
            """
        When I PUT "/datasets/static-series-a"
            """
            {
                "id": "static-series-b",
                "type": "static",
                "title": "Static Series A",
                "description": "A static dataset",
                "next_release": "2026-01-01T00:00:00Z",
                "license": "Open Government Licence v3.0",
                "contacts": [
                    {
                        "name": "John Doe",
                        "email": "john@example.com"
                    }
                ],
                "keywords": [
                    "economy"
                ],
                "topics": [
                    "shared-topic"
                ]
            }
            """
        Then the HTTP status code should be "200"
        And the dataset "static-series-a" should not exist
        And the number of events with action "UPDATE" and resource "/datasets/static-series-a" should be 1
        Given I am a JWT user with email "viewer3@ons.gov.uk" and group "role-viewer-allowed"
        And I have viewer access to the dataset "static-series-a"
        When I GET "/datasets/static-series-b"
        Then the HTTP status code should be "200"
        And the total number of audit events should be 2
        And the number of events with action "READ" and resource "/datasets/static-series-b" should be 1

    Scenario: Renaming a static dataset does not grant access to unrelated viewers
        Given I am an admin user
        And I have realistic datasets:
            """
            [
                {
                    "next": {
                        "id": "static-series-c",
                        "state": "created",
                        "title": "Static Series C",
                        "type": "static",
                        "topics": [
                            "shared-topic"
                        ]
                    }
                }
            ]
            """
        When I PUT "/datasets/static-series-c"
            """
            {
                "id": "static-series-d",
                "type": "static",
                "title": "Static Series C",
                "description": "A static dataset",
                "next_release": "2026-01-01T00:00:00Z",
                "license": "Open Government Licence v3.0",
                "contacts": [
                    {
                        "name": "John Doe",
                        "email": "john@example.com"
                    }
                ],
                "keywords": [
                    "economy"
                ],
                "topics": [
                    "shared-topic"
                ]
            }
            """
        Then the HTTP status code should be "200"
        And the number of events with action "UPDATE" and resource "/datasets/static-series-c" should be 1
        Given I am a JWT user with email "viewer4@ons.gov.uk" and group "role-viewer-allowed"
        And I have viewer access to the dataset "some-unrelated-dataset"
        When I GET "/datasets/static-series-d"
        Then the HTTP status code should be "403"
        And the total number of audit events should be 1