Feature: GET /datasets/{id} in publishing mode

    Background:
        Given I have these datasets:
            """
            [
                {
                    "id": "unpublished-filterable-dataset",
                    "state": "created",
                    "title": "Unpublished Filterable Dataset",
                    "type": "filterable"
                },
                {
                    "id": "unpublished-static-dataset",
                    "state": "created",
                    "title": "Unpublished Static Dataset",
                    "type": "static"
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
                "current": {
                    "id": "unpublished-filterable-dataset",
                    "last_updated": "0001-01-01T00:00:00Z",
                    "state": "created",
                    "title": "Unpublished Filterable Dataset",
                    "type": "filterable"
                },
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
        Given I am a viewer user with permission
        And I have viewer access to the dataset "unpublished-static-dataset"
        When I GET "/datasets/unpublished-static-dataset"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "unpublished-static-dataset",
                "current": {
                    "id": "unpublished-static-dataset",
                    "last_updated": "0001-01-01T00:00:00Z",
                    "state": "created",
                    "title": "Unpublished Static Dataset",
                    "type": "static"
                },
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
        Given I am a viewer user without permission
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