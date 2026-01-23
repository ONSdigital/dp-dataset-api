Feature: Dataset search by dataset id

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
                },
                {
                    "id": "filterable-dataset-1",
                    "title": "Filterable Dataset 1",
                    "state": "created",
                    "type": "static"
                },
                {
                    "id": "filterable-dataset-2",
                    "title": "Filterable Dataset 4",
                    "state": "created",
                    "type": "static"
                }
            ]
            """

    Scenario: Using an existing id as the param
        When I GET "/datasets?id=static-dataset-4"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "total_count": 1,
                "limit": 20,
                "offset": 0,
                "items": [
                    {
                        "id": "static-dataset-4",
                        "title": "Static Dataset 4",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    }
                ]
            }
            """

    Scenario: Using a partial string as id as the param
        When I GET "/datasets?id=static"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 4,
                "total_count": 4,
                "limit": 20,
                "offset": 0,
                "items": [
                    {
                        "id": "static-dataset-4",
                        "title": "Static Dataset 4",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
                    {
                        "id": "static-dataset-3",
                        "title": "Static Dataset 3",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
                    {
                        "id": "static-dataset-2",
                        "title": "Static Dataset 2",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
                    {
                        "id": "static-dataset-1",
                        "title": "Static Dataset 1",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    }
                ]
            }
            """

    Scenario: Using multiple query parameters and a partial string as id as the param
        When I GET "/datasets?id=static-dataset&type=static"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 4,
                "total_count": 4,
                "limit": 20,
                "offset": 0,
                "items": [
                    {
                        "id": "static-dataset-4",
                        "title": "Static Dataset 4",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
                    {
                        "id": "static-dataset-3",
                        "title": "Static Dataset 3",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
                    {
                        "id": "static-dataset-2",
                        "title": "Static Dataset 2",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
                    {
                        "id": "static-dataset-1",
                        "title": "Static Dataset 1",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    }
                ]
            }
            """

    Scenario: id is invalid
        When I GET "/datasets?id=invalid-dataset-id"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            dataset not found
            """

    Scenario: id is empty
        When I GET "/datasets?id="
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """

    Scenario: Viewer with permission to read the dataset receives 200
        Given private endpoints are enabled
        And I am a viewer user with permission
        And I have viewer access to the dataset "static-dataset-4"
        When I GET "/datasets/static-dataset-4"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "static-dataset-4",
                "current": {
                    "id": "static-dataset-4",
                    "last_updated": "0001-01-01T00:00:00Z",
                    "state": "created",
                    "title": "Static Dataset 4",
                    "type": "static"
                },
                "next": {
                    "id": "static-dataset-4",
                    "last_updated": "0001-01-01T00:00:00Z",
                    "state": "created",
                    "title": "Static Dataset 4",
                    "type": "static"
                }
            }
            """

    Scenario: Viewer with no permission to read the dataset receives 403
        Given private endpoints are enabled
        And I am a viewer user without permission
        And I don't have viewer access to the dataset "static-dataset-4"
        When I GET "/datasets/static-dataset-3"
        Then the HTTP status code should be "403"