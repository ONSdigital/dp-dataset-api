Feature: Dataset sorting

    Background:
        Given I have these datasets:
            """
            [
                {
                    "id": "Static-dataset-1",
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
                    "id": "Static-dataset-3",
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

    Scenario: No sort_order param defaults to DESC (z to a, case sensitive)
        When I GET "/datasets"
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
                        "id": "static-dataset-2",
                        "title": "Static Dataset 2",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
                    {
                        "id": "Static-dataset-3",
                        "title": "Static Dataset 3",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
                    {
                        "id": "Static-dataset-1",
                        "title": "Static Dataset 1",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    }
                ]
            }
            """


    Scenario: sort_order=ASC returns datasets sorted a to z (case insensitive)
        When I GET "/datasets?sort_order=ASC"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 4,
                "total_count": 4,
                "limit": 20,
                "offset": 0,
                "items": [
                    {
                        "id": "Static-dataset-1",
                        "title": "Static Dataset 1",
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
                        "id": "Static-dataset-3",
                        "title": "Static Dataset 3",
                        "state": "created",
                        "type": "static",
                        "last_updated": "0001-01-01T00:00:00Z"
                    },
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

    Scenario: sort_order is invalid
        When I GET "/datasets?sort_order=INVALID"
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid query parameter
            """