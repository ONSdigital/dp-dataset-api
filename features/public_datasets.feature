Feature: Dataset API

    Scenario: GET /datasets excludes unpublished datasets
        Given I have realistic datasets:
            """
            [
                {
                    "current": {"id": "population-estimates", "state": "published"},
                    "next": {"id": "population-estimates", "state": "published"}
                },
                {
                    "current": {"id": "income-by-age", "state": "published"},
                    "next": {"id": "income-by-age", "state": "published"}
                },
                {
                    "next": {"id": "cpih01", "state": "created"}
                }
            ]
            """
        When I GET "/datasets"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count":2,
                "items": [
                    {
                        "id": "population-estimates",
                        "last_updated":"0001-01-01T00:00:00Z",
                        "state": "published"
                    },
                    {
                        "id": "income-by-age",
                        "last_updated":"0001-01-01T00:00:00Z",
                        "state": "published"
                    }
                ],
                "limit":20, 
                "offset":0, 
                "total_count":2
            }
            """

    Scenario: GET a published dataset returns 200
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "published"
                },
                {
                    "id": "income-by-age",
                    "state": "published"
                }
            ]
            """
        When I GET "/datasets/income-by-age"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "income-by-age",
                "last_updated":"0001-01-01T00:00:00Z",
                "state": "published"
            }
            """

    Scenario: GET an unpublished dataset returns 404
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "state": "created"
                }
            ]
            """
        When I GET "/datasets/population-estimates"
        Then the HTTP status code should be "404"
        And I should receive the following response:
            """
            dataset not found
            """


    Scenario: Adding topic and survey fields to a dataset
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates"
                }
            ]
            """
        When I PUT "/datasets/population-estimates"
            """
            {
                    "canonical_topic": "canonical-topic-ID",
                    "subtopics": ["subtopic-ID"],
                    "survey": "mockSurvey"
            }
            """
        Then the HTTP status code should be "405"

    Scenario: Adding related content to a dataset
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates"
                }
            ]
            """
        When I PUT "/datasets/population-estimates"
            """
            {
                	"related_content": [{
		                "description": "Related content description",
		                "href": "http://localhost:22000/datasets/123/relatedContent",
		                "title": "Related content"
	                }]
            }
            """
        Then the HTTP status code should be "405"

    Scenario: Get /datasets does not return redacted fields
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "previous_series_id": ["old-dataset-id"],
                    "is_migration": true
                }
            ]
            """
        When I GET "/datasets"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count":1,
                "items": [
                    {
                        "id": "population-estimates",
                        "last_updated":"0001-01-01T00:00:00Z"
                    }
                ],
                "limit":20,
                "offset":0,
                "total_count":1
            }
            """

    Scenario: GET /datasets with URL rewriting enabled does not return redacted fields
        Given URL rewriting is enabled
        And I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "previous_series_id": ["old-dataset-id"],
                    "is_migration": true
                }
            ]
            """
        When I GET "/datasets"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count":1,
                "items": [
                    {
                        "id": "population-estimates",
                        "last_updated":"0001-01-01T00:00:00Z"
                    }
                ],
                "limit":20,
                "offset":0,
                "total_count":1
            }
            """
