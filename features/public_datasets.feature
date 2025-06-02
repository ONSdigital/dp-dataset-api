Feature: Dataset API

    Scenario: GET /datasets
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates"
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

    Scenario: GET a specific dataset
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates"
                },
                {
                    "id": "income-by-age"
                }
            ]
            """
        When I GET "/datasets/income-by-age"
        Then I should receive the following JSON response with status "200":
            """
            {
                "id": "income-by-age",
                "last_updated":"0001-01-01T00:00:00Z"
            }
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
        