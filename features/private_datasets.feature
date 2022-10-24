Feature: Private Dataset API

    Background:
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised

    Scenario: Successfully creating a new dataset document
        When I POST "/datasets/ageing-population-estimates"
            """
            {
                "title": "CID"
            }
            """
        Then the HTTP status code should be "201"
        And the document in the database for id "ageing-population-estimates" should be:
            """
            {
                "id": "ageing-population-estimates",
                "state": "created",
                "title": "CID",
                "type": "filterable"
            }
            """

    Scenario: A document with the same ID already exists in the database
        Given I have these datasets:
            """
            [
                {
                    "id": "ageing-population-estimates"
                }
            ]
            """
        When I POST "/datasets/ageing-population-estimates"
            """
            {
                "title": "Hello"
            }
            """
        Then the HTTP status code should be "403"
        And I should receive the following response:
            """
            forbidden - dataset already exists
            """

    Scenario: Adding survey field to a dataset
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
                    "survey": "mockSurvey"
            }
            """
        Then the HTTP status code should be "200"
        And the document in the database for id "population-estimates" should be:
        """
            {
                "id": "population-estimates",
                "survey": "mockSurvey"
            }
        """

    Scenario: Adding topic fields to a dataset
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
                    "subtopics": ["subtopic-ID"]
            }
            """
        Then the HTTP status code should be "200"
        And the document in the database for id "population-estimates" should be:
        """
            {
                "id": "population-estimates",
                "canonical_topic": "canonical-topic-ID",
                "subtopics": ["subtopic-ID"]
            }
        """
    
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
        Then the HTTP status code should be "200"
        And the document in the database for id "population-estimates" should be:
        """
            {
                "id": "population-estimates",
                "related_content": [{
		                "description": "Related content description",
		                "href": "http://localhost:22000//datasets/123/relatedContent",
		                "title": "Related content"
	                }]
            }
        """

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
                "count": 1,
                "items": [
                    {
                        "id": "population-estimates",
                        "next": {
                            "id": "population-estimates"
                        },
                        "current": {
                            "id": "population-estimates"
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 1
            }
            """
    
    Scenario: GET /datasets with topics included
        Given I have these datasets:
            """
            [
                {
                    "id": "population-estimates",
                    "canonical_topic": "canonical-topic-ID",
                    "subtopics": ["subtopic-ID"]
                }
            ]
            """
        When I GET "/datasets"
        Then I should receive the following JSON response with status "200":
            """
            {
            	"count": 1,
            	"items": [{
            		"id": "population-estimates",
            		"next": {
            			"id": "population-estimates",
            			"canonical_topic": "canonical-topic-ID",
            			"subtopics": ["subtopic-ID"]
            		},
                    "current": {
                        "id": "population-estimates",
            			"canonical_topic": "canonical-topic-ID",
            			"subtopics": ["subtopic-ID"]
                    }
            	}],
            	"limit": 20,
            	"offset": 0,
            	"total_count": 1
            }
        """