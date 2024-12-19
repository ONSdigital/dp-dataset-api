Feature: Private Dataset API

    Background:
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised

    Scenario: Successfully creating a new dataset document
        When I POST "/datasets/ageing-population-estimates"
            """
            {
                "state": "anything",
                "title": "CID",
                "type": "filterable"
            }
            """
        Then the HTTP status code should be "201"
        And the document in the database for id "ageing-population-estimates" should be:
            """
            {
                "id": "ageing-population-estimates",
                "state": "created",
                "title": "CID",
                "type": "filterable",
                "links": {
                    "editions": {
                        "href":"http://localhost:22000/datasets/ageing-population-estimates/editions"
                    },
                    "self": {
                        "href":"http://localhost:22000/datasets/ageing-population-estimates"
                    }
                }
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
		                "href": "http://localhost:22000/datasets/123/relatedContent",
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

    Scenario: Successfully createing a new dataset document with ID in request body
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "canonical_topic": "canonical-topic-ID",
                "subtopics": ["subtopic-ID"],
                "state": "anything",
                "title": "CID",
                "type": "filterable",
                "description": "census",
                "keywords": ["keyword"],
                "next_release":"2016-04-04",
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}]
            }
            """
        Then the HTTP status code should be "201"
        And the document in the database for id "ageing-population-estimates" should be:
            """
            {
                "id": "ageing-population-estimates",
                "canonical_topic": "canonical-topic-ID",
                "subtopics": ["subtopic-ID"],
                "state": "created",
                "title": "CID",
                "type": "filterable",
                "links": {
                    "editions": {
                        "href":"http://localhost:22000/datasets/ageing-population-estimates/editions"
                    },
                    "self": {
                        "href":"http://localhost:22000/datasets/ageing-population-estimates"
                    }
                },
                "themes": ["canonical-topic-ID", "subtopic-ID"],
                "description": "census",
                "keywords": ["keyword"],
                "next_release":"2016-04-04",
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}]
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
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "static",
                "description": "description",
                "keywords": ["keyword"],
                "next_release":"2016-04-04",
                "themes": ["theme"],
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}]
            }
            """
        Then the HTTP status code should be "403"
        And I should receive the following response:
            """
            forbidden - dataset already exists
            """

    Scenario: Missing dataset ID in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "title": "",
                "state": "anything",
                "type": "filterable"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            missing dataset id in request body
            """

    Scenario: Missing dataset type in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "state": "anything"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            missing dataset type in request body
            """

    Scenario: Missing dataset title in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "type": "filterable",
                "state": "anything"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            missing dataset title in request body
            """

    Scenario: Missing dataset description in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "filterable",
                "state": "anything",
                "next_release":"2016-04-04"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            missing dataset description in request body
            """

    Scenario: Missing dataset keywords in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "filterable",
                "state": "anything",
                "description": "census",
                "next_release":"2016-04-04"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            missing dataset keywords in request body
            """

    Scenario: Missing dataset next release in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "filterable",
                "state": "anything",
                "description": "census",
                "keywords":["keyword"]
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            missing dataset next release in request body
            """

    Scenario: Missing dataset themes in body and dataset type is static when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "static",
                "state": "anything",
                "description": "census",
                "keywords":["keyword"],
                "next_release":"2016-04-04"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            missing dataset themes in request body
            """

    Scenario: Missing dataset contacts in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "static",
                "state": "anything",
                "description": "census",
                "keywords":["keyword"],
                "next_release":"2016-04-04",
                "themes": ["theme"]
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            missing dataset contacts in request body
            """