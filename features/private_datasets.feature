Feature: Private Dataset API

    Background:
        Given private endpoints are enabled
        And I am an admin user

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
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            dataset already exists
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
        Then I should receive the following JSON response with status "200":
            """
            {
                "survey": "mockSurvey",
                "last_updated":"0001-01-01T00:00:00Z"
            }
            """
        And the document in the database for id "population-estimates" should be:
        """
            {
                "id": "population-estimates",
                "survey": "mockSurvey"
            }
        """

    Scenario: Successfully change title of a static dataset
        Given I have these datasets:
            """
            [
                {
                    "id": "static-dataset",
                    "next": {
                        "id": "static-dataset",
                        "contacts": [
                            {
                                "name": "abc",
                                "email": "abc@email.com"
                            }
                        ],
                        "type": "static",
                        "title": "example 1",
                        "description": "some description",
                        "license": "Open Government Licence v3.0",
                        "topics": [ 
                            "topic-0", 
                            "topic-1"
                        ]
                    }
                }
            ]
            """
        When I PUT "/datasets/static-dataset"
            """
            {
                "id": "static-dataset",
                "contacts": [
                    {
                        "name": "abc",
                        "email": "abc@email.com"
                    }
                ],
                "type": "static",
                "title": "new title",
                "description": "some description",
                "license": "Open Government Licence v3.0",
                "topics": [ 
                    "topic-0", 
                    "topic-1"
                ]
            }
            """
        Then I should receive the following JSON response with status "200":
            """
            {
                "contacts": [
                    {
                        "name": "abc",
                        "email": "abc@email.com"
                    }
                ],
                "description": "some description",
                "license": "Open Government Licence v3.0",
                "title": "new title",
                "topics": [ 
                    "topic-0", 
                    "topic-1"
                ],
                "id": "static-dataset",
                "last_updated": "0001-01-01T00:00:00Z"
            }
            """
        And the document in the database for id "static-dataset" should be:
        """
            {
                "id": "static-dataset",
                "contacts": [
                    {
                        "name": "abc",
                        "email": "abc@email.com"
                    }
                ],
                "title": "new title",
                "description": "some description",
                "license": "Open Government Licence v3.0",
                "next": {
                    "topics": [ 
                        "topic-0", 
                        "topic-1"
                    ],
                    "type": "static"
                }
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
        Then I should receive the following JSON response with status "200":
            """
            {
                    "canonical_topic": "canonical-topic-ID",
                    "subtopics": ["subtopic-ID"],
                    "last_updated":"0001-01-01T00:00:00Z"
            }
            """
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
        Then I should receive the following JSON response with status "200":
            """
            {
                    "related_content": [{
		                "description": "Related content description",
		                "href": "http://localhost:22000/datasets/123/relatedContent",
		                "title": "Related content"
	                }],
                    "last_updated":"0001-01-01T00:00:00Z"
            }
            """
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
                            "id": "population-estimates",
                            "last_updated":"0001-01-01T00:00:00Z"
                        },
                        "current": {
                            "id": "population-estimates",
                            "last_updated":"0001-01-01T00:00:00Z"
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
            			"subtopics": ["subtopic-ID"],
                        "last_updated":"0001-01-01T00:00:00Z"
            		},
                    "current": {
                        "id": "population-estimates",
            			"canonical_topic": "canonical-topic-ID",
            			"subtopics": ["subtopic-ID"],
                        "last_updated":"0001-01-01T00:00:00Z"
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
                "topics": ["topic"],
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],
                "license":"license"
            }
            """
        Then the HTTP status code should be "409"
        And I should receive the following response:
            """
            dataset already exists
            """

    Scenario: Missing dataset ID in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "title": "title",
                "type": "static",
                "state": "anything",
                "next_release":"2016-04-04",
                "description": "census",
                "keywords":["keyword"],
                "topics": ["topic"],
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],
                "license":"license"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid fields: [ID]
            """

    Scenario: Missing dataset title in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "type": "static",
                "state": "anything",
                "next_release":"2016-04-04",
                "description": "census",
                "keywords":["keyword"],
                "topics": ["topic"],
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],
                "license":"license"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid fields: [Title]
            """

    Scenario: Missing dataset description in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "static",
                "state": "anything",
                "next_release":"2016-04-04",
                "keywords":["keyword"],
                "topics": ["topic"],
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],
                "license":"license"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid fields: [Description]
            """

    Scenario: Missing dataset keywords in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "static",
                "state": "anything",
                "next_release":"2016-04-04",
                "description": "census",
                "topics": ["topic"],
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],
                "license":"license"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid fields: [Keywords]
            """

    Scenario: Missing dataset next release in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "static",
                "state": "anything",
                "description": "census",
                "keywords":["keyword"],
                "topics": ["topic"],
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],
                "license":"license"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid fields: [NextRelease]
            """

    Scenario: Missing dataset topics in body and dataset type is static when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "static",
                "state": "anything",
                "next_release":"2016-04-04",
                "description": "census",
                "keywords":["keyword"],
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],
                "license":"license"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid fields: [Topics]
            """

    Scenario: Missing dataset contacts in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "static",
                "state": "anything",
                "next_release":"2016-04-04",
                "description": "census",
                "keywords":["keyword"],
                "topics": ["topic"],
                "license":"license"
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid fields: [Contacts]
            """

    Scenario: Missing dataset license in body when creating a new dataset
        When I POST "/datasets"
            """
            {
                "id": "ageing-population-estimates",
                "title": "title",
                "type": "static",
                "state": "anything",
                "next_release":"2016-04-04",
                "description": "census",
                "keywords":["keyword"],
                "contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],
                "topics": ["topic"]
            }
            """
        Then the HTTP status code should be "400"
        And I should receive the following response:
            """
            invalid fields: [License]
            """