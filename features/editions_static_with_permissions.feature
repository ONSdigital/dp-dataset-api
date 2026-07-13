Feature: Dataset API - Static Editions Permissions

    Background:
        Given I have realistic datasets:
            """
            [
              {
                  "current": {
                      "id": "population-estimates",
                      "title": "Static Dataset 3",
                      "description": "Static Dataset 3 Description",
                      "state": "published",
                      "type": "static"
                  },
                  "next": {
                      "id": "population-estimates",
                      "title": "Static Dataset 3",
                      "description": "Static Dataset 3 Description",
                      "state": "assosciated",
                      "type": "static"
                  }
              }
            ]
            """
        And I have these static versions:
            """
            [
                {
                    "id": "January-version-1",
                    "edition": "January",
                    "edition_title": "January Edition Title",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "edition": {
                            "href": "/datasets/population-estimates/editions/January",
                            "id": "January"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/January/versions/1"
                        },
                        "version": {
                            "href": "/datasets/population-estimates/editions/January/versions/1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-01-01T07:00:00.000Z",
                    "state": "published",
                    "type": "static"
                },
                {
                    "id": "January-version-2",
                    "edition": "January",
                    "edition_title": "January Edition Title",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "edition": {
                            "href": "/datasets/population-estimates/editions/January",
                            "id": "January"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/January/versions/2"
                        },
                        "version": {
                            "href": "/datasets/population-estimates/editions/January/versions/2"
                        }
                    },
                    "version": 2,
                    "release_date": "2025-03-01T07:00:00.000Z",
                    "state": "associated",
                    "type": "static"
                },
                {
                    "id": "February-version-1",
                    "edition": "February",
                    "edition_title": "February Edition Title",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "edition": {
                            "href": "/datasets/population-estimates/editions/February",
                            "id": "February"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/February/versions/1"
                        },
                        "version": {
                            "href": "/datasets/population-estimates/editions/February/versions/1"
                        }
                    },
                    "version": 1,
                    "release_date": "2025-02-01T07:00:00.000Z",
                    "state": "associated",
                    "type": "static"
                }
            ]
            """

    Scenario: GET /datasets/{id}/editions returns both published and associated editions for Admin
        Given private endpoints are enabled
        And I am an admin user
        When I GET "/datasets/population-estimates/editions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "next": {
                            "edition": "February",
                            "edition_title": "February Edition Title",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                },
                                "latest_version": {
                                    "href": "/datasets/population-estimates/editions/February/versions/1",
                                    "id": "1"
                                },
                                "self": {
                                    "href": "/datasets/population-estimates/editions/February",
                                    "id": "February"
                                },
                                "versions": {
                                    "href": "/datasets/population-estimates/editions/February/versions"
                                }
                            },
                            "release_date": "2025-02-01T07:00:00.000Z",
                            "state": "associated",
                            "version": 1
                        }
                    },
                    {
                        "current": {
                            "edition": "January",
                            "edition_title": "January Edition Title",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                },
                                "latest_version": {
                                    "href": "/datasets/population-estimates/editions/January/versions/1",
                                    "id": "1"
                                },
                                "self": {
                                    "href": "/datasets/population-estimates/editions/January",
                                    "id": "January"
                                },
                                "versions": {
                                    "href": "/datasets/population-estimates/editions/January/versions"
                                }
                            },
                            "release_date": "2025-01-01T07:00:00.000Z",
                            "state": "published",
                            "version": 1
                        },
                        "next": {
                            "edition": "January",
                            "edition_title": "January Edition Title",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                },
                                "latest_version": {
                                    "href": "/datasets/population-estimates/editions/January/versions/2",
                                    "id": "2"
                                },
                                "self": {
                                    "href": "/datasets/population-estimates/editions/January",
                                    "id": "January"
                                },
                                "versions": {
                                    "href": "/datasets/population-estimates/editions/January/versions"
                                }
                            },
                            "release_date": "2025-03-01T07:00:00.000Z",
                            "state": "associated",
                            "version": 2
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """

    Scenario: GET /datasets/{id}/editions returns paginated results for Admin
        Given private endpoints are enabled
        And I am an admin user
        When I GET "/datasets/population-estimates/editions?limit=1&offset=1"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 1,
                "items": [
                    {
                        "current": {
                            "edition": "January",
                            "edition_title": "January Edition Title",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                },
                                "latest_version": {
                                    "href": "/datasets/population-estimates/editions/January/versions/1",
                                    "id": "1"
                                },
                                "self": {
                                    "href": "/datasets/population-estimates/editions/January",
                                    "id": "January"
                                },
                                "versions": {
                                    "href": "/datasets/population-estimates/editions/January/versions"
                                }
                            },
                            "release_date": "2025-01-01T07:00:00.000Z",
                            "state": "published",
                            "version": 1
                        },
                        "next": {
                            "edition": "January",
                            "edition_title": "January Edition Title",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                },
                                "latest_version": {
                                    "href": "/datasets/population-estimates/editions/January/versions/2",
                                    "id": "2"
                                },
                                "self": {
                                    "href": "/datasets/population-estimates/editions/January",
                                    "id": "January"
                                },
                                "versions": {
                                    "href": "/datasets/population-estimates/editions/January/versions"
                                }
                            },
                            "release_date": "2025-03-01T07:00:00.000Z",
                            "state": "associated",
                            "version": 2
                        }
                    }
                ],
                "limit": 1,
                "offset": 1,
                "total_count": 2
            }
            """

    Scenario: GET /datasets/{id}/editions returns 200 for an authorised viewer
        Given private endpoints are enabled
        And I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
        And I have viewer access to the dataset "population-estimates"
        When I GET "/datasets/population-estimates/editions"
        Then I should receive the following JSON response with status "200":
            """
            {
                "count": 2,
                "items": [
                    {
                        "next": {
                            "edition": "February",
                            "edition_title": "February Edition Title",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                },
                                "latest_version": {
                                    "href": "/datasets/population-estimates/editions/February/versions/1",
                                    "id": "1"
                                },
                                "self": {
                                    "href": "/datasets/population-estimates/editions/February",
                                    "id": "February"
                                },
                                "versions": {
                                    "href": "/datasets/population-estimates/editions/February/versions"
                                }
                            },
                            "release_date": "2025-02-01T07:00:00.000Z",
                            "state": "associated",
                            "version": 1
                        }
                    },
                    {
                        "current": {
                            "edition": "January",
                            "edition_title": "January Edition Title",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                },
                                "latest_version": {
                                    "href": "/datasets/population-estimates/editions/January/versions/1",
                                    "id": "1"
                                },
                                "self": {
                                    "href": "/datasets/population-estimates/editions/January",
                                    "id": "January"
                                },
                                "versions": {
                                    "href": "/datasets/population-estimates/editions/January/versions"
                                }
                            },
                            "release_date": "2025-01-01T07:00:00.000Z",
                            "state": "published",
                            "version": 1
                        },
                        "next": {
                            "edition": "January",
                            "edition_title": "January Edition Title",
                            "links": {
                                "dataset": {
                                    "id": "population-estimates"
                                },
                                "latest_version": {
                                    "href": "/datasets/population-estimates/editions/January/versions/2",
                                    "id": "2"
                                },
                                "self": {
                                    "href": "/datasets/population-estimates/editions/January",
                                    "id": "January"
                                },
                                "versions": {
                                    "href": "/datasets/population-estimates/editions/January/versions"
                                }
                            },
                            "release_date": "2025-03-01T07:00:00.000Z",
                            "state": "associated",
                            "version": 2
                        }
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total_count": 2
            }
            """

    Scenario: GET /datasets/{id}/editions returns 403 for an unauthorised viewer
        Given private endpoints are enabled
        And I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
        And I don't have viewer access to the dataset "population-estimates"
        When I GET "/datasets/population-estimates/editions"
        Then the HTTP status code should be "403"

    Scenario: GET /datasets/{id}/editions/{edition} returns 200 for an authorised viewer
        Given private endpoints are enabled
        And I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
        And I have viewer access to the dataset "population-estimates/February"
        When I GET "/datasets/population-estimates/editions/February"
        Then I should receive the following JSON response with status "200":
            """
            {
                "next": {
                    "edition": "February",
                    "edition_title": "February Edition Title",
                    "links": {
                        "dataset": {
                            "id": "population-estimates"
                        },
                        "latest_version": {
                            "href": "/datasets/population-estimates/editions/February/versions/1",
                            "id": "1"
                        },
                        "self": {
                            "href": "/datasets/population-estimates/editions/February",
                            "id": "February"
                        },
                        "versions": {
                            "href": "/datasets/population-estimates/editions/February/versions"
                        }
                    },
                    "release_date": "2025-02-01T07:00:00.000Z",
                    "state": "associated",
                    "version": 1
                }
            }
            """

    Scenario: GET /datasets/{id}/editions/{edition} returns 403 for an unauthorised viewer
        Given private endpoints are enabled
        And I am a JWT user with email "viewer2@ons.gov.uk" and group "role-viewer-denied"
        And I don't have viewer access to the dataset "population-estimates/January"
        When I GET "/datasets/population-estimates/editions/January"
        Then the HTTP status code should be "403"