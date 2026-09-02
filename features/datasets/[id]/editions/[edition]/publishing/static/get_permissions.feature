Feature: Get static edition in publishing mode (permissions)

  Background:
    Given private endpoints are enabled
    And I have realistic datasets:
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
            "state": "associated",
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

  Scenario: Get an edition as an authorised viewer
    Given I am a JWT user with email "viewer1@ons.gov.uk" and group "role-viewer-allowed"
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

  Scenario: Get an edition as an unauthorised viewer
    Given I am a JWT user with email "viewer2@ons.gov.uk" and group "role-viewer-denied"
    And I don't have viewer access to the dataset "population-estimates/January"
    When I GET "/datasets/population-estimates/editions/January"
    Then the HTTP status code should be "403"
