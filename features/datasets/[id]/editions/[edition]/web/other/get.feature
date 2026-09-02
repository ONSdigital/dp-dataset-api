Feature: Get edition in web mode

  Scenario: Get an edition
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published"
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "population-estimates",
          "edition": "2019",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions/2019"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "population-estimates",
        "edition": "2019",
        "state": "published",
        "links": {
          "dataset": {
            "id": "population-estimates"
          }
        }
      }
      """

  Scenario: Get an edition with URL rewriting enabled
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published"
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "population-estimates",
          "edition": "2019",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    And URL rewriting is enabled
    When I GET "/datasets/population-estimates/editions/2019"
    Then I should receive the following JSON response with status "200":
      """
      {
        "id": "population-estimates",
        "edition": "2019",
        "state": "published",
        "links": {
          "dataset": {
            "id": "population-estimates"
          }
        }
      }
      """

  Scenario: Get an edition of an unpublished dataset
    Given I have these datasets:
      """
      [
        {
          "id": "unpublished-dataset",
          "state": "associated"
        }
      ]
      """
    When I GET "/datasets/unpublished-dataset/editions/unpublished-edition"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      dataset not found
      """

  Scenario: Get an edition of a dataset that does not exist
    When I GET "/datasets/non-existent-dataset/editions/january"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      dataset not found
      """

  Scenario: Get an unpublished edition
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published"
        }
      ]
      """
    And I have these editions:
      """
      [
        {
          "id": "1",
          "edition": "2019",
          "state": "published",
          "links": {
            "dataset": {
              "id": "population-estimates"
            }
          }
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions/unpublished-edition"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      edition not found
      """

  Scenario: Get an edition that does not exist
    Given I have these datasets:
      """
      [
        {
          "id": "population-estimates",
          "state": "published"
        }
      ]
      """
    When I GET "/datasets/population-estimates/editions/non-existent-edition"
    Then the HTTP status code should be "404"
    And I should receive the following response:
      """
      edition not found
      """
