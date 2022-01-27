Feature: Census endpoint
  As an API user
  I want to know all the population-types for Census 2021
  So that I can use them to query further data

  Scenario: The root census endpoint should return a list of population types
    Given I have some population types in cantabular
    When I access the root census endpoint
    Then a list of named cantabular population types is returned

    Scenario: If the root census endpoint fails, it should return correct errors
      Given cantabular is unresponsive
      When I access the root census endpoint
      Then the service responds with an internal server error saying "nope"