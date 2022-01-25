Feature: Census endpoint
  As an API user
  I want to know all the population-types for Census 2021
  So that I can use them to query further data

  Scenario: The root census endpoint should return a list of blobs
    When I access the root census endpoint
    Then a list of cantabular blobs is returned