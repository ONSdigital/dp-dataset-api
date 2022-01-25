Feature: Census endpoint
  As an API user
  I want to know all the population-types for Census 2021
  So that I can use them to query further data

  Scenario: Not being allowed access to a private end point when no id provided
    When I access the /census endpoint
    Then a list of cantabular-blobs is returned