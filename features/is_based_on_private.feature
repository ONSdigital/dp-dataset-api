Feature: Private Dataset API
    Background:
        Given private endpoints are enabled
        And I am identified as "user@ons.gov.uk"
        And I am authorised
        And I have these "public" datasets:
                """
                [
                  {
                    "id": "1",
                    "is_based_on": {
                      "@type": "",
                      "@id": "not-included"
                    }
                  },
                  {
                    "id": "test-1",
                    "is_based_on": {
                      "@type": "",
                      "@id": "included"
                    }
                  }
                ]
                """
        And I have these "private" datasets:
        """
        [
          {
            "id": "test-2",
            "is_based_on": {
              "@type": "",
              "@id": "included"
            }
          },
          {
            "id": "2",
            "is_based_on": {
              "@type": "",
              "@id": "not-included"
            }
          }
        ]
        """
    Scenario: Get /datasets is_based_on is provided
        When I GET "/datasets?is_based_on=included"
        Then I should receive the following JSON response with status "200":
        """
        {
           "count":2,
           "total_count":2,
           "limit":20,
           "offset":0,
           "items":[
              {
                 "id":"test-2",
                 "current":{
                    "id":"test-2",
                    "is_based_on":{
                       "@type":"",
                       "@id":"included"
                    },
                    "last_updated":"0001-01-01T00:00:00Z"
                 },
                 "next":{
                    "id":"test-2",
                    "is_based_on":{
                       "@type":"",
                       "@id":"included"
                    },
                    "last_updated":"0001-01-01T00:00:00Z"
                 }
              },
              {
                 "id":"test-1",
                 "current":{
                    "id":"test-1",
                    "is_based_on":{
                       "@type":"",
                       "@id":"included"
                    },
                    "last_updated":"0001-01-01T00:00:00Z"
                 }
              }
           ]
        }

        """
    Scenario: Get /datasets is_based_on is malformed
        When I GET "/datasets?is_based_on="
        Then the HTTP status code should be "400"

    Scenario: Get /datasets is_based_on returns nothing
        When I GET "/datasets?is_based_on=not-exists"
        Then the HTTP status code should be "404"
