# Feature: Prevent Spaces in Dataset and Edition IDs

#     Background:
#         Given private endpoints are enabled
#         And I am an admin user

#     # Dataset ID validation scenarios

#     Scenario: Creating a dataset with spaces in the dataset ID should return 400
#         When I POST "/datasets"
#             """
#             {
#                 "id": "test dataset with spaces",
#                 "title": "Test Dataset",
#                 "type": "static",
#                 "state": "created"
#             }
#             """
#         Then the HTTP status code should be "400"
#         And I should receive the following response:
#             """
#             spaces are not allowed in the ID field
#             """

#     Scenario: Updating a dataset with spaces in the dataset ID should return 400
#         Given I have these datasets:
#             """
#             [
#                 {
#                     "id": "valid-dataset-id",
#                     "title": "Valid Dataset",
#                     "state": "created",
#                     "type": "static"
#                 }
#             ]
#             """
#         When I PUT "/datasets/valid dataset with spaces"
#             """
#             {
#                 "title": "Updated Dataset"
#             }
#             """
#         Then the HTTP status code should be "400"
#         And I should receive the following response:
#             """
#             spaces are not allowed in the ID field
#             """

#     Scenario: Updating a dataset with spaces in the request body ID should return 400
#         Given I have these datasets:
#             """
#             [
#                 {
#                     "id": "valid-dataset-id",
#                     "title": "Valid Dataset",
#                     "state": "created",
#                     "type": "static"
#                 }
#             ]
#             """
#         When I PUT "/datasets/valid-dataset-id"
#             """
#             {
#                 "id": "dataset with spaces",
#                 "title": "Updated Dataset"
#             }
#             """
#         Then the HTTP status code should be "400"
#         And I should receive the following response:
#             """
#             spaces are not allowed in the ID field
#             """

#     # Edition ID validation scenarios

#     Scenario: Creating an edition with spaces in the edition ID should return 400
#         Given I have these datasets:
#             """
#             [
#                 {
#                     "id": "valid-dataset-id",
#                     "title": "Valid Dataset",
#                     "state": "created",
#                     "type": "static"
#                 }
#             ]
#             """
#         When I POST "/datasets/valid-dataset-id/editions/edition with spaces/versions"
#             """
#             {
#                 "release_date": "2024-12-01T09:00:00.000Z",
#                 "edition_title": "Edition Title",
#                 "type": "static",
#                 "distributions": [
#                     {
#                         "title": "Full Dataset CSV",
#                         "format": "csv",
#                         "media_type": "text/csv",
#                         "download_url": "/downloads/files/test/edition/1/filename.csv",
#                         "byte_size": 100000
#                     }
#                 ]
#             }
#             """
#         Then the HTTP status code should be "400"
#         And I should receive the following response:
#             """
#             spaces are not allowed in the ID field
#             """

#     Scenario: Updating an edition with spaces in the edition ID should return 400
#         Given I have these datasets:
#             """
#             [
#                 {
#                     "id": "valid-dataset-id",
#                     "title": "Valid Dataset",
#                     "state": "associated",
#                     "type": "static"
#                 }
#             ]
#             """
#         And I have these static versions:
#             """
#             [
#                 {
#                     "id": "static-version-1",
#                     "edition": "valid-edition",
#                     "edition_title": "Valid Edition",
#                     "links": {
#                         "dataset": {
#                             "id": "valid-dataset-id"
#                         },
#                         "edition": {
#                             "href": "/datasets/valid-dataset-id/editions/valid-edition",
#                             "id": "valid-edition"
#                         }
#                     },
#                     "version": 1,
#                     "release_date": "2024-01-01T09:00:00.000Z",
#                     "state": "published",
#                     "type": "static",
#                     "distributions": [
#                         {
#                             "title": "Dataset CSV",
#                             "format": "csv",
#                             "media_type": "text/csv",
#                             "download_url": "/uuid/filename.csv",
#                             "byte_size": 150000
#                         }
#                     ]
#                 }
#             ]
#             """
#         When I PUT "/datasets/valid-dataset-id/editions/edition with spaces/versions/1"
#             """
#             {
#                 "release_date": "2024-12-01T09:00:00.000Z",
#                 "type": "static"
#             }
#             """
#         Then the HTTP status code should be "400"
#         And I should receive the following response:
#             """
#             spaces are not allowed in the ID field
#             """

#     Scenario: Updating a version with spaces in the edition field in request body should return 400
#         Given I have these datasets:
#             """
#             [
#                 {
#                     "id": "valid-dataset-id",
#                     "title": "Valid Dataset",
#                     "state": "associated",
#                     "type": "static"
#                 }
#             ]
#             """
#         And I have these static versions:
#             """
#             [
#                 {
#                     "id": "static-version-1",
#                     "edition": "valid-edition",
#                     "edition_title": "Valid Edition",
#                     "links": {
#                         "dataset": {
#                             "id": "valid-dataset-id"
#                         },
#                         "edition": {
#                             "href": "/datasets/valid-dataset-id/editions/valid-edition",
#                             "id": "valid-edition"
#                         }
#                     },
#                     "version": 1,
#                     "release_date": "2024-01-01T09:00:00.000Z",
#                     "state": "published",
#                     "type": "static",
#                     "distributions": [
#                         {
#                             "title": "Dataset CSV",
#                             "format": "csv",
#                             "media_type": "text/csv",
#                             "download_url": "/uuid/filename.csv",
#                             "byte_size": 150000
#                         }
#                     ]
#                 }
#             ]
#             """
#         When I PUT "/datasets/valid-dataset-id/editions/valid-edition/versions/1"
#             """
#             {
#                 "edition": "edition with spaces",
#                 "release_date": "2024-12-01T09:00:00.000Z",
#                 "type": "static"
#             }
#             """
#         Then the HTTP status code should be "400"
#         And I should receive the following response:
#             """
#             spaces are not allowed in the ID field
#             """

#     # Valid scenarios (no spaces)

#     Scenario: Creating a dataset without spaces in the ID should succeed
#         When I POST "/datasets"
#             """
#             {
#                 "id": "valid-dataset-id",
#                 "title": "Valid Dataset",
#                 "type": "static",
#                 "state": "created"
#             }
#             """
#         Then the HTTP status code should be "201"

#     Scenario: Creating an edition without spaces in the ID should succeed
#         Given I have these datasets:
#             """
#             [
#                 {
#                     "id": "valid-dataset-id",
#                     "title": "Valid Dataset",
#                     "state": "created",
#                     "type": "static"
#                 }
#             ]
#             """
#         When I POST "/datasets/valid-dataset-id/editions/valid-edition/versions"
#             """
#             {
#                 "release_date": "2024-12-01T09:00:00.000Z",
#                 "edition_title": "Valid Edition Title",
#                 "type": "static",
#                 "distributions": [
#                     {
#                         "title": "Full Dataset CSV",
#                         "format": "csv",
#                         "media_type": "text/csv",
#                         "download_url": "/downloads/files/test/edition/1/filename.csv",
#                         "byte_size": 100000
#                     }
#                 ]
#             }
#             """
#         Then the HTTP status code should be "201"
