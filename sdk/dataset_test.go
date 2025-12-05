package sdk

import (
	"net/http"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

// Tests for the `GetDataset` client method
func TestGetDataset(t *testing.T) {
	mockGetResponse := models.Dataset{
		ID:           datasetID,
		CollectionID: collectionID,
		Title:        "Test Dataset",
		Description:  "Dataset for testing",
	}

	Convey("If requested dataset is valid and get request returns 200", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedDataset, err := datasetAPIClient.GetDataset(ctx, headers, datasetID)

		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := "/datasets/" + datasetID
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})

		Convey("Test that the requested dataset is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedDataset, ShouldResemble, mockGetResponse)
		})
	})

	Convey("If requested dataset is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrDatasetNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetDataset(ctx, headers, datasetID)

		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrDatasetNotFound.Error())
		})
	})

	Convey("If the request encounters a server error and returns 500", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusInternalServerError, "Internal server error", map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetDataset(ctx, headers, datasetID)

		Convey("Test that an error is raised with the correct message", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "Internal server error")
		})
	})

	Convey("If authenticated and dataset response includes a 'next' field", t, func() {
		// dataset response nested under "next"
		responseWithNext := &models.DatasetUpdate{
			Next: &mockGetResponse,
		}

		// Provide service token to simulate authenticated request
		authHeaders := Headers{
			AccessToken: "valid-token",
		}
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, responseWithNext, map[string]string{}})

		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedDataset, err := datasetAPIClient.GetDataset(ctx, authHeaders, datasetID)

		Convey("Test that the dataset is extracted from the 'next' object", func() {
			So(err, ShouldBeNil)
			So(returnedDataset.ID, ShouldEqual, datasetID)
			So(returnedDataset.CollectionID, ShouldEqual, collectionID)
			So(returnedDataset.Title, ShouldEqual, "Test Dataset")
			So(returnedDataset.Description, ShouldEqual, "Dataset for testing")
		})
	})
}

func TestClient_GetDatasetsInBatches(t *testing.T) {
	versionsResponse1 := List{
		Items:      []models.DatasetUpdate{{ID: "testDataset1"}},
		TotalCount: 2, // Total count is read from the first response to determine how many batches are required
		Offset:     0,
		Count:      1,
	}

	versionsResponse2 := List{
		Items:      []models.DatasetUpdate{{ID: "testDataset2"}},
		TotalCount: 2,
		Offset:     1,
		Count:      1,
	}

	expectedDatasets := List{
		Items: []models.DatasetUpdate{
			versionsResponse1.Items[0],
			versionsResponse2.Items[0],
		},
		Count:      2,
		TotalCount: 2,
	}

	batchSize := 1
	maxWorkers := 1

	Convey("When a 200 OK status is returned in 2 consecutive calls", t, func() {
		httpClient := createHTTPClientMock(
			MockedHTTPResponse{http.StatusOK, versionsResponse1, nil},
			MockedHTTPResponse{http.StatusOK, versionsResponse2, nil})

		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		processedBatches := []List{}
		var testProcess DatasetsBatchProcessor = func(batch List) (abort bool, err error) {
			processedBatches = append(processedBatches, batch)
			return false, nil
		}

		// Provide service token to simulate authenticated request
		authHeaders := Headers{
			AccessToken: "valid-token",
		}

		Convey("then GetDatasetsInBatches succeeds and returns the accumulated items from all the batches", func() {
			datasets, err := datasetAPIClient.GetDatasetsInBatches(ctx, authHeaders, batchSize, maxWorkers)

			So(err, ShouldBeNil)
			So(datasets, ShouldResemble, expectedDatasets)
		})

		Convey("then GetDatasetsBatchProcess calls the batchProcessor function twice, with the expected batches", func() {
			err := datasetAPIClient.GetDatasetsBatchProcess(ctx, headers, testProcess, batchSize, maxWorkers)
			So(err, ShouldBeNil)
			So(processedBatches, ShouldResemble, []List{versionsResponse1, versionsResponse2})
			So(httpClient.DoCalls(), ShouldHaveLength, 2)
			So(httpClient.DoCalls()[0].Req.URL.String(), ShouldResemble,
				"http://localhost:22000/datasets?limit=1&offset=0")
			So(httpClient.DoCalls()[1].Req.URL.String(), ShouldResemble,
				"http://localhost:22000/datasets?limit=1&offset=1")
		})
	})

	Convey("When a 400 error status is returned in the first call", t, func() {
		httpClient := createHTTPClientMock(
			MockedHTTPResponse{http.StatusBadRequest, "", nil})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		processedBatches := []List{}
		var testProcess DatasetsBatchProcessor = func(batch List) (abort bool, err error) {
			processedBatches = append(processedBatches, batch)
			return false, nil
		}

		// Provide service token to simulate authenticated request
		authHeaders := Headers{
			AccessToken: "valid-token",
		}

		Convey("then GetOptionsInBatches fails with the expected error and the process is aborted", func() {
			_, err := datasetAPIClient.GetDatasetsInBatches(ctx, authHeaders, batchSize, maxWorkers)
			So(err, ShouldNotBeNil)
			So(err.(*ErrInvalidDatasetAPIResponse).actualCode, ShouldEqual, http.StatusBadRequest)
			So(err.(*ErrInvalidDatasetAPIResponse).uri, ShouldResemble, "http://localhost:22000/datasets?limit=1&offset=0")
		})

		Convey("then GetDatasetsBatchProcess fails with the expected error and doesn't call the batchProcessor", func() {
			err := datasetAPIClient.GetDatasetsBatchProcess(ctx, headers, testProcess, batchSize, maxWorkers)
			So(err, ShouldNotBeNil)
			So(err.(*ErrInvalidDatasetAPIResponse).actualCode, ShouldEqual, http.StatusBadRequest)
			So(err.(*ErrInvalidDatasetAPIResponse).uri, ShouldResemble, "http://localhost:22000/datasets?limit=1&offset=0")
			So(processedBatches, ShouldResemble, []List{})
		})
	})

	Convey("When a 200 error status is returned in the first call and 400 error is returned in the second call", t, func() {
		httpClient := createHTTPClientMock(
			MockedHTTPResponse{http.StatusOK, versionsResponse1, nil},
			MockedHTTPResponse{http.StatusBadRequest, "", nil})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		// testProcess is a generic batch processor for testing
		processedBatches := []List{}
		var testProcess DatasetsBatchProcessor = func(batch List) (abort bool, err error) {
			processedBatches = append(processedBatches, batch)
			return false, nil
		}

		// Provide service token to simulate authenticated request
		authHeaders := Headers{
			AccessToken: "valid-token",
		}

		Convey("then GetDatasetsInBatches fails with the expected error, corresponding to the second batch, and the process is aborted", func() {
			_, err := datasetAPIClient.GetDatasetsInBatches(ctx, authHeaders, batchSize, maxWorkers)
			So(err, ShouldNotBeNil)
			So(err.(*ErrInvalidDatasetAPIResponse).actualCode, ShouldEqual, http.StatusBadRequest)
			So(err.(*ErrInvalidDatasetAPIResponse).uri, ShouldResemble, "http://localhost:22000/datasets?limit=1&offset=1")
		})

		Convey("then GetDatasetsBatchProcess fails with the expected error and calls the batchProcessor for the first batch only", func() {
			err := datasetAPIClient.GetDatasetsBatchProcess(ctx, headers, testProcess, batchSize, maxWorkers)
			So(err, ShouldNotBeNil)
			So(err.(*ErrInvalidDatasetAPIResponse).actualCode, ShouldEqual, http.StatusBadRequest)
			So(err.(*ErrInvalidDatasetAPIResponse).uri, ShouldResemble, "http://localhost:22000/datasets?limit=1&offset=1")
			So(processedBatches, ShouldResemble, []List{versionsResponse1})
		})
	})
}

func TestGetDatasetCurrentAndNext(t *testing.T) {
	mockGetResponse := models.DatasetUpdate{
		Next: &models.Dataset{ID: datasetID,
			CollectionID: collectionID,
			Title:        "Test Dataset",
			Description:  "Dataset for testing"},
		Current: &models.Dataset{ID: datasetID,
			CollectionID: collectionID,
			Title:        "Test Dataset",
			Description:  "Dataset for testing"},
	}

	Convey("If requested dataset is valid and get request returns 200", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedDataset, err := datasetAPIClient.GetDatasetCurrentAndNext(ctx, headers, datasetID)

		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := "/datasets/" + datasetID
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})

		Convey("Test that the requested dataset update object is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedDataset, ShouldResemble, mockGetResponse)
		})
	})

	Convey("If requested dataset is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrDatasetNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetDatasetCurrentAndNext(ctx, headers, datasetID)

		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrDatasetNotFound.Error())
		})
	})

	Convey("If the request encounters a server error and returns 500", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusInternalServerError, "Internal server error", map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetDatasetCurrentAndNext(ctx, headers, datasetID)

		Convey("Test that an error is raised with the correct message", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "Internal server error")
		})
	})

	Convey("If not authenticated then no response is returned ", t, func() {
		// dataset response nested under "next"
		responseWithNext := &models.DatasetUpdate{
			// Next: &mockGetResponse,
		}

		// Provide service token to simulate authenticated request
		authHeaders := Headers{
			AccessToken: "invalid-token",
		}
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusForbidden, responseWithNext, map[string]string{}})

		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedDataset, err := datasetAPIClient.GetDatasetCurrentAndNext(ctx, authHeaders, datasetID)

		Convey("Test that the dataset is extracted from the 'next' object", func() {
			So(err, ShouldNotBeNil)
			So(returnedDataset.ID, ShouldEqual, "")
		})
	})
}

func TestGetDatasetByPath(t *testing.T) {
	testPath := "datasets/custom/path"

	mockGetResponse := models.Dataset{
		ID:           datasetID,
		CollectionID: collectionID,
		Title:        "Test Dataset",
		Description:  "Dataset for testing",
	}

	Convey("If requested path is valid and get request returns 200", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedDataset, err := datasetAPIClient.GetDatasetByPath(ctx, headers, testPath)

		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := "/datasets/custom/path"
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})

		Convey("Test that the requested dataset is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedDataset, ShouldResemble, mockGetResponse)
		})
	})

	Convey("If path has leading/trailing slashes, they are correctly trimmed", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedDataset, err := datasetAPIClient.GetDatasetByPath(ctx, headers, "/datasets/custom/path/")

		Convey("Test that the request URI is constructed correctly with trimmed path", func() {
			expectedURI := "/datasets/custom/path"
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})

		Convey("Test that the requested dataset is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedDataset, ShouldResemble, mockGetResponse)
		})
	})

	Convey("If requested path is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrDatasetNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetDatasetByPath(ctx, headers, testPath)

		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrDatasetNotFound.Error())
		})
	})

	Convey("If the request encounters a server error and returns 500", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusInternalServerError, "Internal server error", map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetDatasetByPath(ctx, headers, testPath)

		Convey("Test that an error is raised with the correct message", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "Internal server error")
		})
	})
}

func TestGetDatasetEditions(t *testing.T) {
	mockedDatasetEditions := []models.DatasetEdition{
		{
			DatasetID:    "test-1",
			Title:        "Test Title 1",
			Edition:      "January",
			EditionTitle: "January Edition Title",
			LatestVersion: models.LinkObject{
				HRef: "/datasets/test-1/editions/January/versions/1",
				ID:   "1",
			},
			ReleaseDate: "2025-01-01",
		},
		{
			DatasetID:    "test-2",
			Title:        "Test Title 2",
			Edition:      "February",
			EditionTitle: "February Edition Title",
			LatestVersion: models.LinkObject{
				HRef: "/datasets/test-2/editions/February/versions/1",
				ID:   "1",
			},
			ReleaseDate: "2025-02-01",
		},
	}

	mockedGetResponse := DatasetEditionsList{
		Items:      mockedDatasetEditions,
		Count:      len(mockedDatasetEditions),
		Offset:     0,
		Limit:      20,
		TotalCount: len(mockedDatasetEditions),
	}

	Convey("Given valid query parameters", t, func() {
		queryParams := &QueryParams{
			Limit:  20,
			Offset: 0,
			State:  "associated",
		}

		Convey("When the API returns a successful response", func() {
			httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockedGetResponse, map[string]string{}})
			datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

			results, err := datasetAPIClient.GetDatasetEditions(ctx, headers, queryParams)
			So(err, ShouldBeNil)

			Convey("Then the response should match the expected dataset editions", func() {
				So(results, ShouldResemble, mockedGetResponse)
			})

			Convey("And the request URI should be constructed correctly", func() {
				expectedURI := "/dataset-editions?limit=20&offset=0&state=associated"
				So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
				So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
			})
		})

		Convey("When the API returns a 404 error", func() {
			httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrVersionsNotFound.Error(), map[string]string{}})
			datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

			_, err := datasetAPIClient.GetDatasetEditions(ctx, headers, queryParams)

			Convey("Then an editions not found error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, apierrors.ErrVersionsNotFound.Error())
			})

			Convey("And the request URI should be constructed correctly", func() {
				expectedURI := "/dataset-editions?limit=20&offset=0&state=associated"
				So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
				So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
			})
		})
	})

	Convey("Given invalid query parameters", t, func() {
		queryParams := &QueryParams{
			Limit:  -1,
			Offset: -1,
		}

		Convey("When the API is called", func() {
			httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusBadRequest, nil, map[string]string{}})
			datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

			_, err := datasetAPIClient.GetDatasetEditions(ctx, headers, queryParams)

			Convey("Then an error should be returned indicating the error", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "negative offsets or limits are not allowed")
			})
		})
	})
}

func Test_PutDataset(t *testing.T) {
	Convey("Given a valid dataset", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, "", nil})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		Convey("when put dataset is called", func() {
			dataset := models.Dataset{ID: "666"}
			err := datasetAPIClient.PutDataset(ctx, headers, "666", dataset)

			Convey("then no error is returned", func() {
				So(err, ShouldBeNil)
			})
		})
	})

	Convey("Given no auth token has been configured so the request is unauthorized", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusUnauthorized, "", nil})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		Convey("when put dataset is called", func() {
			dataset := models.Dataset{ID: "666"}
			err := datasetAPIClient.PutDataset(ctx, headers, "666", dataset)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "did not receive success response. received status 401")
			})
		})
	})

	Convey("given an invalid request body is provided then a 400 response is returned", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusBadRequest, "", nil})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		Convey("when put dataset is called", func() {
			dataset := models.Dataset{ID: ""}
			err := datasetAPIClient.PutDataset(ctx, headers, "666", dataset)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "did not receive success response. received status 400")
			})
		})
	})
}

// Test CreateDataset SDK method
func TestCreateDataset(t *testing.T) {
	Convey("Given a static dataset to be created", t, func() {
		mockStaticDataset := models.Dataset{
			ID:          "static-dataset-123",
			Title:       "Static Test Dataset",
			Description: "Static dataset for testing",
			Type:        "static",
			NextRelease: "2025-12-01",
			Keywords:    []string{"test", "static"},
			Contacts: []models.ContactDetails{
				{
					Name:      "Test Contact",
					Email:     "test@example.com",
					Telephone: "01234567890",
				},
			},
			Topics: []string{"economy"},
		}

		mockStaticResponse := models.DatasetUpdate{
			ID: "static-dataset-123",
			Next: &models.Dataset{
				ID:          "static-dataset-123",
				Title:       "Static Test Dataset",
				Description: "Static dataset for testing",
				Type:        "static",
				State:       "created",
				NextRelease: "2025-12-01",
				Keywords:    []string{"test", "static"},
				Contacts: []models.ContactDetails{
					{
						Name:      "Test Contact",
						Email:     "test@example.com",
						Telephone: "01234567890",
					},
				},
				Topics: []string{"economy"},
				Links: &models.DatasetLinks{
					Self: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/static-dataset-123",
					},
					Editions: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/static-dataset-123/editions",
					},
				},
			},
		}

		Convey("When the API returns 201 Created", func() {
			httpClient := createHTTPClientMock(MockedHTTPResponse{
				StatusCode: http.StatusCreated,
				Body:       mockStaticResponse,
				Headers:    map[string]string{},
			})
			datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

			returnedDataset, err := datasetAPIClient.CreateDataset(ctx, headers, mockStaticDataset)

			Convey("Then the static dataset should be created successfully", func() {
				So(err, ShouldBeNil)
				So(returnedDataset.ID, ShouldEqual, "static-dataset-123")
				So(returnedDataset.Next.Type, ShouldEqual, "static")
				So(returnedDataset.Next.State, ShouldEqual, "created")
				So(returnedDataset.Next.Keywords, ShouldResemble, []string{"test", "static"})
				So(returnedDataset.Next.Topics, ShouldResemble, []string{"economy"})
			})
		})
	})

	Convey("Given a filterable dataset to be created", t, func() {
		mockDataset := models.Dataset{
			ID:          datasetID,
			Title:       "Test Dataset",
			Description: "Dataset for testing",
			Type:        "filterable",
		}

		mockResponse := models.DatasetUpdate{
			ID: datasetID,
			Next: &models.Dataset{
				ID:          datasetID,
				Title:       "Test Dataset",
				Description: "Dataset for testing",
				Type:        "filterable",
				State:       "created",
				Links: &models.DatasetLinks{
					Self: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/" + datasetID,
					},
					Editions: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/" + datasetID + "/editions",
					},
				},
			},
		}

		Convey("When the API returns 201 Created", func() {
			httpClient := createHTTPClientMock(MockedHTTPResponse{
				StatusCode: http.StatusCreated,
				Body:       mockResponse,
				Headers:    map[string]string{},
			})
			datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

			returnedDataset, err := datasetAPIClient.CreateDataset(ctx, headers, mockDataset)

			Convey("Then the correct POST request should be made", func() {
				So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodPost)
				So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldEqual, "/datasets")
			})

			Convey("And the filterable dataset should be created successfully", func() {
				So(err, ShouldBeNil)
				So(returnedDataset.ID, ShouldEqual, datasetID)
				So(returnedDataset.Next.Title, ShouldEqual, "Test Dataset")
				So(returnedDataset.Next.State, ShouldEqual, "created")
			})
		})
	})

	Convey("Given dataset creation fails with 400 Bad Request", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{
			StatusCode: http.StatusBadRequest,
			Body:       apierrors.ErrAddUpdateDatasetBadRequest.Error(),
		})
		client := newDatasetAPIHealthcheckClient(t, httpClient)

		_, err := client.CreateDataset(ctx, headers, models.Dataset{})

		Convey("Then the correct error should be returned", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrAddUpdateDatasetBadRequest.Error())
		})
	})

	Convey("Given dataset already exists and API returns 403 Forbidden", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{
			StatusCode: http.StatusForbidden,
			Body:       apierrors.ErrAddDatasetAlreadyExists.Error(),
		})
		client := newDatasetAPIHealthcheckClient(t, httpClient)

		_, err := client.CreateDataset(ctx, headers, models.Dataset{})

		Convey("Then the correct error should be returned", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrAddDatasetAlreadyExists.Error())
		})
	})

	Convey("Given the server encounters a 500 Internal Server Error", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Internal server error",
		})
		client := newDatasetAPIHealthcheckClient(t, httpClient)

		_, err := client.CreateDataset(ctx, headers, models.Dataset{})

		Convey("Then a server error should be returned", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "Internal server error")
		})
	})
}
