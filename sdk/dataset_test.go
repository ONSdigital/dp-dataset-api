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
		returnedDataset, err := datasetAPIClient.GetDataset(ctx, headers, collectionID, datasetID)

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
		_, err := datasetAPIClient.GetDataset(ctx, headers, collectionID, datasetID)

		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrDatasetNotFound.Error())
		})
	})

	Convey("If the request encounters a server error and returns 500", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusInternalServerError, "Internal server error", map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetDataset(ctx, headers, collectionID, datasetID)

		Convey("Test that an error is raised with the correct message", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "Internal server error")
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
			httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrEditionsNotFound.Error(), map[string]string{}})
			datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

			_, err := datasetAPIClient.GetDatasetEditions(ctx, headers, queryParams)

			Convey("Then an editions not found error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, apierrors.ErrEditionsNotFound.Error())
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
