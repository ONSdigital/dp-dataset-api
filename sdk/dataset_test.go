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

	Convey("If authenticated and dataset response includes a 'next' field", t, func() {
		// Simulated dataset response nested under "next"
		responseWithNext := map[string]interface{}{
			"next": map[string]interface{}{
				"id":            datasetID,
				"collection_id": collectionID,
				"title":         "Test Dataset",
				"description":   "Dataset for testing",
			},
		}

		// Provide service token to simulate authenticated request
		authHeaders := Headers{
			ServiceToken:    "valid-token",
			UserAccessToken: "",
		}
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, responseWithNext, map[string]string{}})

		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedDataset, err := datasetAPIClient.GetDataset(ctx, authHeaders, collectionID, datasetID)

		Convey("Test that the dataset is extracted from the 'next' object", func() {
			So(err, ShouldBeNil)
			So(returnedDataset.ID, ShouldEqual, datasetID)
			So(returnedDataset.CollectionID, ShouldEqual, collectionID)
			So(returnedDataset.Title, ShouldEqual, "Test Dataset")
			So(returnedDataset.Description, ShouldEqual, "Dataset for testing")
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
