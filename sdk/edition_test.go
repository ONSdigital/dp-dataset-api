package sdk

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

// Tests for the `GetEdition` client method
func TestGetEdition(t *testing.T) {
	mockGetResponse := models.Edition{
		DatasetID: datasetID,
		Edition:   editionID,
	}

	Convey("If requested edition is valid and get request returns 200", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedEdition, err := datasetAPIClient.GetEdition(ctx, headers, datasetID, editionID)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s", datasetID, editionID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
		Convey("Test that the requested edition is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedEdition, ShouldResemble, mockGetResponse)
		})
	})

	Convey("If requested edition is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrEditionNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetEdition(ctx, headers, datasetID, editionID)
		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrEditionNotFound.Error())
		})
	})
}

// Tests for the `GetEditions` client method
func TestGetEditions(t *testing.T) {
	editions := []models.Edition{
		{
			DatasetID: datasetID,
			Edition:   editionID,
		},
		{
			DatasetID: datasetID,
			Edition:   editionID,
		},
	}
	mockGetResponse := MockGetListRequestResponse{
		Items: editions,
	}

	Convey("If input query params are nil", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, nil, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		datasetAPIClient.GetEditions(ctx, headers, datasetID, nil)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions", datasetID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
	})
	Convey("If input query params are empty", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, nil, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		datasetAPIClient.GetEditions(ctx, headers, datasetID, &queryParams)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			// URI should be built with default values
			expectedURI := fmt.Sprintf("/datasets/%s/editions?limit=0&offset=0", datasetID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
	})
	Convey("If input query params are not empty but invalid", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, nil, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		// Create some invalid query params
		queryParams := QueryParams{
			IDs:       []string{"1", "2", "3"},
			IsBasedOn: "mytestdataset",
			Limit:     -1,
			Offset:    2,
		}
		_, err := datasetAPIClient.GetEditions(ctx, headers, datasetID, &queryParams)
		Convey("Test that the client method raises an error", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "negative offsets or limits are not allowed")
		})
	})
	Convey("If input query params are not empty and valid", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, nil, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		// Create some valid query params
		limit := 1
		offset := 2
		queryParams := QueryParams{
			IDs:       []string{"1", "2", "3"},
			IsBasedOn: "mytestdataset",
			Limit:     limit,
			Offset:    offset,
		}
		datasetAPIClient.GetEditions(ctx, headers, datasetID, &queryParams)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions?limit=%d&offset=%d", datasetID, limit, offset)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
	})
	Convey("If requested dataset and edition is valid", t, func() {
		requestedEditionList := EditionsList{
			Items: editions,
		}
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		returnedEditionsList, err := datasetAPIClient.GetEditions(ctx, headers, datasetID, &queryParams)
		Convey("Test that the requested edition is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedEditionsList, ShouldResemble, requestedEditionList)
		})
	})
	Convey("If requested dataset and edition is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrEditionNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		_, err := datasetAPIClient.GetEditions(ctx, headers, datasetID, &queryParams)
		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrEditionNotFound.Error())
		})
	})
}
