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
	testCurrentEdition := models.Edition{
		DatasetID: datasetID,
		Edition:   "current-edition",
	}

	testNextEdition := models.Edition{
		DatasetID: datasetID,
		Edition:   "next-edition",
	}

	testEditionUpdate := models.EditionUpdate{
		Current: &testCurrentEdition,
		Next:    &testNextEdition,
	}

	Convey("When the edition response is of type Edition", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, testCurrentEdition, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedEdition, err := datasetAPIClient.GetEdition(ctx, headers, datasetID, editionID)

		Convey("Then request URI should be constructed correctly", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s", datasetID, editionID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})

		Convey("And the edition should be returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedEdition, ShouldResemble, testCurrentEdition)
		})
	})

	Convey("When the edition response is of type EditionUpdate", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, testEditionUpdate, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedEdition, err := datasetAPIClient.GetEdition(ctx, headers, datasetID, editionID)

		Convey("Then request URI should be constructed correctly", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s", datasetID, editionID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})

		Convey("And the next edition should be returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedEdition, ShouldResemble, testNextEdition)
		})
	})

	Convey("When the edition is not found", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrEditionNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetEdition(ctx, headers, datasetID, editionID)

		Convey("Then an error should be returned containing the status code and error message", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "status 404")
			So(err.Error(), ShouldContainSubstring, apierrors.ErrEditionNotFound.Error())
		})
	})
}

// Tests for the `GetEditions` client method
func TestGetEditions(t *testing.T) {
	editions := []models.Edition{
		{DatasetID: datasetID, Edition: editionID},
		{DatasetID: datasetID, Edition: editionID},
	}

	currentEdition := models.Edition{DatasetID: datasetID, Edition: "current-edition"}
	nextEdition := models.Edition{DatasetID: datasetID, Edition: "next-edition"}

	editionUpdates := []models.EditionUpdate{
		{Current: &currentEdition, Next: &nextEdition},
		{Current: &currentEdition, Next: &nextEdition},
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

	Convey("When the items in the response are of type Edition", t, func() {
		mockBody := map[string]interface{}{"items": editions}
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockBody, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		returnedEditionsList, err := datasetAPIClient.GetEditions(ctx, headers, datasetID, &queryParams)

		Convey("Then the editions should be returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedEditionsList.Items, ShouldHaveLength, 2)
			So(returnedEditionsList.Items[0], ShouldResemble, editions[0])
			So(returnedEditionsList.Items[1], ShouldResemble, editions[1])
		})
	})

	Convey("When the items in the response are of type EditionUpdate", t, func() {
		mockBody := map[string]interface{}{"items": editionUpdates}
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockBody, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		returnedEditionsList, err := datasetAPIClient.GetEditions(ctx, Headers{}, datasetID, &queryParams)

		Convey("Then the next edition from each EditionUpdate should be returned", func() {
			So(err, ShouldBeNil)
			So(returnedEditionsList.Items, ShouldHaveLength, 2)
			So(returnedEditionsList.Items[0], ShouldResemble, nextEdition)
			So(returnedEditionsList.Items[1], ShouldResemble, nextEdition)
		})
	})

	Convey("If requested dataset and edition is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrEditionNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		_, err := datasetAPIClient.GetEditions(ctx, headers, datasetID, &queryParams)
		Convey("Test that an error is raised and should contain status code and error message", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "status 404")
			So(err.Error(), ShouldContainSubstring, apierrors.ErrEditionNotFound.Error())
		})
	})
}
