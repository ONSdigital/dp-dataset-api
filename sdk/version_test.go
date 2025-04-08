package sdk

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dpNetRequest "github.com/ONSdigital/dp-net/v2/request"
	. "github.com/smartystreets/goconvey/convey"
)

// Tests for the `QueryParams.Validate()` method
func TestQueryParamsValidate(t *testing.T) {
	Convey("If query params are valid", t, func() {
		// Create some valid query params
		queryParams := QueryParams{
			Offset:    1,
			Limit:     1,
			IsBasedOn: "IsBasedOn",
			IDs:       []string{"1", "2", "3"},
		}

		Convey("Test `Validate()` method returns nil", func() {
			So(queryParams.Validate(), ShouldBeNil)
		})
	})

	Convey("If query params are invalid", t, func() {
		// Create some valid query params
		queryParams := QueryParams{
			Offset:    1,
			Limit:     1,
			IsBasedOn: "IsBasedOn",
			IDs:       []string{"1", "2", "3"},
		}
		Convey("Test `Validate()` method raises error if `Offset` is negative", func() {
			// Update `queryParams` to make `Offset` negative
			queryParams.Offset = -1
			result := queryParams.Validate()
			So(result, ShouldNotBeNil)
			So(result.Error(), ShouldEqual, "negative offsets or limits are not allowed")
		})
		Convey("Test `Validate()` method raises error if `Limit` is negative", func() {
			// Update `queryParams` to make `Limit` negative
			queryParams.Limit = -1
			result := queryParams.Validate()
			So(result, ShouldNotBeNil)
			So(result.Error(), ShouldEqual, "negative offsets or limits are not allowed")
		})
		Convey("Test `Validate()` method raises error if `IDs` is too long", func() {
			// Update `queryParams` to make `IDs` longer than `maxIDs` constant
			iDsArray := make([]string, maxIDs+1)
			for i := range iDsArray {
				iDsArray[i] = strconv.Itoa(i)
			}

			queryParams.IDs = iDsArray
			result := queryParams.Validate()
			So(result, ShouldNotBeNil)
			So(result.Error(), ShouldEqual, fmt.Sprintf("too many query parameters have been provided. Maximum allowed: %d", maxIDs))
		})
	})
}

// Tests for the `GetVersion` client method
func TestGetVersion(t *testing.T) {
	datasetID := "1234"
	downloadServiceToken := "mydownloadservicetoken"
	collectionID := "collection"
	ctx := context.Background()
	editionID := "my-edition"
	serviceToken := "myservicetoken"
	userAccessToken := "myuseraccesstoken"

	versionID := 1

	requestedVersion := models.Version{
		CollectionID: collectionID,
		DatasetID:    datasetID,
		Edition:      editionID,
		Version:      versionID,
	}

	Convey("If requested version is valid and get request returns 200", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, requestedVersion, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedVersion, err := datasetAPIClient.GetVersion(ctx, userAccessToken, serviceToken,
			downloadServiceToken, collectionID, datasetID, editionID, strconv.Itoa(versionID))
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions/%s", datasetID, editionID, strconv.Itoa(versionID))
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
		Convey("Test that the correct auth headers are added to the request", func() {
			So(httpClient.DoCalls()[0].Req.Header.Get(dpNetRequest.CollectionIDHeaderKey), ShouldEqual, collectionID)
			So(httpClient.DoCalls()[0].Req.Header.Get(dpNetRequest.FlorenceHeaderKey), ShouldEqual, userAccessToken)
			So(httpClient.DoCalls()[0].Req.Header.Get(dpNetRequest.AuthHeaderKey), ShouldContainSubstring, serviceToken)
			So(httpClient.DoCalls()[0].Req.Header.Get(dpNetRequest.DownloadServiceHeaderKey), ShouldEqual, downloadServiceToken)
		})
		Convey("Test that the requested version is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedVersion, ShouldResemble, requestedVersion)
		})
	})

	Convey("If requested version is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrVersionNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetVersion(ctx, userAccessToken, serviceToken,
			downloadServiceToken, collectionID, datasetID, editionID, strconv.Itoa(versionID))
		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrVersionNotFound.Error())
		})
	})
}
