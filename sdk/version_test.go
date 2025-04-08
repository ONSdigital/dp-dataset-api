package sdk

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
	dpNetRequest "github.com/ONSdigital/dp-net/v2/request"
	. "github.com/smartystreets/goconvey/convey"
)

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

	Convey("If requested version is valid", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, requestedVersion, map[string]string{}})
		datasetApiClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedVersion, err := datasetApiClient.GetVersion(ctx, userAccessToken, serviceToken,
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
}
