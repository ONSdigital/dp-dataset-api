package sdk

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

// This is the shape of a paginated list get request response
type MockGetListRequestResponse struct {
	Items      interface{}
	Count      int
	Offset     int
	Limit      int
	TotalCount int
}

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
	versionID := 1

	mockGetResponse := models.Version{
		CollectionID: collectionID,
		DatasetID:    datasetID,
		Edition:      editionID,
		Version:      versionID,
	}

	Convey("If requested version is valid and get request returns 200", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedVersion, err := datasetAPIClient.GetVersion(ctx, headers, datasetID, editionID, strconv.Itoa(versionID))
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions/%d", datasetID, editionID, versionID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
		Convey("Test that the requested version is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedVersion, ShouldResemble, mockGetResponse)
		})
	})

	Convey("If requested version is not valid and get request returns 404", t, func() {
		responseErr := models.ErrorResponse{
			Errors: []models.Error{
				{
					Cause:       apierrors.ErrVersionNotFound,
					Code:        "version_not_found",
					Description: "version not found",
				},
			},
		}
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, responseErr, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetVersion(ctx, headers, datasetID, editionID, strconv.Itoa(versionID))
		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, apierrors.ErrVersionNotFound.Error())
		})
	})
}

// Tests for the `GetVersionDimensions` client method
func TestGetVersionDimensions(t *testing.T) {
	versionID := 1
	versionDimensions := []models.Dimension{
		{
			Description: "my 1st dimension",
			ID:          "1",
		},
		{
			Description: "my 2nd dimension",
			ID:          "2",
		},
	}
	mockGetResponse := MockGetListRequestResponse{
		Items: versionDimensions,
	}

	Convey("If requested version is valid and get request returns 200", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedVersionDimensions, err := datasetAPIClient.GetVersionDimensions(ctx, headers, datasetID, editionID, strconv.Itoa(versionID))
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions/%d/dimensions", datasetID, editionID, versionID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
		Convey("Test that the requested version is returned without error", func() {
			expectedVersionDimensionsList := VersionDimensionsList{
				Items: versionDimensions,
			}
			So(err, ShouldBeNil)
			So(returnedVersionDimensions, ShouldResemble, expectedVersionDimensionsList)
		})
	})
	Convey("If requested version is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrVersionNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetVersionDimensions(ctx, headers, datasetID, editionID, strconv.Itoa(versionID))
		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrVersionNotFound.Error())
		})
	})
}

// Tests for the `GetVersionDimensionOptions` client method
func TestGetVersionDimensionOptions(t *testing.T) {
	versionID := "1"
	dimensionID := "1"
	dimensionOptions := []models.PublicDimensionOption{
		{
			Label: "my 1st option",
		},
		{
			Label: "my 2nd option",
		},
	}
	mockGetResponse := MockGetListRequestResponse{
		Items: dimensionOptions,
	}

	Convey("If input query params are nil", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, nil, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		datasetAPIClient.GetVersionDimensionOptions(ctx, headers, datasetID, editionID, versionID, dimensionID, nil)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions/%s/dimensions/%s/options", datasetID, editionID, versionID, dimensionID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
	})
	Convey("If input query params are empty", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, nil, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		datasetAPIClient.GetVersionDimensionOptions(ctx, headers, datasetID, editionID, versionID, dimensionID, &queryParams)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			// URI should be built with default values
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions/%s/dimensions/%s/options?limit=0&offset=0", datasetID, editionID, versionID, dimensionID)
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
		_, err := datasetAPIClient.GetVersionDimensionOptions(ctx, headers, datasetID, editionID, versionID, dimensionID, &queryParams)
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
		datasetAPIClient.GetVersionDimensionOptions(ctx, headers, datasetID, editionID, versionID, dimensionID, &queryParams)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions/%s/dimensions/%s/options?limit=%d&offset=%d", datasetID, editionID, versionID, dimensionID, limit, offset)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
	})
	Convey("If requested dataset and edition is valid", t, func() {
		requestedVersionDimensionOptions := VersionDimensionOptionsList{
			Items: dimensionOptions,
		}
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		returnedVersionDimensionOptions, err := datasetAPIClient.GetVersionDimensionOptions(ctx, headers, datasetID, editionID, versionID, dimensionID, &queryParams)
		Convey("Test that the requested version is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedVersionDimensionOptions, ShouldResemble, requestedVersionDimensionOptions)
		})
	})
	Convey("If requested dataset and edition is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrVersionNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		_, err := datasetAPIClient.GetVersionDimensionOptions(ctx, headers, datasetID, editionID, versionID, dimensionID, &queryParams)
		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrVersionNotFound.Error())
		})
	})
}

// Tests for the `GetVersionDimensionOptions` model `ToString` method
func TestGetVersionDimensionOptionsToString(t *testing.T) {
	Convey("If VersionDimensionOptionsList model is empty", t, func() {
		m := VersionDimensionOptionsList{}
		Convey("Test `ToString()` method returns an empty string", func() {
			So(m.ToString(), ShouldEqual, "")
		})
	})
	Convey("If VersionDimensionOptionsList model is not empty", t, func() {
		dimensionOptions := []models.PublicDimensionOption{
			{
				Label:  "my 1st option",
				Name:   "Option 1",
				Option: "op1",
			},
			{
				Label:  "my 2nd option",
				Name:   "Option 2",
				Option: "op2",
			},
		}
		m := VersionDimensionOptionsList{
			Items: dimensionOptions,
		}
		Convey("Test `ToString()` method returns the correct string", func() {
			expectedString := fmt.Sprintf("\n\tTitle: %s\n", dimensionOptions[0].Name) +
				fmt.Sprintf("\tLabels: %s\n", []string{dimensionOptions[0].Label, dimensionOptions[1].Label}) +
				fmt.Sprintf("\tOptions: %s\n", []string{dimensionOptions[0].Option, dimensionOptions[1].Option})

			So(m.ToString(), ShouldEqual, expectedString)
		})
	})
}

// Tests for the `GetVersionMetadata` client method
func TestGetVersionMetadata(t *testing.T) {
	versionID := 1
	versionMetadata := models.Metadata{
		Edition: editionID,
		Version: versionID,
	}
	mockGetResponse := versionMetadata

	Convey("If requested version is valid and get request returns 200", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		returnedVersionDimensions, err := datasetAPIClient.GetVersionMetadata(ctx, headers, datasetID, editionID, strconv.Itoa(versionID))
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions/%d/metadata", datasetID, editionID, versionID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
		Convey("Test that the requested version is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedVersionDimensions, ShouldResemble, versionMetadata)
		})
	})
	Convey("If requested version is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrVersionNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		_, err := datasetAPIClient.GetVersionMetadata(ctx, headers, datasetID, editionID, strconv.Itoa(versionID))
		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrVersionNotFound.Error())
		})
	})
}

// Tests for the `GetVersions` client method
func TestGetVersions(t *testing.T) {
	versions := []models.Version{
		{
			CollectionID: collectionID,
			DatasetID:    datasetID,
			Edition:      editionID,
			Version:      1,
		},
		{
			CollectionID: collectionID,
			DatasetID:    datasetID,
			Edition:      editionID,
			Version:      2,
		},
	}
	mockGetResponse := MockGetListRequestResponse{
		Items: versions,
	}

	Convey("If input query params are nil", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, nil, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		datasetAPIClient.GetVersions(ctx, headers, datasetID, editionID, nil)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions", datasetID, editionID)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
	})
	Convey("If input query params are empty", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, nil, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		datasetAPIClient.GetVersions(ctx, headers, datasetID, editionID, &queryParams)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			// URI should be built with default values
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions?limit=0&offset=0", datasetID, editionID)
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
		_, err := datasetAPIClient.GetVersions(ctx, headers, datasetID, editionID, &queryParams)
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
		datasetAPIClient.GetVersions(ctx, headers, datasetID, editionID, &queryParams)
		Convey("Test that the request URI is constructed correctly and the correct method is used", func() {
			expectedURI := fmt.Sprintf("/datasets/%s/editions/%s/versions?limit=%d&offset=%d", datasetID, editionID, limit, offset)
			So(httpClient.DoCalls()[0].Req.Method, ShouldEqual, http.MethodGet)
			So(httpClient.DoCalls()[0].Req.URL.RequestURI(), ShouldResemble, expectedURI)
		})
	})
	Convey("If requested dataset and edition is valid", t, func() {
		requestedVersionList := VersionsList{
			Items: versions,
		}
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, mockGetResponse, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		returnedVersionsList, err := datasetAPIClient.GetVersions(ctx, headers, datasetID, editionID, &queryParams)
		Convey("Test that the requested version is returned without error", func() {
			So(err, ShouldBeNil)
			So(returnedVersionsList, ShouldResemble, requestedVersionList)
		})
	})
	Convey("If requested dataset and edition is not valid and get request returns 404", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusNotFound, apierrors.ErrVersionNotFound.Error(), map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)
		queryParams := QueryParams{}
		_, err := datasetAPIClient.GetVersions(ctx, headers, datasetID, editionID, &queryParams)
		Convey("Test that an error is raised and should contain status code", func() {
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, apierrors.ErrVersionNotFound.Error())
		})
	})
}

func TestPutVersionState(t *testing.T) {
	Convey("When updating a version state", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, nil, map[string]string{}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		Convey("With valid parameters", func() {
			err := datasetAPIClient.PutVersionState(ctx, headers, "dataset-123", "edition-456", "1", "published")
			So(err, ShouldBeNil)

			call := httpClient.DoCalls()[0]
			So(call.Req.Method, ShouldEqual, http.MethodPut)
			expectedURI := "/datasets/dataset-123/editions/edition-456/versions/1/state"
			So(call.Req.URL.RequestURI(), ShouldResemble, expectedURI)
			So(call.Req.Header.Get("Authorization"), ShouldResemble, fmt.Sprintf("Bearer %s", headers.ServiceToken))
			var stateUpdate models.StateUpdate
			err = json.NewDecoder(call.Req.Body).Decode(&stateUpdate)
			So(err, ShouldBeNil)
			So(stateUpdate.State, ShouldEqual, "published")
		})

		Convey("With invalid dataset ID", func() {
			err := datasetAPIClient.PutVersionState(ctx, headers, "", "edition-456", "1", "published")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "required args cannot be empty")
			So(err.Error(), ShouldContainSubstring, "datasetID")
		})

		Convey("With invalid edition ID", func() {
			err := datasetAPIClient.PutVersionState(ctx, headers, "dataset-123", "", "1", "published")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "required args cannot be empty")
			So(err.Error(), ShouldContainSubstring, "editionID")
		})

		Convey("With invalid version ID", func() {
			err := datasetAPIClient.PutVersionState(ctx, headers, "dataset-123", "edition-456", "", "published")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "required args cannot be empty")
			So(err.Error(), ShouldContainSubstring, "versionID")
		})

		Convey("With invalid state", func() {
			err := datasetAPIClient.PutVersionState(ctx, headers, "dataset-123", "edition-456", "1", "")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "required args cannot be empty")
			So(err.Error(), ShouldContainSubstring, "state")
		})

		Convey("With multiple invalid args", func() {
			err := datasetAPIClient.PutVersionState(ctx, headers, "", "", "", "")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "required args cannot be empty")
			So(err.Error(), ShouldContainSubstring, "datasetID")
			So(err.Error(), ShouldContainSubstring, "editionID")
			So(err.Error(), ShouldContainSubstring, "versionID")
			So(err.Error(), ShouldContainSubstring, "state")
		})

		Convey("When HTTP request fails", func() {
			mockedErrorResponse := "error response message"
			httpClient = createHTTPClientMock(MockedHTTPResponse{http.StatusInternalServerError, mockedErrorResponse, map[string]string{}})
			datasetAPIClient = newDatasetAPIHealthcheckClient(t, httpClient)

			err := datasetAPIClient.PutVersionState(ctx, headers, "dataset-123", "edition-456", "1", "published")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "did not receive sucess response")
			So(err.Error(), ShouldContainSubstring, strconv.Itoa(http.StatusInternalServerError))
			So(err.Error(), ShouldContainSubstring, mockedErrorResponse)
		})
	})
}
